// Package eventstore is the persistence layer for events, and the single
// place the MySQL pool is opened for every service.
//
// The pool is instrumented with otelsql, so each query is a child span of
// whatever context it runs under. That's what makes BM‑42's trace complete:
// a POST /events trace now shows the intake INSERT under the HTTP server
// span, and the enrollment INSERT under campaign.process on the worker —
// the two writes the V2-2 measurement named as the ceiling. Before this they
// were invisible, and the "MySQL is the bottleneck" conclusion rested
// entirely on container CPU plus the journey_enrollments row slope.
package eventstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/XSAM/otelsql"
	_ "github.com/go-sql-driver/mysql" // registers "mysql" sql driver
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

	"github.com/steve-weiland/behavioral-messaging/internal/event"
)

// Open returns an OTel-instrumented *sql.DB connected to MySQL.
func Open(dsn string) (*sql.DB, error) {
	// otelsql.Open wraps the registered "mysql" driver. Spans carry the
	// statement text: safe here because every query in this codebase is a
	// constant with bound parameters — no user data is interpolated into
	// SQL, so the span can't leak a person's attributes.
	db, err := otelsql.Open("mysql", dsn,
		otelsql.WithAttributes(semconv.DBSystemMySQL),
		otelsql.WithSpanOptions(otelsql.SpanOptions{
			// One span per statement is the useful grain. Left unfiltered,
			// otelsql emits sql.conn.exec AND sql.stmt.exec for the same
			// INSERT (the driver prepares, then executes), plus a sql.rows
			// span per result set — three spans per query, which buries the
			// two writes this exists to show. Keep the sql.stmt.* layer.
			Ping:                 false,
			RowsNext:             false,
			DisableErrSkip:       true,
			OmitConnResetSession: true,
			OmitConnPrepare:      true,
			OmitConnQuery:        true,
			OmitRows:             true,
			OmitConnectorConnect: true,
			// OmitConnQuery covers sql.conn.query but not sql.conn.exec, and
			// that one is pure noise here: go-sql-driver returns ErrSkip for a
			// parameterized ExecContext (params aren't interpolated), so
			// database/sql falls back to prepare + stmt.exec. The conn.exec
			// span therefore describes a call that never executed, sitting
			// next to the stmt.exec span that did.
			SpanFilter: func(_ context.Context, method otelsql.Method, _ string, _ []driver.NamedValue) bool {
				return method != otelsql.MethodConnExec
			},
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("sql open: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	// Pool utilization as metrics (db_client_connections_*). The V2-2x pool
	// finding — requests queuing on the pool in front of MySQL — was
	// diagnosed by inference; these make wait_count/wait_duration directly
	// observable next time.
	if _, err := otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(semconv.DBSystemMySQL)); err != nil {
		return nil, fmt.Errorf("register db stats metrics: %w", err)
	}
	// Pool size: default 50, overridable via MYSQL_MAX_OPEN_CONNS. The
	// V2-2 write-ceiling diagnostic showed the old default of 25 throttled
	// fan-out by ~30% (intake 2,173 → 2,836/s, p99 306 → 197ms going
	// 25 → 64) — requests queued on the pool before MySQL. 50 banks most
	// of that while keeping pool×3 services under MySQL's max_connections.
	// Idle == open to keep connections warm under sustained load.
	maxOpen := 50
	if v := os.Getenv("MYSQL_MAX_OPEN_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxOpen = n
		}
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	return db, nil
}

// Insert appends one event. (workspace_id, event_id) is the PK, so a
// retry with the same event_id silently succeeds (ON DUPLICATE KEY → no
// row written; idempotency at the storage layer). This is intentional —
// V1 doesn't yet have the dispatch-side idempotency table to back it up,
// but the storage layer's behavior is correct either way.
func Insert(ctx context.Context, db *sql.DB, e event.Event) error {
	const q = `
		INSERT INTO events (workspace_id, event_id, person_id, event_name, payload, received_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE event_id = event_id
	`
	if _, err := db.ExecContext(ctx, q,
		e.WorkspaceID, e.EventID, e.PersonID, e.Name, []byte(e.Payload), e.ReceivedAt,
	); err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// PersonEvent is one (workspace, person, event_name) triple — the only
// fields event_seen membership needs.
type PersonEvent struct {
	WorkspaceID string
	PersonID    string
	Name        string
}

// DistinctPersonEvents streams every distinct (workspace, person,
// event_name) for the segment-worker's boot backfill of the eventSeen
// bitmaps. DISTINCT because event_seen is "ever", not a count — one row
// per (person, event_name) is all the index needs, however many times
// the event fired.
func DistinctPersonEvents(ctx context.Context, db *sql.DB) ([]PersonEvent, error) {
	const q = `SELECT DISTINCT workspace_id, person_id, event_name FROM events`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("select distinct events: %w", err)
	}
	defer rows.Close()
	var out []PersonEvent
	for rows.Next() {
		var pe PersonEvent
		if err := rows.Scan(&pe.WorkspaceID, &pe.PersonID, &pe.Name); err != nil {
			return nil, fmt.Errorf("scan person event: %w", err)
		}
		out = append(out, pe)
	}
	return out, rows.Err()
}

// RecentByPerson returns the most recent N events for a person, newest
// first. Used by the V1 scan-based segment evaluator to answer
// `event_seen` predicates. V2's bitmap path obviates this — see V2 spec.
//
// `limit` caps the rows pulled into memory. V1 chooses 1000 in the
// handler — enough to answer "ever seen?" for typical demo profiles
// without unbounded scans on hot profiles.
func RecentByPerson(ctx context.Context, db *sql.DB, workspaceID, personID string, limit int) ([]event.Event, error) {
	const q = `
		SELECT workspace_id, event_id, person_id, event_name, payload, received_at
		FROM events
		WHERE workspace_id = ? AND person_id = ?
		ORDER BY received_at DESC
		LIMIT ?
	`
	rows, err := db.QueryContext(ctx, q, workspaceID, personID, limit)
	if err != nil {
		return nil, fmt.Errorf("select events: %w", err)
	}
	defer rows.Close()
	var out []event.Event
	for rows.Next() {
		var e event.Event
		var payload []byte
		if err := rows.Scan(&e.WorkspaceID, &e.EventID, &e.PersonID, &e.Name, &payload, &e.ReceivedAt); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		e.Payload = json.RawMessage(payload)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return out, nil
}
