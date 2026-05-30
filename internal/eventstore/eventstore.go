// Package eventstore is the persistence layer for events.
//
// Thin wrapper around database/sql. otelsql is *not* wired in the V1
// kickoff — that lands as a small follow-up commit so the bigger
// scaffolding diff stays focused. Same pattern Build 5 used: minimal
// V1 → tighten in a later PR.
package eventstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	_ "github.com/go-sql-driver/mysql" // registers "mysql" sql driver

	"github.com/steveweiland/behavioral-messaging/internal/event"
)

// Open returns a *sql.DB connected to MySQL via the canonical DSN.
func Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql open: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping: %w", err)
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
