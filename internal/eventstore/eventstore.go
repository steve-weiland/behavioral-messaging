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
	"fmt"

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
	// Modest pool defaults for V1; we'll tune in V2 when fan-out arrives.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	return db, nil
}

// Insert appends one event. (workspace_id, event_id) is the PK, so a
// retry with the same event_id silently succeeds (ON DUPLICATE KEY → no
// row written; idempotency at the storage layer). This is intentional —
// V1 doesn't yet have the dispatch-side idempotency table to back it up,
// but the storage layer's behaviour is correct either way.
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
