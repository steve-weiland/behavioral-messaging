// Package person — V1 person profile store.
//
// Customer.io's identify semantics: an upsert that MERGES new attributes
// into existing ones. Send `{plan: "pro"}` then `{city: "Sydney"}` and the
// row holds both. Send `{plan: null}` and the `plan` key is deleted.
//
// MySQL's `JSON_MERGE_PATCH` implements RFC 7396 exactly — new keys
// overwrite, omitted keys preserved, null deletes. That's the kernel of
// this package; everything else is plumbing.
package person

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Person is one tracked end-user, keyed by (workspace_id, person_id).
type Person struct {
	WorkspaceID string          `json:"workspace_id"`
	PersonID    string          `json:"person_id"`
	Attributes  json.RawMessage `json:"attributes"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// ErrNotFound is returned by Get when no row exists.
var ErrNotFound = errors.New("person not found")

// EnsureExists is the cheap upsert called on the event-ingest path —
// creates an empty-attributes row if one doesn't already exist; otherwise
// no-op. Idempotent. Doesn't update timestamps if the row already exists
// (INSERT IGNORE is a no-op on PK collision).
func EnsureExists(ctx context.Context, db *sql.DB, workspaceID, personID string) error {
	const q = `
		INSERT IGNORE INTO people (workspace_id, person_id, attributes)
		VALUES (?, ?, JSON_OBJECT())
	`
	if _, err := db.ExecContext(ctx, q, workspaceID, personID); err != nil {
		return fmt.Errorf("ensure person: %w", err)
	}
	return nil
}

// Upsert merges `attrs` into the row's existing JSON attributes via
// JSON_MERGE_PATCH (RFC 7396). On first insert the new attributes are
// stored verbatim; on subsequent calls they merge with previous state.
// updated_at is refreshed in both cases.
//
// `attrs` is passed as a raw JSON object (the body the caller already
// validated). Passing an empty object is allowed and amounts to "touch
// updated_at," which the function does NOT do — empty merges are a no-op
// at the SQL layer.
func Upsert(ctx context.Context, db *sql.DB, workspaceID, personID string, attrs json.RawMessage) error {
	const q = `
		INSERT INTO people (workspace_id, person_id, attributes)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE
		  attributes = JSON_MERGE_PATCH(attributes, VALUES(attributes)),
		  updated_at = CURRENT_TIMESTAMP
	`
	if _, err := db.ExecContext(ctx, q, workspaceID, personID, []byte(attrs)); err != nil {
		return fmt.Errorf("upsert person: %w", err)
	}
	return nil
}

// Get reads one person row. Returns ErrNotFound if missing.
func Get(ctx context.Context, db *sql.DB, workspaceID, personID string) (*Person, error) {
	const q = `
		SELECT workspace_id, person_id, attributes, created_at, updated_at
		FROM people
		WHERE workspace_id = ? AND person_id = ?
	`
	var p Person
	var attrs []byte
	err := db.QueryRowContext(ctx, q, workspaceID, personID).Scan(
		&p.WorkspaceID, &p.PersonID, &attrs, &p.CreatedAt, &p.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get person: %w", err)
	}
	p.Attributes = json.RawMessage(attrs)
	return &p, nil
}
