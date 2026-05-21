package segment

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Segment is the persisted shape of one segment definition.
type Segment struct {
	WorkspaceID string          `json:"workspace_id"`
	SegmentID   string          `json:"segment_id"`
	Name        string          `json:"name"`
	Definition  json.RawMessage `json:"definition"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// ErrNotFound is returned by Get when no row exists.
var ErrNotFound = errors.New("segment not found")

// Create inserts a new segment. The caller is responsible for validating
// the condition tree first (handler does this via Condition.Validate).
// On primary-key collision returns the underlying mysql error.
func Create(ctx context.Context, db *sql.DB, s Segment) error {
	const q = `
		INSERT INTO segments (workspace_id, segment_id, name, definition)
		VALUES (?, ?, ?, ?)
	`
	if _, err := db.ExecContext(ctx, q,
		s.WorkspaceID, s.SegmentID, s.Name, []byte(s.Definition),
	); err != nil {
		return fmt.Errorf("create segment: %w", err)
	}
	return nil
}

// Get reads one segment back. Returns ErrNotFound when absent.
func Get(ctx context.Context, db *sql.DB, workspaceID, segmentID string) (*Segment, error) {
	const q = `
		SELECT workspace_id, segment_id, name, definition, created_at, updated_at
		FROM segments
		WHERE workspace_id = ? AND segment_id = ?
	`
	var s Segment
	var def []byte
	err := db.QueryRowContext(ctx, q, workspaceID, segmentID).Scan(
		&s.WorkspaceID, &s.SegmentID, &s.Name, &def, &s.CreatedAt, &s.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get segment: %w", err)
	}
	s.Definition = json.RawMessage(def)
	return &s, nil
}

// DecodeCondition parses the JSON definition into a Condition tree.
// Helper for the handler so it doesn't need to import json directly.
func DecodeCondition(def json.RawMessage) (Condition, error) {
	var c Condition
	if err := json.Unmarshal(def, &c); err != nil {
		return Condition{}, fmt.Errorf("decode condition: %w", err)
	}
	return c, nil
}
