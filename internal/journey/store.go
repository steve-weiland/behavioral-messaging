package journey

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

// ErrNotFound is returned when no row exists.
var ErrNotFound = errors.New("journey not found")

// ErrConflict is returned when the journey_id already exists. A duplicate ID
// is the caller's mistake, so it deserves a 409 rather than the 500 a bare
// duplicate-key error would produce.
var ErrConflict = errors.New("journey already exists")

// isDuplicateKey reports whether err is MySQL's ER_DUP_ENTRY (1062).
func isDuplicateKey(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

// Create inserts a new journey at version 1. Caller validates trigger + steps
// first (BM-110), so a bad definition never reaches the scheduler.
func Create(ctx context.Context, db *sql.DB, j Journey) error {
	steps, err := json.Marshal(j.Steps)
	if err != nil {
		return fmt.Errorf("marshal steps: %w", err)
	}
	const q = `
		INSERT INTO journeys (workspace_id, journey_id, name, trigger_def, steps, version)
		VALUES (?, ?, ?, ?, ?, 1)
	`
	if _, err := db.ExecContext(ctx, q,
		j.WorkspaceID, j.JourneyID, j.Name, []byte(j.Trigger), steps,
	); err != nil {
		if isDuplicateKey(err) {
			return ErrConflict
		}
		return fmt.Errorf("create journey: %w", err)
	}
	return nil
}

// Get reads one journey definition. Returns ErrNotFound when absent.
func Get(ctx context.Context, db *sql.DB, workspaceID, journeyID string) (*Journey, error) {
	const q = `
		SELECT workspace_id, journey_id, name, trigger_def, steps, version, created_at, updated_at
		FROM journeys
		WHERE workspace_id = ? AND journey_id = ?
	`
	var j Journey
	var trig, steps []byte
	err := db.QueryRowContext(ctx, q, workspaceID, journeyID).Scan(
		&j.WorkspaceID, &j.JourneyID, &j.Name, &trig, &steps, &j.Version, &j.CreatedAt, &j.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get journey: %w", err)
	}
	j.Trigger = json.RawMessage(trig)
	if err := json.Unmarshal(steps, &j.Steps); err != nil {
		return nil, fmt.Errorf("decode steps: %w", err)
	}
	return &j, nil
}

// ListByWorkspace returns every journey in a workspace. Called once per cache
// refresh, never per event — see Cache.
func ListByWorkspace(ctx context.Context, db *sql.DB, workspaceID string) ([]Journey, error) {
	const q = `
		SELECT workspace_id, journey_id, name, trigger_def, steps, version, created_at, updated_at
		FROM journeys
		WHERE workspace_id = ?
	`
	rows, err := db.QueryContext(ctx, q, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list journeys: %w", err)
	}
	defer rows.Close()
	var out []Journey
	for rows.Next() {
		var j Journey
		var trig, steps []byte
		if err := rows.Scan(&j.WorkspaceID, &j.JourneyID, &j.Name, &trig, &steps,
			&j.Version, &j.CreatedAt, &j.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan journey: %w", err)
		}
		j.Trigger = json.RawMessage(trig)
		if err := json.Unmarshal(steps, &j.Steps); err != nil {
			return nil, fmt.Errorf("decode steps for %s: %w", j.JourneyID, err)
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// execer is the subset of *sql.DB run creation needs, injectable so the
// idempotency contract is testable without MySQL (same seam as the V2-3
// enrollment batcher).
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// CreateRun enrolls a person at step 0. Idempotent per BM-113: the UNIQUE key
// on (workspace_id, journey_id, triggered_by) plus ON DUPLICATE KEY UPDATE
// makes a redelivered trigger event a no-op rather than a second run.
//
// Returns created=false when the row already existed, so the caller can log
// and count redeliveries instead of guessing.
func CreateRun(ctx context.Context, db execer, r Run) (created bool, err error) {
	if r.RunID == "" {
		r.RunID = uuid.NewString()
	}
	// status='ready', wake_at=NULL → immediately due. The scheduler (V3-1b)
	// picks it up on its next claim; nothing executes on the event path.
	const q = `
		INSERT INTO journey_runs
			(workspace_id, run_id, journey_id, journey_version, person_id, triggered_by,
			 step_index, status, wake_at)
		VALUES (?, ?, ?, ?, ?, ?, 0, 'ready', NULL)
		ON DUPLICATE KEY UPDATE run_id = run_id
	`
	res, err := db.ExecContext(ctx, q,
		r.WorkspaceID, r.RunID, r.JourneyID, r.JourneyVersion, r.PersonID, r.TriggeredBy,
	)
	if err != nil {
		return false, fmt.Errorf("create journey run: %w", err)
	}
	// MySQL reports 1 affected row for an insert and 0 for an ON DUPLICATE
	// no-op (the UPDATE sets run_id to itself, so nothing changes).
	n, err := res.RowsAffected()
	if err != nil {
		return false, nil // driver didn't report; treat as created, harmless for logging
	}
	return n == 1, nil
}

// GetRunByPerson reads a person's run for one journey — the read behind
// GET /journeys/{id}/runs/{person_id}, and the V3-1a Done-when check.
func GetRunByPerson(ctx context.Context, db *sql.DB, workspaceID, journeyID, personID string) (*Run, error) {
	const q = `
		SELECT workspace_id, run_id, journey_id, journey_version, person_id, triggered_by,
		       step_index, status, wake_at, attempt, COALESCE(last_error, ''),
		       created_at, updated_at
		FROM journey_runs
		WHERE workspace_id = ? AND journey_id = ? AND person_id = ?
		ORDER BY created_at DESC
		LIMIT 1
	`
	var r Run
	err := db.QueryRowContext(ctx, q, workspaceID, journeyID, personID).Scan(
		&r.WorkspaceID, &r.RunID, &r.JourneyID, &r.JourneyVersion, &r.PersonID, &r.TriggeredBy,
		&r.StepIndex, &r.Status, &r.WakeAt, &r.Attempt, &r.LastError, &r.CreatedAt, &r.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get journey run: %w", err)
	}
	return &r, nil
}

// CountRuns returns how many runs exist for a journey — used by the V3-1a
// verification (exactly one row per trigger event, none on redelivery).
func CountRuns(ctx context.Context, db *sql.DB, workspaceID, journeyID string) (int, error) {
	const q = `SELECT COUNT(*) FROM journey_runs WHERE workspace_id = ? AND journey_id = ?`
	var n int
	if err := db.QueryRowContext(ctx, q, workspaceID, journeyID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count journey runs: %w", err)
	}
	return n, nil
}
