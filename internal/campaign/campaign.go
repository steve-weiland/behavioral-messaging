// Package campaign — campaign trigger + single-step render/dispatch.
//
// A campaign is:
//   - a trigger (segment.Condition) evaluated against the inbound event
//     and the person it belongs to, and
//   - a Go text/template rendered with that same context.
//
// Fan-out is asynchronous over RabbitMQ (V2-1b). track-api's /events
// handler calls Publisher.Submit, which publishes to `campaigns.fanout`
// keyed by workspace_id; cmd/campaign-worker consumes the per-workspace
// queue and runs Processor.Process per event. V1's in-process Dispatcher
// (one buffered channel, one consumer goroutine) is retired — it is the
// ceiling the V2 tier exists to remove; see spec §3.2.
package campaign

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"text/template"
	"time"

	"github.com/steve-weiland/behavioral-messaging/internal/segment"
)

// Campaign is the persisted shape of one campaign definition.
type Campaign struct {
	WorkspaceID string          `json:"workspace_id"`
	CampaignID  string          `json:"campaign_id"`
	Name        string          `json:"name"`
	Trigger     json.RawMessage `json:"trigger"`
	Template    string          `json:"template"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`

	// Compiled artifacts, populated by Compile(). Both are pure functions
	// of Trigger/Template, and campaigns are create-only, so compiling once
	// per cache refresh removes a JSON decode + a text/template parse from
	// every event on the fan-out hot path. Nil on the direct-MySQL path,
	// which recompiles per event by design — that's the A/B baseline
	// (CAMPAIGN_HOT_PATH=mysql), so it must stay honestly slow.
	cond *segment.Condition
	tpl  *template.Template
}

// Compile decodes the trigger and parses the template, caching both on the
// Campaign. Called by CampaignCache after a refresh. A failure leaves the
// artifacts nil, so the per-event path falls back to decoding inline and
// logs there — a corrupt stored definition degrades, it doesn't crash the
// worker.
func (c *Campaign) Compile() error {
	cond, err := segment.DecodeCondition(c.Trigger)
	if err != nil {
		return fmt.Errorf("decode trigger: %w", err)
	}
	tpl, err := template.New("campaign").Option("missingkey=zero").Parse(c.Template)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}
	c.cond, c.tpl = &cond, tpl
	return nil
}

// ErrNotFound is returned by Get when no row exists.
var ErrNotFound = errors.New("campaign not found")

// Create inserts a new campaign. Caller validates the trigger condition
// and parses the template at insert time so bad shapes never reach the
// consumer (BM-41 / BM-42).
func Create(ctx context.Context, db *sql.DB, c Campaign) error {
	const q = `
		INSERT INTO campaigns (workspace_id, campaign_id, name, trigger_def, template)
		VALUES (?, ?, ?, ?, ?)
	`
	if _, err := db.ExecContext(ctx, q,
		c.WorkspaceID, c.CampaignID, c.Name, []byte(c.Trigger), c.Template,
	); err != nil {
		return fmt.Errorf("create campaign: %w", err)
	}
	return nil
}

// Get reads one campaign back. Returns ErrNotFound when absent.
func Get(ctx context.Context, db *sql.DB, workspaceID, campaignID string) (*Campaign, error) {
	const q = `
		SELECT workspace_id, campaign_id, name, trigger_def, template, created_at, updated_at
		FROM campaigns
		WHERE workspace_id = ? AND campaign_id = ?
	`
	var c Campaign
	var trig []byte
	err := db.QueryRowContext(ctx, q, workspaceID, campaignID).Scan(
		&c.WorkspaceID, &c.CampaignID, &c.Name, &trig, &c.Template, &c.CreatedAt, &c.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get campaign: %w", err)
	}
	c.Trigger = json.RawMessage(trig)
	return &c, nil
}

// ListByWorkspace returns every campaign in a workspace. V1 reads them
// all per inbound event; V2 caches in-memory with invalidation on
// /campaigns mutation events.
func ListByWorkspace(ctx context.Context, db *sql.DB, workspaceID string) ([]Campaign, error) {
	const q = `
		SELECT workspace_id, campaign_id, name, trigger_def, template, created_at, updated_at
		FROM campaigns
		WHERE workspace_id = ?
	`
	rows, err := db.QueryContext(ctx, q, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list campaigns: %w", err)
	}
	defer rows.Close()
	var out []Campaign
	for rows.Next() {
		var c Campaign
		var trig []byte
		if err := rows.Scan(&c.WorkspaceID, &c.CampaignID, &c.Name, &trig, &c.Template, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan campaign: %w", err)
		}
		c.Trigger = json.RawMessage(trig)
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return out, nil
}
