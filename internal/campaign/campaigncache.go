package campaign

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"
)

// CampaignCache is the V2-2c hot-path replacement for the per-event
// ListByWorkspace MySQL read. It caches each workspace's campaign list
// with a short TTL. Campaigns are create-only (no update/delete), so a
// TTL refresh is enough to pick up newly created campaigns without an
// invalidation feed — a new campaign starts matching within one TTL.
//
// On a refresh error it serves the last good list (graceful degradation):
// a transient MySQL blip must not stop fan-out.
type CampaignCache struct {
	db  *sql.DB
	ttl time.Duration
	mu  sync.Mutex
	e   map[string]*campaignEntry
}

type campaignEntry struct {
	campaigns []Campaign
	fetchedAt time.Time
}

func NewCampaignCache(db *sql.DB, ttl time.Duration) *CampaignCache {
	return &CampaignCache{db: db, ttl: ttl, e: make(map[string]*campaignEntry)}
}

// List returns the workspace's campaigns, refreshing from MySQL when the
// cached entry is missing or older than the TTL. The returned slice is
// read-only for callers (entries are replaced, never mutated in place).
func (c *CampaignCache) List(ctx context.Context, workspaceID string) ([]Campaign, error) {
	now := time.Now()

	c.mu.Lock()
	if e := c.e[workspaceID]; e != nil && now.Sub(e.fetchedAt) < c.ttl {
		cs := e.campaigns
		c.mu.Unlock()
		return cs, nil
	}
	c.mu.Unlock()

	// Stale or missing — fetch outside the lock. A handful of goroutines
	// may race to refresh at the TTL boundary; that's harmless (idempotent,
	// last write wins).
	cs, err := ListByWorkspace(ctx, c.db, workspaceID)
	if err != nil {
		c.mu.Lock()
		stale := c.e[workspaceID]
		c.mu.Unlock()
		if stale != nil {
			return stale.campaigns, nil // serve last good
		}
		return nil, err
	}

	// Compile the trigger + template once per refresh rather than per event.
	// A compile failure is logged and left uncompiled: the per-event path
	// decodes inline and logs there, so one corrupt definition degrades to
	// V1 behavior instead of taking the workspace's fan-out down.
	for i := range cs {
		if err := cs[i].Compile(); err != nil {
			slog.WarnContext(ctx, "campaign definition failed to compile — falling back to per-event decode",
				slog.String("workspace_id", workspaceID),
				slog.String("campaign_id", cs[i].CampaignID),
				slog.Any("error", err))
		}
	}

	c.mu.Lock()
	c.e[workspaceID] = &campaignEntry{campaigns: cs, fetchedAt: now}
	c.mu.Unlock()
	return cs, nil
}
