package campaign

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"

	"github.com/steve-weiland/behavioral-messaging/internal/person"
)

// AttrCache is the V2-2c hot-path replacement for the per-event
// person.Get. It holds each person's current attributes in memory,
// maintained from the people.changes feed (+ a boot backfill), so the
// campaign-worker evaluates triggers without a MySQL read per event.
//
// Consistency: this trades the per-event read's strong consistency for
// eventual consistency — an event arriving just after an attribute change
// may see the pre-change value until the feed propagates. Get falls back
// to a single MySQL read on a miss (then caches), so a brand-new person
// is never silently missed; only a very-recently-changed value can be
// briefly stale.
//
// Set is idempotent (last write wins), so the at-least-once people.changes
// feed is safe to replay.
type AttrCache struct {
	db *sql.DB
	mu sync.RWMutex
	m  map[string]json.RawMessage
}

func NewAttrCache(db *sql.DB) *AttrCache {
	return &AttrCache{db: db, m: make(map[string]json.RawMessage)}
}

func attrKey(workspaceID, personID string) string {
	return workspaceID + "\x00" + personID
}

// Set stores a person's full current attributes. The bytes are copied so
// the cache never aliases a caller's (e.g. AMQP delivery) buffer.
func (c *AttrCache) Set(workspaceID, personID string, attrs json.RawMessage) {
	cp := append(json.RawMessage(nil), attrs...)
	c.mu.Lock()
	c.m[attrKey(workspaceID, personID)] = cp
	c.mu.Unlock()
}

// Get returns the person (attributes only) for trigger evaluation. On a
// cache miss it reads MySQL once and populates the cache. Propagates
// person.ErrNotFound so the caller keeps the V1 terminal-drop behavior
// for a person row that genuinely doesn't exist.
func (c *AttrCache) Get(ctx context.Context, workspaceID, personID string) (*person.Person, error) {
	c.mu.RLock()
	attrs, ok := c.m[attrKey(workspaceID, personID)]
	c.mu.RUnlock()
	if ok {
		return &person.Person{WorkspaceID: workspaceID, PersonID: personID, Attributes: attrs}, nil
	}
	p, err := person.Get(ctx, c.db, workspaceID, personID)
	if err != nil {
		return nil, err
	}
	c.Set(workspaceID, personID, p.Attributes)
	return p, nil
}

// Len reports the number of cached people (for boot-backfill logging).
func (c *AttrCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}
