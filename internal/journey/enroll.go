package journey

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/steveweiland/behavioral-messaging/internal/event"
	"github.com/steveweiland/behavioral-messaging/internal/person"
	"github.com/steveweiland/behavioral-messaging/internal/segment"
)

// Cache is the per-workspace journey list behind a TTL, mirroring
// campaign.CampaignCache.
//
// This is not an optimization to add later — it's required by the V3 design's
// first structural rule (spec §3.3): the FSM must not put new per-event MySQL
// work on the fan-out path. A per-event ListByWorkspace here would reintroduce
// precisely the V2-1 bottleneck that V2-2c spent a PR removing, and would
// invalidate every measurement in §3.2.
//
// Journeys are versioned rather than mutated in place, so a TTL refresh is
// enough — there's no invalidation feed to build.
type Cache struct {
	db  *sql.DB
	ttl time.Duration
	mu  sync.Mutex
	e   map[string]*entry
}

type entry struct {
	journeys  []Journey
	fetchedAt time.Time
}

func NewCache(db *sql.DB, ttl time.Duration) *Cache {
	return &Cache{db: db, ttl: ttl, e: make(map[string]*entry)}
}

// List returns the workspace's journeys, refreshing past the TTL and
// compiling each definition once per refresh. On a refresh error it serves the
// last good list — a transient MySQL blip must not stop enrollment.
func (c *Cache) List(ctx context.Context, workspaceID string) ([]Journey, error) {
	now := time.Now()

	c.mu.Lock()
	if e := c.e[workspaceID]; e != nil && now.Sub(e.fetchedAt) < c.ttl {
		js := e.journeys
		c.mu.Unlock()
		return js, nil
	}
	c.mu.Unlock()

	js, err := ListByWorkspace(ctx, c.db, workspaceID)
	if err != nil {
		c.mu.Lock()
		stale := c.e[workspaceID]
		c.mu.Unlock()
		if stale != nil {
			return stale.journeys, nil
		}
		return nil, err
	}
	for i := range js {
		if err := js[i].Compile(); err != nil {
			slog.WarnContext(ctx, "journey failed to compile — falling back to per-event decode",
				slog.String("workspace_id", workspaceID),
				slog.String("journey_id", js[i].JourneyID),
				slog.Any("error", err))
		}
	}

	c.mu.Lock()
	c.e[workspaceID] = &entry{journeys: js, fetchedAt: now}
	c.mu.Unlock()
	return js, nil
}

// Enroller creates runs for the journeys an inbound event triggers. It is the
// ONLY journey work on the per-event path: match the trigger, write one row.
// Everything after that belongs to the scheduler (V3-1b).
type Enroller struct {
	db    *sql.DB
	cache *Cache

	enrolled metric.Int64Counter
}

func NewEnroller(db *sql.DB, cache *Cache) *Enroller {
	m := otel.Meter("journeys")
	enrolled, err := m.Int64Counter(
		"journey_runs_created_total",
		metric.WithDescription("Journey runs created. outcome=created|duplicate."),
	)
	if err != nil {
		panic(err)
	}
	return &Enroller{db: db, cache: cache, enrolled: enrolled}
}

// Enroll evaluates every journey trigger in the workspace against the inbound
// event and creates a run at step 0 for each match. Returns an error only for
// failures worth retrying the whole event over — a per-journey failure is
// logged and skipped, matching the campaign processor's best-effort shape.
//
// The EvalContext is the same single-event view campaigns use: attr_eq reads
// the person's attributes, event_seen matches iff the inbound event's name is
// the target.
func (e *Enroller) Enroll(ctx context.Context, per *person.Person, ev event.Event) error {
	journeys, err := e.cache.List(ctx, ev.WorkspaceID)
	if err != nil {
		return err
	}
	if len(journeys) == 0 {
		return nil
	}
	ec := &segment.EvalContext{Person: per, Events: []event.Event{ev}}

	var firstErr error
	for i := range journeys {
		j := &journeys[i]
		cond := j.TriggerCond()
		if cond == nil {
			decoded, derr := segment.DecodeCondition(j.Trigger)
			if derr != nil {
				slog.ErrorContext(ctx, "stored journey trigger undecodable",
					slog.String("journey_id", j.JourneyID), slog.Any("error", derr))
				continue
			}
			cond = &decoded
		}
		if !cond.Evaluate(ec) {
			continue
		}
		created, cerr := CreateRun(ctx, e.db, Run{
			WorkspaceID:    ev.WorkspaceID,
			JourneyID:      j.JourneyID,
			JourneyVersion: j.Version,
			PersonID:       ev.PersonID,
			TriggeredBy:    ev.EventID,
		})
		if cerr != nil {
			slog.ErrorContext(ctx, "journey run create failed",
				slog.String("journey_id", j.JourneyID),
				slog.String("person_id", ev.PersonID),
				slog.Any("error", cerr))
			if firstErr == nil {
				firstErr = cerr
			}
			continue
		}
		outcome := "duplicate"
		if created {
			outcome = "created"
		}
		e.enrolled.Add(ctx, 1, metric.WithAttributes(
			attribute.String("workspace_id", ev.WorkspaceID),
			attribute.String("journey_id", j.JourneyID),
			attribute.String("outcome", outcome),
		))
		slog.InfoContext(ctx, "journey run "+outcome,
			slog.String("journey_id", j.JourneyID),
			slog.String("person_id", ev.PersonID),
			slog.String("event_id", ev.EventID),
			slog.Uint64("journey_version", uint64(j.Version)))
	}
	return firstErr
}
