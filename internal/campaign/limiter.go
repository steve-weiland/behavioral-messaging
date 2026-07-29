package campaign

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Limiter caps concurrent in-flight event processing, per workspace and in
// aggregate (BM-130).
//
// What V2 already had, and what it didn't — worth being precise, because the
// V3-3 requirement was written before checking:
//
// amqpx calls ch.Qos(prefetch, 0, false) — global=false, so the prefetch
// applies PER CONSUMER, and the worker registers one consumer per workspace
// queue. So a per-workspace bound on unacked deliveries already existed: no
// single workspace could hold more than PREFETCH in flight.
//
// The two real gaps:
//
//  1. The AGGREGATE was unbounded. N workspaces × PREFETCH goroutines all
//     contend for one MySQL pool (50 conns). Two busy workspaces at
//     PREFETCH=32 is already 64 concurrent — past the pool, so a quiet
//     workspace's queries wait behind a noisy one's. The broker bounds each
//     tenant; nothing bounded the sum.
//  2. PREFETCH is a delivery bound, not a work bound. It caps how many
//     messages a consumer holds, which is the right dial for the broker and
//     the wrong one for a shared database.
//
// So this caps work, not delivery: a small per-workspace slice (so no tenant
// can take the pool) under a global ceiling (so the sum stays inside it).
type Limiter struct {
	global chan struct{}

	mu      sync.Mutex
	perWS   map[string]chan struct{}
	wsLimit int

	inflight metric.Int64UpDownCounter
	waited   metric.Int64Counter
}

// NewLimiter builds a limiter. globalLimit should sit below the MySQL pool
// size so in-flight work can't queue on connections; wsLimit is each
// workspace's slice of it.
func NewLimiter(globalLimit, wsLimit int) *Limiter {
	if globalLimit < 1 {
		globalLimit = 1
	}
	if wsLimit < 1 {
		wsLimit = 1
	}
	m := otel.Meter("campaigns")
	inflight, err := m.Int64UpDownCounter("campaign_inflight_events",
		metric.WithDescription("Events being processed right now, by workspace (BM-132)."))
	if err != nil {
		panic(err)
	}
	waited, err := m.Int64Counter("campaign_admission_waits_total",
		metric.WithDescription("Times a workspace waited for an admission slot. Labels: workspace_id, scope."))
	if err != nil {
		panic(err)
	}
	return &Limiter{
		global:   make(chan struct{}, globalLimit),
		perWS:    make(map[string]chan struct{}),
		wsLimit:  wsLimit,
		inflight: inflight,
		waited:   waited,
	}
}

func (l *Limiter) wsSem(workspaceID string) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	sem, ok := l.perWS[workspaceID]
	if !ok {
		sem = make(chan struct{}, l.wsLimit)
		l.perWS[workspaceID] = sem
	}
	return sem
}

// Acquire takes a per-workspace slot then a global one, blocking until both
// are available or ctx is done. The release func is nil when acquisition
// failed.
//
// Order matters and is fixed: workspace first, then global. Acquiring in a
// consistent order across all callers is what keeps two workspaces from each
// holding half of what the other needs.
func (l *Limiter) Acquire(ctx context.Context, workspaceID string) (release func(), err error) {
	sem := l.wsSem(workspaceID)
	wsAttr := metric.WithAttributes(attribute.String("workspace_id", workspaceID))

	select {
	case sem <- struct{}{}:
	default:
		// Full — record that this workspace is being throttled before blocking,
		// so "tenant A is at its cap" is visible rather than inferred from
		// latency.
		l.waited.Add(ctx, 1, metric.WithAttributes(
			attribute.String("workspace_id", workspaceID),
			attribute.String("scope", "workspace")))
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	select {
	case l.global <- struct{}{}:
	default:
		l.waited.Add(ctx, 1, metric.WithAttributes(
			attribute.String("workspace_id", workspaceID),
			attribute.String("scope", "global")))
		select {
		case l.global <- struct{}{}:
		case <-ctx.Done():
			<-sem // don't leak the workspace slot
			return nil, ctx.Err()
		}
	}

	l.inflight.Add(ctx, 1, wsAttr)
	var once sync.Once
	return func() {
		once.Do(func() {
			l.inflight.Add(context.Background(), -1, wsAttr)
			<-l.global
			<-sem
		})
	}, nil
}

// InFlight reports the current in-flight count for a workspace — used by the
// noisy-neighbour demo to show one tenant pinned at its cap while another
// stays served.
func (l *Limiter) InFlight(workspaceID string) int {
	l.mu.Lock()
	sem, ok := l.perWS[workspaceID]
	l.mu.Unlock()
	if !ok {
		return 0
	}
	return len(sem)
}
