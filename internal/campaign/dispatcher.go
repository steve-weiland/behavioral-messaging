package campaign

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"text/template"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/steveweiland/behavioral-messaging/internal/event"
	"github.com/steveweiland/behavioral-messaging/internal/person"
	"github.com/steveweiland/behavioral-messaging/internal/segment"
)

// Dispatcher is the in-process campaign fan-out. Owns a buffered channel
// + single consumer goroutine. The /events handler calls Submit after
// the event is durably stored; the consumer evaluates each workspace's
// campaigns against (person, [inbound_event]) and dispatches matches.
//
// V2 replaces the channel with RabbitMQ; the API here (Submit / Stop)
// stays compatible.
type Dispatcher struct {
	db         *sql.DB
	httpClient *http.Client
	stubURL    string

	ch   chan job
	done chan struct{}
	wg   sync.WaitGroup

	// Metrics emitted via the global OTel meter; surfaces on Prometheus
	// via the existing collector pipeline.
	queueDropped metric.Int64Counter
	dispatched   metric.Int64Counter
}

type job struct {
	// parentCtx is detached from the request context so it survives the
	// HTTP handler returning. It carries the producer's span context
	// so the async campaign.process span joins the same trace.
	parentCtx context.Context
	event     event.Event
}

const (
	// V1 queue size + drop semantics. Hitting the cap means the consumer
	// is behind by ~1000 events; in V1 we drop on full and let the
	// queueDropped counter surface the loss. V2 with RabbitMQ has real
	// backpressure + persistence.
	defaultBufferSize = 1024
)

// NewDispatcher constructs a dispatcher. Start() must be called before
// Submit() to spin up the consumer goroutine.
func NewDispatcher(db *sql.DB, hc *http.Client, stubURL string) *Dispatcher {
	m := otel.Meter("campaigns")
	dropped, err := m.Int64Counter(
		"campaign_queue_dropped_total",
		metric.WithDescription("Events dropped because the in-process campaign queue was full."),
	)
	if err != nil {
		panic(fmt.Errorf("campaign drop counter: %w", err))
	}
	dispatched, err := m.Int64Counter(
		"campaign_dispatched_total",
		metric.WithDescription("Campaign messages dispatched to the stub receiver."),
	)
	if err != nil {
		panic(fmt.Errorf("campaign dispatched counter: %w", err))
	}
	return &Dispatcher{
		db:           db,
		httpClient:   hc,
		stubURL:      stubURL,
		ch:           make(chan job, defaultBufferSize),
		done:         make(chan struct{}),
		queueDropped: dropped,
		dispatched:   dispatched,
	}
}

// Start spins up the single consumer goroutine. Pass the service's root
// context so the consumer drains its in-flight job on SIGTERM.
func (d *Dispatcher) Start(rootCtx context.Context) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for {
			select {
			case j, ok := <-d.ch:
				if !ok {
					return
				}
				d.process(j.parentCtx, j.event)
			case <-rootCtx.Done():
				// Drain whatever's already in the channel — bounded by buffer size.
				for {
					select {
					case j := <-d.ch:
						d.process(j.parentCtx, j.event)
					default:
						return
					}
				}
			case <-d.done:
				return
			}
		}
	}()
}

// Stop closes the queue and waits for the consumer to drain.
// Idempotent — safe to call from a defer.
func (d *Dispatcher) Stop() {
	select {
	case <-d.done:
		return // already closed
	default:
		close(d.done)
	}
	d.wg.Wait()
}

// Submit queues one event for campaign processing. Drops the event (and
// increments queueDropped) if the buffer is full. parentCtx carries the
// producer's span context so the consumer's span joins the trace.
func (d *Dispatcher) Submit(reqCtx context.Context, ev event.Event) {
	// Detach from the request context so the consumer can outlive the
	// HTTP handler. Trace context is preserved by re-attaching the span.
	parent := trace.ContextWithSpanContext(
		context.Background(),
		trace.SpanContextFromContext(reqCtx),
	)
	select {
	case d.ch <- job{parentCtx: parent, event: ev}:
		// queued
	default:
		d.queueDropped.Add(reqCtx, 1,
			metric.WithAttributes(attribute.String("workspace_id", ev.WorkspaceID)))
		slog.WarnContext(reqCtx, "campaign queue full — dropping event",
			slog.String("workspace_id", ev.WorkspaceID),
			slog.String("event_id", ev.EventID))
	}
}

// process is the per-event consumer body. Wrapped in a parent span so
// the work is visible in Tempo as a child of the originating HTTP span.
func (d *Dispatcher) process(parent context.Context, ev event.Event) {
	tracer := otel.Tracer("campaigns")
	ctx, span := tracer.Start(parent, "campaign.process",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("workspace_id", ev.WorkspaceID),
			attribute.String("event_id", ev.EventID),
			attribute.String("event_name", ev.Name),
		),
	)
	defer span.End()

	// Pull all campaigns for the workspace. V1 reads on every event;
	// V2 caches.
	campaigns, err := ListByWorkspace(ctx, d.db, ev.WorkspaceID)
	if err != nil {
		slog.ErrorContext(ctx, "campaign list failed",
			slog.String("workspace_id", ev.WorkspaceID),
			slog.Any("error", err))
		span.RecordError(err)
		return
	}
	if len(campaigns) == 0 {
		return
	}

	// Pull the person row once. EnsureExists on the producer side
	// guarantees the row exists.
	p, err := person.Get(ctx, d.db, ev.WorkspaceID, ev.PersonID)
	if err != nil {
		slog.ErrorContext(ctx, "person get failed during campaign processing",
			slog.String("workspace_id", ev.WorkspaceID),
			slog.String("person_id", ev.PersonID),
			slog.Any("error", err))
		span.RecordError(err)
		return
	}

	// For each campaign whose trigger matches, render + dispatch + audit.
	for i := range campaigns {
		c := &campaigns[i]
		if !d.triggerMatches(ctx, c, p, ev) {
			continue
		}
		rendered, err := renderTemplate(c.Template, p, ev)
		if err != nil {
			slog.ErrorContext(ctx, "template render failed",
				slog.String("campaign_id", c.CampaignID),
				slog.Any("error", err))
			span.RecordError(err)
			continue
		}
		enrollID := uuid.NewString()
		if err := d.dispatch(ctx, c, p, ev, enrollID, rendered); err != nil {
			slog.ErrorContext(ctx, "campaign dispatch failed",
				slog.String("campaign_id", c.CampaignID),
				slog.String("person_id", ev.PersonID),
				slog.Any("error", err))
			span.RecordError(err)
			continue
		}
		if err := insertEnrollment(ctx, d.db, ev.WorkspaceID, enrollID, c.CampaignID, ev.PersonID, ev.EventID); err != nil {
			slog.ErrorContext(ctx, "enrollment audit insert failed",
				slog.String("campaign_id", c.CampaignID),
				slog.Any("error", err))
			span.RecordError(err)
			// Audit failure doesn't reverse the dispatch — V1 acceptable.
		}
		d.dispatched.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("workspace_id", ev.WorkspaceID),
				attribute.String("campaign_id", c.CampaignID),
			))
		slog.InfoContext(ctx, "campaign dispatched",
			slog.String("campaign_id", c.CampaignID),
			slog.String("person_id", ev.PersonID),
			slog.String("event_id", ev.EventID),
			slog.String("enrollment_id", enrollID))
	}
}

func (d *Dispatcher) triggerMatches(ctx context.Context, c *Campaign, p *person.Person, ev event.Event) bool {
	cond, err := segment.DecodeCondition(c.Trigger)
	if err != nil {
		slog.ErrorContext(ctx, "stored trigger undecodable",
			slog.String("campaign_id", c.CampaignID),
			slog.Any("error", err))
		return false
	}
	// EvalContext: the person + a single-event view containing just the
	// inbound event. attr_eq looks at the person's attributes; event_seen
	// matches iff the inbound event's name is the target.
	ec := &segment.EvalContext{
		Person: p,
		Events: []event.Event{ev},
	}
	return cond.Evaluate(ec)
}

// dispatch posts the rendered message to the stub receiver. Sync HTTP
// call — V1 simplicity. V2 introduces idempotency keys + two-phase
// dispatch + retry; V1 logs and moves on if the stub returns non-2xx.
type stubPayload struct {
	CampaignID     string `json:"campaign_id"`
	PersonID       string `json:"person_id"`
	EnrollmentID   string `json:"enrollment_id"`
	TriggerEventID string `json:"trigger_event_id"`
	Rendered       string `json:"rendered"`
}

func (d *Dispatcher) dispatch(ctx context.Context, c *Campaign, p *person.Person, ev event.Event, enrollID, rendered string) error {
	body, err := json.Marshal(stubPayload{
		CampaignID:     c.CampaignID,
		PersonID:       ev.PersonID,
		EnrollmentID:   enrollID,
		TriggerEventID: ev.EventID,
		Rendered:       rendered,
	})
	if err != nil {
		return fmt.Errorf("marshal stub payload: %w", err)
	}
	dispatchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(dispatchCtx, http.MethodPost, d.stubURL+"/", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("stub status=%d", resp.StatusCode)
	}
	return nil
}

func insertEnrollment(ctx context.Context, db *sql.DB, workspaceID, enrollID, campaignID, personID, triggerEventID string) error {
	const q = `
		INSERT INTO journey_enrollments (workspace_id, enrollment_id, campaign_id, person_id, triggered_by)
		VALUES (?, ?, ?, ?, ?)
	`
	if _, err := db.ExecContext(ctx, q, workspaceID, enrollID, campaignID, personID, triggerEventID); err != nil {
		return fmt.Errorf("insert journey_enrollment: %w", err)
	}
	return nil
}

// ----- template rendering --------------------------------------------

// renderContext is what templates see. Attributes are decoded once into
// a map[string]any so callers can write {{.Attrs.plan}} without dealing
// with json.RawMessage at template-eval time.
type renderContext struct {
	Person *person.Person
	Event  event.Event
	Attrs  map[string]any
	Now    time.Time
}

// ParseTemplate validates a template string at insert-time. Returns an
// error if the template doesn't parse. Used by the POST /campaigns
// handler before the row is stored.
func ParseTemplate(name, body string) error {
	_, err := template.New(name).Option("missingkey=zero").Parse(body)
	return err
}

func renderTemplate(body string, p *person.Person, ev event.Event) (string, error) {
	tpl, err := template.New("campaign").Option("missingkey=zero").Parse(body)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	var attrs map[string]any
	if len(p.Attributes) > 0 {
		if err := json.Unmarshal(p.Attributes, &attrs); err != nil {
			// Person attributes are a JSON column — decode failure means
			// corruption. Best-effort: render with empty map.
			attrs = map[string]any{}
		}
	}
	ctx := renderContext{
		Person: p,
		Event:  ev,
		Attrs:  attrs,
		Now:    time.Now().UTC(),
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, ctx); err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}
	return buf.String(), nil
}
