package campaign

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"text/template"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/steve-weiland/behavioral-messaging/internal/amqpx"
	"github.com/steve-weiland/behavioral-messaging/internal/event"
	"github.com/steve-weiland/behavioral-messaging/internal/person"
	"github.com/steve-weiland/behavioral-messaging/internal/segment"
)

// Processor is the per-event fan-out body shared between the V1
// in-process dispatcher (retired in V2-1b) and the new
// cmd/campaign-worker consumer. Holds the deps the per-event work
// needs and a single OTel counter for successful dispatches.
type Processor struct {
	db         *sql.DB
	httpClient *http.Client
	stubURL    string
	dispatched metric.Int64Counter

	// V2-2c hot-path caches. When nil, Process reads MySQL directly per
	// event (the V1 path — kept as the A/B baseline via CAMPAIGN_HOT_PATH=
	// mysql). When set, the per-event ListByWorkspace + person.Get reads
	// are served from memory.
	campaignCache *CampaignCache
	attrCache     *AttrCache

	// V2-3 enrollment batcher. When nil, enrollments insert directly (one
	// commit each); when set, they coalesce into batched commits. Either
	// way the insert is idempotent (uniq_enrollment_dispatch).
	batcher *Batcher

	// V3-1a journey enrollment. Nil disables it (JOURNEYS=off).
	enroller Enroller
}

// Enroller creates journey runs for an inbound event (V3-1a).
//
// Declared as an interface here rather than importing internal/journey,
// because journey already imports this package for ParseTemplate — the
// interface is what keeps that from being an import cycle. Same seam pattern
// as execFunc and the traffic-api speedSource: the consumer declares the
// narrow contract it needs.
type Enroller interface {
	Enroll(ctx context.Context, per *person.Person, ev event.Event) error
}

// SetEnroller attaches journey enrollment. Callers MUST NOT pass a typed nil
// pointer — check for nil at the call site — since a non-nil interface holding
// a nil pointer would defeat the nil check in Process.
func (p *Processor) SetEnroller(e Enroller) { p.enroller = e }

// NewProcessor wires the deps with the direct-MySQL hot path (no caches,
// no batcher).
func NewProcessor(db *sql.DB, hc *http.Client, stubURL string) *Processor {
	return NewProcessorWithCaches(db, hc, stubURL, nil, nil, nil)
}

// NewProcessorWithCaches wires the deps and (optionally) the V2-2c
// in-process caches + V2-3 enrollment batcher. Nil caches fall back to
// direct MySQL reads; a nil batcher inserts enrollments directly.
func NewProcessorWithCaches(db *sql.DB, hc *http.Client, stubURL string, attrs *AttrCache, campaigns *CampaignCache, batcher *Batcher) *Processor {
	m := otel.Meter("campaigns")
	dispatched, err := m.Int64Counter(
		"campaign_dispatched_total",
		metric.WithDescription("Campaign messages dispatched to the stub receiver."),
	)
	if err != nil {
		panic(fmt.Errorf("campaign dispatched counter: %w", err))
	}
	return &Processor{
		db:            db,
		httpClient:    hc,
		stubURL:       stubURL,
		dispatched:    dispatched,
		campaignCache: campaigns,
		attrCache:     attrs,
		batcher:       batcher,
	}
}

// recordEnrollment durably records one dispatch — via the batcher when
// configured, else a direct idempotent insert.
func (p *Processor) recordEnrollment(ctx context.Context, r enrollRow) error {
	if p.batcher != nil {
		return p.batcher.Submit(ctx, r)
	}
	return insertEnrollment(ctx, p.db, r.workspaceID, r.enrollmentID, r.campaignID, r.personID, r.triggeredBy)
}

// listCampaigns and getPerson resolve from the cache when configured,
// else straight from MySQL (the V1 hot path).
func (p *Processor) listCampaigns(ctx context.Context, workspaceID string) ([]Campaign, error) {
	if p.campaignCache != nil {
		return p.campaignCache.List(ctx, workspaceID)
	}
	return ListByWorkspace(ctx, p.db, workspaceID)
}

func (p *Processor) getPerson(ctx context.Context, workspaceID, personID string) (*person.Person, error) {
	if p.attrCache != nil {
		return p.attrCache.Get(ctx, workspaceID, personID)
	}
	return person.Get(ctx, p.db, workspaceID, personID)
}

// Process is the per-event consumer body. Wrapped in a `campaign.process`
// span so the work shows up in Tempo as a child of the producer's HTTP
// span (the worker's amqpx.Consume already extracted the producer span
// context from the AMQP headers).
//
// Outcome maps to RabbitMQ semantics:
//
//   - OutcomeAck         — success or no-match (best-effort: per-campaign
//     dispatch failures are logged but don't fail the
//     event; V2-3 idempotency closes the partial-
//     failure gap)
//   - OutcomeNackRequeue — transient infra failure (MySQL down, etc.) —
//     worth retrying
//   - OutcomeNackDrop    — terminal: person row missing (race vs delete),
//     will route to the DLQ
//
// Process returns the outcome plus a human-readable reason (empty on success).
// The reason travels onto the retry header and into the DLQ, because a DLQ you
// intend to drain needs to say why it gave up (BM-121/124).
func (p *Processor) Process(parent context.Context, ev event.Event) (amqpx.Outcome, string) {
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

	// Pull all campaigns for the workspace. V2-2c serves this from the
	// in-process cache when configured; otherwise reads MySQL per event.
	campaigns, err := p.listCampaigns(ctx, ev.WorkspaceID)
	if err != nil {
		slog.ErrorContext(ctx, "campaign list failed",
			slog.String("workspace_id", ev.WorkspaceID),
			slog.Any("error", err))
		span.RecordError(err)
		return amqpx.OutcomeNackRequeue, "campaign list failed: " + err.Error()
	}
	// Nothing to do only when BOTH paths are idle. Gating on campaigns alone
	// would silently skip journey enrollment in any workspace that has
	// journeys but no campaigns — which is exactly the state a fresh V3
	// install is in.
	if len(campaigns) == 0 && p.enroller == nil {
		return amqpx.OutcomeAck, ""
	}

	// The person is needed by both paths: campaign triggers and journey
	// triggers evaluate against the same attributes.
	per, err := p.getPerson(ctx, ev.WorkspaceID, ev.PersonID)
	if errors.Is(err, person.ErrNotFound) {
		// EnsureExists on the producer side should make this near-
		// impossible; if it does happen, the person row was deleted
		// after the event was published — terminal, route to DLQ.
		slog.WarnContext(ctx, "person not found during campaign processing",
			slog.String("workspace_id", ev.WorkspaceID),
			slog.String("person_id", ev.PersonID))
		span.RecordError(err)
		return amqpx.OutcomeNackDrop, "person row missing (deleted after publish)"
	}
	if err != nil {
		slog.ErrorContext(ctx, "person get failed during campaign processing",
			slog.String("workspace_id", ev.WorkspaceID),
			slog.String("person_id", ev.PersonID),
			slog.Any("error", err))
		span.RecordError(err)
		return amqpx.OutcomeNackRequeue, "person read failed: " + err.Error()
	}

	// For each campaign whose trigger matches, render + dispatch + record.
	// Render/dispatch failures are logged and skipped (best-effort). An
	// enrollment-record failure is durable-state we can't drop, so it
	// requeues the whole event — safe now that the insert is idempotent
	// (uniq_enrollment_dispatch): re-dispatched campaigns re-enroll as
	// no-ops, only the stub re-POSTs (harmless for the stub sink).
	requeue := false
	reason := ""
	for i := range campaigns {
		c := &campaigns[i]
		if !p.triggerMatches(ctx, c, per, ev) {
			continue
		}
		rendered, err := renderCampaign(c, per, ev)
		if err != nil {
			slog.ErrorContext(ctx, "template render failed",
				slog.String("campaign_id", c.CampaignID),
				slog.Any("error", err))
			span.RecordError(err)
			continue
		}
		enrollID := uuid.NewString()
		if err := p.dispatch(ctx, c, per, ev, enrollID, rendered); err != nil {
			slog.ErrorContext(ctx, "campaign dispatch failed",
				slog.String("campaign_id", c.CampaignID),
				slog.String("person_id", ev.PersonID),
				slog.Any("error", err))
			span.RecordError(err)
			continue
		}
		if err := p.recordEnrollment(ctx, enrollRow{
			workspaceID:  ev.WorkspaceID,
			enrollmentID: enrollID,
			campaignID:   c.CampaignID,
			personID:     ev.PersonID,
			triggeredBy:  ev.EventID,
		}); err != nil {
			slog.ErrorContext(ctx, "enrollment record failed — requeueing event",
				slog.String("campaign_id", c.CampaignID),
				slog.Any("error", err))
			span.RecordError(err)
			requeue = true
			continue
		}
		p.dispatched.Add(ctx, 1,
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
	// V3-1a: enroll the journeys this event triggers. One row per match and
	// nothing executed — the scheduler advances runs. A failure here is
	// durable state we can't drop, so it requeues the event, which is safe
	// because run creation is idempotent on
	// (workspace_id, journey_id, triggered_by) (BM-113).
	if p.enroller != nil {
		if err := p.enroller.Enroll(ctx, per, ev); err != nil {
			slog.ErrorContext(ctx, "journey enrollment failed — requeueing event",
				slog.String("workspace_id", ev.WorkspaceID),
				slog.String("event_id", ev.EventID),
				slog.Any("error", err))
			span.RecordError(err)
			requeue = true
			reason = "journey enrollment failed: " + err.Error()
		}
	}

	if requeue {
		return amqpx.OutcomeNackRequeue, reason
	}
	return amqpx.OutcomeAck, ""
}

func (p *Processor) triggerMatches(ctx context.Context, c *Campaign, per *person.Person, ev event.Event) bool {
	// Compiled by the cache layer when the hot path is cache-backed; decode
	// inline otherwise (the CAMPAIGN_HOT_PATH=mysql baseline, and the
	// fallback for a definition that failed to compile).
	cond := c.cond
	if cond == nil {
		decoded, err := segment.DecodeCondition(c.Trigger)
		if err != nil {
			slog.ErrorContext(ctx, "stored trigger undecodable",
				slog.String("campaign_id", c.CampaignID),
				slog.Any("error", err))
			return false
		}
		cond = &decoded
	}
	// EvalContext: the person + a single-event view containing just the
	// inbound event. attr_eq looks at the person's attributes; event_seen
	// matches iff the inbound event's name is the target.
	ec := &segment.EvalContext{
		Person: per,
		Events: []event.Event{ev},
	}
	return cond.Evaluate(ec)
}

// dispatch posts the rendered message to the stub receiver. Sync HTTP
// call — V2-1 simplicity. V2-3 introduces idempotency keys + two-phase
// dispatch + per-campaign retry semantics.
type stubPayload struct {
	CampaignID     string `json:"campaign_id"`
	PersonID       string `json:"person_id"`
	EnrollmentID   string `json:"enrollment_id"`
	TriggerEventID string `json:"trigger_event_id"`
	Rendered       string `json:"rendered"`
}

func (p *Processor) dispatch(ctx context.Context, c *Campaign, per *person.Person, ev event.Event, enrollID, rendered string) error {
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
	req, err := http.NewRequestWithContext(dispatchCtx, http.MethodPost, p.stubURL+"/", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	// Drain the body to EOF before closing — net/http only returns a
	// connection to the idle pool when the response body is fully read
	// AND closed. Closing an unread body discards the TCP connection, so
	// under high prefetch every dispatch dials fresh and exhausts the
	// ephemeral port range ("cannot assign requested address"). Draining
	// is what makes the transport's keep-alive pool actually reusable.
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("stub status=%d", resp.StatusCode)
	}
	return nil
}

func insertEnrollment(ctx context.Context, db *sql.DB, workspaceID, enrollID, campaignID, personID, triggerEventID string) error {
	// Idempotent on uniq_enrollment_dispatch (migration 004): a redelivered
	// (workspace, campaign, event) is a no-op rather than a duplicate row.
	const q = `
		INSERT INTO journey_enrollments (workspace_id, enrollment_id, campaign_id, person_id, triggered_by)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE enrollment_id = enrollment_id
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

// renderCampaign renders a campaign's template, reusing the parsed template
// cached on the Campaign when available (the hot path) and parsing inline
// otherwise. Parsing per event is a measurable waste: the template is already
// validated at POST time by ParseTemplate, and BM‑88 named render CPU as an
// expected V2 bottleneck candidate.
func renderCampaign(c *Campaign, per *person.Person, ev event.Event) (string, error) {
	if c.tpl != nil {
		return executeTemplate(c.tpl, per, ev)
	}
	return renderTemplate(c.Template, per, ev)
}

func renderTemplate(body string, per *person.Person, ev event.Event) (string, error) {
	tpl, err := template.New("campaign").Option("missingkey=zero").Parse(body)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	return executeTemplate(tpl, per, ev)
}

func executeTemplate(tpl *template.Template, per *person.Person, ev event.Event) (string, error) {
	var attrs map[string]any
	if len(per.Attributes) > 0 {
		if err := json.Unmarshal(per.Attributes, &attrs); err != nil {
			// Person attributes are a JSON column — decode failure means
			// corruption. Best-effort: render with empty map.
			attrs = map[string]any{}
		}
	}
	ctx := renderContext{
		Person: per,
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
