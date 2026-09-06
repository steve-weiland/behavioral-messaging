package campaign

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/steve-weiland/behavioral-messaging/internal/amqpx"
	"github.com/steve-weiland/behavioral-messaging/internal/event"
)

// Publisher hands events from the /events HTTP handler off to the
// RabbitMQ `campaigns.fanout` exchange. Replaces V1's in-process
// Dispatcher consumer.
//
// Shape: a buffered in-process chan + one publish goroutine. The chan
// exists purely so the /events HTTP handler can return without waiting
// on the broker (BM-89). The single publish worker drains the chan and
// calls amqpx.Publish with the workspace_id as routing key.
//
// One amqp.Channel with a single publish worker as its only writer.
// (amqp091 serializes channel operations internally, so this is a design
// convention, not a thread-safety requirement — and a channel is a shared
// failure domain either way; see amqpx.ExitOnClose.) One worker is
// plenty of headroom
// over V1's measured ~196 events/s fan-out ceiling (publish is far
// cheaper than the full process() body it replaces). If a future load
// test names this single channel as the bottleneck, V2-stretch adds
// channel-per-worker.
//
// On publish failure the worker retries up to 3× with linear backoff.
// After exhaustion the event is dropped + counted on the
// `campaign_publish_dropped_total{workspace_id,reason}` counter. The
// producer edge is deliberately best-effort and COUNTED — an outbox was
// once slated for V2-3, which shipped enrollment idempotency instead
// (spec: BM-100 vs the deferred BM-105); it remains future work, and
// "RabbitMQ is unreachable for > a second" is operationally noisy
// enough to alert on directly.
type Publisher struct {
	amqpCh *amqp.Channel
	ch     chan publishJob
	done   chan struct{}
	wg     sync.WaitGroup

	publishDropped metric.Int64Counter
	bufferDepth    metric.Int64UpDownCounter
}

type publishJob struct {
	// parentCtx carries the producer's span context so the
	// `amqp.publish` span (and downstream `amqp.consume` /
	// `campaign.process` spans on the worker) chain into the same
	// trace as the originating HTTP request.
	parentCtx context.Context
	ev        event.Event
	body      []byte
}

const publisherBufferSize = 1024

// NewPublisher wires the deps + initializes the publisher metrics.
// Start() must be called before Submit() to spin up the publish worker.
func NewPublisher(ch *amqp.Channel) *Publisher {
	return newPublisher(ch, publisherBufferSize)
}

// newPublisher is NewPublisher with an injectable buffer size, so the
// drop-on-full path is testable without enqueuing 1024 events.
func newPublisher(ch *amqp.Channel, bufSize int) *Publisher {
	m := otel.Meter("campaigns")
	dropped, err := m.Int64Counter(
		"campaign_publish_dropped_total",
		metric.WithDescription("Events the producer failed to publish to RabbitMQ. Reason label: buffer_full | retry_exhausted | shutdown."),
	)
	if err != nil {
		panic(fmt.Errorf("publish drop counter: %w", err))
	}
	depth, err := m.Int64UpDownCounter(
		"campaign_publish_buffer_depth",
		metric.WithDescription("Current depth of the in-process producer buffer (events waiting to publish to RabbitMQ)."),
	)
	if err != nil {
		panic(fmt.Errorf("publish depth gauge: %w", err))
	}
	return &Publisher{
		amqpCh:         ch,
		ch:             make(chan publishJob, bufSize),
		done:           make(chan struct{}),
		publishDropped: dropped,
		bufferDepth:    depth,
	}
}

// Start spins up the publish worker goroutine. Returns immediately.
func (p *Publisher) Start() {
	p.wg.Add(1)
	go p.run()
}

// Stop signals shutdown and waits for the worker to drain the buffer.
// Idempotent; call ordering in main must be:
// httpServer.Shutdown(stopCtx) → Publisher.Stop() → amqpCh.Close().
func (p *Publisher) Stop() {
	select {
	case <-p.done:
		return
	default:
		close(p.done)
	}
	p.wg.Wait()
}

// Submit hands the event to the in-process buffer. Three outcomes:
//
//   - publisher stopping → drop + count (reason="shutdown")
//   - buffer has room    → enqueue
//   - buffer full        → drop + count (reason="buffer_full")
//
// Never blocks the caller. BM-89.
func (p *Publisher) Submit(reqCtx context.Context, ev event.Event) {
	body, err := json.Marshal(ev)
	if err != nil {
		// event.Event is a fixed struct of strings + JSON RawMessage;
		// a marshal error here is a programming bug, not a runtime
		// failure. Log + drop without counting (this isn't capacity).
		slog.ErrorContext(reqCtx, "publish marshal failed",
			slog.String("event_id", ev.EventID),
			slog.Any("error", err))
		return
	}
	// Detach the context so the publish goroutine outlives the HTTP
	// handler. The producer span context is preserved so amqpx.Publish's
	// `amqp.publish` span joins this trace.
	parent := trace.ContextWithSpanContext(
		context.Background(),
		trace.SpanContextFromContext(reqCtx),
	)
	// Shutdown is checked FIRST, in its own select. Folding it in as a third
	// case alongside `p.ch <- job` would make the two cases ready
	// simultaneously after Stop — and select picks uniformly at random, so
	// ~half of those events would land in a buffer nothing drains and be
	// counted as neither drop reason. Silent loss is the exact V1 bug this
	// producer replaced, so shutdown gets deterministic precedence.
	select {
	case <-p.done:
		p.recordDrop(reqCtx, ev, "shutdown")
		slog.WarnContext(reqCtx, "publisher stopping — dropping event",
			slog.String("workspace_id", ev.WorkspaceID),
			slog.String("event_id", ev.EventID))
		return
	default:
	}
	select {
	case p.ch <- publishJob{parentCtx: parent, ev: ev, body: body}:
		p.bufferDepth.Add(reqCtx, 1)
	default:
		p.recordDrop(reqCtx, ev, "buffer_full")
		slog.WarnContext(reqCtx, "publisher buffer full — dropping event",
			slog.String("workspace_id", ev.WorkspaceID),
			slog.String("event_id", ev.EventID))
	}
	// Residual (benign): Stop can close done between the check and the
	// enqueue. That event is still published — run()'s shutdown branch drains
	// the buffer before returning — so it's a late success, not a loss.
}

func (p *Publisher) recordDrop(ctx context.Context, ev event.Event, reason string) {
	p.publishDropped.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workspace_id", ev.WorkspaceID),
		attribute.String("reason", reason),
	))
}

func (p *Publisher) run() {
	defer p.wg.Done()
	for {
		select {
		case job := <-p.ch:
			p.bufferDepth.Add(job.parentCtx, -1)
			p.publishWithRetry(job)
		case <-p.done:
			// Drain anything still in the buffer, then exit.
			for {
				select {
				case job := <-p.ch:
					p.bufferDepth.Add(job.parentCtx, -1)
					p.publishWithRetry(job)
				default:
					return
				}
			}
		}
	}
}

func (p *Publisher) publishWithRetry(job publishJob) {
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := amqpx.Publish(
			job.parentCtx,
			p.amqpCh,
			ExchangeFanout,
			job.ev.WorkspaceID, // routing key — direct exchange picks the per-workspace queue
			job.ev.EventID,     // amqp.MessageId
			job.body,
		)
		if err == nil {
			return
		}
		lastErr = err
		slog.WarnContext(job.parentCtx, "publish failed, retrying",
			slog.String("event_id", job.ev.EventID),
			slog.String("workspace_id", job.ev.WorkspaceID),
			slog.Int("attempt", attempt),
			slog.Any("error", err))
		// Linear backoff: 100ms, 200ms, 300ms. Bounded by V1's
		// HTTP-handler-already-returned semantics (no upstream waiting).
		time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
	}
	p.recordDrop(job.parentCtx, job.ev, "retry_exhausted")
	slog.ErrorContext(job.parentCtx, "publish failed after retries — event dropped",
		slog.String("event_id", job.ev.EventID),
		slog.String("workspace_id", job.ev.WorkspaceID),
		slog.Any("last_error", lastErr))
}
