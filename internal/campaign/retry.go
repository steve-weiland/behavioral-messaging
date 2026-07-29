package campaign

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Retrier moves a failed delivery onto the next rung of the backoff ladder, or
// to the DLQ once the ladder is exhausted (BM-120/121).
//
// V2-1 nacked transient failures with requeue=true, which retried them
// immediately and forever: a message that could never succeed spun at full
// speed while the DLQ stayed empty and read as health. This replaces that with
// spaced, bounded attempts and a DLQ that actually fills.
type Retrier struct {
	ch *amqp.Channel

	retried    metric.Int64Counter
	deadLetter metric.Int64Counter
}

func NewRetrier(ch *amqp.Channel) *Retrier {
	m := otel.Meter("campaigns")
	retried, err := m.Int64Counter("campaign_retry_scheduled_total",
		metric.WithDescription("Deliveries scheduled onto a retry tier. Labels: tier, workspace_id."))
	if err != nil {
		panic(err)
	}
	dl, err := m.Int64Counter("campaign_dead_lettered_total",
		metric.WithDescription("Deliveries sent to the DLQ. Labels: workspace_id, cause (exhausted|terminal)."))
	if err != nil {
		panic(err)
	}
	return &Retrier{ch: ch, retried: retried, deadLetter: dl}
}

// Attempt reads the attempt count off a delivery. Absent header = first try.
func Attempt(d amqp.Delivery) int {
	if v, ok := d.Headers[HeaderAttempt]; ok {
		switch n := v.(type) {
		case int32:
			return int(n)
		case int64:
			return int(n)
		case int:
			return n
		case string:
			if i, err := strconv.Atoi(n); err == nil {
				return i
			}
		}
	}
	return 0
}

// Retry republishes the delivery onto the rung for its attempt count. Returns
// the tier used. When the ladder is exhausted it dead-letters instead, and
// reports exhausted=true so the caller can log it as terminal.
//
// The caller ACKs the original delivery after this returns nil: the message is
// durably on the retry queue (persistent, durable queue), so acking is correct
// — leaving it unacked would double it up on the next redelivery.
func (r *Retrier) Retry(ctx context.Context, d amqp.Delivery, workspaceID, reason string) (tier string, exhausted bool, err error) {
	attempt := Attempt(d)
	if attempt >= len(RetryLadder) {
		if err := r.DeadLetter(ctx, d, workspaceID, reason, "exhausted"); err != nil {
			return "", true, err
		}
		return "", true, nil
	}
	t := RetryLadder[attempt]

	headers := copyHeaders(d.Headers)
	headers[HeaderAttempt] = int32(attempt + 1)
	headers[HeaderReason] = truncate(reason, 500)

	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.ch.PublishWithContext(pubCtx, t.Exchange,
		// routing_key is preserved through the fanout and reused by the
		// dead-letter republish, which is how the message finds its way back
		// to the right per-workspace queue.
		workspaceID,
		false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    d.MessageId,
			Timestamp:    time.Now().UTC(),
			Body:         d.Body,
			Headers:      headers,
		},
	); err != nil {
		return "", false, fmt.Errorf("publish retry tier %s: %w", t.Name, err)
	}
	r.retried.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tier", t.Name),
		attribute.String("workspace_id", workspaceID)))
	slog.WarnContext(ctx, "delivery scheduled for retry",
		slog.String("workspace_id", workspaceID),
		slog.String("tier", t.Name),
		slog.Int("attempt", attempt+1),
		slog.Int("of", len(RetryLadder)),
		slog.String("reason", reason))
	return t.Name, false, nil
}

// DeadLetter publishes to the DLX with our own reason header, rather than
// relying on a bare Nack(requeue=false). RabbitMQ's x-death chain records the
// hops but not WHY we gave up, and "why" is the whole value of a DLQ you intend
// to drain (BM-121/124).
func (r *Retrier) DeadLetter(ctx context.Context, d amqp.Delivery, workspaceID, reason, cause string) error {
	headers := copyHeaders(d.Headers)
	headers[HeaderReason] = truncate(reason, 500)
	headers["x-failure-cause"] = cause
	headers["x-original-routing-key"] = workspaceID
	headers["x-dead-lettered-at"] = time.Now().UTC().Format(time.RFC3339)

	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.ch.PublishWithContext(pubCtx, ExchangeDLX, "", false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    d.MessageId,
			Timestamp:    time.Now().UTC(),
			Body:         d.Body,
			Headers:      headers,
		},
	); err != nil {
		return fmt.Errorf("publish dead letter: %w", err)
	}
	r.deadLetter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workspace_id", workspaceID),
		attribute.String("cause", cause)))
	slog.ErrorContext(ctx, "delivery dead-lettered",
		slog.String("workspace_id", workspaceID),
		slog.String("cause", cause),
		slog.Int("attempts", Attempt(d)),
		slog.String("reason", reason))
	return nil
}

func copyHeaders(in amqp.Table) amqp.Table {
	out := amqp.Table{}
	for k, v := range in {
		// x-death is RabbitMQ's own bookkeeping; copying it forward confuses
		// the broker's chain, so let it rebuild naturally.
		if k == "x-death" {
			continue
		}
		out[k] = v
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
