// Package amqpx is a thin wrapper around rabbitmq/amqp091-go.
//
// V2-1: Behavioral-messaging adopts the same Connect / Publish / Consume
// primitives used in Build 5's observability project. The package is
// topology-agnostic — callers declare exchanges + queues themselves
// (see internal/campaign/topology.go for the V2-1 fan-out shape).
//
// Cross-cutting features:
//   - Publish injects W3C traceparent onto Headers so consumer spans
//     join the producer's trace.
//   - Consume extracts traceparent, manages manual Ack/Nack via an
//     Outcome enum, and records consume duration + dropped counters
//     to the global OTel meter.
package amqpx

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Outcome reports what the handler decided about a delivery; amqpx
// translates it into Ack/Nack and records the metric tag.
type Outcome int

const (
	OutcomeAck         Outcome = iota // success — ch.Ack
	OutcomeNackRequeue                // transient — ch.Nack(requeue=true)
	OutcomeNackDrop                   // terminal — ch.Nack(requeue=false); routed to DLQ
)

func (o Outcome) String() string {
	switch o {
	case OutcomeAck:
		return "ack"
	case OutcomeNackRequeue:
		return "nack_requeue"
	case OutcomeNackDrop:
		return "nack_drop"
	}
	return "unknown"
}

// Handler is the consumer signature. The handler MUST return an
// Outcome; amqpx will Ack/Nack the delivery itself.
type Handler func(ctx context.Context, d amqp.Delivery) Outcome

var (
	metricsOnce       sync.Once
	consumeDuration   metric.Float64Histogram
	consumeDroppedCt  metric.Int64Counter
	consumeRequeuedCt metric.Int64Counter
)

func initMetrics() {
	metricsOnce.Do(func() {
		m := otel.Meter("amqpx")
		var err error
		consumeDuration, err = m.Float64Histogram(
			"messaging.consume.duration",
			metric.WithDescription("Time spent processing one AMQP delivery, by outcome."),
			metric.WithUnit("s"),
		)
		if err != nil {
			panic(fmt.Errorf("amqpx histogram: %w", err))
		}
		consumeDroppedCt, err = m.Int64Counter(
			"messaging.consume.dropped",
			metric.WithDescription("Deliveries terminally rejected (nack with requeue=false)."),
		)
		if err != nil {
			panic(fmt.Errorf("amqpx counter: %w", err))
		}
		// Requeues are unbounded by design until V3-2 adds retry-with-backoff
		// (BM-85): a delivery that keeps failing for a non-transient reason
		// requeues forever at full speed. This counter (and the redelivered
		// attribute) is what makes that visible — a rising requeue rate with
		// flat throughput is the signature. Alert on it rather than assuming
		// an empty DLQ means healthy.
		consumeRequeuedCt, err = m.Int64Counter(
			"messaging.consume.requeued",
			metric.WithDescription("Deliveries nacked with requeue=true. Sustained non-zero = a poison message is spinning (no retry bound until V3-2)."),
		)
		if err != nil {
			panic(fmt.Errorf("amqpx requeue counter: %w", err))
		}
	})
}

// ExitOnClose watches for ABNORMAL closure of the connection or channel and
// exits the process. Without this, a channel-level error (a failed declare, a
// broker restart) silently closes every Consume's deliveries loop: the
// process keeps running, /healthz stays green, and nothing consumes — a
// zombie. Crash-fast + the compose restart policy turns that into a clean
// reconnect-and-resubscribe cycle. Graceful Close delivers nil on the notify
// channels, which is ignored, so normal shutdown is unaffected.
func ExitOnClose(conn *amqp.Connection, ch *amqp.Channel, service string) {
	connErrs := conn.NotifyClose(make(chan *amqp.Error, 1))
	chanErrs := ch.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		if abnormalClose(connErrs, chanErrs) {
			slog.Error("AMQP connection/channel closed abnormally — exiting for restart",
				slog.String("service", service))
			os.Exit(1)
		}
	}()
}

// abnormalClose blocks until either notify channel yields, and reports
// whether the closure carried an error (true = broker/channel fault; false =
// graceful Close, which delivers nil / closes the channel).
func abnormalClose(a, b <-chan *amqp.Error) bool {
	select {
	case err := <-a:
		return err != nil
	case err := <-b:
		return err != nil
	}
}

// Connect opens an AMQP connection and a single channel.
// Caller is responsible for Close on both at shutdown.
func Connect(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("amqp channel: %w", err)
	}
	return conn, ch, nil
}

// ConnectWithRetry retries Connect until it succeeds or the deadline
// passes. Matches the openDBWithRetry pattern used for MySQL —
// container healthcheck flips before the broker is fully accepting.
func ConnectWithRetry(url string, total time.Duration) (*amqp.Connection, *amqp.Channel, error) {
	deadline := time.Now().Add(total)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, ch, err := Connect(url)
		if err == nil {
			return conn, ch, nil
		}
		lastErr = err
		time.Sleep(1 * time.Second)
	}
	return nil, nil, fmt.Errorf("amqp connect after %s: %w", total, lastErr)
}

// Publish sends a JSON body to an exchange with the given routing key.
// Producer span + traceparent injected onto Headers.
func Publish(ctx context.Context, ch *amqp.Channel, exchange, routingKey, messageID string, body []byte) error {
	tracer := otel.Tracer("amqpx")
	ctx, span := tracer.Start(ctx, "amqp.publish "+exchange,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", exchange),
			attribute.String("messaging.rabbitmq.routing_key", routingKey),
			attribute.String("messaging.message.id", messageID),
			attribute.String("messaging.message.conversation_id", messageID),
			attribute.String("messaging.operation", "publish"),
		),
	)
	defer span.End()

	headers := amqp.Table{}
	otel.GetTextMapPropagator().Inject(ctx, AmqpHeaderCarrier(headers))

	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := ch.PublishWithContext(pubCtx,
		exchange,
		routingKey,
		false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    messageID,
			Timestamp:    time.Now().UTC(),
			Body:         body,
			Headers:      headers,
		},
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// Consume registers a consumer with manual ack and the given prefetch.
// traceparent extracted from delivery.Headers; consumer span is a
// child of the producer's span across the wire. Handler returns an
// Outcome; amqpx Ack/Nacks accordingly and records duration + dropped
// metrics.
func Consume(
	parent context.Context,
	ch *amqp.Channel,
	queue string,
	prefetch int,
	consumerTag string,
	handler Handler,
) error {
	initMetrics()
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	deliveries, err := ch.Consume(queue, consumerTag, false /* manual ack */, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", queue, err)
	}
	tracer := otel.Tracer("amqpx")
	propagator := otel.GetTextMapPropagator()
	queueAttr := attribute.String("messaging.destination.name", queue)
	go func() {
		for d := range deliveries {
			// Goroutine-per-message bounded by prefetch.
			go func(d amqp.Delivery) {
				start := time.Now()
				parentCtx := propagator.Extract(parent, AmqpHeaderCarrier(d.Headers))
				ctx, span := tracer.Start(parentCtx, "amqp.consume "+queue,
					trace.WithSpanKind(trace.SpanKindConsumer),
					trace.WithAttributes(
						attribute.String("messaging.system", "rabbitmq"),
						attribute.String("messaging.source.name", queue),
						attribute.String("messaging.message.id", d.MessageId),
						attribute.String("messaging.message.conversation_id", d.MessageId),
						attribute.String("messaging.operation", "process"),
					),
				)

				span.SetAttributes(attribute.Bool("messaging.rabbitmq.redelivered", d.Redelivered))

				outcome := handler(ctx, d)
				switch outcome {
				case OutcomeAck:
					_ = d.Ack(false)
				case OutcomeNackRequeue:
					_ = d.Nack(false, true)
					span.SetStatus(codes.Error, "consumer returned nack_requeue")
				case OutcomeNackDrop:
					_ = d.Nack(false, false)
					span.SetStatus(codes.Error, "consumer returned nack_drop")
				}

				dur := time.Since(start).Seconds()
				attrs := metric.WithAttributes(queueAttr,
					attribute.String("outcome", outcome.String()),
				)
				consumeDuration.Record(ctx, dur, attrs)
				switch outcome {
				case OutcomeNackDrop:
					consumeDroppedCt.Add(ctx, 1, metric.WithAttributes(queueAttr))
				case OutcomeNackRequeue:
					// Split first-attempt failures from repeats: a climbing
					// redelivered=true rate is a message that will never
					// succeed, which is otherwise invisible (the DLQ stays
					// empty because requeue never dead-letters).
					consumeRequeuedCt.Add(ctx, 1, metric.WithAttributes(queueAttr,
						attribute.Bool("redelivered", d.Redelivered)))
					if d.Redelivered {
						slog.WarnContext(ctx, "delivery requeued after a previous attempt",
							slog.String("queue", queue),
							slog.String("message_id", d.MessageId))
					}
				}
				span.End()
			}(d)
		}
	}()
	return nil
}
