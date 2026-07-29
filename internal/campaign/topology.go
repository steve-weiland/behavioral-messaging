package campaign

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// V2-1 RabbitMQ topology for campaign fan-out (BM-80..89).
//
// Shape:
//
//	+------------------+   routing_key=<ws_id>   +---------------------------------+
//	| campaigns.fanout |  ────────────────────▶ | campaigns.fanout.<workspace_id> |
//	|   (direct)       |                         |   (durable, per workspace)      |
//	+------------------+                         +---------------------------------+
//	                                                          │
//	                                  consumer Nack(drop) ────┘
//	                                                          ▼
//	                                              +-----------------------+
//	                                              | campaigns.fanout.dlx  |
//	                                              |     (fanout)          |
//	                                              +-----------------------+
//	                                                          │
//	                                                          ▼
//	                                              +-----------------------+
//	                                              | campaigns.fanout.dlq  |
//	                                              |     (durable)         |
//	                                              +-----------------------+
//
// Producer-side (track-api) declares the exchange + DLX + DLQ at startup
// via DeclareProducerTopology. Per-workspace queues are declared by the
// worker (campaign-worker, lands in V2-1b) via EnsureWorkerQueue when it
// subscribes, on the principle that consumers own the queues they read.
const (
	ExchangeFanout = "campaigns.fanout"     // direct exchange — routing_key = workspace_id
	ExchangeDLX    = "campaigns.fanout.dlx" // fanout exchange — collects poison messages
	QueueDLQ       = "campaigns.fanout.dlq" // single global DLQ for all workspaces
)

// queueName builds the per-workspace queue name. Workspace IDs are
// already validated to [A-Za-z0-9_-]{1,64}, so they're safe to embed.
func queueName(workspaceID string) string {
	return "campaigns.fanout." + workspaceID
}

// DeclareProducerTopology declares the exchange + DLX + DLQ. Idempotent
// — re-declaring identical entities is a no-op in RabbitMQ. Per-workspace
// queues are NOT declared here; see EnsureWorkerQueue.
//
// Declared at track-api startup so the broker is in the expected shape
// before the first publish.
func DeclareProducerTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(
		ExchangeFanout,
		"direct",
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare exchange %s: %w", ExchangeFanout, err)
	}
	if err := ch.ExchangeDeclare(
		ExchangeDLX,
		"fanout",
		true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("declare exchange %s: %w", ExchangeDLX, err)
	}
	if _, err := ch.QueueDeclare(
		QueueDLQ,
		true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("declare queue %s: %w", QueueDLQ, err)
	}
	if err := ch.QueueBind(QueueDLQ, "", ExchangeDLX, false, nil); err != nil {
		return fmt.Errorf("bind %s → %s: %w", QueueDLQ, ExchangeDLX, err)
	}
	return nil
}

// EnsureWorkerQueue declares a per-workspace queue + binds it to the
// fanout exchange with routing_key=workspaceID. Idempotent. Each
// source queue gets `x-dead-letter-exchange=campaigns.fanout.dlx` so
// terminal Nack(requeue=false) routes to the DLQ rather than vanishing.
//
// Called from the worker (V2-1b) when it starts subscribing — either
// for the workspaces it's responsible for at boot, or lazily on demand
// when a new workspace publishes its first event.
func EnsureWorkerQueue(ch *amqp.Channel, workspaceID string) (string, error) {
	name := queueName(workspaceID)
	args := amqp.Table{
		"x-dead-letter-exchange": ExchangeDLX,
	}
	if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
		return "", fmt.Errorf("declare queue %s: %w", name, err)
	}
	if err := ch.QueueBind(name, workspaceID, ExchangeFanout, false, nil); err != nil {
		return "", fmt.Errorf("bind %s → %s: %w", name, ExchangeFanout, err)
	}
	return name, nil
}

// ----- V3-2: retry ladder (BM-120/121) --------------------------------

// RetryTier is one rung of the backoff ladder: a queue that holds a message
// for TTL, then dead-letters it back to the work exchange.
//
// Why a TTL ladder rather than quorum-queue `delivery-limit` (spec Q13):
// delivery-limit bounds attempts and dead-letters for free, which is
// attractive — but it can only redeliver IMMEDIATELY. Every failure worth
// retrying here (MySQL blip, stub timeout, broker hiccup) needs the retry
// SPACED; instant redelivery burns the whole attempt budget inside the outage
// window and dead-letters a message a 30-second wait would have delivered.
//
// Each tier is a fanout exchange + a queue. Fanout matters: the message is
// published with routing_key=workspace_id, a fanout ignores the routing key for
// routing but PRESERVES it on the message, so when the TTL expires the
// dead-letter republish to ExchangeFanout still carries workspace_id and lands
// back in the right per-workspace queue. Publishing straight to the queue via
// the default exchange would overwrite the routing key with the queue name.
type RetryTier struct {
	Name     string
	Exchange string
	Queue    string
	TTL      time.Duration
}

// RetryLadder is the ordered ladder. A message climbs one rung per attempt;
// falling off the top means the DLQ (BM-121).
var RetryLadder = []RetryTier{
	{Name: "5s", Exchange: "campaigns.retry.5s", Queue: "campaigns.retry.5s", TTL: 5 * time.Second},
	{Name: "30s", Exchange: "campaigns.retry.30s", Queue: "campaigns.retry.30s", TTL: 30 * time.Second},
	{Name: "2m", Exchange: "campaigns.retry.2m", Queue: "campaigns.retry.2m", TTL: 2 * time.Minute},
	{Name: "10m", Exchange: "campaigns.retry.10m", Queue: "campaigns.retry.10m", TTL: 10 * time.Minute},
}

// HeaderAttempt carries the attempt count across retries. RabbitMQ's own
// x-death chain records the hops, but it's awkward to read and doesn't survive
// a republish to a different exchange, so the count is explicit.
const (
	HeaderAttempt = "x-attempt"
	HeaderReason  = "x-failure-reason"
)

// DeclareRetryTopology declares every rung. Idempotent, declared by the
// producer alongside the rest of the topology so the shape exists before the
// first failure.
func DeclareRetryTopology(ch *amqp.Channel) error {
	for _, t := range RetryLadder {
		if err := ch.ExchangeDeclare(t.Exchange, "fanout", true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare exchange %s: %w", t.Exchange, err)
		}
		args := amqp.Table{
			"x-message-ttl": int32(t.TTL / time.Millisecond),
			// On expiry, back to the work exchange with the original routing
			// key (the workspace) intact.
			"x-dead-letter-exchange": ExchangeFanout,
		}
		if _, err := ch.QueueDeclare(t.Queue, true, false, false, false, args); err != nil {
			return fmt.Errorf("declare queue %s: %w", t.Queue, err)
		}
		if err := ch.QueueBind(t.Queue, "", t.Exchange, false, nil); err != nil {
			return fmt.Errorf("bind %s → %s: %w", t.Queue, t.Exchange, err)
		}
	}
	return nil
}
