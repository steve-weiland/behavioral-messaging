package campaign

import (
	"fmt"

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
