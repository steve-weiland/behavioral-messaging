// Package peoplefeed is the V2-2 attribute-change stream that keeps the
// segment-worker's bitmap index current.
//
// Events already flow through campaigns.fanout, so the segment-worker
// binds its own queue there for event_seen maintenance. Attribute
// changes (POST /people) had no stream — this package adds one. It's a
// deliberately separate, low-volume fanout exchange, OFF the hot
// /events path, so attribute indexing never regresses intake throughput.
//
// track-api publishes the person's FULL resolved attribute set after each
// upsert; segment-worker applies it via segmentidx.SetAttributes (which
// diffs against its own last-known state, so moves/deletes are handled).
package peoplefeed

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/steve-weiland/behavioral-messaging/internal/amqpx"
)

// Exchange is a durable fanout — every bound consumer queue gets every
// change. Fanout (not direct-by-workspace) because the change body
// already carries workspace_id and the single segment-worker wants all
// of them; per-workspace routing is a later lever if the index shards.
const Exchange = "people.changes"

// Change is the wire message: a person's full current attributes after an
// upsert. Attributes is the merged JSON object (RFC 7396 already applied
// by the person store), so deleted keys are simply absent.
type Change struct {
	WorkspaceID string          `json:"workspace_id"`
	PersonID    string          `json:"person_id"`
	Attributes  json.RawMessage `json:"attributes"`
}

// DeclareExchange declares the fanout exchange. Idempotent. Called by the
// producer (track-api) at startup and by the consumer before binding.
func DeclareExchange(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(Exchange, "fanout", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", Exchange, err)
	}
	return nil
}

// DeclareConsumerQueue declares a durable queue and binds it to the
// exchange (fanout ignores the routing key). The consumer owns its queue.
func DeclareConsumerQueue(ch *amqp.Channel, queueName string) (string, error) {
	if err := DeclareExchange(ch); err != nil {
		return "", err
	}
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return "", fmt.Errorf("declare queue %s: %w", queueName, err)
	}
	if err := ch.QueueBind(queueName, "", Exchange, false, nil); err != nil {
		return "", fmt.Errorf("bind %s → %s: %w", queueName, Exchange, err)
	}
	return queueName, nil
}

// Publisher serializes publishes onto one dedicated amqp channel.
// amqp091 channels are single-goroutine, and POST /people handlers run
// concurrently, so a mutex guards the channel. Volume is low (attribute
// changes ≪ events), so this never gates throughput.
type Publisher struct {
	mu sync.Mutex
	ch *amqp.Channel
}

// NewPublisher declares the exchange on ch and returns a publisher over
// it. ch should be dedicated to this publisher (not shared with the
// campaign Publisher's channel).
func NewPublisher(ch *amqp.Channel) (*Publisher, error) {
	if err := DeclareExchange(ch); err != nil {
		return nil, err
	}
	return &Publisher{ch: ch}, nil
}

// Publish sends one Change. Best-effort from the caller's perspective:
// the bitmap index is eventually consistent, so callers log-and-continue
// rather than failing the HTTP request on a publish error.
func (p *Publisher) Publish(ctx context.Context, c Change) error {
	body, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal change: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return amqpx.Publish(ctx, p.ch, Exchange, "", uuid.NewString(), body)
}
