// AMQP propagation carrier.
//
// OTel's W3C TraceContext propagator is media-agnostic — it operates over
// any `propagation.TextMapCarrier` (Get / Set / Keys on a key-value store).
// AmqpHeaderCarrier exposes `amqp.Table` as such a carrier so we can use
// the standard global propagator without writing a custom one.
//
// AMQP header values are typed (string, []byte, int, …). W3C trace-context
// values are always strings, so we coerce on Get and store strings on Set.
package amqpx

import amqp "github.com/rabbitmq/amqp091-go"

// AmqpHeaderCarrier wraps an amqp.Table so OTel propagators can read+write
// trace context onto AMQP message Headers.
type AmqpHeaderCarrier amqp.Table

func (c AmqpHeaderCarrier) Get(key string) string {
	v, ok := c[key]
	if !ok {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	}
	return ""
}

func (c AmqpHeaderCarrier) Set(key, value string) {
	c[key] = value
}

func (c AmqpHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
