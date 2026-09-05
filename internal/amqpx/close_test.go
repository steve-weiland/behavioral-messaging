package amqpx

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// The zombie-consumer guard: a broker/channel FAULT must be reported so the
// process can exit; a graceful Close (nil / closed notify channel) must not.
func TestAbnormalClose(t *testing.T) {
	fault := make(chan *amqp.Error, 1)
	fault <- &amqp.Error{Code: amqp.PreconditionFailed, Reason: "inequivalent arg"}
	quiet := make(chan *amqp.Error)
	if !abnormalClose(fault, quiet) {
		t.Fatal("channel fault must be reported as abnormal")
	}

	graceful := make(chan *amqp.Error)
	close(graceful) // what amqp091 does on a clean Close: no error value
	if abnormalClose(graceful, quiet) {
		t.Fatal("graceful close must not be treated as abnormal")
	}
}
