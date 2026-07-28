package campaign

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/steveweiland/behavioral-messaging/internal/event"
)

// The producer's whole reason for existing is BM-89: never block the HTTP
// reply, and never lose an event silently. V1's silent 69% channel drop is
// the bug this replaced, so the drop paths need to be observable — these
// tests assert the metric contract, not just the behavior.

// withManualReader points the global meter at a manual reader so a test can
// collect what the publisher actually recorded.
func withManualReader(t *testing.T) *metric.ManualReader {
	t.Helper()
	r := metric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(metric.NewMeterProvider(metric.WithReader(r)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
	return r
}

// dropsByReason collects campaign_publish_dropped_total, summing by the
// reason attribute.
func dropsByReason(t *testing.T, r *metric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "campaign_publish_dropped_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("campaign_publish_dropped_total is %T, want Sum[int64]", m.Data)
			}
			for _, dp := range sum.DataPoints {
				reason, _ := dp.Attributes.Value("reason")
				out[reason.AsString()] += dp.Value
			}
		}
	}
	return out
}

func testEvent() event.Event {
	return event.Event{WorkspaceID: "ws_alpha", EventID: "e1", PersonID: "p1", Name: "signed_up"}
}

// A full buffer must drop and count — never block the caller. Start() is
// deliberately not called, so nothing drains: every Submit past capacity
// takes the default branch.
func TestPublisherSubmit_DropsOnFullBuffer(t *testing.T) {
	r := withManualReader(t)
	p := newPublisher(nil, 2)

	for i := 0; i < 5; i++ {
		p.Submit(context.Background(), testEvent()) // must not block
	}

	if got := len(p.ch); got != 2 {
		t.Errorf("buffered events = %d, want 2 (capacity)", got)
	}
	drops := dropsByReason(t, r)
	if drops["buffer_full"] != 3 {
		t.Errorf("buffer_full drops = %d, want 3", drops["buffer_full"])
	}
	if drops["shutdown"] != 0 {
		t.Errorf("shutdown drops = %d, want 0", drops["shutdown"])
	}
}

// After Stop, Submit must drop with reason=shutdown rather than enqueue into
// a buffer nothing will drain.
func TestPublisherSubmit_DropsAfterStop(t *testing.T) {
	r := withManualReader(t)
	p := newPublisher(nil, 8)
	p.Stop() // no Start(), so wg is empty and Stop returns immediately

	p.Submit(context.Background(), testEvent())

	if got := len(p.ch); got != 0 {
		t.Errorf("buffered events = %d, want 0 — nothing may enqueue after Stop", got)
	}
	drops := dropsByReason(t, r)
	if drops["shutdown"] != 1 {
		t.Errorf("shutdown drops = %d, want 1", drops["shutdown"])
	}
}

// Stop must be idempotent — main's shutdown path can reach it twice.
func TestPublisherStop_Idempotent(t *testing.T) {
	withManualReader(t)
	p := newPublisher(nil, 1)
	p.Stop()
	p.Stop() // must not panic on a double close
}
