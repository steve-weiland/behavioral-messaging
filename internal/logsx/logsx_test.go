package logsx

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// recordedLine is the subset of the JSON log line we assert on.
type recordedLine struct {
	Msg     string `json:"msg"`
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
}

func parseLast(t *testing.T, buf *bytes.Buffer) recordedLine {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 {
		t.Fatal("no log lines emitted")
	}
	var rl recordedLine
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &rl); err != nil {
		t.Fatalf("parse json: %v\n  raw: %s", err, lines[len(lines)-1])
	}
	return rl
}

// Records emitted with no span context must omit trace_id/span_id (OBS-142).
func TestOmitsTraceFieldsOutsideSpanContext(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf, "test", "0.0.0")
	logger.InfoContext(context.Background(), "no span here")

	rl := parseLast(t, &buf)
	if rl.TraceID != "" || rl.SpanID != "" {
		t.Errorf("expected trace_id/span_id absent; got trace_id=%q span_id=%q", rl.TraceID, rl.SpanID)
	}
	if rl.Msg != "no span here" {
		t.Errorf("msg=%q", rl.Msg)
	}
}

// Records emitted inside a span context must carry both fields (OBS-141).
func TestInjectsTraceFieldsFromSpanContext(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf, "test", "0.0.0")

	// A real span from the SDK gives us a valid SpanContext.
	tp := trace.NewTracerProvider()
	defer tp.Shutdown(context.Background())
	ctx, span := tp.Tracer("logsx-test").Start(context.Background(), "op")
	defer span.End()

	logger.InfoContext(ctx, "inside span")

	rl := parseLast(t, &buf)
	if rl.TraceID == "" || rl.SpanID == "" {
		t.Errorf("expected trace_id+span_id present; got trace_id=%q span_id=%q", rl.TraceID, rl.SpanID)
	}
	if got := span.SpanContext().TraceID().String(); rl.TraceID != got {
		t.Errorf("trace_id=%q want %q", rl.TraceID, got)
	}
}

// A noop tracer produces an invalid SpanContext — must NOT inject zeroed IDs.
func TestSkipsInvalidSpanContext(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf, "test", "0.0.0")

	ctx, _ := noop.NewTracerProvider().Tracer("noop").Start(context.Background(), "op")
	logger.InfoContext(ctx, "noop span")

	rl := parseLast(t, &buf)
	if rl.TraceID != "" || rl.SpanID != "" {
		t.Errorf("noop span should omit ids; got trace_id=%q span_id=%q", rl.TraceID, rl.SpanID)
	}
}
