// Package logsx wraps log/slog with a handler that injects trace_id +
// span_id from the active span context (V2 / PR 5 — OBS-140/141/142).
//
// Init returns a JSON-emitting *slog.Logger seeded with service.name and
// service.version, which the caller installs as slog.Default. Every
// subsequent slog.InfoContext / slog.ErrorContext call gets trace_id +
// span_id auto-attached when its ctx carries an active span. Records
// emitted outside any span context (e.g., startup logs) get neither
// field — emit-empty was rejected so absence is the signal.
package logsx

import (
	"context"
	"io"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// traceContextHandler wraps any slog.Handler and adds trace_id + span_id
// attributes from ctx-carried span context.
type traceContextHandler struct {
	slog.Handler
}

func (h traceContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if span := trace.SpanFromContext(ctx); span != nil {
		sc := span.SpanContext()
		if sc.IsValid() {
			r.AddAttrs(
				slog.String("trace_id", sc.TraceID().String()),
				slog.String("span_id", sc.SpanID().String()),
			)
		}
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs/WithGroup must rewrap so the attribute injection survives.
func (h traceContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceContextHandler{Handler: h.Handler.WithAttrs(attrs)}
}
func (h traceContextHandler) WithGroup(name string) slog.Handler {
	return traceContextHandler{Handler: h.Handler.WithGroup(name)}
}

// Init returns a logger configured for service.
func Init(serviceName, version string) *slog.Logger {
	return NewWithWriter(os.Stdout, serviceName, version)
}

// NewWithWriter is the test-friendly constructor.
func NewWithWriter(w io.Writer, serviceName, version string) *slog.Logger {
	base := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(traceContextHandler{Handler: base}).With(
		slog.String("service.name", serviceName),
		slog.String("service.version", version),
	)
	return logger
}
