// Package otelinit bootstraps OpenTelemetry for each service.
//
// V2 (PR 3): both a TracerProvider and a MeterProvider are registered
// against the global SDK; both export OTLP/gRPC to the collector
// (OBS-70 / OBS-120). The collector then fans traces to Tempo and
// metrics to a Prometheus exporter on :8889 (OBS-123) which Prometheus
// scrapes (OBS-124). Logs remain stdout-only until §3.14 (PR 5).
package otelinit

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// otelErrorHandler routes OTel SDK errors (which would otherwise be
// silent) into slog so export failures surface.
type otelErrorHandler struct{}

func (otelErrorHandler) Handle(err error) {
	slog.Warn("otel sdk error", slog.Any("error", err))
}

// Init wires the global TracerProvider and MeterProvider for the calling
// service. Returns a single shutdown that flushes both (OBS-80).
func Init(ctx context.Context, serviceName, version string) (func(context.Context) error, error) {
	// Surface SDK errors (export failures etc.) — default is silent.
	otel.SetErrorHandler(otelErrorHandler{})

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "otel-collector:4317"
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version),
			semconv.DeploymentEnvironment("dev"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("build resource: %w", err)
	}

	// Each exporter creates its own gRPC connection via WithEndpoint +
	// WithInsecure. WithGRPCConn(shared) had the trace pipeline silently
	// dropping spans on this Docker Desktop host while metrics still
	// flowed — root cause unclear; per-exporter connections sidestep it.
	traceExp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp, sdktrace.WithMaxExportBatchSize(64)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	metricExp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(15*time.Second),
		)),
	)
	otel.SetMeterProvider(mp)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func(ctx context.Context) error {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		// Shut both providers; report the first error but keep going so
		// neither leaks an exporter goroutine.
		mErr := mp.Shutdown(shutdownCtx)
		tErr := tp.Shutdown(shutdownCtx)
		if tErr != nil {
			return tErr
		}
		return mErr
	}, nil
}
