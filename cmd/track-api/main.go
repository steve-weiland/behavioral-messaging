// track-api: behavioral-messaging V1 HTTP intake.
//
// Routes:
//   - POST /events             accept an event, durably store it, hand to the campaign Dispatcher.
//   - POST /people, GET /people/{id}           identify / read-back.
//   - POST /segments, GET /segments/{id}[/check]  define + scan-evaluate.
//   - POST /campaigns, GET /campaigns/{id}     define + read-back.
//   - GET  /healthz            liveness.
//
// Handler bodies + the dependency-bag live in server.go; main() does only
// wiring (signals, observability, DB, HTTP client, Dispatcher) and runs
// the HTTP server with graceful shutdown.
//
// Instrumentation:
//   - otelhttp middleware around the mux (HTTP server span).
//   - otelhttp.NewTransport on the outbound HTTP client (client span;
//     traceparent propagated to stub-receiver via the Dispatcher).
//   - slog JSON to stdout with trace_id/span_id auto-attached.
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/steveweiland/behavioral-messaging/internal/campaign"
	"github.com/steveweiland/behavioral-messaging/internal/eventstore"
	"github.com/steveweiland/behavioral-messaging/internal/logsx"
	"github.com/steveweiland/behavioral-messaging/internal/otelinit"
)

const serviceName = "track-api"

func main() {
	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	slog.SetDefault(logsx.Init(serviceName, "0.1.5"))

	shutdownTrace, err := otelinit.Init(rootCtx, serviceName, "0.1.5")
	if err != nil {
		log.Fatalf("otel init: %v", err)
	}
	defer func() { _ = shutdownTrace(context.Background()) }()

	db, err := openDBWithRetry(envOr("MYSQL_DSN", "bm:bm@tcp(mysql:3306)/bm?parseTime=true"), 30*time.Second)
	if err != nil {
		log.Fatalf("mysql connect: %v", err)
	}
	defer db.Close()

	stubURL := strings.TrimRight(envOr("STUB_RECEIVER_URL", "http://stub-receiver:8081"), "/")
	httpClient := &http.Client{
		Timeout:   5 * time.Second,
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	// V1 PR 4: in-process campaign fan-out. Single consumer goroutine,
	// buffered channel, drop-on-full with a Prometheus counter. V2
	// replaces the channel with RabbitMQ; the Dispatcher API stays.
	dispatcher := campaign.NewDispatcher(db, httpClient, stubURL)
	dispatcher.Start()
	defer dispatcher.Stop()

	srv := newServer(db, dispatcher, stubURL)
	mux := http.NewServeMux()
	srv.routes(mux)

	addr := ":" + envOr("PORT", "8080")
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           otelhttp.NewHandler(mux, "track-api.http"),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("listening", slog.String("addr", addr))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-rootCtx.Done()
	slog.Info("shutdown: draining HTTP server")
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	_ = httpServer.Shutdown(stopCtx)
}

// openDBWithRetry — MySQL's healthcheck flips before it's truly ready for
// connections; bound retry loop here parallels the pattern Build 5 used
// for Postgres.
func openDBWithRetry(dsn string, total time.Duration) (*sql.DB, error) {
	deadline := time.Now().Add(total)
	var lastErr error
	for time.Now().Before(deadline) {
		db, err := eventstore.Open(dsn)
		if err == nil {
			return db, nil
		}
		lastErr = err
		slog.Warn("mysql not ready, retrying", slog.Any("error", err))
		time.Sleep(2 * time.Second)
	}
	return nil, lastErr
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
