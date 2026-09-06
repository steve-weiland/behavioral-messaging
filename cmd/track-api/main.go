// track-api: behavioral-messaging HTTP intake.
//
// Routes:
//   - POST /events             accept an event, durably store it, publish to RabbitMQ.
//   - POST /people, GET /people/{id}           identify / read-back.
//   - POST /segments, GET /segments/{id}[/check]  define + scan-evaluate.
//   - POST /campaigns, GET /campaigns/{id}     define + read-back.
//   - GET  /healthz            liveness.
//
// Handler bodies + the dep-bag live in server.go; main() does only
// wiring (signals, observability, DB, AMQP, Publisher) and runs the
// HTTP server with graceful shutdown.
//
// V2-1b: track-api is producer-only. Accepted events are published to
// the `campaigns.fanout` exchange with routing_key=workspace_id. The
// per-event fan-out work (campaign list, person.Get, trigger eval,
// template render, stub-receiver POST, journey_enrollments audit)
// now runs in cmd/campaign-worker, consuming per-workspace queues
// with manual ack.
//
// Instrumentation:
//   - otelhttp middleware around the mux (HTTP server span).
//   - amqpx.Publish injects W3C traceparent onto AMQP headers, so the
//     worker's amqp.consume + campaign.process spans join the same
//     trace as the original HTTP request.
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

	"github.com/steve-weiland/behavioral-messaging/internal/amqpx"
	"github.com/steve-weiland/behavioral-messaging/internal/campaign"
	"github.com/steve-weiland/behavioral-messaging/internal/eventstore"
	"github.com/steve-weiland/behavioral-messaging/internal/logsx"
	"github.com/steve-weiland/behavioral-messaging/internal/otelinit"
	"github.com/steve-weiland/behavioral-messaging/internal/peoplefeed"
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

	// V2-1: connect to RabbitMQ, declare the producer-side topology
	// (campaigns.fanout exchange + DLX + DLQ). Per-workspace queues are
	// declared by cmd/campaign-worker.
	amqpURL := envOr("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")
	amqpConn, amqpCh, err := amqpx.ConnectWithRetry(amqpURL, 30*time.Second)
	if err != nil {
		log.Fatalf("amqp connect: %v", err)
	}
	defer amqpConn.Close()
	defer amqpCh.Close()
	if err := campaign.DeclareProducerTopology(amqpCh); err != nil {
		log.Fatalf("amqp topology: %v", err)
	}
	// V3-2 retry ladder — declared producer-side so the rungs exist before the
	// first failure can need them.
	if err := campaign.DeclareRetryTopology(amqpCh); err != nil {
		log.Fatalf("amqp retry topology: %v", err)
	}
	slog.Info("amqp topology declared",
		slog.String("exchange", campaign.ExchangeFanout),
		slog.String("dlx", campaign.ExchangeDLX),
		slog.String("dlq", campaign.QueueDLQ),
		slog.Int("retry_tiers", len(campaign.RetryLadder)))

	// V2-1b: the Publisher replaces V1's in-process Dispatcher. The
	// /events handler hands accepted events to Submit; a single
	// publish goroutine drains a small in-process buffer and publishes
	// to RabbitMQ. Per-event fan-out runs in cmd/campaign-worker.
	publisher := campaign.NewPublisher(amqpCh)
	publisher.Start()
	defer publisher.Stop()

	srv := newServer(db, publisher)

	// V2-2: the people.changes feed keeps segment-worker's bitmap index
	// current. Its own dedicated channel — the campaign Publisher owns
	// amqpCh, and one writer per channel keeps failure domains and
	// back-pressure separable (amqp091 itself is concurrency-safe). /check
	// delegates membership to segment-worker when SEGMENT_INDEX_URL is set.
	peopleCh, err := amqpConn.Channel()
	if err != nil {
		log.Fatalf("amqp people-feed channel: %v", err)
	}
	defer peopleCh.Close()
	// A dead broker link must not leave a zombie publisher behind a green
	// /healthz — exit and let the restart policy reconnect (BM review #5).
	amqpx.ExitOnClose(amqpConn, amqpCh, serviceName)
	amqpx.ExitOnClose(amqpConn, peopleCh, serviceName)
	peopleFeed, err := peoplefeed.NewPublisher(peopleCh)
	if err != nil {
		log.Fatalf("people feed: %v", err)
	}
	srv.peopleFeed = peopleFeed
	srv.segmentIndexURL = strings.TrimRight(envOr("SEGMENT_INDEX_URL", ""), "/")
	srv.indexClient = &http.Client{
		Timeout:   3 * time.Second,
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
	if srv.segmentIndexURL != "" {
		slog.Info("segment /check delegating to bitmap index",
			slog.String("segment_index_url", srv.segmentIndexURL))
	}
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
