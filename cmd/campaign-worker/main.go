// campaign-worker: behavioral-messaging V2-1b campaign fan-out consumer.
//
// Consumes per-workspace queues bound to the `campaigns.fanout`
// exchange. For each delivery: decode the event, evaluate every
// campaign in the workspace against (person, [inbound_event]), render
// + POST + audit each match. Manual ack only after the per-event
// processor returns OutcomeAck. Transient infra failures (MySQL down,
// etc.) return OutcomeNackRequeue and the message comes back. Terminal
// errors (person row missing) return OutcomeNackDrop and the message
// routes to the DLQ via the queue's x-dead-letter-exchange arg.
//
// Workspace discovery: at boot we SELECT workspace_id FROM workspaces
// and subscribe to one queue per row. New workspaces created after boot
// aren't auto-picked-up in V2-1; that's V2-stretch (periodic refresh)
// or V2-3 (when /workspaces becomes a real API).
//
// Per-event work runs on per-queue goroutines via amqpx.Consume; prefetch
// (default 32) bounds in-flight per queue.
//
// Instrumentation:
//   - amqpx.Consume extracts W3C traceparent from delivery headers
//     and starts an `amqp.consume <queue>` consumer span; the
//     processor wraps the per-event work in a `campaign.process`
//     span underneath it.
//   - slog JSON to stdout with trace_id/span_id auto-attached.
package main

import (
	"context"
	"database/sql"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/steveweiland/behavioral-messaging/internal/amqpx"
	"github.com/steveweiland/behavioral-messaging/internal/campaign"
	"github.com/steveweiland/behavioral-messaging/internal/event"
	"github.com/steveweiland/behavioral-messaging/internal/eventstore"
	"github.com/steveweiland/behavioral-messaging/internal/logsx"
	"github.com/steveweiland/behavioral-messaging/internal/otelinit"

	"encoding/json"
)

const serviceName = "campaign-worker"

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
	// The stub POST runs once per in-flight delivery, so the connection
	// pool must be sized to prefetch — http.DefaultTransport caps
	// MaxIdleConnsPerHost at 2, which under high prefetch forces a fresh
	// TCP dial per dispatch and exhausts the ephemeral port range
	// ("cannot assign requested address"). Pool to prefetch so warm
	// connections are reused instead. (V2 throughput-test finding.)
	prefetch := envInt("PREFETCH", 32)
	stubTransport := http.DefaultTransport.(*http.Transport).Clone()
	stubTransport.MaxIdleConns = prefetch * 2
	stubTransport.MaxIdleConnsPerHost = prefetch
	stubTransport.MaxConnsPerHost = prefetch
	httpClient := &http.Client{
		Timeout:   5 * time.Second,
		Transport: otelhttp.NewTransport(stubTransport),
	}

	amqpURL := envOr("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")
	amqpConn, amqpCh, err := amqpx.ConnectWithRetry(amqpURL, 30*time.Second)
	if err != nil {
		log.Fatalf("amqp connect: %v", err)
	}
	defer amqpConn.Close()
	defer amqpCh.Close()

	// The worker declares per-workspace queues (it owns the queues it
	// reads from). The producer-side exchange + DLX are already declared
	// by track-api; re-declaring them here is idempotent so we ensure
	// the topology even if the worker boots first.
	if err := campaign.DeclareProducerTopology(amqpCh); err != nil {
		log.Fatalf("amqp topology: %v", err)
	}

	processor := campaign.NewProcessor(db, httpClient, stubURL)

	workspaces, err := listWorkspaces(rootCtx, db)
	if err != nil {
		log.Fatalf("list workspaces: %v", err)
	}
	slog.Info("subscribing to workspaces",
		slog.Int("count", len(workspaces)),
		slog.Int("prefetch", prefetch))

	for _, ws := range workspaces {
		queueName, err := campaign.EnsureWorkerQueue(amqpCh, ws)
		if err != nil {
			log.Fatalf("ensure queue %s: %v", ws, err)
		}
		handler := makeHandler(processor)
		consumerTag := serviceName + "." + ws
		if err := amqpx.Consume(rootCtx, amqpCh, queueName, prefetch, consumerTag, handler); err != nil {
			log.Fatalf("consume %s: %v", queueName, err)
		}
		slog.Info("consumer started",
			slog.String("workspace_id", ws),
			slog.String("queue", queueName))
	}

	// Liveness endpoint — kept tiny; the worker is otherwise a pure
	// AMQP consumer so there's nothing else for an external HTTP
	// healthcheck to hit.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	addr := ":" + envOr("PORT", "8082")
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("listening", slog.String("addr", addr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-rootCtx.Done()
	slog.Info("shutdown: closing AMQP + HTTP")
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	_ = httpServer.Shutdown(stopCtx)
	// amqpCh.Close + amqpConn.Close fire via defer; in-flight deliveries
	// in amqpx.Consume's goroutines are bounded by prefetch and will
	// either Ack or be redelivered after the broker notices the channel
	// drop. V2-1's at-least-once posture covers the redelivery case.
}

// makeHandler wraps the Processor in the amqpx.Handler signature.
// Decode failure is terminal (poison message → DLQ); everything else
// delegates to Processor.Process which returns the Outcome.
func makeHandler(p *campaign.Processor) amqpx.Handler {
	return func(ctx context.Context, d amqp.Delivery) amqpx.Outcome {
		var ev event.Event
		if err := json.Unmarshal(d.Body, &ev); err != nil {
			slog.ErrorContext(ctx, "decode event from AMQP body",
				slog.String("message_id", d.MessageId),
				slog.Any("error", err))
			return amqpx.OutcomeNackDrop
		}
		return p.Process(ctx, ev)
	}
}

func listWorkspaces(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT workspace_id FROM workspaces`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var ws string
		if err := rows.Scan(&ws); err != nil {
			return nil, err
		}
		out = append(out, ws)
	}
	return out, rows.Err()
}

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

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
