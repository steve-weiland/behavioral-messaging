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
	"github.com/steveweiland/behavioral-messaging/internal/journey"
	"github.com/steveweiland/behavioral-messaging/internal/logsx"
	"github.com/steveweiland/behavioral-messaging/internal/otelinit"
	"github.com/steveweiland/behavioral-messaging/internal/peoplefeed"
	"github.com/steveweiland/behavioral-messaging/internal/person"

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
	// V3-2: the retry ladder. Declared here as well as producer-side so the
	// rungs exist even if the worker boots first.
	if err := campaign.DeclareRetryTopology(amqpCh); err != nil {
		log.Fatalf("amqp retry topology: %v", err)
	}

	// V2-2c hot path. Default "cache": serve per-event campaign list +
	// person attributes from in-process caches (no MySQL read per event).
	// "mysql": the V1 direct-read path, kept as the A/B ceiling baseline.
	var attrCache *campaign.AttrCache
	var campCache *campaign.CampaignCache
	if envOr("CAMPAIGN_HOT_PATH", "cache") == "mysql" {
		slog.Info("hot path: direct MySQL (baseline)")
	} else {
		attrCache = campaign.NewAttrCache(db)
		campCache = campaign.NewCampaignCache(db, envDuration("CAMPAIGN_CACHE_TTL", 5*time.Second))

		// Bind a per-replica ephemeral queue to the people.changes fanout
		// BEFORE backfilling, so attribute changes during backfill are
		// buffered (not lost). A unique queue per replica (not a shared
		// name) means each replica receives every change — the cache stays
		// complete even when campaign-worker scales out. In-order delivery
		// + idempotent Set means a backfilled value is simply overwritten
		// by any newer change that follows.
		attrQueue, err := declareAttrFeedQueue(amqpCh)
		if err != nil {
			log.Fatalf("declare attr feed queue: %v", err)
		}
		if err := backfillAttrs(rootCtx, db, attrCache); err != nil {
			log.Fatalf("attr backfill: %v", err)
		}
		if err := amqpx.Consume(rootCtx, amqpCh, attrQueue, prefetch, serviceName+".attrs", attrFeedHandler(attrCache)); err != nil {
			log.Fatalf("consume %s: %v", attrQueue, err)
		}
		slog.Info("hot path: in-process caches",
			slog.String("attr_feed_queue", attrQueue),
			slog.Int("attrs_backfilled", attrCache.Len()))
	}

	// V2-3 enrollment batching (default on). Coalesces journey_enrollments
	// inserts into batched, idempotent commits — fewer fsyncs, no backlog
	// under write pressure. "off" uses direct (still idempotent) inserts,
	// the A/B baseline. Stop() (flush) is deferred before db.Close (LIFO).
	var batcher *campaign.Batcher
	if envOr("CAMPAIGN_BATCH", "on") != "off" {
		maxRows := envInt("BATCH_MAX", 64)
		batcher = campaign.NewBatcher(db, maxRows, envDuration("BATCH_WINDOW", 10*time.Millisecond))
		batcher.Start()
		defer batcher.Stop()
		slog.Info("enrollment batching enabled", slog.Int("batch_max", maxRows))
	} else {
		slog.Info("enrollment batching disabled (direct inserts)")
	}

	// V3-1a journey enrollment. This is the ONLY journey work on the
	// per-event path: evaluate triggers against the cached journey list and
	// write one run row per match. Step execution belongs to the scheduler
	// (V3-1b) — the design's first structural rule is that the FSM stays off
	// the fan-out path, or V2's measurements stop meaning anything.
	// JOURNEYS=off skips it entirely, keeping the V2 path available as the
	// A/B baseline for BM-140's before/after.
	var enroller *journey.Enroller
	if envOr("JOURNEYS", "on") != "off" {
		jCache := journey.NewCache(db, envDuration("JOURNEY_CACHE_TTL", 5*time.Second))
		enroller = journey.NewEnroller(db, jCache)
		slog.Info("journey enrollment enabled")
	} else {
		slog.Info("journey enrollment disabled")
	}

	// RETRY_LADDER=off falls back to V2's immediate requeue, kept as the A/B
	// baseline for showing what bounded retry changed.
	var retrier *campaign.Retrier
	if envOr("RETRY_LADDER", "on") != "off" {
		retrier = campaign.NewRetrier(amqpCh)
		slog.Info("retry ladder enabled", slog.Int("tiers", len(campaign.RetryLadder)))
	} else {
		slog.Info("retry ladder disabled (immediate requeue — V2 behavior)")
	}

	// V3-3 admission control (BM-130). Defaults: the global ceiling sits below
	// the MySQL pool so in-flight work never queues on connections, and each
	// workspace gets a slice of it.
	var limiter *campaign.Limiter
	if envOr("WORKSPACE_CAPS", "on") != "off" {
		globalCap := envInt("MAX_INFLIGHT_TOTAL", 40)
		wsCap := envInt("MAX_INFLIGHT_PER_WORKSPACE", 8)
		limiter = campaign.NewLimiter(globalCap, wsCap)
		slog.Info("per-workspace admission caps enabled",
			slog.Int("max_inflight_total", globalCap),
			slog.Int("max_inflight_per_workspace", wsCap))
	} else {
		slog.Info("per-workspace admission caps disabled (V2 behavior — aggregate unbounded)")
	}

	processor := campaign.NewProcessorWithCaches(db, httpClient, stubURL, attrCache, campCache, batcher)
	if enroller != nil {
		// Guarded: passing a nil *journey.Enroller through the interface
		// would make it non-nil and panic on first use.
		processor.SetEnroller(enroller)
	}

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
		handler := makeHandler(processor, retrier, limiter)
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

// makeHandler wraps the Processor in the amqpx.Handler signature and applies
// the V3-2 failure policy (BM-120/121/126).
//
// The classification lives here, explicitly, rather than being inferred from
// error strings:
//
//	terminal  — decode failure, person row missing. Retrying renders the same
//	            failure, so it goes straight to the DLQ with a reason.
//	transient — MySQL/broker/stub failures. Climbs the retry ladder; the
//	            original delivery is then ACKed because the message is durably
//	            on the retry queue.
//	exhausted — off the top of the ladder → DLQ, cause=exhausted.
//
// The old immediate Nack(requeue=true) is gone: it retried forever at full
// speed while the DLQ stayed empty and looked healthy.
func makeHandler(p *campaign.Processor, r *campaign.Retrier, lim *campaign.Limiter) amqpx.Handler {
	return func(ctx context.Context, d amqp.Delivery) amqpx.Outcome {
		var ev event.Event
		if err := json.Unmarshal(d.Body, &ev); err != nil {
			slog.ErrorContext(ctx, "decode event from AMQP body",
				slog.String("message_id", d.MessageId),
				slog.Any("error", err))
			if r != nil {
				if derr := r.DeadLetter(ctx, d, "", "undecodable body: "+err.Error(), "terminal"); derr == nil {
					return amqpx.OutcomeAck
				}
			}
			return amqpx.OutcomeNackDrop
		}
		// V3-3: admission control before any work. Holding the slot for the
		// whole of Process is the point — it bounds concurrent DB work, not
		// just concurrent deliveries.
		if lim != nil {
			release, err := lim.Acquire(ctx, ev.WorkspaceID)
			if err != nil {
				// Shutting down; requeue rather than drop.
				return amqpx.OutcomeNackRequeue
			}
			defer release()
		}
		outcome, reason := p.Process(ctx, ev)
		if r == nil {
			return outcome // retry ladder disabled — V2 behavior
		}
		switch outcome {
		case amqpx.OutcomeNackRequeue:
			if _, _, err := r.Retry(ctx, d, ev.WorkspaceID, reason); err != nil {
				// Couldn't even schedule the retry (broker trouble). Fall back
				// to the old requeue so the message isn't lost.
				slog.ErrorContext(ctx, "retry scheduling failed — falling back to requeue",
					slog.Any("error", err))
				return amqpx.OutcomeNackRequeue
			}
			return amqpx.OutcomeAck
		case amqpx.OutcomeNackDrop:
			if err := r.DeadLetter(ctx, d, ev.WorkspaceID, reason, "terminal"); err != nil {
				return amqpx.OutcomeNackDrop
			}
			return amqpx.OutcomeAck
		}
		return outcome
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

// declareAttrFeedQueue ensures the people.changes exchange and binds a
// per-replica ephemeral queue (server-named, exclusive, auto-delete) so
// this worker receives every attribute change. Returns the queue name.
func declareAttrFeedQueue(ch *amqp.Channel) (string, error) {
	if err := peoplefeed.DeclareExchange(ch); err != nil {
		return "", err
	}
	q, err := ch.QueueDeclare("", false /*durable*/, true /*autoDelete*/, true /*exclusive*/, false, nil)
	if err != nil {
		return "", err
	}
	if err := ch.QueueBind(q.Name, "", peoplefeed.Exchange, false, nil); err != nil {
		return "", err
	}
	return q.Name, nil
}

// backfillAttrs primes the attribute cache from current MySQL state.
func backfillAttrs(ctx context.Context, db *sql.DB, cache *campaign.AttrCache) error {
	people, err := person.All(ctx, db)
	if err != nil {
		return err
	}
	for _, p := range people {
		cache.Set(p.WorkspaceID, p.PersonID, p.Attributes)
	}
	return nil
}

// attrFeedHandler keeps the attribute cache current from people.changes.
// Decode failure is a terminal drop (Ack — no DLQ on this feed); the
// cache is best-effort with a MySQL fallback-on-miss.
func attrFeedHandler(cache *campaign.AttrCache) amqpx.Handler {
	return func(ctx context.Context, d amqp.Delivery) amqpx.Outcome {
		var c peoplefeed.Change
		if err := json.Unmarshal(d.Body, &c); err != nil {
			slog.ErrorContext(ctx, "campaign-worker: bad people change body, dropping",
				slog.String("message_id", d.MessageId), slog.Any("error", err))
			return amqpx.OutcomeAck
		}
		cache.Set(c.WorkspaceID, c.PersonID, c.Attributes)
		return amqpx.OutcomeAck
	}
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
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
