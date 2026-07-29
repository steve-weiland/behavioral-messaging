// journey-scheduler advances persisted journey runs (V3-1b, BM-115..119).
//
// It is the only component that executes journey steps. The campaign-worker
// creates a run and stops there — spec §3.3's first structural rule is that the
// FSM stays off the per-event fan-out path, so V2's measured hot path keeps
// meaning what it measured.
//
// Multiple replicas are safe: due runs are claimed with FOR UPDATE SKIP LOCKED,
// so two schedulers never take the same row, and a claim carries a lease so a
// dead replica's work is reclaimed rather than stranded. Single-replica is a
// default, not a constraint.
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
	"syscall"
	"time"

	"github.com/steveweiland/behavioral-messaging/internal/eventstore"
	"github.com/steveweiland/behavioral-messaging/internal/journey"
	"github.com/steveweiland/behavioral-messaging/internal/logsx"
	"github.com/steveweiland/behavioral-messaging/internal/otelinit"
)

const serviceName = "journey-scheduler"

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

	// The scheduler's HTTP client dispatches sends. Sized like the
	// campaign-worker's for the same reason: the default transport pools only
	// 2 idle connections per host, which is the V2-1 connection-leak lesson.
	batch := envInt("SCHEDULER_BATCH", 50)
	hc := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        batch * 2,
			MaxIdleConnsPerHost: batch,
			MaxConnsPerHost:     batch * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	sched := journey.NewScheduler(db, hc, journey.SchedulerConfig{
		// claimed_by identifies the replica in journey_runs, so a stuck lease
		// can be traced to the process that took it.
		ID:      envOr("SCHEDULER_ID", serviceName+"-"+hostnameOr("1")),
		StubURL: envOr("STUB_RECEIVER_URL", "http://stub-receiver:8081"),
		Batch:   batch,
		Lease:   envDuration("SCHEDULER_LEASE", 30*time.Second),
		// Cap on idle sleep so a newly created run is picked up promptly even
		// though nothing signals the scheduler (BM-116).
		MaxSleep: envDuration("SCHEDULER_MAX_SLEEP", 5*time.Second),
		// Q16: a send-gate row stuck in 'sending' means we cannot know whether
		// the downstream received it. "skip" never double-sends and may
		// under-send; "retry" is the opposite trade. Default to skip because
		// the stub sink makes under-sending the cheaper mistake here — a real
		// ESP would want this decided per message class.
		StuckPolicy: envOr("SEND_GATE_STUCK_POLICY", "skip"),
		StuckAfter:  envDuration("SEND_GATE_STUCK_AFTER", 2*time.Minute),
	})

	go sched.Run(rootCtx)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	addr := ":" + envOr("PORT", "8084")
	httpServer := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		slog.Info("listening", slog.String("addr", addr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	<-rootCtx.Done()
	slog.Info("shutdown signal received")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	_ = httpServer.Shutdown(shutCtx)
	// The scheduler loop exits on rootCtx; a run it was mid-step on keeps its
	// lease and is reclaimed by any replica once the lease expires. That is the
	// crash path exercised deliberately, not an accident.
	slog.Info("stopped")
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

func hostnameOr(def string) string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return def
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}
