// stub-receiver: stand-in for downstream delivery providers
// (ESP/APNs/FCM/Twilio/webhook receivers).
//
// V1 behaviour:
//   - POST / accepts any JSON body, logs it as INFO with trace_id, returns 200.
//   - GET /healthz → {"status":"ok"}.
//
// V2 will gain a configurable failure rate + a retry knob so the
// idempotency PR has something to break against.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/steveweiland/behavioral-messaging/internal/logsx"
	"github.com/steveweiland/behavioral-messaging/internal/otelinit"
)

const serviceName = "stub-receiver"

func main() {
	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	slog.SetDefault(logsx.Init(serviceName, "0.1.0"))

	shutdownTrace, err := otelinit.Init(rootCtx, serviceName, "0.1.0")
	if err != nil {
		log.Fatalf("otel init: %v", err)
	}
	defer func() { _ = shutdownTrace(context.Background()) }()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
		if err != nil {
			http.Error(w, `{"error":"read body"}`, http.StatusBadRequest)
			return
		}
		// Peek for an event_id we can surface as a top-level log field —
		// not strictly necessary but useful when scrolling Loki.
		eventID := peekEventID(body)
		slog.InfoContext(r.Context(), "delivery received",
			slog.String("event_id", eventID),
			slog.Int("bytes", len(body)))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received":true}`))
	})

	addr := ":" + envOr("PORT", "8081")
	server := &http.Server{
		Addr:              addr,
		Handler:           otelhttp.NewHandler(mux, "stub-receiver.http"),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("listening", slog.String("addr", addr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-rootCtx.Done()
	slog.Info("shutdown: draining HTTP server")
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	_ = server.Shutdown(stopCtx)
}

func peekEventID(body []byte) string {
	var x struct {
		EventID string `json:"event_id"`
	}
	_ = json.Unmarshal(body, &x)
	return x.EventID
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
