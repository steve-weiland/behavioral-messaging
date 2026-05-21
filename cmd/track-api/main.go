// track-api: behavioral-messaging V1 HTTP intake.
//
// POST /events
//   - Validates body + X-Workspace-ID header.
//   - Generates UUIDv4 event_id server-side.
//   - INSERTs to MySQL events (idempotent on (workspace_id, event_id)).
//   - Fires a synchronous HTTP POST to STUB_RECEIVER_URL — the simplest
//     downstream that proves trace propagation works end-to-end. V1 PR 2+
//     will route through an in-process channel + campaign worker; for the
//     kickoff PR the direct call is the smoke test for the scaffolding.
//
// Instrumentation:
//   - otelhttp middleware around the mux (HTTP server span).
//   - otelhttp.NewTransport on the outbound HTTP client (client span;
//     traceparent propagated to stub-receiver).
//   - slog JSON to stdout with trace_id/span_id auto-attached.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/steveweiland/behavioral-messaging/internal/event"
	"github.com/steveweiland/behavioral-messaging/internal/eventstore"
	"github.com/steveweiland/behavioral-messaging/internal/logsx"
	"github.com/steveweiland/behavioral-messaging/internal/otelinit"
	"github.com/steveweiland/behavioral-messaging/internal/person"
)

const serviceName = "track-api"

type intakeRequest struct {
	PersonID  string          `json:"person_id"`
	EventName string          `json:"event_name"`
	Payload   json.RawMessage `json:"payload"`
}

type intakeResponse struct {
	EventID string `json:"event_id"`
}

type identifyRequest struct {
	PersonID   string          `json:"person_id"`
	Attributes json.RawMessage `json:"attributes"`
}

// 16 KiB cap on /people attribute payloads — matches the implicit cap
// on event payloads. Reviewed in the V1 spec as BM-26.
const maxAttributesBytes = 16 * 1024

func main() {
	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	slog.SetDefault(logsx.Init(serviceName, "0.1.0"))

	shutdownTrace, err := otelinit.Init(rootCtx, serviceName, "0.1.0")
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

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// V1 PR 2: POST /people — identify / attribute upsert.
	// MySQL JSON_MERGE_PATCH semantics per RFC 7396: new keys overwrite,
	// omitted keys preserved, null values delete the key. (BM-26..BM-29.)
	mux.HandleFunc("/people", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		workspace := r.Header.Get("X-Workspace-ID")
		if !validID(workspace) {
			writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
			return
		}
		body, err := readBody(r, maxAttributesBytes)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		var req identifyRequest
		if err := json.Unmarshal(body, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if !validID(req.PersonID) {
			writeErr(w, http.StatusBadRequest, "person_id required ([A-Za-z0-9_-]{1,128})")
			return
		}
		if len(req.Attributes) == 0 {
			req.Attributes = json.RawMessage(`{}`)
		} else if !isJSONObject(req.Attributes) {
			writeErr(w, http.StatusBadRequest, "attributes must be a JSON object")
			return
		}

		if err := person.Upsert(r.Context(), db, workspace, req.PersonID, req.Attributes); err != nil {
			slog.ErrorContext(r.Context(), "people upsert failed",
				slog.String("workspace_id", workspace),
				slog.String("person_id", req.PersonID),
				slog.Any("error", err))
			writeErr(w, http.StatusInternalServerError, "store failed")
			return
		}
		slog.InfoContext(r.Context(), "person upserted",
			slog.String("workspace_id", workspace),
			slog.String("person_id", req.PersonID),
			slog.Int("bytes", len(req.Attributes)))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// V1 PR 2: GET /people/{person_id} — read one person back. 404 if absent.
	mux.HandleFunc("/people/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		workspace := r.Header.Get("X-Workspace-ID")
		if !validID(workspace) {
			writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
			return
		}
		personID := strings.TrimPrefix(r.URL.Path, "/people/")
		if !validID(personID) {
			writeErr(w, http.StatusBadRequest, "person_id path segment required")
			return
		}
		p, err := person.Get(r.Context(), db, workspace, personID)
		if errors.Is(err, person.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not found")
			return
		}
		if err != nil {
			slog.ErrorContext(r.Context(), "person get failed",
				slog.String("workspace_id", workspace),
				slog.String("person_id", personID),
				slog.Any("error", err))
			writeErr(w, http.StatusInternalServerError, "store failed")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p)
	})

	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		workspace := r.Header.Get("X-Workspace-ID")
		if !validID(workspace) {
			writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
			return
		}
		var req intakeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if !validID(req.PersonID) {
			writeErr(w, http.StatusBadRequest, "person_id required ([A-Za-z0-9_-]{1,128})")
			return
		}
		if !validEventName(req.EventName) {
			writeErr(w, http.StatusBadRequest, "event_name required ([A-Za-z0-9_-]{1,128})")
			return
		}
		if len(req.Payload) == 0 {
			req.Payload = json.RawMessage(`{}`)
		}

		ev := event.Event{
			WorkspaceID: workspace,
			EventID:     uuid.NewString(),
			PersonID:    req.PersonID,
			Name:        req.EventName,
			Payload:     req.Payload,
			ReceivedAt:  time.Now().UTC(),
		}
		// V1 PR 2: tracking auto-creates the person row with empty
		// attributes if absent — matches Customer.io's identify-on-track
		// behaviour. INSERT IGNORE is a no-op on PK collision so
		// existing rows are never clobbered. (BM-29.)
		if err := person.EnsureExists(r.Context(), db, workspace, req.PersonID); err != nil {
			slog.ErrorContext(r.Context(), "person ensure failed",
				slog.String("workspace_id", workspace),
				slog.String("person_id", req.PersonID),
				slog.Any("error", err))
			writeErr(w, http.StatusInternalServerError, "store failed")
			return
		}
		if err := eventstore.Insert(r.Context(), db, ev); err != nil {
			slog.ErrorContext(r.Context(), "events insert failed",
				slog.String("workspace_id", workspace),
				slog.String("event_id", ev.EventID),
				slog.Any("error", err))
			writeErr(w, http.StatusInternalServerError, "store failed")
			return
		}

		// Smoke-test downstream: hand the event to stub-receiver synchronously.
		// V1 PR 2 will route via an in-process channel + campaign worker; this
		// direct call exists only to prove HTTP→DB→HTTP trace propagation.
		if err := forwardToStub(r.Context(), httpClient, stubURL, ev); err != nil {
			slog.WarnContext(r.Context(), "stub forward failed",
				slog.String("event_id", ev.EventID),
				slog.Any("error", err))
			// Don't fail the request — event is durably stored.
		}

		slog.InfoContext(r.Context(), "event accepted",
			slog.String("workspace_id", workspace),
			slog.String("event_id", ev.EventID),
			slog.String("person_id", ev.PersonID),
			slog.String("event_name", ev.Name))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(intakeResponse{EventID: ev.EventID})
	})

	addr := ":" + envOr("PORT", "8080")
	server := &http.Server{
		Addr:              addr,
		Handler:           otelhttp.NewHandler(mux, "track-api.http"),
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

func forwardToStub(ctx context.Context, c *http.Client, base string, ev event.Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("stub status=%d", resp.StatusCode)
	}
	return nil
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

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func validID(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

func validEventName(s string) bool { return validID(s) }

// readBody reads up to max+1 bytes; rejects if over max. Used by the
// people-identify endpoint to enforce the V1 attribute payload cap.
func readBody(r *http.Request, max int) (json.RawMessage, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(max)+1))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if len(body) > max {
		return nil, fmt.Errorf("body exceeds %d bytes", max)
	}
	return body, nil
}

// isJSONObject — peek validation. Accepts {} or {...}; rejects arrays,
// scalars, etc. Cheap pre-check that catches the most common mistakes
// before they hit MySQL's JSON column validator.
func isJSONObject(b []byte) bool {
	for _, c := range b {
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			continue
		}
		return c == '{'
	}
	return false
}
