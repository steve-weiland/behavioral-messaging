// track-api: behavioral-messaging V1 HTTP intake.
//
// POST /events
//   - Validates body + X-Workspace-ID header.
//   - Generates UUIDv4 event_id server-side.
//   - INSERTs to MySQL events (idempotent on (workspace_id, event_id)).
//   - Hands the event to the in-process campaign Dispatcher, which fans
//     out to whichever campaigns the workspace has defined and POSTs
//     rendered messages to STUB_RECEIVER_URL. V2 replaces the in-process
//     channel with RabbitMQ; the producer-side Submit call stays.
//
// Instrumentation:
//   - otelhttp middleware around the mux (HTTP server span).
//   - otelhttp.NewTransport on the outbound HTTP client (client span;
//     traceparent propagated to stub-receiver).
//   - slog JSON to stdout with trace_id/span_id auto-attached.
package main

import (
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

	"github.com/steveweiland/behavioral-messaging/internal/campaign"
	"github.com/steveweiland/behavioral-messaging/internal/event"
	"github.com/steveweiland/behavioral-messaging/internal/eventstore"
	"github.com/steveweiland/behavioral-messaging/internal/logsx"
	"github.com/steveweiland/behavioral-messaging/internal/otelinit"
	"github.com/steveweiland/behavioral-messaging/internal/person"
	"github.com/steveweiland/behavioral-messaging/internal/segment"
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

// V1 PR 3 caps. 16 KiB is plenty for an `and` of `or`s; depth is bounded
// in internal/segment at 8 to bound stack usage on Evaluate.
const maxSegmentDefBytes = 16 * 1024

// Cap how many events the scan-based segment evaluator pulls per check.
// Enough to answer `event_seen ever` on demo-sized profiles. V2's
// incremental bitmap path removes this scan entirely.
const segmentScanEventLimit = 1000

type defineSegmentRequest struct {
	SegmentID  string          `json:"segment_id"`
	Name       string          `json:"name"`
	Definition json.RawMessage `json:"definition"`
}

type segmentCheckResponse struct {
	WorkspaceID string `json:"workspace_id"`
	SegmentID   string `json:"segment_id"`
	PersonID    string `json:"person_id"`
	Member      bool   `json:"member"`
}

// V1 PR 4 caps. Trigger uses the same depth-8 condition tree as segments;
// template is bounded to keep the dispatcher's render loop predictable.
const maxCampaignBodyBytes = 32 * 1024
const maxTemplateBytes = 16 * 1024

type defineCampaignRequest struct {
	CampaignID string          `json:"campaign_id"`
	Name       string          `json:"name"`
	Trigger    json.RawMessage `json:"trigger"`
	Template   string          `json:"template"`
}

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

	// V1 PR 4: in-process campaign fan-out. Single consumer goroutine,
	// buffered channel, drop-on-full with a Prometheus counter. V2
	// replaces the channel with RabbitMQ; the Dispatcher API stays.
	dispatcher := campaign.NewDispatcher(db, httpClient, stubURL)
	dispatcher.Start(rootCtx)
	defer dispatcher.Stop()

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

	// V1 PR 3: segments — define + read + scan-based membership check.
	// One handler intentionally serves both `/segments` (POST) and
	// `/segments/{segment_id}` and `/segments/{segment_id}/check`.
	// net/http's ServeMux doesn't pattern-match path segments before
	// Go 1.22; we route on a trimmed suffix instead.
	mux.HandleFunc("/segments", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		workspace := r.Header.Get("X-Workspace-ID")
		if !validID(workspace) {
			writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
			return
		}
		body, err := readBody(r, maxSegmentDefBytes)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		var req defineSegmentRequest
		if err := json.Unmarshal(body, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if !validID(req.SegmentID) {
			writeErr(w, http.StatusBadRequest, "segment_id required ([A-Za-z0-9_-]{1,64})")
			return
		}
		if req.Name == "" || len(req.Name) > 255 {
			writeErr(w, http.StatusBadRequest, "name required, ≤ 255 chars")
			return
		}
		if len(req.Definition) == 0 {
			writeErr(w, http.StatusBadRequest, "definition required")
			return
		}
		cond, err := segment.DecodeCondition(req.Definition)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "definition: "+err.Error())
			return
		}
		if err := cond.Validate(); err != nil {
			writeErr(w, http.StatusBadRequest, "definition: "+err.Error())
			return
		}
		s := segment.Segment{
			WorkspaceID: workspace,
			SegmentID:   req.SegmentID,
			Name:        req.Name,
			Definition:  req.Definition,
		}
		if err := segment.Create(r.Context(), db, s); err != nil {
			slog.ErrorContext(r.Context(), "segment create failed",
				slog.String("workspace_id", workspace),
				slog.String("segment_id", req.SegmentID),
				slog.Any("error", err))
			writeErr(w, http.StatusInternalServerError, "store failed")
			return
		}
		slog.InfoContext(r.Context(), "segment created",
			slog.String("workspace_id", workspace),
			slog.String("segment_id", req.SegmentID),
			slog.String("name", req.Name))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/segments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		workspace := r.Header.Get("X-Workspace-ID")
		if !validID(workspace) {
			writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
			return
		}
		// /segments/{id}              → read one
		// /segments/{id}/check        → scan-evaluate for ?person_id=<id>
		suffix := strings.TrimPrefix(r.URL.Path, "/segments/")
		segmentID, remainder, _ := strings.Cut(suffix, "/")
		if !validID(segmentID) {
			writeErr(w, http.StatusBadRequest, "segment_id path segment required")
			return
		}

		s, err := segment.Get(r.Context(), db, workspace, segmentID)
		if errors.Is(err, segment.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "segment not found")
			return
		}
		if err != nil {
			slog.ErrorContext(r.Context(), "segment get failed",
				slog.String("workspace_id", workspace),
				slog.String("segment_id", segmentID),
				slog.Any("error", err))
			writeErr(w, http.StatusInternalServerError, "store failed")
			return
		}

		switch remainder {
		case "":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(s)
			return
		case "check":
			// Scan path: pull person + recent events, evaluate.
			personID := r.URL.Query().Get("person_id")
			if !validID(personID) {
				writeErr(w, http.StatusBadRequest, "person_id query parameter required")
				return
			}
			p, err := person.Get(r.Context(), db, workspace, personID)
			if errors.Is(err, person.ErrNotFound) {
				// A person that doesn't exist is, by definition, not a member.
				_ = json.NewEncoder(w).Encode(segmentCheckResponse{
					WorkspaceID: workspace, SegmentID: segmentID, PersonID: personID, Member: false,
				})
				return
			}
			if err != nil {
				slog.ErrorContext(r.Context(), "person get failed during segment check",
					slog.String("workspace_id", workspace),
					slog.String("person_id", personID),
					slog.Any("error", err))
				writeErr(w, http.StatusInternalServerError, "store failed")
				return
			}
			events, err := eventstore.RecentByPerson(r.Context(), db, workspace, personID, segmentScanEventLimit)
			if err != nil {
				slog.ErrorContext(r.Context(), "events scan failed",
					slog.String("workspace_id", workspace),
					slog.String("person_id", personID),
					slog.Any("error", err))
				writeErr(w, http.StatusInternalServerError, "store failed")
				return
			}
			cond, err := segment.DecodeCondition(s.Definition)
			if err != nil {
				// Stored row failed to decode — should never happen post-Validate.
				slog.ErrorContext(r.Context(), "stored segment definition undecodable",
					slog.String("segment_id", segmentID),
					slog.Any("error", err))
				writeErr(w, http.StatusInternalServerError, "definition decode failed")
				return
			}
			member := cond.Evaluate(&segment.EvalContext{Person: p, Events: events})
			slog.InfoContext(r.Context(), "segment check",
				slog.String("workspace_id", workspace),
				slog.String("segment_id", segmentID),
				slog.String("person_id", personID),
				slog.Bool("member", member),
				slog.Int("events_scanned", len(events)))
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(segmentCheckResponse{
				WorkspaceID: workspace, SegmentID: segmentID, PersonID: personID, Member: member,
			})
			return
		default:
			writeErr(w, http.StatusNotFound, "unknown segment sub-path")
			return
		}
	})

	// V1 PR 4: campaigns. trigger validates as a segment.Condition
	// (same six-op tree); template parses with text/template at insert
	// time so bad shapes can't reach the dispatcher.
	mux.HandleFunc("/campaigns", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		workspace := r.Header.Get("X-Workspace-ID")
		if !validID(workspace) {
			writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
			return
		}
		body, err := readBody(r, maxCampaignBodyBytes)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		var req defineCampaignRequest
		if err := json.Unmarshal(body, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if !validID(req.CampaignID) {
			writeErr(w, http.StatusBadRequest, "campaign_id required ([A-Za-z0-9_-]{1,64})")
			return
		}
		if req.Name == "" || len(req.Name) > 255 {
			writeErr(w, http.StatusBadRequest, "name required, ≤ 255 chars")
			return
		}
		if len(req.Trigger) == 0 {
			writeErr(w, http.StatusBadRequest, "trigger required")
			return
		}
		if req.Template == "" || len(req.Template) > maxTemplateBytes {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("template required, ≤ %d bytes", maxTemplateBytes))
			return
		}
		cond, err := segment.DecodeCondition(req.Trigger)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "trigger: "+err.Error())
			return
		}
		if err := cond.Validate(); err != nil {
			writeErr(w, http.StatusBadRequest, "trigger: "+err.Error())
			return
		}
		if err := campaign.ParseTemplate(req.CampaignID, req.Template); err != nil {
			writeErr(w, http.StatusBadRequest, "template: "+err.Error())
			return
		}
		c := campaign.Campaign{
			WorkspaceID: workspace,
			CampaignID:  req.CampaignID,
			Name:        req.Name,
			Trigger:     req.Trigger,
			Template:    req.Template,
		}
		if err := campaign.Create(r.Context(), db, c); err != nil {
			slog.ErrorContext(r.Context(), "campaign create failed",
				slog.String("workspace_id", workspace),
				slog.String("campaign_id", req.CampaignID),
				slog.Any("error", err))
			writeErr(w, http.StatusInternalServerError, "store failed")
			return
		}
		slog.InfoContext(r.Context(), "campaign created",
			slog.String("workspace_id", workspace),
			slog.String("campaign_id", req.CampaignID),
			slog.String("name", req.Name))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/campaigns/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		workspace := r.Header.Get("X-Workspace-ID")
		if !validID(workspace) {
			writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
			return
		}
		campaignID := strings.TrimPrefix(r.URL.Path, "/campaigns/")
		if !validID(campaignID) {
			writeErr(w, http.StatusBadRequest, "campaign_id path segment required")
			return
		}
		c, err := campaign.Get(r.Context(), db, workspace, campaignID)
		if errors.Is(err, campaign.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "campaign not found")
			return
		}
		if err != nil {
			slog.ErrorContext(r.Context(), "campaign get failed",
				slog.String("workspace_id", workspace),
				slog.String("campaign_id", campaignID),
				slog.Any("error", err))
			writeErr(w, http.StatusInternalServerError, "store failed")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(c)
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

		// Hand the event to the in-process campaign dispatcher. Submit
		// detaches from the request context so the consumer outlives the
		// handler; the producer's span context is preserved so the async
		// campaign.process span joins this trace. Drop-on-full is logged
		// + counted inside Submit.
		dispatcher.Submit(r.Context(), ev)

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
