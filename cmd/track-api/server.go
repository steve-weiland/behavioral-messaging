package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/steveweiland/behavioral-messaging/internal/campaign"
	"github.com/steveweiland/behavioral-messaging/internal/event"
	"github.com/steveweiland/behavioral-messaging/internal/eventstore"
	"github.com/steveweiland/behavioral-messaging/internal/person"
	"github.com/steveweiland/behavioral-messaging/internal/segment"
)

// Body-size caps. Worked out per surface; documented as BM-26 (people),
// BM-30 (segments), BM-70/72 (campaigns).
const (
	maxAttributesBytes    = 16 * 1024
	maxSegmentDefBytes    = 16 * 1024
	segmentScanEventLimit = 1000
	maxCampaignBodyBytes  = 32 * 1024
	maxTemplateBytes      = 16 * 1024
)

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

type defineCampaignRequest struct {
	CampaignID string          `json:"campaign_id"`
	Name       string          `json:"name"`
	Trigger    json.RawMessage `json:"trigger"`
	Template   string          `json:"template"`
}

// server is the dep-bag every track-api handler closes over. main() builds
// one and calls routes() once; tests build one with fakes/stubs and call
// the handler methods directly via httptest.
type server struct {
	db         *sql.DB
	dispatcher *campaign.Dispatcher
	stubURL    string
}

func newServer(db *sql.DB, d *campaign.Dispatcher, stubURL string) *server {
	return &server{db: db, dispatcher: d, stubURL: stubURL}
}

func (s *server) routes(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/people", s.handlePeoplePost)
	mux.HandleFunc("/people/", s.handlePeopleGet)
	mux.HandleFunc("/segments", s.handleSegmentsPost)
	mux.HandleFunc("/segments/", s.handleSegmentsGet)
	mux.HandleFunc("/campaigns", s.handleCampaignsPost)
	mux.HandleFunc("/campaigns/", s.handleCampaignsGet)
	mux.HandleFunc("/events", s.handleEventsPost)
}

func (s *server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// V1 PR 2: POST /people — identify / attribute upsert.
// MySQL JSON_MERGE_PATCH semantics per RFC 7396: new keys overwrite,
// omitted keys preserved, null values delete the key. (BM-26..BM-29.)
func (s *server) handlePeoplePost(w http.ResponseWriter, r *http.Request) {
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

	if err := person.Upsert(r.Context(), s.db, workspace, req.PersonID, req.Attributes); err != nil {
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
}

// V1 PR 2: GET /people/{person_id} — read one person back. 404 if absent.
func (s *server) handlePeopleGet(w http.ResponseWriter, r *http.Request) {
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
	p, err := person.Get(r.Context(), s.db, workspace, personID)
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
}

// V1 PR 3: POST /segments — store the condition tree after validating it.
func (s *server) handleSegmentsPost(w http.ResponseWriter, r *http.Request) {
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
	rec := segment.Segment{
		WorkspaceID: workspace,
		SegmentID:   req.SegmentID,
		Name:        req.Name,
		Definition:  req.Definition,
	}
	if err := segment.Create(r.Context(), s.db, rec); err != nil {
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
}

// V1 PR 3: GET /segments/{id} and /segments/{id}/check.
// net/http's ServeMux doesn't pattern-match path segments before
// Go 1.22; we route on a trimmed suffix instead.
func (s *server) handleSegmentsGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	workspace := r.Header.Get("X-Workspace-ID")
	if !validID(workspace) {
		writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
		return
	}
	suffix := strings.TrimPrefix(r.URL.Path, "/segments/")
	segmentID, remainder, _ := strings.Cut(suffix, "/")
	if !validID(segmentID) {
		writeErr(w, http.StatusBadRequest, "segment_id path segment required")
		return
	}

	rec, err := segment.Get(r.Context(), s.db, workspace, segmentID)
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
		_ = json.NewEncoder(w).Encode(rec)
		return
	case "check":
		personID := r.URL.Query().Get("person_id")
		if !validID(personID) {
			writeErr(w, http.StatusBadRequest, "person_id query parameter required")
			return
		}
		p, err := person.Get(r.Context(), s.db, workspace, personID)
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
		events, err := eventstore.RecentByPerson(r.Context(), s.db, workspace, personID, segmentScanEventLimit)
		if err != nil {
			slog.ErrorContext(r.Context(), "events scan failed",
				slog.String("workspace_id", workspace),
				slog.String("person_id", personID),
				slog.Any("error", err))
			writeErr(w, http.StatusInternalServerError, "store failed")
			return
		}
		cond, err := segment.DecodeCondition(rec.Definition)
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
}

// V1 PR 4: POST /campaigns. trigger validates as a segment.Condition
// (same six-op tree); template parses with text/template at insert
// time so bad shapes can't reach the dispatcher.
func (s *server) handleCampaignsPost(w http.ResponseWriter, r *http.Request) {
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
	if err := campaign.Create(r.Context(), s.db, c); err != nil {
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
}

func (s *server) handleCampaignsGet(w http.ResponseWriter, r *http.Request) {
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
	c, err := campaign.Get(r.Context(), s.db, workspace, campaignID)
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
}

func (s *server) handleEventsPost(w http.ResponseWriter, r *http.Request) {
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
	// behavior. INSERT IGNORE is a no-op on PK collision so
	// existing rows are never clobbered. (BM-29.)
	if err := person.EnsureExists(r.Context(), s.db, workspace, req.PersonID); err != nil {
		slog.ErrorContext(r.Context(), "person ensure failed",
			slog.String("workspace_id", workspace),
			slog.String("person_id", req.PersonID),
			slog.Any("error", err))
		writeErr(w, http.StatusInternalServerError, "store failed")
		return
	}
	if err := eventstore.Insert(r.Context(), s.db, ev); err != nil {
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
	s.dispatcher.Submit(r.Context(), ev)

	slog.InfoContext(r.Context(), "event accepted",
		slog.String("workspace_id", workspace),
		slog.String("event_id", ev.EventID),
		slog.String("person_id", ev.PersonID),
		slog.String("event_name", ev.Name))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(intakeResponse{EventID: ev.EventID})
}

// ----- helpers --------------------------------------------------------

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

// readBody reads up to max+1 bytes; rejects if over max.
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
