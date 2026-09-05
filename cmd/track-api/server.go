package main

import (
	"bytes"
	"context"
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

	"github.com/steve-weiland/behavioral-messaging/internal/campaign"
	"github.com/steve-weiland/behavioral-messaging/internal/event"
	"github.com/steve-weiland/behavioral-messaging/internal/eventstore"
	"github.com/steve-weiland/behavioral-messaging/internal/journey"
	"github.com/steve-weiland/behavioral-messaging/internal/peoplefeed"
	"github.com/steve-weiland/behavioral-messaging/internal/person"
	"github.com/steve-weiland/behavioral-messaging/internal/segment"
)

// Body-size caps. Worked out per surface; documented as BM-26 (people),
// BM-30 (segments), BM-70/72 (campaigns). BM-20 (events) cap is the same
// 32 KiB as campaigns — the payload is an arbitrary JSON object and a
// few KiB of structured business data is reasonable.
const (
	maxAttributesBytes    = 16 * 1024
	maxSegmentDefBytes    = 16 * 1024
	maxEventBodyBytes     = 32 * 1024
	segmentScanEventLimit = 1000
	maxCampaignBodyBytes  = 32 * 1024
	maxTemplateBytes      = 16 * 1024
	// A journey holds up to MaxSteps steps, each of which may carry a
	// template, so its body cap is larger than a single campaign's.
	maxJourneyBodyBytes = 128 * 1024
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

type defineJourneyRequest struct {
	JourneyID string          `json:"journey_id"`
	Name      string          `json:"name"`
	Trigger   json.RawMessage `json:"trigger"`
	Steps     []journey.Step  `json:"steps"`
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
//
// V2-1b: dispatcher field replaced with publisher — the /events handler
// hands accepted events to the Publisher, which puts them on RabbitMQ.
// The actual fan-out work (stub-receiver POST + audit insert) lives in
// cmd/campaign-worker.
type server struct {
	db        *sql.DB
	publisher *campaign.Publisher

	// V2-2: set by main(). When peopleFeed is non-nil, POST /people
	// publishes the merged attributes so segment-worker can maintain its
	// bitmaps. When segmentIndexURL is non-empty, /check delegates
	// membership to segment-worker (bitmaps) and falls back to the V1
	// scan only if the index is unavailable. Both zero-valued in unit
	// tests, so the scan path stays exercised with no broker/worker.
	peopleFeed      *peoplefeed.Publisher
	segmentIndexURL string
	indexClient     *http.Client
}

func newServer(db *sql.DB, pub *campaign.Publisher) *server {
	return &server{db: db, publisher: pub}
}

func (s *server) routes(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/people", s.handlePeoplePost)
	mux.HandleFunc("/people/", s.handlePeopleGet)
	mux.HandleFunc("/segments", s.handleSegmentsPost)
	mux.HandleFunc("/segments/", s.handleSegmentsGet)
	mux.HandleFunc("/campaigns", s.handleCampaignsPost)
	mux.HandleFunc("/campaigns/", s.handleCampaignsGet)
	mux.HandleFunc("/journeys", s.handleJourneysPost)
	mux.HandleFunc("/journeys/", s.handleJourneyByID)
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
	if !validShortID(workspace) {
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
	if !validLongID(req.PersonID) {
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

	// V2-2: stream the person's FULL merged attributes to segment-worker
	// so it can maintain the bitmap index. Best-effort: the index is
	// eventually consistent, so a publish failure logs and does not fail
	// the request. Re-reads the row to get the post-merge-patch state
	// (Upsert returns nothing).
	s.publishPersonChange(r.Context(), workspace, req.PersonID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// publishPersonChange re-reads the merged person row and publishes it to
// the people.changes feed. No-op when the feed is not wired (unit tests).
func (s *server) publishPersonChange(ctx context.Context, workspace, personID string) {
	if s.peopleFeed == nil {
		return
	}
	p, err := person.Get(ctx, s.db, workspace, personID)
	if err != nil {
		slog.WarnContext(ctx, "person change feed: re-read failed, skipping publish",
			slog.String("workspace_id", workspace),
			slog.String("person_id", personID),
			slog.Any("error", err))
		return
	}
	if err := s.peopleFeed.Publish(ctx, peoplefeed.Change{
		WorkspaceID: workspace,
		PersonID:    personID,
		Attributes:  p.Attributes,
	}); err != nil {
		slog.WarnContext(ctx, "person change feed: publish failed",
			slog.String("workspace_id", workspace),
			slog.String("person_id", personID),
			slog.Any("error", err))
	}
}

// V1 PR 2: GET /people/{person_id} — read one person back. 404 if absent.
func (s *server) handlePeopleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	workspace := r.Header.Get("X-Workspace-ID")
	if !validShortID(workspace) {
		writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
		return
	}
	personID := strings.TrimPrefix(r.URL.Path, "/people/")
	if !validLongID(personID) {
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
	if !validShortID(workspace) {
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
	if !validShortID(req.SegmentID) {
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

// indexCheckRequest/Response is the membership contract with
// segment-worker's POST /internal/check. track-api supplies the
// definition it already loaded (so the worker needs no segment store);
// the worker answers from its bitmaps.
type indexCheckRequest struct {
	WorkspaceID string          `json:"workspace_id"`
	PersonID    string          `json:"person_id"`
	Definition  json.RawMessage `json:"definition"`
}

type indexCheckResponse struct {
	Member bool `json:"member"`
}

// checkViaIndex asks segment-worker whether personID is a member of the
// segment defined by def. Returns ok=false on any error (transport,
// non-200, decode) so the caller can fall back to the scan path.
func (s *server) checkViaIndex(ctx context.Context, workspace, personID string, def json.RawMessage) (member bool, ok bool) {
	body, err := json.Marshal(indexCheckRequest{
		WorkspaceID: workspace, PersonID: personID, Definition: def,
	})
	if err != nil {
		return false, false
	}
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		s.segmentIndexURL+"/internal/check", bytes.NewReader(body))
	if err != nil {
		return false, false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.indexClient.Do(req)
	if err != nil {
		return false, false
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return false, false
	}
	var out indexCheckResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, false
	}
	return out.Member, true
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
	if !validShortID(workspace) {
		writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
		return
	}
	suffix := strings.TrimPrefix(r.URL.Path, "/segments/")
	segmentID, remainder, _ := strings.Cut(suffix, "/")
	if !validShortID(segmentID) {
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
		if !validLongID(personID) {
			writeErr(w, http.StatusBadRequest, "person_id query parameter required")
			return
		}
		// V2-2: delegate membership to segment-worker's bitmap index.
		// rec already proves the segment exists (404 handled above), so
		// the index call is pure membership — no per-person event scan.
		if s.segmentIndexURL != "" {
			if member, ok := s.checkViaIndex(r.Context(), workspace, personID, rec.Definition); ok {
				slog.InfoContext(r.Context(), "segment check",
					slog.String("workspace_id", workspace),
					slog.String("segment_id", segmentID),
					slog.String("person_id", personID),
					slog.Bool("member", member),
					slog.String("backend", "bitmap"))
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(segmentCheckResponse{
					WorkspaceID: workspace, SegmentID: segmentID, PersonID: personID, Member: member,
				})
				return
			}
			slog.WarnContext(r.Context(), "segment index unavailable, falling back to scan",
				slog.String("segment_id", segmentID),
				slog.String("person_id", personID))
		}
		p, err := person.Get(r.Context(), s.db, workspace, personID)
		if errors.Is(err, person.ErrNotFound) {
			// A person that doesn't exist is, by definition, not a member.
			w.Header().Set("Content-Type", "application/json")
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
			slog.String("backend", "scan"),
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
	if !validShortID(workspace) {
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
	if !validShortID(req.CampaignID) {
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
	if !validShortID(workspace) {
		writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
		return
	}
	campaignID := strings.TrimPrefix(r.URL.Path, "/campaigns/")
	if !validShortID(campaignID) {
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
	if !validShortID(workspace) {
		writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
		return
	}
	body, err := readBody(r, maxEventBodyBytes)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	var req intakeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if !validLongID(req.PersonID) {
		writeErr(w, http.StatusBadRequest, "person_id required ([A-Za-z0-9_-]{1,128})")
		return
	}
	if !validLongID(req.EventName) {
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
	// attributes if absent — the conventional identify-on-track
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

	// V2-1b: hand the event to the RabbitMQ publisher. Submit detaches
	// from the request context (so the publish goroutine outlives the
	// handler), preserves the producer's span context (so the amqp.publish
	// span + downstream worker spans join this trace), and never blocks.
	// Buffer-full and shutdown drops are counted on
	// campaign_publish_dropped_total{workspace_id, reason}.
	s.publisher.Submit(r.Context(), ev)

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

// validShortID — IDs backed by VARCHAR(64) columns: workspace_id,
// segment_id, campaign_id. Spec error messages quote ([A-Za-z0-9_-]{1,64}).
func validShortID(s string) bool { return validIDWithMax(s, 64) }

// validLongID — IDs backed by VARCHAR(128) columns: person_id, event_name.
// Spec error messages quote ([A-Za-z0-9_-]{1,128}).
func validLongID(s string) bool { return validIDWithMax(s, 128) }

func validIDWithMax(s string, max int) bool {
	if s == "" || len(s) > max {
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

// ----- V3-1a journeys -------------------------------------------------

// handleJourneysPost defines a journey (BM-110). Validation is strict and at
// insert time: a definition that reaches the scheduler is one the scheduler
// can execute, so a bad step shape fails here rather than three days into a
// delay.
func (s *server) handleJourneysPost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	workspace := r.Header.Get("X-Workspace-ID")
	if !validShortID(workspace) {
		writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
		return
	}
	body, err := readBody(r, maxJourneyBodyBytes)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	var req defineJourneyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if !validShortID(req.JourneyID) {
		writeErr(w, http.StatusBadRequest, "journey_id required ([A-Za-z0-9_-]{1,64})")
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
	cond, err := segment.DecodeCondition(req.Trigger)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "trigger: "+err.Error())
		return
	}
	if err := cond.Validate(); err != nil {
		writeErr(w, http.StatusBadRequest, "trigger: "+err.Error())
		return
	}
	if err := journey.ValidateSteps(req.Steps); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	j := journey.Journey{
		WorkspaceID: workspace,
		JourneyID:   req.JourneyID,
		Name:        req.Name,
		Trigger:     req.Trigger,
		Steps:       req.Steps,
	}
	if err := journey.Create(r.Context(), s.db, j); errors.Is(err, journey.ErrConflict) {
		writeErr(w, http.StatusConflict, "journey_id already exists")
		return
	} else if err != nil {
		slog.ErrorContext(r.Context(), "journey create failed",
			slog.String("workspace_id", workspace),
			slog.String("journey_id", req.JourneyID),
			slog.Any("error", err))
		writeErr(w, http.StatusInternalServerError, "store failed")
		return
	}
	slog.InfoContext(r.Context(), "journey created",
		slog.String("workspace_id", workspace),
		slog.String("journey_id", req.JourneyID),
		slog.String("name", req.Name),
		slog.Int("steps", len(req.Steps)))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(`{"status":"ok","version":1}`))
}

// handleJourneysGet serves both
//
//	GET /journeys/{journey_id}
//	GET /journeys/{journey_id}/runs/{person_id}
//
// The run read is what makes the V3-1a milestone checkable without opening a
// MySQL shell: it shows the step index and status the scheduler will act on.
// handleJourneyByID routes GET (definition / run) and PUT (new version).
func (s *server) handleJourneyByID(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleJourneysGet(w, r)
	case http.MethodPut:
		s.handleJourneysPut(w, r)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleJourneysPut publishes a new version of a journey (V3-1c / BM-114).
// In-flight runs keep executing the version they pinned — the whole reason
// versions exist rather than editing in place.
func (s *server) handleJourneysPut(w http.ResponseWriter, r *http.Request) {
	workspace := r.Header.Get("X-Workspace-ID")
	if !validShortID(workspace) {
		writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
		return
	}
	journeyID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/journeys/"), "/")
	if !validShortID(journeyID) {
		writeErr(w, http.StatusBadRequest, "journey_id required in path")
		return
	}
	body, err := readBody(r, maxJourneyBodyBytes)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	var req defineJourneyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
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
	cond, err := segment.DecodeCondition(req.Trigger)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "trigger: "+err.Error())
		return
	}
	if err := cond.Validate(); err != nil {
		writeErr(w, http.StatusBadRequest, "trigger: "+err.Error())
		return
	}
	if err := journey.ValidateSteps(req.Steps); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	version, err := journey.Update(r.Context(), s.db, journey.Journey{
		WorkspaceID: workspace, JourneyID: journeyID,
		Name: req.Name, Trigger: req.Trigger, Steps: req.Steps,
	})
	if errors.Is(err, journey.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		slog.ErrorContext(r.Context(), "journey update failed",
			slog.String("journey_id", journeyID), slog.Any("error", err))
		writeErr(w, http.StatusInternalServerError, "store failed")
		return
	}
	slog.InfoContext(r.Context(), "journey version published",
		slog.String("workspace_id", workspace),
		slog.String("journey_id", journeyID),
		slog.Uint64("version", uint64(version)))
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"ok","version":%d}`, version)))
}

func (s *server) handleJourneysGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	workspace := r.Header.Get("X-Workspace-ID")
	if !validShortID(workspace) {
		writeErr(w, http.StatusBadRequest, "X-Workspace-ID required ([A-Za-z0-9_-]{1,64})")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/journeys/")
	parts := strings.Split(rest, "/")
	journeyID := parts[0]
	if !validShortID(journeyID) {
		writeErr(w, http.StatusBadRequest, "journey_id required in path")
		return
	}

	// /journeys/{id}/runs/{person_id}
	if len(parts) == 3 && parts[1] == "runs" {
		personID := parts[2]
		if personID == "" || len(personID) > 128 {
			writeErr(w, http.StatusBadRequest, "person_id required in path")
			return
		}
		run, err := journey.GetRunByPerson(r.Context(), s.db, workspace, journeyID, personID)
		if errors.Is(err, journey.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "no run for this person")
			return
		}
		if err != nil {
			slog.ErrorContext(r.Context(), "journey run read failed", slog.Any("error", err))
			writeErr(w, http.StatusInternalServerError, "read failed")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(run)
		return
	}
	if len(parts) != 1 {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}

	j, err := journey.Get(r.Context(), s.db, workspace, journeyID)
	if errors.Is(err, journey.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		slog.ErrorContext(r.Context(), "journey read failed", slog.Any("error", err))
		writeErr(w, http.StatusInternalServerError, "read failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(j)
}
