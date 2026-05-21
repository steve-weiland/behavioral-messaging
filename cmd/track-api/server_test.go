package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Rejection-path coverage for the track-api handlers. Every case here
// returns *before* any DB access, so a server built with nil deps is
// sufficient — no MySQL container, no sqlmock. Happy-path coverage that
// would hit storage lives in the per-package tests (internal/person,
// internal/segment, internal/campaign) and the seed-* Make targets.

func newTestServer() *server {
	return &server{db: nil, publisher: nil}
}

type rejectionCase struct {
	name       string
	method     string
	target     string
	workspace  string
	body       string
	wantStatus int
	wantSubstr string
}

func runRejections(t *testing.T, h http.HandlerFunc, cases []rejectionCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
			if tc.workspace != "" {
				req.Header.Set("X-Workspace-ID", tc.workspace)
			}
			rr := httptest.NewRecorder()
			h(rr, req)
			if rr.Code != tc.wantStatus {
				t.Fatalf("status: got %d want %d (body: %s)", rr.Code, tc.wantStatus, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), tc.wantSubstr) {
				t.Fatalf("body should contain %q, got: %s", tc.wantSubstr, rr.Body.String())
			}
		})
	}
}

func TestHealthz(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	s.handleHealthz(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"status":"ok"`) {
		t.Errorf("unexpected body: %s", rr.Body.String())
	}
}

func TestHandlePeoplePost_Rejections(t *testing.T) {
	oversize := strings.Repeat("a", maxAttributesBytes+1)
	cases := []rejectionCase{
		{"method GET", http.MethodGet, "/people", "ws_alpha", `{}`, http.StatusMethodNotAllowed, "method not allowed"},
		{"missing workspace", http.MethodPost, "/people", "", `{}`, http.StatusBadRequest, "X-Workspace-ID"},
		{"invalid workspace chars", http.MethodPost, "/people", "bad ws!", `{}`, http.StatusBadRequest, "X-Workspace-ID"},
		{"oversized body", http.MethodPost, "/people", "ws_alpha", oversize, http.StatusBadRequest, "body exceeds"},
		{"invalid JSON", http.MethodPost, "/people", "ws_alpha", `not json`, http.StatusBadRequest, "invalid JSON body"},
		{"missing person_id", http.MethodPost, "/people", "ws_alpha", `{"attributes":{}}`, http.StatusBadRequest, "person_id"},
		{"invalid person_id chars", http.MethodPost, "/people", "ws_alpha", `{"person_id":"bad person!","attributes":{}}`, http.StatusBadRequest, "person_id"},
		{"attributes is array", http.MethodPost, "/people", "ws_alpha", `{"person_id":"p_1","attributes":[1,2,3]}`, http.StatusBadRequest, "must be a JSON object"},
		{"attributes is scalar", http.MethodPost, "/people", "ws_alpha", `{"person_id":"p_1","attributes":42}`, http.StatusBadRequest, "must be a JSON object"},
	}
	s := newTestServer()
	runRejections(t, s.handlePeoplePost, cases)
}

func TestHandlePeopleGet_Rejections(t *testing.T) {
	cases := []rejectionCase{
		{"method POST", http.MethodPost, "/people/p_1", "ws_alpha", "", http.StatusMethodNotAllowed, "method not allowed"},
		{"missing workspace", http.MethodGet, "/people/p_1", "", "", http.StatusBadRequest, "X-Workspace-ID"},
		{"empty person_id", http.MethodGet, "/people/", "ws_alpha", "", http.StatusBadRequest, "person_id path segment"},
		{"invalid person_id chars", http.MethodGet, "/people/bad.person", "ws_alpha", "", http.StatusBadRequest, "person_id path segment"},
	}
	s := newTestServer()
	runRejections(t, s.handlePeopleGet, cases)
}

func TestHandleSegmentsPost_Rejections(t *testing.T) {
	oversize := strings.Repeat("a", maxSegmentDefBytes+1)
	longName := strings.Repeat("n", 256)
	cases := []rejectionCase{
		{"method GET", http.MethodGet, "/segments", "ws_alpha", `{}`, http.StatusMethodNotAllowed, "method not allowed"},
		{"missing workspace", http.MethodPost, "/segments", "", `{}`, http.StatusBadRequest, "X-Workspace-ID"},
		{"oversized body", http.MethodPost, "/segments", "ws_alpha", oversize, http.StatusBadRequest, "body exceeds"},
		{"invalid JSON", http.MethodPost, "/segments", "ws_alpha", `not json`, http.StatusBadRequest, "invalid JSON body"},
		{"missing segment_id", http.MethodPost, "/segments", "ws_alpha", `{"name":"n","definition":{"op":"attr_exists","key":"plan"}}`, http.StatusBadRequest, "segment_id"},
		{"missing name", http.MethodPost, "/segments", "ws_alpha", `{"segment_id":"s1","definition":{"op":"attr_exists","key":"plan"}}`, http.StatusBadRequest, "name required"},
		{"name too long", http.MethodPost, "/segments", "ws_alpha", `{"segment_id":"s1","name":"` + longName + `","definition":{"op":"attr_exists","key":"plan"}}`, http.StatusBadRequest, "name required"},
		{"missing definition", http.MethodPost, "/segments", "ws_alpha", `{"segment_id":"s1","name":"n"}`, http.StatusBadRequest, "definition required"},
		{"unknown op", http.MethodPost, "/segments", "ws_alpha", `{"segment_id":"s1","name":"n","definition":{"op":"nonsense"}}`, http.StatusBadRequest, "definition:"},
	}
	s := newTestServer()
	runRejections(t, s.handleSegmentsPost, cases)
}

func TestHandleSegmentsGet_Rejections(t *testing.T) {
	cases := []rejectionCase{
		{"method POST", http.MethodPost, "/segments/s1", "ws_alpha", "", http.StatusMethodNotAllowed, "method not allowed"},
		{"missing workspace", http.MethodGet, "/segments/s1", "", "", http.StatusBadRequest, "X-Workspace-ID"},
		{"empty segment_id", http.MethodGet, "/segments/", "ws_alpha", "", http.StatusBadRequest, "segment_id path segment"},
		{"invalid segment_id chars", http.MethodGet, "/segments/bad.seg", "ws_alpha", "", http.StatusBadRequest, "segment_id path segment"},
	}
	s := newTestServer()
	runRejections(t, s.handleSegmentsGet, cases)
}

func TestHandleCampaignsPost_Rejections(t *testing.T) {
	oversize := strings.Repeat("a", maxCampaignBodyBytes+1)
	bigTemplate := strings.Repeat("t", maxTemplateBytes+1)
	validTrigger := `{"op":"attr_exists","key":"plan"}`
	cases := []rejectionCase{
		{"method GET", http.MethodGet, "/campaigns", "ws_alpha", `{}`, http.StatusMethodNotAllowed, "method not allowed"},
		{"missing workspace", http.MethodPost, "/campaigns", "", `{}`, http.StatusBadRequest, "X-Workspace-ID"},
		{"oversized body", http.MethodPost, "/campaigns", "ws_alpha", oversize, http.StatusBadRequest, "body exceeds"},
		{"invalid JSON", http.MethodPost, "/campaigns", "ws_alpha", `not json`, http.StatusBadRequest, "invalid JSON body"},
		{"missing campaign_id", http.MethodPost, "/campaigns", "ws_alpha", `{"name":"n","trigger":` + validTrigger + `,"template":"hi"}`, http.StatusBadRequest, "campaign_id"},
		{"missing name", http.MethodPost, "/campaigns", "ws_alpha", `{"campaign_id":"c1","trigger":` + validTrigger + `,"template":"hi"}`, http.StatusBadRequest, "name required"},
		{"missing trigger", http.MethodPost, "/campaigns", "ws_alpha", `{"campaign_id":"c1","name":"n","template":"hi"}`, http.StatusBadRequest, "trigger required"},
		{"missing template", http.MethodPost, "/campaigns", "ws_alpha", `{"campaign_id":"c1","name":"n","trigger":` + validTrigger + `}`, http.StatusBadRequest, "template required"},
		{"oversized template", http.MethodPost, "/campaigns", "ws_alpha", `{"campaign_id":"c1","name":"n","trigger":` + validTrigger + `,"template":"` + bigTemplate + `"}`, http.StatusBadRequest, "template required"},
		{"bad trigger op", http.MethodPost, "/campaigns", "ws_alpha", `{"campaign_id":"c1","name":"n","trigger":{"op":"nonsense"},"template":"hi"}`, http.StatusBadRequest, "trigger:"},
		{"bad template syntax", http.MethodPost, "/campaigns", "ws_alpha", `{"campaign_id":"c1","name":"n","trigger":` + validTrigger + `,"template":"hi {{.Person."}`, http.StatusBadRequest, "template:"},
	}
	s := newTestServer()
	runRejections(t, s.handleCampaignsPost, cases)
}

func TestHandleCampaignsGet_Rejections(t *testing.T) {
	cases := []rejectionCase{
		{"method POST", http.MethodPost, "/campaigns/c1", "ws_alpha", "", http.StatusMethodNotAllowed, "method not allowed"},
		{"missing workspace", http.MethodGet, "/campaigns/c1", "", "", http.StatusBadRequest, "X-Workspace-ID"},
		{"empty campaign_id", http.MethodGet, "/campaigns/", "ws_alpha", "", http.StatusBadRequest, "campaign_id path segment"},
		{"invalid campaign_id chars", http.MethodGet, "/campaigns/bad.camp", "ws_alpha", "", http.StatusBadRequest, "campaign_id path segment"},
	}
	s := newTestServer()
	runRejections(t, s.handleCampaignsGet, cases)
}

func TestHandleEventsPost_Rejections(t *testing.T) {
	cases := []rejectionCase{
		{"method GET", http.MethodGet, "/events", "ws_alpha", `{}`, http.StatusMethodNotAllowed, "method not allowed"},
		{"missing workspace", http.MethodPost, "/events", "", `{}`, http.StatusBadRequest, "X-Workspace-ID"},
		{"invalid JSON", http.MethodPost, "/events", "ws_alpha", `not json`, http.StatusBadRequest, "invalid JSON body"},
		{"missing person_id", http.MethodPost, "/events", "ws_alpha", `{"event_name":"x"}`, http.StatusBadRequest, "person_id"},
		{"invalid person_id chars", http.MethodPost, "/events", "ws_alpha", `{"person_id":"bad person!","event_name":"x"}`, http.StatusBadRequest, "person_id"},
		{"missing event_name", http.MethodPost, "/events", "ws_alpha", `{"person_id":"p_1"}`, http.StatusBadRequest, "event_name"},
		{"invalid event_name chars", http.MethodPost, "/events", "ws_alpha", `{"person_id":"p_1","event_name":"bad name!"}`, http.StatusBadRequest, "event_name"},
	}
	s := newTestServer()
	runRejections(t, s.handleEventsPost, cases)
}

// routes() registers every handler. A 404 on a non-route confirms the
// mux is exhaustive — guards against accidentally removing a route in
// a future refactor.
func TestRoutes_Registered(t *testing.T) {
	s := newTestServer()
	mux := http.NewServeMux()
	s.routes(mux)
	for _, path := range []string{"/healthz", "/people", "/people/p_1", "/segments", "/segments/s_1", "/campaigns", "/campaigns/c_1", "/events"} {
		_, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, path, nil))
		if pattern == "" {
			t.Errorf("path %q has no handler registered", path)
		}
	}
}
