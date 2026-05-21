package segment

import (
	"encoding/json"
	"testing"

	"github.com/steveweiland/behavioral-messaging/internal/event"
	"github.com/steveweiland/behavioral-messaging/internal/person"
)

func cond(b string) Condition {
	var c Condition
	if err := json.Unmarshal([]byte(b), &c); err != nil {
		panic(err)
	}
	return c
}

func ctxWith(attrs string, eventNames ...string) *EvalContext {
	events := make([]event.Event, len(eventNames))
	for i, n := range eventNames {
		events[i] = event.Event{Name: n}
	}
	return &EvalContext{
		Person: &person.Person{Attributes: json.RawMessage(attrs)},
		Events: events,
	}
}

// ----- Validate -------------------------------------------------------

func TestValidate_OK(t *testing.T) {
	cases := []string{
		`{"op":"attr_eq","key":"plan","value":"pro"}`,
		`{"op":"attr_eq","key":"count","value":42}`,
		`{"op":"attr_eq","key":"active","value":true}`,
		`{"op":"attr_exists","key":"email"}`,
		`{"op":"event_seen","name":"viewed_pricing"}`,
		`{"op":"and","conditions":[
			{"op":"attr_eq","key":"plan","value":"pro"},
			{"op":"event_seen","name":"viewed_pricing"}]}`,
		`{"op":"or","conditions":[
			{"op":"attr_eq","key":"plan","value":"pro"},
			{"op":"attr_eq","key":"plan","value":"trial"}]}`,
		`{"op":"not","conditions":[{"op":"attr_eq","key":"plan","value":"free"}]}`,
	}
	for i, s := range cases {
		if err := cond(s).Validate(); err != nil {
			t.Errorf("case %d unexpectedly invalid: %v\n  json: %s", i, err, s)
		}
	}
}

func TestValidate_Reject(t *testing.T) {
	cases := []struct {
		json string
		why  string
	}{
		{`{"op":"unknown"}`, "unknown op"},
		{`{"op":"attr_eq","key":"plan"}`, "attr_eq missing value"},
		{`{"op":"attr_eq","value":"pro"}`, "attr_eq missing key"},
		{`{"op":"attr_exists"}`, "attr_exists missing key"},
		{`{"op":"event_seen"}`, "event_seen missing name"},
		{`{"op":"and","conditions":[]}`, "and with zero conditions"},
		{`{"op":"not","conditions":[
			{"op":"attr_exists","key":"a"},
			{"op":"attr_exists","key":"b"}]}`, "not with two conditions"},
	}
	for _, c := range cases {
		if err := cond(c.json).Validate(); err == nil {
			t.Errorf("expected reject (%s) but got nil error\n  json: %s", c.why, c.json)
		}
	}
}

func TestValidate_DepthCap(t *testing.T) {
	// Build a tree of depth 9 — should reject (cap is 8).
	nested := Condition{Op: "attr_exists", Key: "leaf"}
	for i := 0; i < 9; i++ {
		nested = Condition{Op: "and", Conditions: []Condition{nested}}
	}
	if err := nested.Validate(); err == nil {
		t.Error("expected depth-cap rejection, got nil")
	}
}

// ----- Evaluate -------------------------------------------------------

func TestEval_AttrEq_String(t *testing.T) {
	c := cond(`{"op":"attr_eq","key":"plan","value":"pro"}`)
	if !c.Evaluate(ctxWith(`{"plan":"pro"}`)) {
		t.Error("expected true")
	}
	if c.Evaluate(ctxWith(`{"plan":"free"}`)) {
		t.Error("expected false (mismatch)")
	}
	if c.Evaluate(ctxWith(`{}`)) {
		t.Error("expected false (missing key)")
	}
}

func TestEval_AttrEq_Number(t *testing.T) {
	c := cond(`{"op":"attr_eq","key":"signup_year","value":2026}`)
	if !c.Evaluate(ctxWith(`{"signup_year":2026}`)) {
		t.Error("expected true on int 2026")
	}
	if c.Evaluate(ctxWith(`{"signup_year":2025}`)) {
		t.Error("expected false")
	}
}

func TestEval_AttrEq_Bool(t *testing.T) {
	c := cond(`{"op":"attr_eq","key":"active","value":true}`)
	if !c.Evaluate(ctxWith(`{"active":true}`)) {
		t.Error("expected true")
	}
	if c.Evaluate(ctxWith(`{"active":false}`)) {
		t.Error("expected false")
	}
}

func TestEval_AttrExists(t *testing.T) {
	c := cond(`{"op":"attr_exists","key":"email"}`)
	if !c.Evaluate(ctxWith(`{"email":"a@b.com"}`)) {
		t.Error("expected true (present string)")
	}
	if !c.Evaluate(ctxWith(`{"email":null}`)) {
		t.Error("expected true (present null is still present)")
	}
	if c.Evaluate(ctxWith(`{}`)) {
		t.Error("expected false (absent)")
	}
}

func TestEval_EventSeen(t *testing.T) {
	c := cond(`{"op":"event_seen","name":"viewed_pricing"}`)
	if c.Evaluate(ctxWith(`{}`)) {
		t.Error("expected false (no events)")
	}
	if c.Evaluate(ctxWith(`{}`, "signed_up")) {
		t.Error("expected false (different event)")
	}
	if !c.Evaluate(ctxWith(`{}`, "signed_up", "viewed_pricing")) {
		t.Error("expected true (event present)")
	}
}

func TestEval_AndOrNot(t *testing.T) {
	c := cond(`{"op":"and","conditions":[
		{"op":"attr_eq","key":"plan","value":"pro"},
		{"op":"event_seen","name":"viewed_pricing"}]}`)
	if !c.Evaluate(ctxWith(`{"plan":"pro"}`, "viewed_pricing")) {
		t.Error("AND: expected true (both)")
	}
	if c.Evaluate(ctxWith(`{"plan":"pro"}`)) {
		t.Error("AND: expected false (no event)")
	}
	if c.Evaluate(ctxWith(`{"plan":"free"}`, "viewed_pricing")) {
		t.Error("AND: expected false (wrong plan)")
	}

	o := cond(`{"op":"or","conditions":[
		{"op":"attr_eq","key":"plan","value":"pro"},
		{"op":"attr_eq","key":"plan","value":"trial"}]}`)
	if !o.Evaluate(ctxWith(`{"plan":"trial"}`)) {
		t.Error("OR: expected true on trial branch")
	}
	if o.Evaluate(ctxWith(`{"plan":"free"}`)) {
		t.Error("OR: expected false")
	}

	n := cond(`{"op":"not","conditions":[{"op":"attr_eq","key":"plan","value":"free"}]}`)
	if !n.Evaluate(ctxWith(`{"plan":"pro"}`)) {
		t.Error("NOT: expected true")
	}
	if n.Evaluate(ctxWith(`{"plan":"free"}`)) {
		t.Error("NOT: expected false")
	}
}
