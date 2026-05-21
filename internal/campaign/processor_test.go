package campaign

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/steveweiland/behavioral-messaging/internal/event"
	"github.com/steveweiland/behavioral-messaging/internal/person"
	"github.com/steveweiland/behavioral-messaging/internal/segment"
)

func TestParseTemplate_Reject(t *testing.T) {
	cases := []string{
		`{{.Person.PersonID`,        // unterminated
		`{{.Person.NoSuchMethod()}}`, // invalid call shape
	}
	for _, s := range cases {
		if err := ParseTemplate("test", s); err == nil {
			t.Errorf("expected parse error for %q", s)
		}
	}
}

func TestRenderTemplate(t *testing.T) {
	p := &person.Person{
		WorkspaceID: "ws_alpha",
		PersonID:    "p_alice",
		Attributes:  json.RawMessage(`{"plan":"pro","city":"Sydney"}`),
	}
	ev := event.Event{
		WorkspaceID: "ws_alpha",
		EventID:     "ev_1",
		PersonID:    "p_alice",
		Name:        "signed_up",
	}
	out, err := renderTemplate(
		"hello {{.Person.PersonID}} on {{.Attrs.plan}} — event was {{.Event.Name}}",
		p, ev,
	)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	want := "hello p_alice on pro — event was signed_up"
	if out != want {
		t.Errorf("got %q want %q", out, want)
	}
}

func TestRenderTemplate_MissingAttrRendersZero(t *testing.T) {
	// missingkey=zero option means absent attribute renders as "<no value>".
	p := &person.Person{Attributes: json.RawMessage(`{}`)}
	out, err := renderTemplate(`tier:{{.Attrs.plan}}`, p, event.Event{})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(out, "tier:") {
		t.Errorf("missing-attr render unexpected: %q", out)
	}
}

// Trigger evaluation — same Condition tree as segments, but with a
// single-event EvalContext. event_seen against this matches iff the
// inbound event is the target.
func TestTriggerEval_SingleEvent(t *testing.T) {
	trigger := segment.Condition{
		Op: "and",
		Conditions: []segment.Condition{
			{Op: "attr_eq", Key: "plan", Value: json.RawMessage(`"pro"`)},
			{Op: "event_seen", Name: "signed_up"},
		},
	}
	p := &person.Person{Attributes: json.RawMessage(`{"plan":"pro"}`)}

	// Match: plan=pro AND inbound event is signed_up
	ctx := &segment.EvalContext{
		Person: p,
		Events: []event.Event{{Name: "signed_up"}},
	}
	if !trigger.Evaluate(ctx) {
		t.Error("expected match (plan=pro AND inbound=signed_up)")
	}

	// Miss: inbound is wrong event
	ctx.Events = []event.Event{{Name: "viewed_pricing"}}
	if trigger.Evaluate(ctx) {
		t.Error("expected miss (wrong inbound event)")
	}

	// Miss: plan attribute mismatch
	p.Attributes = json.RawMessage(`{"plan":"free"}`)
	ctx.Events = []event.Event{{Name: "signed_up"}}
	if trigger.Evaluate(ctx) {
		t.Error("expected miss (wrong plan)")
	}
}
