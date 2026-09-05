package campaign

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/steve-weiland/behavioral-messaging/internal/event"
	"github.com/steve-weiland/behavioral-messaging/internal/person"
	"github.com/steve-weiland/behavioral-messaging/internal/segment"
)

func TestParseTemplate_Reject(t *testing.T) {
	cases := []string{
		`{{.Person.PersonID`,         // unterminated
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

// The V2-2c caches removed the per-event MySQL reads but left a JSON decode
// and a text/template parse on every event. Compile() hoists both to once per
// cache refresh; these tests pin that the compiled artifacts are actually used
// and that an uncompiled campaign still behaves identically (the
// CAMPAIGN_HOT_PATH=mysql baseline and the corrupt-definition fallback).
func TestCampaignCompile_UsedByRenderAndTrigger(t *testing.T) {
	c := &Campaign{
		WorkspaceID: "ws",
		CampaignID:  "welcome_pro",
		Trigger:     json.RawMessage(`{"op":"event_seen","name":"signed_up"}`),
		Template:    `hi {{.Person.PersonID}}`,
	}
	if err := c.Compile(); err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if c.cond == nil || c.tpl == nil {
		t.Fatalf("Compile left artifacts nil: cond=%v tpl=%v", c.cond, c.tpl)
	}

	p := &person.Person{WorkspaceID: "ws", PersonID: "p_alice"}
	ev := event.Event{WorkspaceID: "ws", PersonID: "p_alice", Name: "signed_up"}

	// Render must go through the cached template.
	out, err := renderCampaign(c, p, ev)
	if err != nil {
		t.Fatalf("renderCampaign: %v", err)
	}
	if out != "hi p_alice" {
		t.Errorf("rendered = %q, want %q", out, "hi p_alice")
	}

	// A compiled trigger evaluates the same as a decoded one.
	proc := &Processor{}
	if !proc.triggerMatches(context.Background(), c, p, ev) {
		t.Error("compiled trigger did not match signed_up")
	}
	ev.Name = "other"
	if proc.triggerMatches(context.Background(), c, p, ev) {
		t.Error("compiled trigger matched the wrong event name")
	}
}

func TestCampaignUncompiled_FallsBackToInlineDecode(t *testing.T) {
	c := &Campaign{
		CampaignID: "c1",
		Trigger:    json.RawMessage(`{"op":"event_seen","name":"signed_up"}`),
		Template:   `hi {{.Person.PersonID}}`,
	}
	// Deliberately NOT compiled.
	p := &person.Person{WorkspaceID: "ws", PersonID: "p_bob"}
	ev := event.Event{WorkspaceID: "ws", PersonID: "p_bob", Name: "signed_up"}

	out, err := renderCampaign(c, p, ev)
	if err != nil {
		t.Fatalf("renderCampaign (uncompiled): %v", err)
	}
	if out != "hi p_bob" {
		t.Errorf("rendered = %q, want %q", out, "hi p_bob")
	}
	proc := &Processor{}
	if !proc.triggerMatches(context.Background(), c, p, ev) {
		t.Error("uncompiled trigger did not match — inline decode fallback is broken")
	}
}

// Compile must fail on exactly what the inline per-event path fails on and no
// more: it's an optimization of that path, so if it rejected something the
// inline decode accepts, a campaign would match under CAMPAIGN_HOT_PATH=mysql
// and not under =cache. (Op-grammar validation is segment.Validate's job at
// POST time — DecodeCondition deliberately doesn't do it, and an unknown op
// evaluates false either way.)
func TestCampaignCompile_RejectsBadDefinitions(t *testing.T) {
	cases := map[string]Campaign{
		"bad trigger json": {Trigger: json.RawMessage(`{`), Template: `x`},
		"unparseable tmpl": {Trigger: json.RawMessage(`{"op":"event_seen","name":"a"}`), Template: `{{.Person`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if err := c.Compile(); err == nil {
				t.Error("Compile accepted a bad definition")
			}
			if c.cond != nil || c.tpl != nil {
				t.Error("failed Compile must leave artifacts nil so the inline path takes over")
			}
		})
	}
}
