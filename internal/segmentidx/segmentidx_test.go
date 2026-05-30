package segmentidx

import (
	"encoding/json"
	"testing"

	"github.com/steveweiland/behavioral-messaging/internal/segment"
)

func cond(t *testing.T, jsonStr string) segment.Condition {
	t.Helper()
	c, err := segment.DecodeCondition(json.RawMessage(jsonStr))
	if err != nil {
		t.Fatalf("decode condition %q: %v", jsonStr, err)
	}
	return c
}

func attrs(pairs map[string]string) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(pairs))
	for k, v := range pairs {
		out[k] = json.RawMessage(v)
	}
	return out
}

func TestEventSeen(t *testing.T) {
	idx := New()
	c := cond(t, `{"op":"event_seen","name":"signed_up"}`)

	if idx.Member("ws", c, "p1") {
		t.Fatal("unknown person must not be a member")
	}
	idx.ObserveEvent("ws", "p1", "viewed_pricing")
	if idx.Member("ws", c, "p1") {
		t.Fatal("p1 fired a different event; should not match signed_up")
	}
	idx.ObserveEvent("ws", "p1", "signed_up")
	if !idx.Member("ws", c, "p1") {
		t.Fatal("p1 fired signed_up; should match")
	}
	// Re-observing is idempotent and stays a member.
	idx.ObserveEvent("ws", "p1", "signed_up")
	if !idx.Member("ws", c, "p1") {
		t.Fatal("re-observe must not drop membership")
	}
}

func TestAttrEqScalars(t *testing.T) {
	idx := New()
	idx.SetAttributes("ws", "p1", attrs(map[string]string{
		"plan":   `"pro"`,
		"count":  `42`,
		"active": `true`,
		"note":   `null`,
	}))

	cases := []struct {
		name string
		def  string
		want bool
	}{
		{"string match", `{"op":"attr_eq","key":"plan","value":"pro"}`, true},
		{"string miss", `{"op":"attr_eq","key":"plan","value":"trial"}`, false},
		{"number match", `{"op":"attr_eq","key":"count","value":42}`, true},
		{"number formatting match", `{"op":"attr_eq","key":"count","value":42.0}`, true},
		{"bool match", `{"op":"attr_eq","key":"active","value":true}`, true},
		{"explicit null match", `{"op":"attr_eq","key":"note","value":null}`, true},
		{"missing key", `{"op":"attr_eq","key":"nope","value":"x"}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := idx.Member("ws", cond(t, tc.def), "p1"); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestAttrExists(t *testing.T) {
	idx := New()
	idx.SetAttributes("ws", "p1", attrs(map[string]string{"city": `"Sydney"`}))
	if !idx.Member("ws", cond(t, `{"op":"attr_exists","key":"city"}`), "p1") {
		t.Fatal("city exists; should match")
	}
	if idx.Member("ws", cond(t, `{"op":"attr_exists","key":"plan"}`), "p1") {
		t.Fatal("plan absent; should not match")
	}
}

func TestAttrUpdateMovesBit(t *testing.T) {
	idx := New()
	pro := cond(t, `{"op":"attr_eq","key":"plan","value":"pro"}`)
	trial := cond(t, `{"op":"attr_eq","key":"plan","value":"trial"}`)

	idx.SetAttributes("ws", "p1", attrs(map[string]string{"plan": `"trial"`}))
	if !idx.Member("ws", trial, "p1") || idx.Member("ws", pro, "p1") {
		t.Fatal("initial: should be trial, not pro")
	}
	// Upgrade trial -> pro. The old bit must clear so the person is not in
	// both bitmaps.
	idx.SetAttributes("ws", "p1", attrs(map[string]string{"plan": `"pro"`}))
	if idx.Member("ws", trial, "p1") {
		t.Fatal("after upgrade p1 must no longer match trial")
	}
	if !idx.Member("ws", pro, "p1") {
		t.Fatal("after upgrade p1 must match pro")
	}
}

func TestAttrDeleteClears(t *testing.T) {
	idx := New()
	idx.SetAttributes("ws", "p1", attrs(map[string]string{"plan": `"pro"`, "city": `"Sydney"`}))
	// Re-upsert with city gone (merge-patch null-delete already applied
	// upstream => key simply absent from the full set).
	idx.SetAttributes("ws", "p1", attrs(map[string]string{"plan": `"pro"`}))
	if idx.Member("ws", cond(t, `{"op":"attr_exists","key":"city"}`), "p1") {
		t.Fatal("city was removed; attr_exists must be false")
	}
	if !idx.Member("ws", cond(t, `{"op":"attr_eq","key":"plan","value":"pro"}`), "p1") {
		t.Fatal("plan should be untouched by the city delete")
	}
}

func TestBooleanComposition(t *testing.T) {
	idx := New()
	// active_pro = plan=pro AND viewed_pricing
	def := `{"op":"and","conditions":[
		{"op":"attr_eq","key":"plan","value":"pro"},
		{"op":"event_seen","name":"viewed_pricing"}]}`
	c := cond(t, def)

	idx.SetAttributes("ws", "alice", attrs(map[string]string{"plan": `"pro"`}))
	idx.SetAttributes("ws", "bob", attrs(map[string]string{"plan": `"trial"`}))

	// alice is pro but hasn't viewed pricing yet.
	if idx.Member("ws", c, "alice") {
		t.Fatal("alice has not viewed pricing; AND must be false")
	}
	idx.ObserveEvent("ws", "alice", "viewed_pricing")
	if !idx.Member("ws", c, "alice") {
		t.Fatal("alice is pro AND viewed pricing; should match")
	}
	// bob views pricing but is only trial.
	idx.ObserveEvent("ws", "bob", "viewed_pricing")
	if idx.Member("ws", c, "bob") {
		t.Fatal("bob is trial; AND must be false")
	}

	// not(plan=pro): bob matches, alice does not.
	notPro := cond(t, `{"op":"not","conditions":[{"op":"attr_eq","key":"plan","value":"pro"}]}`)
	if !idx.Member("ws", notPro, "bob") {
		t.Fatal("bob is not pro; NOT should be true")
	}
	if idx.Member("ws", notPro, "alice") {
		t.Fatal("alice is pro; NOT should be false")
	}

	// or
	orDef := cond(t, `{"op":"or","conditions":[
		{"op":"attr_eq","key":"plan","value":"pro"},
		{"op":"attr_eq","key":"plan","value":"trial"}]}`)
	if !idx.Member("ws", orDef, "alice") || !idx.Member("ws", orDef, "bob") {
		t.Fatal("OR over pro/trial should match both")
	}
}

func TestWorkspaceIsolation(t *testing.T) {
	idx := New()
	c := cond(t, `{"op":"event_seen","name":"signed_up"}`)
	idx.ObserveEvent("ws_a", "p1", "signed_up")
	if !idx.Member("ws_a", c, "p1") {
		t.Fatal("p1 in ws_a should match")
	}
	// Same person_id string in a different workspace is a different person.
	if idx.Member("ws_b", c, "p1") {
		t.Fatal("ws_b must not see ws_a's events")
	}
}

func TestNotMemberUnknownPerson(t *testing.T) {
	idx := New()
	// not(attr_exists plan) would be true under empty-world evaluation,
	// but an unknown person must be a non-member per the /check contract.
	notExists := cond(t, `{"op":"not","conditions":[{"op":"attr_exists","key":"plan"}]}`)
	if idx.Member("ws", notExists, "ghost") {
		t.Fatal("unknown person must be a non-member regardless of NOT")
	}
}
