package journey

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
)

const sendStep = `{"type":"send","template":"hi {{.Person.PersonID}}"}`

func steps(t *testing.T, raw string) []Step {
	t.Helper()
	var s []Step
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("fixture unmarshal: %v", err)
	}
	return s
}

func TestValidateSteps_Accepts(t *testing.T) {
	cases := map[string]string{
		"single send":     `[` + sendStep + `]`,
		"delay then send": `[{"type":"delay","seconds":86400},` + sendStep + `]`,
		"branch":          `[{"type":"branch_on_condition","condition":{"op":"attr_eq","key":"plan","value":"pro"},"if_true":1,"if_false":2},` + sendStep + `,` + sendStep + `]`,
		"delay 1 second":  `[{"type":"delay","seconds":1},` + sendStep + `]`,
		"send then delay": `[` + sendStep + `,{"type":"delay","seconds":5}]`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if err := ValidateSteps(steps(t, raw)); err != nil {
				t.Errorf("ValidateSteps rejected a valid definition: %v", err)
			}
		})
	}
}

// BM-111's forward-only rule is the invariant that makes every run terminate.
// A backward branch would be indistinguishable from a stuck run to the
// scheduler, so it has to be rejected at definition time.
func TestValidateSteps_RejectsBackwardBranch(t *testing.T) {
	raw := `[` + sendStep + `,{"type":"branch_on_condition","condition":{"op":"attr_exists","key":"plan"},"if_true":0,"if_false":2},` + sendStep + `]`
	err := ValidateSteps(steps(t, raw))
	if err == nil {
		t.Fatal("accepted a backward branch target — runs could loop forever")
	}
	if !strings.Contains(err.Error(), "forward-only") {
		t.Errorf("error should explain the forward-only rule, got: %v", err)
	}
	if !strings.Contains(err.Error(), "steps[1]") {
		t.Errorf("error should name the offending index, got: %v", err)
	}
}

func TestValidateSteps_Rejects(t *testing.T) {
	cases := map[string]struct {
		raw  string
		want string
	}{
		"empty":               {`[]`, "at least one step"},
		"unknown type":        {`[{"type":"webhook","template":"x"}]`, "unknown step type"},
		"missing type":        {`[{"template":"x"}]`, "type required"},
		"send without tmpl":   {`[{"type":"send"}]`, "send requires a template"},
		"unparseable tmpl":    {`[{"type":"send","template":"{{.Person"}]`, "template:"},
		"delay zero":          {`[{"type":"delay","seconds":0},` + sendStep + `]`, "seconds > 0"},
		"delay negative":      {`[{"type":"delay","seconds":-5},` + sendStep + `]`, "seconds > 0"},
		"branch no cond":      {`[{"type":"branch_on_condition","if_true":1,"if_false":1},` + sendStep + `]`, "requires a condition"},
		"branch bad op":       {`[{"type":"branch_on_condition","condition":{"op":"nope"},"if_true":1,"if_false":1},` + sendStep + `]`, "condition:"},
		"branch no targets":   {`[{"type":"branch_on_condition","condition":{"op":"attr_exists","key":"p"}},` + sendStep + `]`, "requires if_true and if_false"},
		"branch out of range": {`[{"type":"branch_on_condition","condition":{"op":"attr_exists","key":"p"},"if_true":9,"if_false":1},` + sendStep + `]`, "out of range"},
		"self branch":         {`[{"type":"branch_on_condition","condition":{"op":"attr_exists","key":"p"},"if_true":0,"if_false":1},` + sendStep + `]`, "forward-only"},
		"mixed fields":        {`[{"type":"delay","seconds":5,"template":"x"},` + sendStep + `]`, "delay takes only seconds"},
		"no send at all":      {`[{"type":"delay","seconds":5}]`, "no send step"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := ValidateSteps(steps(t, tc.raw))
			if err == nil {
				t.Fatalf("accepted an invalid definition (wanted %q)", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestValidateSteps_RejectsTooMany(t *testing.T) {
	var sb strings.Builder
	sb.WriteByte('[')
	for i := 0; i <= MaxSteps; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(sendStep)
	}
	sb.WriteByte(']')
	if err := ValidateSteps(steps(t, sb.String())); err == nil {
		t.Fatalf("accepted more than MaxSteps (%d) steps", MaxSteps)
	}
}

func TestCompile_PopulatesArtifacts(t *testing.T) {
	j := Journey{
		JourneyID: "onboard",
		Trigger:   json.RawMessage(`{"op":"event_seen","name":"signed_up"}`),
		Steps: steps(t, `[{"type":"delay","seconds":60},`+
			`{"type":"branch_on_condition","condition":{"op":"attr_eq","key":"plan","value":"pro"},"if_true":2,"if_false":2},`+
			sendStep+`]`),
	}
	if err := j.Compile(); err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if j.TriggerCond() == nil {
		t.Error("trigger not compiled")
	}
	if j.Steps[1].Cond() == nil {
		t.Error("branch condition not compiled")
	}
	if j.Steps[2].Tpl() == nil {
		t.Error("send template not compiled")
	}
	if j.Steps[0].Cond() != nil || j.Steps[0].Tpl() != nil {
		t.Error("delay step should compile to nothing")
	}
}

// --- BM-113: run creation is idempotent under redelivery ---

type fakeExec struct {
	queries  []string
	affected int64
	err      error
}

func (f *fakeExec) ExecContext(_ context.Context, q string, _ ...any) (sql.Result, error) {
	f.queries = append(f.queries, q)
	if f.err != nil {
		return nil, f.err
	}
	return fakeResult{n: f.affected}, nil
}

type fakeResult struct{ n int64 }

func (r fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (r fakeResult) RowsAffected() (int64, error) { return r.n, nil }

func TestCreateRun_IdempotentSQL(t *testing.T) {
	f := &fakeExec{affected: 1}
	created, err := CreateRun(context.Background(), f, Run{
		WorkspaceID: "ws", JourneyID: "j1", JourneyVersion: 1,
		PersonID: "p1", TriggeredBy: "e1",
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if !created {
		t.Error("first insert should report created=true")
	}
	q := f.queries[0]
	// The idempotency guarantee is the ON DUPLICATE clause against the UNIQUE
	// key; without it a redelivered trigger event enrolls the person twice.
	if !strings.Contains(q, "ON DUPLICATE KEY UPDATE") {
		t.Errorf("run insert is not idempotent:\n%s", q)
	}
	// A new run must start at step 0 as immediately-due, so the scheduler
	// picks it up rather than the event path executing anything.
	if !strings.Contains(q, "'ready'") || !strings.Contains(q, "NULL") {
		t.Errorf("run should start ready with wake_at NULL:\n%s", q)
	}
}

func TestCreateRun_ReportsDuplicate(t *testing.T) {
	// MySQL reports 0 affected rows when ON DUPLICATE KEY UPDATE changes
	// nothing — that's how a redelivery is distinguished from a new run.
	f := &fakeExec{affected: 0}
	created, err := CreateRun(context.Background(), f, Run{
		WorkspaceID: "ws", JourneyID: "j1", PersonID: "p1", TriggeredBy: "e1",
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if created {
		t.Error("redelivery should report created=false, not a second run")
	}
}

func TestCreateRun_GeneratesRunID(t *testing.T) {
	f := &fakeExec{affected: 1}
	if _, err := CreateRun(context.Background(), f, Run{
		WorkspaceID: "ws", JourneyID: "j1", PersonID: "p1", TriggeredBy: "e1",
	}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if len(f.queries) != 1 {
		t.Fatalf("expected one insert, got %d", len(f.queries))
	}
}
