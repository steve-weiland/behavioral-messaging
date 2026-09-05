// Package journey is the V3 multi-step journey model: a trigger plus an
// ordered step list, and per-person runs through it.
//
// Scope is deliberately three step types (spec Q9): send, delay, and
// branch_on_condition. The cap is what makes the tier finishable, and each
// type earns its place — send is the thing users care about, delay is the
// reason state has to be persisted at all, and branch is what makes it a
// state machine rather than a list.
//
// The trigger reuses segment.Condition verbatim — the same grammar now has
// three callers (segments, campaigns, journeys), which is the payoff for
// BM-38 having insisted on one tree from V1.
package journey

import (
	"encoding/json"
	"fmt"
	"text/template"
	"time"

	"github.com/steve-weiland/behavioral-messaging/internal/campaign"
	"github.com/steve-weiland/behavioral-messaging/internal/segment"
)

// Step types (BM-111). Exactly these three.
const (
	StepSend   = "send"
	StepDelay  = "delay"
	StepBranch = "branch_on_condition"
)

// MaxSteps bounds a definition. Generous for a demo, finite so a pathological
// definition can't make a run unbounded.
const MaxSteps = 20

// Step is one node in a journey. The fields used depend on Type; validation
// rejects a step carrying fields its type doesn't use, so a typo like
// {"type":"delay","template":"..."} fails loudly at POST rather than silently
// sending nothing three days later.
type Step struct {
	Type string `json:"type"`

	// send
	Template string `json:"template,omitempty"`

	// delay
	Seconds int64 `json:"seconds,omitempty"`

	// branch_on_condition
	Condition json.RawMessage `json:"condition,omitempty"`
	IfTrue    *int            `json:"if_true,omitempty"`
	IfFalse   *int            `json:"if_false,omitempty"`

	// Then controls what happens after this step: "" / "next" continues to
	// step_index+1, "end" finishes the run.
	//
	// This exists because branch arms are ranges in a FLAT list, so without it
	// the if_true arm runs off its end and into the if_false arm — found by
	// running a branching journey and watching one person receive both
	// messages. A field rather than a fourth step type, so Q9's cap holds.
	Then string `json:"then,omitempty"`

	// Compiled artifacts, populated by Compile. Same reasoning as
	// campaign.Campaign: decoding a condition and parsing a template per
	// execution is waste on a path that already shares a write bottleneck.
	cond *segment.Condition
	tpl  *template.Template
}

// Cond and Tpl expose the compiled artifacts to the scheduler (V3-1b),
// nil when Compile hasn't run or failed.
func (s *Step) Cond() *segment.Condition { return s.cond }
func (s *Step) Tpl() *template.Template  { return s.tpl }

// Journey is the persisted definition.
type Journey struct {
	WorkspaceID string          `json:"workspace_id"`
	JourneyID   string          `json:"journey_id"`
	Name        string          `json:"name"`
	Trigger     json.RawMessage `json:"trigger"`
	Steps       []Step          `json:"steps"`
	Version     uint32          `json:"version"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`

	cond *segment.Condition
}

// TriggerCond exposes the compiled trigger (nil if uncompiled).
func (j *Journey) TriggerCond() *segment.Condition { return j.cond }

// Run status values (mirrors the ENUM in migration 005).
const (
	StatusReady     = "ready"
	StatusWaiting   = "waiting"
	StatusRunning   = "running"
	StatusDone      = "done"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

// Run is one person's position in one journey.
type Run struct {
	WorkspaceID    string     `json:"workspace_id"`
	RunID          string     `json:"run_id"`
	JourneyID      string     `json:"journey_id"`
	JourneyVersion uint32     `json:"journey_version"`
	PersonID       string     `json:"person_id"`
	TriggeredBy    string     `json:"triggered_by"`
	StepIndex      int        `json:"step_index"`
	Status         string     `json:"status"`
	WakeAt         *time.Time `json:"wake_at,omitempty"`
	Attempt        uint32     `json:"attempt"`
	LastError      string     `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// ValidateSteps enforces BM-111 at POST time. Every failure returns a message
// naming the offending index, because "steps[2]: ..." is the difference
// between a usable 400 and a support ticket.
func ValidateSteps(steps []Step) error {
	if len(steps) == 0 {
		return fmt.Errorf("steps: at least one step required")
	}
	if len(steps) > MaxSteps {
		return fmt.Errorf("steps: at most %d steps (got %d)", MaxSteps, len(steps))
	}
	for i := range steps {
		if err := validateStep(i, &steps[i], len(steps)); err != nil {
			return err
		}
	}
	// A journey whose last reachable action is a delay does nothing useful;
	// allowed, but it's worth failing a definition that can never send.
	hasSend := false
	for i := range steps {
		if steps[i].Type == StepSend {
			hasSend = true
			break
		}
	}
	if !hasSend {
		return fmt.Errorf("steps: journey has no send step — it would never deliver anything")
	}
	return nil
}

// Step.Then values.
const (
	ThenNext = "next"
	ThenEnd  = "end"
)

// EndsRun reports whether the run finishes after this step.
func (s *Step) EndsRun() bool { return s.Then == ThenEnd }

func validateStep(i int, s *Step, n int) error {
	at := func(format string, a ...any) error {
		return fmt.Errorf("steps[%d]: "+format, append([]any{i}, a...)...)
	}
	switch s.Then {
	case "", ThenNext, ThenEnd:
	default:
		return at("then must be %q or %q (got %q)", ThenNext, ThenEnd, s.Then)
	}
	switch s.Type {
	case StepSend:
		if s.Template == "" {
			return at("send requires a template")
		}
		if len(s.Template) > maxTemplateBytes {
			return at("template must be ≤ %d bytes", maxTemplateBytes)
		}
		if err := campaign.ParseTemplate(fmt.Sprintf("step%d", i), s.Template); err != nil {
			return at("template: %v", err)
		}
		if s.Seconds != 0 || len(s.Condition) > 0 {
			return at("send takes only a template")
		}
	case StepDelay:
		if s.Seconds <= 0 {
			return at("delay requires seconds > 0")
		}
		if s.Template != "" || len(s.Condition) > 0 {
			return at("delay takes only seconds")
		}
	case StepBranch:
		if len(s.Condition) == 0 {
			return at("branch_on_condition requires a condition")
		}
		cond, err := segment.DecodeCondition(s.Condition)
		if err != nil {
			return at("condition: %v", err)
		}
		if err := cond.Validate(); err != nil {
			return at("condition: %v", err)
		}
		if s.IfTrue == nil || s.IfFalse == nil {
			return at("branch_on_condition requires if_true and if_false")
		}
		// BM-111: forward-only. This is the invariant that makes every run
		// monotonic and therefore terminating — with backward jumps allowed, a
		// definition could loop forever and the scheduler would have no way to
		// tell a legitimate cycle from a stuck run.
		for label, target := range map[string]int{"if_true": *s.IfTrue, "if_false": *s.IfFalse} {
			if target <= i {
				return at("%s target %d must be greater than this step's index %d (branches are forward-only, so every run terminates)", label, target, i)
			}
			if target >= n {
				return at("%s target %d is out of range (only %d steps)", label, target, n)
			}
		}
		if s.Template != "" || s.Seconds != 0 {
			return at("branch_on_condition takes only condition/if_true/if_false")
		}
		if s.Then == ThenEnd {
			return at("branch_on_condition cannot end the run — it must branch somewhere")
		}
	case "":
		return at("type required (one of %s, %s, %s)", StepSend, StepDelay, StepBranch)
	default:
		return at("unknown step type %q (want %s, %s, or %s)", s.Type, StepSend, StepDelay, StepBranch)
	}
	return nil
}

const maxTemplateBytes = 16 << 10

// Compile decodes the trigger and every step's condition/template once, so
// the per-event enrollment check and the scheduler's step execution don't
// re-parse. A failure leaves artifacts nil and the caller falls back to
// decoding inline, matching campaign.Campaign's contract.
func (j *Journey) Compile() error {
	cond, err := segment.DecodeCondition(j.Trigger)
	if err != nil {
		return fmt.Errorf("decode trigger: %w", err)
	}
	j.cond = &cond
	for i := range j.Steps {
		s := &j.Steps[i]
		switch s.Type {
		case StepSend:
			tpl, err := template.New(fmt.Sprintf("%s.step%d", j.JourneyID, i)).
				Option("missingkey=zero").Parse(s.Template)
			if err != nil {
				return fmt.Errorf("steps[%d]: parse template: %w", i, err)
			}
			s.tpl = tpl
		case StepBranch:
			c, err := segment.DecodeCondition(s.Condition)
			if err != nil {
				return fmt.Errorf("steps[%d]: decode condition: %w", i, err)
			}
			s.cond = &c
		}
	}
	return nil
}
