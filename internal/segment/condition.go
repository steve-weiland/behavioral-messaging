// Package segment — V1 condition tree + scan-based evaluator.
//
// Shared between segments (this package) and campaign triggers
// (PR 4 / internal/campaign). Same tree, two callers. V2 will keep
// the parser/evaluator API and swap the membership store underneath
// for roaring bitmaps with incremental maintenance — see V2 spec.
//
// V1 supported ops:
//
//	Boolean: and / or / not
//	Leaf:    attr_eq, attr_exists, event_seen (ever-in-this-workspace)
//
// Tree shape (serialized as JSON):
//
//	{ "op": "and", "conditions": [
//	    { "op": "attr_eq",     "key": "plan", "value": "pro" },
//	    { "op": "event_seen",  "name": "viewed_pricing" }
//	] }
//
// Depth is capped at 8 at validate-time to bound stack usage.
package segment

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/steveweiland/behavioral-messaging/internal/event"
	"github.com/steveweiland/behavioral-messaging/internal/person"
)

const maxConditionDepth = 8

// Condition is the tagged-union node of the tree. Tag is `Op`; the rest
// of the fields apply only to certain ops.
type Condition struct {
	Op         string          `json:"op"`
	Conditions []Condition     `json:"conditions,omitempty"` // and / or / not
	Key        string          `json:"key,omitempty"`        // attr_eq / attr_exists
	Value      json.RawMessage `json:"value,omitempty"`      // attr_eq
	Name       string          `json:"name,omitempty"`       // event_seen
}

// EvalContext is the world the evaluator sees for one (workspace, person)
// pair. The campaign trigger (PR 4) will populate this from a single
// inbound event; the segment-check endpoint populates it from the
// person's row + their recent events.
type EvalContext struct {
	Person *person.Person
	Events []event.Event
}

// ----- validation ----------------------------------------------------

// Validate walks the tree, rejecting unknown ops, missing required
// fields, and depth > maxConditionDepth. Called at insert-time on
// POST /segments so bad shapes never reach the evaluator.
func (c Condition) Validate() error {
	return c.validateAt(1)
}

func (c Condition) validateAt(depth int) error {
	if depth > maxConditionDepth {
		return fmt.Errorf("condition tree exceeds max depth %d", maxConditionDepth)
	}
	switch c.Op {
	case "and", "or":
		if len(c.Conditions) == 0 {
			return fmt.Errorf("%s: requires at least one sub-condition", c.Op)
		}
	case "not":
		if len(c.Conditions) != 1 {
			return fmt.Errorf("not: requires exactly one sub-condition (got %d)", len(c.Conditions))
		}
	case "attr_eq":
		if c.Key == "" {
			return errors.New("attr_eq: key required")
		}
		if len(c.Value) == 0 {
			return errors.New("attr_eq: value required (use null for an explicit null match)")
		}
		return nil
	case "attr_exists":
		if c.Key == "" {
			return errors.New("attr_exists: key required")
		}
		return nil
	case "event_seen":
		if c.Name == "" {
			return errors.New("event_seen: name required")
		}
		return nil
	default:
		return fmt.Errorf("unknown op %q", c.Op)
	}
	// Boolean recurse.
	for i := range c.Conditions {
		if err := c.Conditions[i].validateAt(depth + 1); err != nil {
			return err
		}
	}
	return nil
}

// ----- evaluation ----------------------------------------------------

// Evaluate runs the tree against ec and returns whether the person
// matches. Trees that pass Validate cannot panic here.
func (c Condition) Evaluate(ec *EvalContext) bool {
	switch c.Op {
	case "and":
		for i := range c.Conditions {
			if !c.Conditions[i].Evaluate(ec) {
				return false
			}
		}
		return true
	case "or":
		for i := range c.Conditions {
			if c.Conditions[i].Evaluate(ec) {
				return true
			}
		}
		return false
	case "not":
		return !c.Conditions[0].Evaluate(ec)
	case "attr_eq":
		return evalAttrEq(ec, c.Key, c.Value)
	case "attr_exists":
		return evalAttrExists(ec, c.Key)
	case "event_seen":
		return evalEventSeen(ec, c.Name)
	}
	return false // unreachable post-Validate
}

// evalAttrEq compares the person's attribute at key against the
// requested JSON value, using deep-equal on parsed JSON. Strings,
// numbers, bools, nulls all work; arrays/objects also compare by
// structure.
func evalAttrEq(ec *EvalContext, key string, want json.RawMessage) bool {
	if ec == nil || ec.Person == nil {
		return false
	}
	got, ok := lookupAttr(ec.Person.Attributes, key)
	if !ok {
		return false
	}
	return jsonEqual(got, want)
}

func evalAttrExists(ec *EvalContext, key string) bool {
	if ec == nil || ec.Person == nil {
		return false
	}
	_, ok := lookupAttr(ec.Person.Attributes, key)
	return ok
}

func evalEventSeen(ec *EvalContext, name string) bool {
	if ec == nil {
		return false
	}
	for i := range ec.Events {
		if ec.Events[i].Name == name {
			return true
		}
	}
	return false
}

// ----- helpers -------------------------------------------------------

func lookupAttr(raw json.RawMessage, key string) (json.RawMessage, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	// One-level lookup. V2 may add dot-paths.
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, false
	}
	v, ok := m[key]
	return v, ok
}

// jsonEqual compares two raw JSON values structurally — decoding both
// into any and recursing. Cheaper than the alternative of canonicalising
// strings, and tolerant of whitespace + numeric formatting differences.
func jsonEqual(a, b json.RawMessage) bool {
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	return deepEqualJSON(av, bv)
}

func deepEqualJSON(a, b any) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case bool:
		bb, ok := b.(bool)
		return ok && av == bb
	case float64:
		bb, ok := b.(float64)
		return ok && av == bb
	case string:
		bb, ok := b.(string)
		return ok && av == bb
	case []any:
		bb, ok := b.([]any)
		if !ok || len(av) != len(bb) {
			return false
		}
		for i := range av {
			if !deepEqualJSON(av[i], bb[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bb, ok := b.(map[string]any)
		if !ok || len(av) != len(bb) {
			return false
		}
		for k, va := range av {
			vb, ok := bb[k]
			if !ok || !deepEqualJSON(va, vb) {
				return false
			}
		}
		return true
	}
	return false
}
