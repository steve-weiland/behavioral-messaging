// Package segmentidx is the V2-2 roaring-bitmap segment engine.
//
// V1 evaluates segment membership by *scanning on read*: every
// /segments/{id}/check reads the person row + their recent events from
// MySQL and walks the condition tree in-process (read-amplification —
// the V1 ceiling named in spec Q10). This package inverts that to
// *maintain on write*: it keeps, per workspace, a set of roaring bitmaps
// of person ordinals —
//
//   - eventSeen[name]        — everyone who has EVER fired `name`
//   - attrEq[key\x00value]   — everyone whose attribute `key` == `value`
//   - attrExists[key]        — everyone who has attribute `key`
//
// and flips a single bit as each event / attribute change streams in
// (Observe*). Membership then reads the bitmaps instead of MySQL.
//
// For a single-person check, the condition tree maps onto bitmap leaf
// membership tests with the boolean ops applied to the bool results —
// `not` is just negation, so no universe bitmap is needed. The same
// segment.Condition grammar drives both the V1 scan evaluator and this
// engine (the "same Condition API survives the swap" promise from Q10).
//
// Concurrency: a single RWMutex guards the whole index. Maintenance
// (Observe*) takes the write lock; Member takes the read lock. Bitmap
// reads are cheap and the write path is one Add/Remove, so contention is
// low for V2-2's scale; per-workspace sharding is a later lever.
package segmentidx

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/steve-weiland/behavioral-messaging/internal/segment"
)

// Index holds bitmap state for all workspaces.
type Index struct {
	mu sync.RWMutex
	ws map[string]*wsIndex
}

type wsIndex struct {
	nextOrd uint32
	ord     map[string]uint32 // person_id -> dense ordinal

	eventSeen  map[string]*roaring.Bitmap // event_name        -> ordinals
	attrEq     map[string]*roaring.Bitmap // key\x00canonical  -> ordinals
	attrExists map[string]*roaring.Bitmap // key               -> ordinals

	// attrState is the engine's view of each person's current attributes
	// (ordinal -> key -> canonical value), kept so an attribute update can
	// remove the bit from the OLD value's bitmap before setting the new
	// one. Without it, re-tagging plan=trial->pro would leave the person
	// in both bitmaps.
	attrState map[uint32]map[string]string
}

// New returns an empty index.
func New() *Index {
	return &Index{ws: make(map[string]*wsIndex)}
}

func newWS() *wsIndex {
	return &wsIndex{
		ord:        make(map[string]uint32),
		eventSeen:  make(map[string]*roaring.Bitmap),
		attrEq:     make(map[string]*roaring.Bitmap),
		attrExists: make(map[string]*roaring.Bitmap),
		attrState:  make(map[uint32]map[string]string),
	}
}

// must be called under w's owning Index write lock.
func (i *Index) wsLocked(workspaceID string) *wsIndex {
	w := i.ws[workspaceID]
	if w == nil {
		w = newWS()
		i.ws[workspaceID] = w
	}
	return w
}

// ensureOrd maps a person_id to a stable dense ordinal, allocating one on
// first sight. Caller holds the write lock.
func (w *wsIndex) ensureOrd(personID string) uint32 {
	if o, ok := w.ord[personID]; ok {
		return o
	}
	o := w.nextOrd
	w.nextOrd++
	w.ord[personID] = o
	return o
}

func bitmapAdd(m map[string]*roaring.Bitmap, key string, ord uint32) {
	bm := m[key]
	if bm == nil {
		bm = roaring.New()
		m[key] = bm
	}
	bm.Add(ord)
}

func bitmapRemove(m map[string]*roaring.Bitmap, key string, ord uint32) {
	if bm := m[key]; bm != nil {
		bm.Remove(ord)
	}
}

// ObserveEvent records that personID fired eventName in workspaceID.
// Idempotent: re-observing the same (person, event_name) is a no-op on
// the bitmap (event_seen is "ever", not a count).
func (i *Index) ObserveEvent(workspaceID, personID, eventName string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	w := i.wsLocked(workspaceID)
	ord := w.ensureOrd(personID)
	bitmapAdd(w.eventSeen, eventName, ord)
}

// SetAttributes reconciles the engine's attribute bitmaps for a person
// against attrs — the person's FULL current attribute set after an
// upsert (merge-patch already applied by the person store; deleted keys
// are simply absent from the map). The engine diffs against its last
// known state so value changes move the bit and removed keys clear it.
func (i *Index) SetAttributes(workspaceID, personID string, attrs map[string]json.RawMessage) {
	i.mu.Lock()
	defer i.mu.Unlock()
	w := i.wsLocked(workspaceID)
	ord := w.ensureOrd(personID)

	prev := w.attrState[ord]
	if prev == nil {
		prev = make(map[string]string)
	}
	next := make(map[string]string, len(attrs))
	for k, raw := range attrs {
		next[k] = canonicalJSON(raw)
	}

	// Removed or changed keys: clear the old (key,value) bit; clear
	// attr_exists only if the key is gone entirely.
	for k, oldVal := range prev {
		newVal, stillThere := next[k]
		if !stillThere {
			bitmapRemove(w.attrEq, attrEqKey(k, oldVal), ord)
			bitmapRemove(w.attrExists, k, ord)
		} else if newVal != oldVal {
			bitmapRemove(w.attrEq, attrEqKey(k, oldVal), ord)
		}
	}
	// Added or changed keys: set the new (key,value) bit + attr_exists.
	for k, newVal := range next {
		oldVal, existed := prev[k]
		if !existed || oldVal != newVal {
			bitmapAdd(w.attrEq, attrEqKey(k, newVal), ord)
			bitmapAdd(w.attrExists, k, ord)
		}
	}
	w.attrState[ord] = next
}

// Member reports whether personID matches cond in workspaceID, reading
// only bitmaps. An unknown person is never a member (matches the V1
// /check contract: a person row that doesn't exist evaluates to false,
// rather than being evaluated against an empty world).
func (i *Index) Member(workspaceID string, cond segment.Condition, personID string) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	w := i.ws[workspaceID]
	if w == nil {
		return false
	}
	ord, ok := w.ord[personID]
	if !ok {
		return false
	}
	return w.eval(cond, ord)
}

// eval mirrors segment.Condition.Evaluate with bitmap-backed leaves.
// Caller holds the read lock.
func (w *wsIndex) eval(c segment.Condition, ord uint32) bool {
	switch c.Op {
	case "and":
		for j := range c.Conditions {
			if !w.eval(c.Conditions[j], ord) {
				return false
			}
		}
		return true
	case "or":
		for j := range c.Conditions {
			if w.eval(c.Conditions[j], ord) {
				return true
			}
		}
		return false
	case "not":
		return !w.eval(c.Conditions[0], ord)
	case "attr_eq":
		return bitmapHas(w.attrEq, attrEqKey(c.Key, canonicalJSON(c.Value)), ord)
	case "attr_exists":
		return bitmapHas(w.attrExists, c.Key, ord)
	case "event_seen":
		return bitmapHas(w.eventSeen, c.Name, ord)
	}
	return false // unreachable for a Validated tree
}

func bitmapHas(m map[string]*roaring.Bitmap, key string, ord uint32) bool {
	bm := m[key]
	return bm != nil && bm.Contains(ord)
}

func attrEqKey(key, canonicalValue string) string {
	return key + "\x00" + canonicalValue
}

// canonicalJSON renders a JSON value to a stable string so equal values
// produce equal bitmap keys regardless of formatting. encoding/json
// marshals object keys in sorted order, so a round-trip through
// interface{} normalizes whitespace and key ordering. Both the indexed
// attribute and the queried condition value go through this same path,
// so they agree with each other.
func canonicalJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "null"
	}
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		// Not decodable as JSON — fall back to the trimmed raw bytes.
		return string(bytes.TrimSpace(raw))
	}
	b, err := json.Marshal(v)
	if err != nil {
		return string(bytes.TrimSpace(raw))
	}
	return string(b)
}
