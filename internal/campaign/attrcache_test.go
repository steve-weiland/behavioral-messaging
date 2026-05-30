package campaign

import (
	"context"
	"encoding/json"
	"testing"
)

// Cache-hit paths need no DB (db is only touched on a miss), so these run
// with a nil *sql.DB. Miss→MySQL-fallback is covered by the end-to-end
// load/seed runs.

func TestAttrCacheHit(t *testing.T) {
	c := NewAttrCache(nil)
	c.Set("ws_alpha", "p1", json.RawMessage(`{"plan":"pro"}`))

	p, err := c.Get(context.Background(), "ws_alpha", "p1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if p.WorkspaceID != "ws_alpha" || p.PersonID != "p1" {
		t.Fatalf("identity not set: %+v", p)
	}
	if string(p.Attributes) != `{"plan":"pro"}` {
		t.Fatalf("attrs = %s", p.Attributes)
	}
}

func TestAttrCacheCopiesBuffer(t *testing.T) {
	c := NewAttrCache(nil)
	buf := []byte(`{"plan":"pro"}`)
	c.Set("ws", "p1", buf)
	// Mutate the caller's buffer after Set — the cache must not alias it
	// (AMQP delivery bodies get reused).
	copy(buf, []byte(`{"plan":"FRE"`))

	p, _ := c.Get(context.Background(), "ws", "p1")
	if string(p.Attributes) != `{"plan":"pro"}` {
		t.Fatalf("cache aliased caller buffer: %s", p.Attributes)
	}
}

func TestAttrCacheKeyIsolation(t *testing.T) {
	c := NewAttrCache(nil)
	c.Set("ws_a", "p1", json.RawMessage(`{"plan":"pro"}`))
	c.Set("ws_b", "p1", json.RawMessage(`{"plan":"free"}`))

	a, _ := c.Get(context.Background(), "ws_a", "p1")
	b, _ := c.Get(context.Background(), "ws_b", "p1")
	if string(a.Attributes) == string(b.Attributes) {
		t.Fatal("same person_id in different workspaces must not collide")
	}
	if c.Len() != 2 {
		t.Fatalf("Len = %d, want 2", c.Len())
	}
}
