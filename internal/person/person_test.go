package person

import (
	"encoding/json"
	"testing"
)

// Person is a thin wrapper around two SQL statements; the real semantics
// belong to MySQL (JSON_MERGE_PATCH per RFC 7396). The test here just
// pins the wire shape of the struct so accidental field renames are
// caught in CI — full merge-behavior verification happens against the
// running MySQL during the integration smoke test (see PR commit body).
func TestPersonJSONShape(t *testing.T) {
	p := Person{
		WorkspaceID: "ws_alpha",
		PersonID:    "p_test",
		Attributes:  json.RawMessage(`{"plan":"pro"}`),
	}
	out, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, k := range []string{"workspace_id", "person_id", "attributes", "created_at", "updated_at"} {
		if _, ok := got[k]; !ok {
			t.Errorf("missing field %q in JSON output: %s", k, out)
		}
	}
	if attrs, ok := got["attributes"].(map[string]any); !ok || attrs["plan"] != "pro" {
		t.Errorf("attributes not nested object as expected: %v", got["attributes"])
	}
}
