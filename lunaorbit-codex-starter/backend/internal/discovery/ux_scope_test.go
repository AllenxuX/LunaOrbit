package discovery

import (
	"encoding/json"
	"lunaorbit/backend/internal/discovery/resource"
	"testing"
)

func TestUXScopeCollectionRange(t *testing.T) {
	s := NewService(nil, []resource.Scope{{ID: "static", Namespaces: []string{"messaging"}}, {ID: "auto", Auto: true, Cluster: "demo"}}, nil, nil)
	defer s.Close()
	rows, err := s.Scopes(Actor{ID: "shared", Role: "information"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(rows)
	var got []map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for _, row := range got {
		want := "allowlist"
		if row["id"] == "auto" {
			want = "all_accessible"
		}
		if row["namespace_mode"] != want {
			t.Fatalf("scope %v: namespace_mode=%v want %s", row["id"], row["namespace_mode"], want)
		}
		ns, ok := row["namespaces"].([]any)
		if !ok {
			t.Fatalf("namespaces must be an array: %s", raw)
		}
		if want == "allowlist" && (len(ns) != 1 || ns[0] != "messaging") {
			t.Fatalf("wrong allowlist: %v", ns)
		}
	}
}
