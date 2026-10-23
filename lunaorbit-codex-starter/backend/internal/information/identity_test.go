package information

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestIdentityMetadataRoundTripAndLimits(t *testing.T) {
	raw := `{"id":"app","name":"业务应用","location":"测试机房 / 订单服务","relations":[{"cluster_id":"a","topic":"events","mode":"produce","account":"User:synthetic","client_id":"order-producer"}]}`
	var a Application
	if err := json.Unmarshal([]byte(raw), &a); err != nil {
		t.Fatal(err)
	}
	s := NewMemoryStore()
	saved, err := s.Create(context.Background(), "w", a)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(saved)
	for _, v := range []string{"测试机房 / 订单服务", "User:synthetic", "order-producer"} {
		if !strings.Contains(string(b), v) {
			t.Fatalf("metadata dropped: %s", v)
		}
	}
	for _, field := range []string{"account", "client_id"} {
		var obj map[string]any
		json.Unmarshal([]byte(raw), &obj)
		obj["relations"].([]any)[0].(map[string]any)[field] = strings.Repeat("x", 257)
		b, _ := json.Marshal(obj)
		json.Unmarshal(b, &a)
		if _, err := s.Create(context.Background(), "w2", a); err != ErrInvalid {
			t.Fatalf("unbounded %s: %v", field, err)
		}
	}
}

func TestLocationLimitsAndLegacyCompatibility(t *testing.T) {
	legacy := Application{ID: "legacy", Name: "old", Relations: []Relation{{ClusterID: "a", Topic: "events", Mode: "produce"}}}
	if !valid(legacy) {
		t.Fatal("old record rejected")
	}
	legacy.Location = strings.Repeat("x", 513)
	if valid(legacy) {
		t.Fatal("unbounded location")
	}
	legacy.Location = "lab"
	legacy.Relations[0].Account = "x\x00y"
	if valid(legacy) {
		t.Fatal("invalid identity")
	}
}
