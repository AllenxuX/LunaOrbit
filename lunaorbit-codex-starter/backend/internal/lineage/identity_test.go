package lineage

import (
	"context"
	"encoding/json"
	"lunaorbit/backend/internal/information"
	"strings"
	"testing"
)

func TestApplicationAccountBindingsStayDistinct(t *testing.T) {
	p := fakeProvider{broken: true, apps: []information.Application{{ID: "app", Name: "生产服务", Location: "实验机房", Owner: "业务负责人", Relations: []information.Relation{{ClusterID: "a", Topic: "orders", Mode: "produce", Account: "user-a", ClientID: "client-a"}, {ClusterID: "a", Topic: "orders", Mode: "produce", Account: "user-b", ClientID: "client-b"}}}}}
	g, e := New(p).Trace(context.Background(), Query{"a", "orders", 2, "both"})
	if e != nil {
		t.Fatal(e)
	}
	if len(g.Edges) != 2 {
		t.Fatalf("distinct identities collapsed: %+v", g.Edges)
	}
	b, _ := json.Marshal(g)
	for _, value := range []string{"user-a", "user-b", "client-a", "client-b", "实验机房", "业务负责人"} {
		if !strings.Contains(string(b), value) {
			t.Fatalf("missing identity %s", value)
		}
	}
	for _, edge := range g.Edges {
		if edge.Evidence != "manual" {
			t.Fatal("identity became observed")
		}
	}
}
