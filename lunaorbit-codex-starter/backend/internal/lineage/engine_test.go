package lineage

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/information"
	"lunaorbit/backend/internal/kafkaops"
	"testing"
)

type fakeProvider struct {
	inventories map[string]kafkaops.Snapshot
	links       []discovery.Snapshot
	apps        []information.Application
	broken      bool
	reads       func(string, string, string) (kafkaops.Snapshot, error)
}

func (f fakeProvider) Clusters(context.Context) (map[string]string, error) {
	return map[string]string{"a": "A", "b": "B", "c": "C"}, nil
}
func (f fakeProvider) Snapshots(context.Context) ([]discovery.Snapshot, error) { return f.links, nil }
func (f fakeProvider) Applications(context.Context) ([]information.Application, bool, error) {
	return f.apps, false, nil
}
func (f fakeProvider) Read(_ context.Context, c, k, r string) (kafkaops.Snapshot, error) {
	if f.reads != nil {
		return f.reads(c, k, r)
	}
	if f.broken {
		return kafkaops.Snapshot{}, errors.New("unavailable")
	}
	if k == "topics" {
		return f.inventories[c], nil
	}
	return kafkaops.Snapshot{}, nil
}
func TestIdentityAndPartialRoot(t *testing.T) {
	if topicID("a:b", "c") == topicID("a", "b:c") {
		t.Fatal("tuple collision")
	}
	p := fakeProvider{broken: true}
	g, e := New(p).Trace(context.Background(), Query{"a", "orders", 2, "both"})
	if e != nil || len(g.Nodes) != 1 || g.Nodes[0].Exists != nil || len(g.Issues) == 0 {
		t.Fatalf("partial root: %+v %v", g, e)
	}
}
func TestMissingRoot(t *testing.T) {
	_, e := New(fakeProvider{}).Trace(context.Background(), Query{"a", "orders", 2, "both"})
	if !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
}
func TestFullMatchAndExclude(t *testing.T) {
	if ok, known := matches("orders", "xorders"); !known || ok {
		t.Fatal("substring accepted")
	}
	if _, known := matches("(?=x)x", "x"); known {
		t.Fatal("unsupported Java regex guessed")
	}
}
