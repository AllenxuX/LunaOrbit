package topology

import (
	"context"
	"errors"
	"math"
	"testing"
)

func TestPrivateViewCASAndValidation(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	initial, err := s.Load(ctx, "alice", "scope")
	if err != nil || initial.Revision != 0 {
		t.Fatal(initial, err)
	}
	v := View{Positions: map[string]Position{"node": {X: 100, Y: 200}}, Filters: Filters{Room: "lab", Environment: "uat"}, Layers: Layers{Kafka: true, Workloads: true}}
	got, err := s.Save(ctx, "alice", "scope", v)
	if err != nil || got.Revision != 1 {
		t.Fatal(got, err)
	}
	v.Positions["node"] = Position{X: 900, Y: 900}
	stored, _ := s.Load(ctx, "alice", "scope")
	if stored.Positions["node"].X != 100 {
		t.Fatal("caller mutated stored view")
	}
	other, _ := s.Load(ctx, "bob", "scope")
	if other.Revision != 0 {
		t.Fatal("cross-user view")
	}
	if _, err = s.Save(ctx, "alice", "scope", v); !errors.Is(err, ErrConflict) {
		t.Fatal("lost update", err)
	}
	stored.Positions["node"] = Position{X: math.NaN()}
	if _, err = s.Save(ctx, "alice", "scope", stored); !errors.Is(err, ErrInvalid) {
		t.Fatal("NaN accepted", err)
	}
	stored.Positions["node"] = Position{X: 100001}
	if _, err = s.Save(ctx, "alice", "scope", stored); !errors.Is(err, ErrInvalid) {
		t.Fatal("unbounded coordinate", err)
	}
}
