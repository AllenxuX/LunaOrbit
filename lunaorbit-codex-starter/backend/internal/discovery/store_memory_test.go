package discovery

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemorySnapshotIsolationAndMonotonic(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	now := time.Now().UTC()
	snap := Snapshot{ScopeID: "scope", ObservedAt: now, AttemptedAt: now, Links: []Link{{Notes: []string{"safe"}, Source: Mapping{CandidateIDs: []string{"r"}}}}}
	if err := s.Save(ctx, "alice", snap); err != nil {
		t.Fatal(err)
	}
	snap.Links[0].Notes[0] = "changed"
	got, err := s.Load(ctx, "alice", "scope")
	if err != nil || got.Links[0].Notes[0] != "safe" {
		t.Fatalf("copy: %+v %v", got, err)
	}
	got.Links[0].Source.CandidateIDs[0] = "mutated"
	again, _ := s.Load(ctx, "alice", "scope")
	if again.Links[0].Source.CandidateIDs[0] != "r" {
		t.Fatal("load aliases storage")
	}
	if _, err = s.Load(ctx, "bob", "scope"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	for _, old := range []Snapshot{{ScopeID: "scope", ObservedAt: now.Add(-time.Second), AttemptedAt: now.Add(time.Second)}, {ScopeID: "scope", ObservedAt: now, AttemptedAt: now.Add(-time.Second)}} {
		if err = s.Save(ctx, "alice", old); !errors.Is(err, ErrConflict) {
			t.Fatalf("old accepted: %v", err)
		}
	}
}
func TestMemoryBindings(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	b := ManualBinding{ScopeID: "s", Namespace: "n", WorkloadUID: "u", Alias: "a", RegistrationID: "r", ApprovedBy: "alice", ApprovedAt: time.Now()}
	if err := s.PutBinding(ctx, "alice", b); !errors.Is(err, ErrNotFound) {
		t.Fatal("binding without snapshot accepted", err)
	}
	snap := Snapshot{ScopeID: "s", Links: []Link{{Namespace: "n", WorkloadUID: "u", SourceAlias: "a"}}}
	if err := s.Save(ctx, "alice", snap); err != nil {
		t.Fatal(err)
	}
	if err := s.PutBinding(ctx, "alice", b); err != nil {
		t.Fatal(err)
	}
	b.RegistrationID = "r2"
	if err := s.PutBinding(ctx, "alice", b); err != nil {
		t.Fatal(err)
	}
	got, _ := s.ListBindings(ctx, "alice", "s")
	if len(got) != 1 || got[0].RegistrationID != "r2" {
		t.Fatal(got)
	}
	got, _ = s.ListBindings(ctx, "bob", "s")
	if len(got) != 0 {
		t.Fatal(got)
	}
	snap.Links[0].Stale = true
	if err := s.Save(ctx, "alice", snap); err != nil {
		t.Fatal(err)
	}
	if err := s.PutBinding(ctx, "alice", b); !errors.Is(err, ErrNotFound) {
		t.Fatal("stale workload binding accepted", err)
	}
	got, _ = s.ListBindings(ctx, "alice", "s")
	if len(got) != 1 || got[0].RegistrationID != "r2" {
		t.Fatal("rejected binding changed storage", got)
	}
	b.ApprovedBy = "bob"
	if err := s.PutBinding(ctx, "alice", b); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	b.ApprovedBy = "alice"
	b.Namespace = ""
	if err := s.PutBinding(ctx, "alice", b); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.PutBinding(canceled, "alice", b); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
