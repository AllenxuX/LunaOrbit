package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"lunaorbit/backend/internal/discovery/resource"
	"strings"
	"testing"
	"time"
)

func waitJob(t *testing.T, s *Service, actor Actor, scope string) Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		job, e := s.Latest(context.Background(), actor, scope)
		if e != nil {
			t.Fatal(e)
		}
		if job.Status == "done" {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestMockWorkerOwnershipBindingsAndFailureRetention(t *testing.T) {
	store := NewMemoryStore()
	s, e := NewMockService(store, func(ctx context.Context, owner string) ([]Registration, error) {
		if owner == "owner" {
			return []Registration{{ID: "registration", Enabled: true, BootstrapServers: []string{"mars.synthetic.invalid:9092"}}}, nil
		}
		return []Registration{}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	actor := Actor{ID: "owner", Role: "admin"}
	scopes, e := s.Scopes(actor)
	if e != nil || len(scopes) != 1 {
		t.Fatal(e)
	}
	scope := scopes[0].ID
	if _, e = s.Start(context.Background(), actor, scope, ""); e != nil {
		t.Fatal(e)
	}
	job := waitJob(t, s, actor, scope)
	if job.ErrorCode != "" {
		t.Fatal(job.ErrorCode)
	}
	snapshot, e := s.Snapshot(context.Background(), actor, scope)
	if e != nil || len(snapshot.Links) != 3 {
		t.Fatalf("snapshot: %d %v", len(snapshot.Links), e)
	}
	body, _ := json.Marshal(snapshot)
	if strings.Contains(string(body), "DO_NOT_EXPOSE") || strings.Contains(string(body), "password") {
		t.Fatal("worker persisted sensitive config")
	}
	if _, e = s.Snapshot(context.Background(), Actor{ID: "other", Role: "admin"}, scope); !errorsIsNotFound(e) {
		t.Fatal("cross-user snapshot")
	}
	var mars Link
	for _, link := range snapshot.Links {
		if link.SourceAlias == "mars" {
			mars = link
		}
	}
	binding := ManualBinding{Namespace: mars.Namespace, WorkloadUID: mars.WorkloadUID, Alias: "mars", RegistrationID: "registration"}
	if _, e = s.Bind(context.Background(), Actor{ID: "other", Role: "admin"}, scope, binding); !errorsIsNotFound(e) {
		t.Fatal("cross-user binding")
	}
	if _, e = s.Bind(context.Background(), actor, scope, binding); e != nil {
		t.Fatal(e)
	}
	// Advance the cooldown directly; no slow sleeps or external services needed.
	s.mu.Lock()
	key := actor.ID + "\x00" + scope
	old := s.jobs[key]
	old.StartedAt = time.Now().Add(-3 * time.Second)
	s.jobs[key] = old
	s.mu.Unlock()
	if _, e = s.Start(context.Background(), actor, scope, "partial_failure"); e != nil {
		t.Fatal(e)
	}
	job = waitJob(t, s, actor, scope)
	if job.ErrorCode != "PARTIAL_COLLECTION" {
		t.Fatal(job.ErrorCode)
	}
	stale, e := s.Snapshot(context.Background(), actor, scope)
	if e != nil || len(stale.Links) != 3 || !stale.Stale {
		t.Fatal("partial failure erased history")
	}
	for _, link := range stale.Links {
		if !link.Stale || link.Verified {
			t.Fatal("stale data treated healthy")
		}
	}
}
func errorsIsNotFound(e error) bool { return CodeOf(e) == "NOT_FOUND" }

func TestExistingOwnerCanRerunAtJobCapacity(t *testing.T) {
	s, err := NewMockService(NewMemoryStore(), func(context.Context, string) ([]Registration, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	actor := Actor{ID: "existing", Role: "admin"}
	scope := "synthetic-mm2"
	for i := 0; i < 127; i++ {
		s.jobs[fmt.Sprint(i)] = Job{Status: "done", StartedAt: time.Now().Add(-3 * time.Second)}
	}
	s.jobs[actor.ID+"\x00"+scope] = Job{Status: "done", StartedAt: time.Now().Add(-3 * time.Second)}
	if _, err = s.Start(context.Background(), Actor{ID: "new", Role: "admin"}, scope, ""); CodeOf(err) != "RATE_LIMITED" {
		t.Fatal(err)
	}
	if _, err = s.Start(context.Background(), actor, scope, ""); err != nil {
		t.Fatal(err)
	}
	if job := waitJob(t, s, actor, scope); job.ErrorCode != "" {
		t.Fatal(job)
	}
}

func TestScopeBoundaryIncludesSourceAndNamespaces(t *testing.T) {
	a := resource.Scope{ID: "scope", Cluster: "cluster", Namespaces: []string{"one"}, Synthetic: true}
	for _, b := range []resource.Scope{
		{ID: "scope", Cluster: "other", Namespaces: []string{"one"}, Synthetic: true},
		{ID: "scope", Cluster: "cluster", Namespaces: []string{"two"}, Synthetic: true},
		{ID: "scope", Cluster: "cluster", Namespaces: []string{"one"}, Synthetic: false},
	} {
		if sameScope(a, b) {
			t.Fatal("scope mismatch accepted")
		}
	}
	if !sameScope(a, a) {
		t.Fatal("identical scope rejected")
	}
}
