package registry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryStoreIsolationCASAndCopies(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	r := Registration{ID: "synthetic", CreatedBy: "owner", Revision: 1, BootstrapServers: []string{"localhost:9092"}, Capabilities: []Capability{{Feature: "metadata"}}, Brokers: []Broker{{Host: "localhost"}}}
	if err := s.Save(ctx, r, 0, "registry.create"); err != nil {
		t.Fatal(err)
	}
	r.BootstrapServers[0] = "changed"
	got, err := s.Get(ctx, "owner", r.ID)
	if err != nil || got.BootstrapServers[0] != "localhost:9092" {
		t.Fatal(got, err)
	}
	got.Capabilities[0].Feature = "changed"
	got.Brokers[0].Host = "changed"
	again, _ := s.Get(ctx, "owner", r.ID)
	if again.Capabilities[0].Feature != "metadata" || again.Brokers[0].Host != "localhost" {
		t.Fatal("copy escaped")
	}
	if _, err = s.Get(ctx, "other", r.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if rows, err := s.List(ctx, "other"); err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	if err = s.Save(ctx, r, 0, "registry.create"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	r.CreatedBy = "other"
	r.Revision = 2
	if err = s.Save(ctx, r, 1, "registry.update"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	r.CreatedBy = "owner"
	now := time.Now()
	r.DeletedAt = &now
	if err = s.Save(ctx, r, 1, "registry.delete"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Get(ctx, "owner", r.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if rows, err := s.List(ctx, "owner"); err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	if err = s.Save(ctx, r, 1, "registry.update"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
}

func TestMemoryStoreConcurrentCAS(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	r := Registration{ID: "cas", CreatedBy: "owner", Revision: 1}
	if err := s.Save(ctx, r, 0, "registry.create"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			next := r
			next.Revision = 2
			results <- s.Save(ctx, next, 1, "registry.update")
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if successes != 1 {
		t.Fatalf("CAS winners: %d", successes)
	}
}
