package information

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMemoryPaginationCopiesAndConcurrentRevision(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	for i := 0; i < 3; i++ {
		_, err := s.Create(ctx, "w", Application{ID: fmt.Sprint(i), Name: fmt.Sprintf("app%d", i), Relations: []Relation{{ClusterID: "test", Topic: "events", Mode: "consume", ConsumerGroup: "group"}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.List(ctx, "w", 2, 2, "APP")
	if err != nil || page.Total != 3 || len(page.Items) != 1 || page.Items[0].Name != "app2" {
		t.Fatal(page, err)
	}
	page.Items[0].Relations[0].Topic = "changed"
	fresh, _ := s.List(ctx, "w", 2, 2, "")
	if fresh.Items[0].Relations[0].Topic != "events" {
		t.Fatal("caller changed stored relation")
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Update(ctx, "w", "2", fresh.Items[0])
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrConflict) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatal("revision winners", wins.Load())
	}
	for _, args := range [][2]int{{0, 20}, {1, 101}, {1000001, 20}} {
		if _, err := s.List(ctx, "w", args[0], args[1], ""); !errors.Is(err, ErrInvalid) {
			t.Fatal("unbounded pagination")
		}
	}
	if _, err := s.List(ctx, "w", 1, 20, strings.Repeat("x", 257)); !errors.Is(err, ErrInvalid) {
		t.Fatal("unbounded query")
	}
}

func TestServerTimestampAndStaleUpdate(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	a, err := s.Create(ctx, "w", Application{ID: "timestamp", Name: "timestamp"})
	if err != nil || a.UpdatedAt.IsZero() {
		t.Fatal(a, err)
	}
	stale := a
	updated, err := s.Update(ctx, "w", a.ID, a)
	if err != nil || !updated.UpdatedAt.After(a.UpdatedAt) {
		t.Fatal(updated, err)
	}
	if _, err = s.Update(ctx, "w", a.ID, stale); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	page, err := s.List(ctx, "w", 1, 20, "")
	if err != nil || !page.Items[0].UpdatedAt.Equal(updated.UpdatedAt) {
		t.Fatal("stale write changed timestamp", page, err)
	}
}
