package registry

import (
	"context"
	"lunaorbit/backend/internal/kafkaops"
	"testing"
	"time"
)

func TestOperationsOwnershipAndDisabled(t *testing.T) {
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	defer s.Close()
	a := Actor{ID: "owner", Role: "admin"}
	r, e := s.Create(context.Background(), a, syntheticInput())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.ReadOperations(context.Background(), Actor{}, r.ID, "topics", "", ""); ErrorCode(e) != "FORBIDDEN" {
		t.Fatal(e)
	}
	if _, e = s.ReadOperations(context.Background(), Actor{ID: "other", Role: "viewer"}, r.ID, "topics", "", ""); ErrorCode(e) != "NOT_FOUND" {
		t.Fatal(e)
	}
	no := false
	_, e = s.Patch(context.Background(), a, r.ID, PatchInput{Revision: r.Revision, Enabled: &no})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.ReadOperations(context.Background(), a, r.ID, "topics", "", ""); ErrorCode(e) != "DISABLED" {
		t.Fatal(e)
	}
}

type operationReaderFunc func(context.Context, string, string) (kafkaops.Snapshot, error)

func (f operationReaderFunc) Read(c context.Context, k, r string) (kafkaops.Snapshot, error) {
	return f(c, k, r)
}
func TestOperationsWaitForSlots(t *testing.T) {
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	defer s.Close()
	a := Actor{ID: "owner", Role: "admin"}
	r, e := s.Create(context.Background(), a, syntheticInput())
	if e != nil {
		t.Fatal(e)
	}
	s.slots <- struct{}{}
	s.slots <- struct{}{}
	done := make(chan error, 1)
	go func() { _, e := s.ReadOperations(context.Background(), a, r.ID, "topics", "", ""); done <- e }()
	select {
	case e := <-done:
		t.Fatalf("request rejected before slot available: %v", e)
	case <-time.After(20 * time.Millisecond):
	}
	<-s.slots
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("slot wait blocked")
	}
	<-s.slots
}
