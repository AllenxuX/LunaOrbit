package discovery

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/discovery/collector"
	"lunaorbit/backend/internal/discovery/resource"
	"testing"
	"time"
)

func TestStartupDiscoveryCollectsEveryActiveScope(t *testing.T) {
	store := NewMemoryStore()
	s := NewService(store, nil, credFactory, func(context.Context, string) ([]Registration, error) { return nil, nil })
	t.Cleanup(s.Close)
	s.EnableAutoScopes()
	s.SyncClusters([]ClusterEntry{{Name: "edge-a"}, {Name: "edge-b"}}, time.Now())
	if started := s.DiscoverAll(autoActor); started != 2 {
		t.Fatalf("started %d", started)
	}
	deadline := time.Now().Add(5 * time.Second)
	for _, name := range []string{"edge-a", "edge-b"} {
		id := scopeByCluster(t, s, name).ID
		for {
			if _, e := store.Load(context.Background(), autoActor.ID, id); e == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s not collected at startup", name)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if len(s.Endpoints()) == 0 {
		t.Fatal("endpoint catalog must be complete after startup discovery")
	}
}

func TestStartupDiscoveryFailureDoesNotPanicOrBlock(t *testing.T) {
	s := NewService(NewMemoryStore(), nil, func(resource.Scope, string) (collector.Source, func(), error) {
		return nil, nil, errors.New("kite down")
	}, func(context.Context, string) ([]Registration, error) { return nil, nil })
	t.Cleanup(s.Close)
	s.EnableAutoScopes()
	s.SyncClusters([]ClusterEntry{{Name: "edge-a"}}, time.Now())
	done := make(chan int, 1)
	go func() { done <- s.DiscoverAll(autoActor) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery must not block")
	}
}
