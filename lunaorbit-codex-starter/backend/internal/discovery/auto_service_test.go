package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"lunaorbit/backend/internal/discovery/collector"
	"lunaorbit/backend/internal/discovery/resource"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeAutoSource struct {
	fail map[string]string // cluster -> error code
}

func (f fakeAutoSource) Collect(_ context.Context, scope resource.Scope) (resource.Collection, error) {
	if code := f.fail[scope.Cluster]; code != "" {
		return resource.Collection{Scope: scope, ObservedAt: time.Now().UTC(), ErrorCode: code}, errors.New(code)
	}
	col := autoCollection(scope.Cluster)
	col.Scope = scope
	col.ObservedAt = time.Now().UTC()
	return col, nil
}

func autoService(t *testing.T, fail map[string]string) *Service {
	t.Helper()
	var mu sync.Mutex
	s := NewService(NewMemoryStore(), nil, func(scope resource.Scope, _ string) (collector.Source, func(), error) {
		mu.Lock()
		defer mu.Unlock()
		return fakeAutoSource{fail: fail}, func() {}, nil
	}, func(context.Context, string) ([]Registration, error) { return nil, nil })
	t.Cleanup(s.Close)
	return s
}

var autoActor = Actor{ID: "owner", Role: "information"}

func runScope(t *testing.T, s *Service, id string) Job {
	t.Helper()
	if _, e := s.Start(context.Background(), autoActor, id, ""); e != nil {
		t.Fatalf("start %s: %v", id, e)
	}
	return waitJob(t, s, autoActor, id)
}

func scopeByCluster(t *testing.T, s *Service, cluster string) ScopeInfo {
	t.Helper()
	rows, e := s.Scopes(autoActor)
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range rows {
		if r.KubeCluster == cluster {
			return r
		}
	}
	t.Fatalf("no scope for %s in %+v", cluster, rows)
	return ScopeInfo{}
}

func TestSyncClustersCreatesOneIndependentScopePerKubernetesCluster(t *testing.T) {
	s := autoService(t, map[string]string{"edge-b": "UPSTREAM_UNAVAILABLE"})
	s.SyncClusters([]ClusterEntry{{Name: "edge-a"}, {Name: "edge-b"}}, time.Now())
	a, b := scopeByCluster(t, s, "edge-a"), scopeByCluster(t, s, "edge-b")
	if a.ID == b.ID || !a.Auto || a.Removed {
		t.Fatalf("scopes %+v %+v", a, b)
	}
	if job := runScope(t, s, a.ID); job.ErrorCode != "" {
		t.Fatalf("healthy cluster failed: %q", job.ErrorCode)
	}
	if job := runScope(t, s, b.ID); job.ErrorCode == "" {
		t.Fatal("failing cluster reported success")
	}
	snap, e := s.Snapshot(context.Background(), autoActor, a.ID)
	if e != nil || len(snap.Links) != 1 || snap.Links[0].KubeCluster != "edge-a" || snap.Stale {
		t.Fatalf("one failure affected another scope: %v %+v", e, snap)
	}
}

func TestSyncClustersAddsNewAndMarksRemovedWithoutDeletingHistory(t *testing.T) {
	s := autoService(t, nil)
	t0 := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	s.SyncClusters([]ClusterEntry{{Name: "edge-a"}}, t0)
	a := scopeByCluster(t, s, "edge-a")
	runScope(t, s, a.ID)
	s.SyncClusters([]ClusterEntry{{Name: "edge-a"}, {Name: "edge-c", Error: "dial timeout"}}, t0.Add(time.Minute))
	if c := scopeByCluster(t, s, "edge-c"); c.KiteError != "dial timeout" {
		t.Fatalf("new cluster not visible on next sync: %+v", c)
	}
	s.SyncClusters([]ClusterEntry{{Name: "edge-c"}}, t0.Add(2*time.Minute))
	a = scopeByCluster(t, s, "edge-a")
	if !a.Removed || a.RemovedAt == nil || !a.RemovedAt.Equal(t0.Add(2*time.Minute)) {
		t.Fatalf("removed cluster not marked: %+v", a)
	}
	if snap, e := s.Snapshot(context.Background(), autoActor, a.ID); e != nil || len(snap.Links) != 1 {
		t.Fatalf("history of removed cluster lost: %v", e)
	}
	if _, e := s.Start(context.Background(), autoActor, a.ID, ""); CodeOf(e) != "SCOPE_REMOVED" {
		t.Fatalf("removed scope must not collect: %v", e)
	}
	s.SyncClusters([]ClusterEntry{{Name: "edge-a"}, {Name: "edge-c"}}, t0.Add(3*time.Minute))
	if a = scopeByCluster(t, s, "edge-a"); a.Removed || a.RemovedAt != nil {
		t.Fatalf("returning cluster must be active again: %+v", a)
	}
}

func TestCollectionNotFoundMarksScopeRemoved(t *testing.T) {
	s := autoService(t, map[string]string{"edge-gone": "NOT_FOUND"})
	s.SyncClusters([]ClusterEntry{{Name: "edge-gone"}}, time.Now())
	id := scopeByCluster(t, s, "edge-gone").ID
	runScope(t, s, id)
	if !scopeByCluster(t, s, "edge-gone").Removed {
		t.Fatal("Kite 404 for a cluster must mark it removed")
	}
}

func TestSyncClustersNeverTouchesStaticScopesAndLegacyListingIsUnchanged(t *testing.T) {
	legacy := resource.Scope{ID: "readonly-lab", Cluster: "in-cluster", Namespaces: []string{"luna-kafka"}}
	s := NewService(NewMemoryStore(), []resource.Scope{legacy}, nil, nil)
	defer s.Close()
	s.SyncClusters([]ClusterEntry{{Name: "edge-a"}}, time.Now())
	s.SyncClusters(nil, time.Now())
	rows, _ := s.Scopes(autoActor)
	raw, _ := json.Marshal(rows)
	if !strings.Contains(string(raw), `{"namespaces":["luna-kafka"],"namespace_mode":"allowlist","id":"readonly-lab","synthetic":false}`) {
		t.Fatalf("legacy scope listing changed: %s", raw)
	}
	for _, r := range rows {
		if r.ID == "readonly-lab" && r.Removed {
			t.Fatal("static scope marked removed")
		}
	}
}

func TestAutoScopeIDIsStableAndSafe(t *testing.T) {
	for _, name := range []string{"in-cluster", "Prod_Cluster.Shanghai", strings.Repeat("x", 300), "a b"} {
		id := AutoScopeID(name)
		if id != AutoScopeID(name) || !safeName.MatchString(id) || len(id) > 63 || !strings.HasPrefix(id, "k8s-") {
			t.Fatalf("unsafe id %q for %q", id, name)
		}
	}
	if AutoScopeID("a-b") == AutoScopeID("a_b") {
		t.Fatal("distinct cluster names must not share a scope id")
	}
}

func TestLabBindingsCanNameTheKubernetesCluster(t *testing.T) {
	s := autoService(t, nil)
	s.EnableAutoScopes()
	raw := `[{"k8s_cluster":"edge-a","namespace":"luna-kafka","workload_name":"mm2","alias":"k1","bootstrap":"k1:9092"},{"k8s_cluster":"edge-b","namespace":"luna-kafka","workload_name":"mm2","alias":"k1","bootstrap":"k1:9092"}]`
	if e := s.ConfigureLabBindings(raw); e != nil {
		t.Fatalf("same workload in two clusters must be allowed: %v", e)
	}
	if e := s.ConfigureLabBindings(`[{"namespace":"luna-kafka","workload_name":"mm2","alias":"k1","bootstrap":"k1:9092"},{"namespace":"luna-kafka","workload_name":"mm2","alias":"k2","bootstrap":"k2:9092"}]`); e == nil {
		t.Fatal("auto mode bindings must name the Kubernetes cluster")
	}
	col := autoCollection("edge-b")
	got := s.labBindingsFor(col.Scope)
	if len(got) != 1 || got[0].Cluster != "edge-b" {
		t.Fatalf("bindings leaked across clusters: %+v", got)
	}
}
