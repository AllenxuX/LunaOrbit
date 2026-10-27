package main

import (
	"encoding/json"
	"lunaorbit/backend/internal/discovery"
	"net/http"
	"net/http/httptest"
	"testing"
)

func labEnv(values map[string]string) func(string) string {
	return func(k string) string { return values[k] }
}

func scopeJSON(t *testing.T, s *discovery.Service) []discovery.ScopeInfo {
	t.Helper()
	rows, err := s.Scopes(discovery.Actor{ID: "owner", Role: "information"})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestLabDiscoveryLegacySingleScopeIsUnchanged(t *testing.T) {
	s, err := newLabDiscovery(discovery.NewMemoryStore(), nil, labEnv(map[string]string{"KITE_LAB_VERSION": "v0.16.0", "KITE_CLUSTER_NAME": "in-cluster", "KITE_NAMESPACE": "luna-kafka", "KITE_BASE_URL": "http://127.0.0.1:28080", "KITE_API_KEY": "synthetic"}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	raw, _ := json.Marshal(scopeJSON(t, s))
	if string(raw) != `[{"namespaces":["luna-kafka"],"namespace_mode":"allowlist","id":"readonly-lab","synthetic":false}]` {
		t.Fatalf("legacy scopes changed: %s", raw)
	}
}

func fakeKiteClusters(t *testing.T, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters" || r.Header.Get("Authorization") != "synthetic" {
			w.WriteHeader(404)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestLabDiscoveryAutoEnumeratesKiteClustersWithExclusions(t *testing.T) {
	base := fakeKiteClusters(t, `[{"name":"c1","version":"v1"},{"name":"c2","version":"v1"},{"name":"c3","version":"v1","error":"unreachable"}]`)
	s, err := newLabDiscovery(discovery.NewMemoryStore(), nil, labEnv(map[string]string{"KITE_LAB_VERSION": "v0.16.0", "KITE_BASE_URL": base, "KITE_API_KEY": "synthetic", "KITE_EXCLUDE_CLUSTERS": "c2"}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got := map[string]discovery.ScopeInfo{}
	for _, r := range scopeJSON(t, s) {
		got[r.KubeCluster] = r
	}
	if len(got) != 2 || !got["c1"].Auto || got["c3"].KiteError != "unreachable" {
		t.Fatalf("auto scopes %+v", got)
	}
}

func TestLabDiscoveryAutoStartsEvenWhenKiteListFails(t *testing.T) {
	base := fakeKiteClusters(t, `not json`)
	s, err := newLabDiscovery(discovery.NewMemoryStore(), nil, labEnv(map[string]string{"KITE_LAB_VERSION": "v0.16.0", "KITE_BASE_URL": base, "KITE_API_KEY": "synthetic"}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if rows := scopeJSON(t, s); len(rows) != 0 {
		t.Fatalf("expected no scopes until Kite answers, got %+v", rows)
	}
}

func TestLabDiscoveryAutoNamespaceAndClusterLimits(t *testing.T) {
	cfg := autoLabConfig(labEnv(map[string]string{"KITE_CLUSTER_NAME": "c1", "KITE_EXCLUDE_NAMESPACES": "ops,kube-system"}))
	if !cfg.includeCluster("c1") || cfg.includeCluster("c2") || !cfg.excludeNS["ops"] || cfg.excludeNS["kube-public"] {
		t.Fatalf("limits %+v", cfg)
	}
	def := autoLabConfig(labEnv(map[string]string{"KITE_NAMESPACE": "luna-kafka"}))
	if !def.excludeNS["kube-system"] || !def.excludeNS["kube-node-lease"] || !def.onlyNS["luna-kafka"] || !def.includeCluster("any") {
		t.Fatalf("defaults %+v", def)
	}
}
