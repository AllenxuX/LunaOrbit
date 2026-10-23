package discovery

import (
	"encoding/json"
	"fmt"
	"lunaorbit/backend/internal/discovery/resource"
	"testing"
)

func TestLabBindingsAcceptBoundedMeshAndRejectInvalidEntries(t *testing.T) {
	s := NewService(NewMemoryStore(), []resource.Scope{{ID: "lab", Namespaces: []string{"synthetic-ns"}}}, nil, nil)
	defer s.Close()
	bindings := []LabBinding{}
	for i := 0; i < 8; i++ {
		bindings = append(bindings, LabBinding{Namespace: "synthetic-ns", WorkloadName: "mm2-mesh", Alias: fmt.Sprintf("alias-%d", i), Bootstrap: fmt.Sprintf("cluster-%d.synthetic.test:9092", i)})
	}
	raw, _ := json.Marshal(bindings)
	if err := s.ConfigureLabBindings(string(raw)); err != nil {
		t.Fatal(err)
	}
	for len(bindings) < 17 {
		bindings = append(bindings, LabBinding{Namespace: "synthetic-ns", WorkloadName: "mm2-mesh", Alias: fmt.Sprintf("alias-%d", len(bindings)), Bootstrap: "other.synthetic.test:9092"})
	}
	oversized, _ := json.Marshal(bindings)
	duplicate, _ := json.Marshal([]LabBinding{bindings[0], bindings[0], bindings[1]})
	bindings[0].Namespace = "unapproved-ns"
	invalid, _ := json.Marshal(bindings[:4])
	for _, v := range []string{string(oversized), string(duplicate), string(invalid)} {
		if s.ConfigureLabBindings(v) == nil {
			t.Fatal("invalid expanded bindings accepted")
		}
	}
}

func TestConfiguredLabBindingsOnlyConfirmOwnedProbedObservedMatches(t *testing.T) {
	col := collection(syntheticConfig)
	col.Scope.Synthetic = false
	col.Scope.Namespaces = []string{"synthetic-ns"}
	s := NewService(NewMemoryStore(), []resource.Scope{col.Scope}, nil, nil)
	defer s.Close()
	raw := `[{"namespace":"synthetic-ns","workload_name":"synthetic-mm2","alias":"mars","bootstrap":"mars.synthetic.test:9092"},{"namespace":"synthetic-ns","workload_name":"synthetic-mm2","alias":"moon-prod","bootstrap":"moon.synthetic.test:9092"}]`
	if err := s.ConfigureLabBindings(raw); err != nil {
		t.Fatal(err)
	}
	regs := []Registration{{ID: "source", Enabled: true, IdentityVerified: true, DiscoveredClusterID: "cluster-one", BootstrapServers: []string{"mars.synthetic.test:9092"}}, {ID: "target", Enabled: true, IdentityVerified: true, DiscoveredClusterID: "cluster-two", BootstrapServers: []string{"moon.synthetic.test:9092"}}}
	b := s.resolveLabBindings(col, regs, nil)
	if len(b) != 2 {
		t.Fatalf("expected two trusted bindings, got %d", len(b))
	}
	snapshot, err := Build(col, regs, b, nil, Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Links[0].Verified || snapshot.Links[0].Source.RegistrationID != "source" || snapshot.Links[0].Target.ClusterID != "cluster-two" {
		t.Fatal("mapping or health state wrong")
	}
	// A different owner's ID is never inserted, failed probes stay candidates,
	// and explicit user bindings take precedence over deployment defaults.
	regs[0].IdentityVerified = false
	if len(s.resolveLabBindings(col, regs, nil)) != 1 {
		t.Fatal("unverified identity mapped")
	}
	if len(s.resolveLabBindings(col, nil, nil)) != 0 {
		t.Fatal("missing owner resources mapped")
	}
	custom := []ManualBinding{{Namespace: "synthetic-ns", WorkloadUID: "deployment-uid", Alias: "moon-prod", RegistrationID: "custom"}}
	got := s.resolveLabBindings(col, regs, custom)
	if len(got) != 1 || got[0].RegistrationID != "custom" {
		t.Fatal("user mapping overwritten")
	}
	col.Namespaces[0].Deployments[0].Metadata.Name = "unrelated"
	if len(s.resolveLabBindings(col, regs, nil)) != 0 {
		t.Fatal("unconfigured workload mapped")
	}
}

func TestLabBindingConfigRejectsSyntheticAndUnknownFields(t *testing.T) {
	s := NewService(NewMemoryStore(), []resource.Scope{{ID: "mock", Synthetic: true, Namespaces: []string{"synthetic-ns"}}}, nil, nil)
	defer s.Close()
	if s.ConfigureLabBindings(`[]`) == nil {
		t.Fatal("empty catalog accepted")
	}
	if s.ConfigureLabBindings(`[{"namespace":"synthetic-ns","workload_name":"mm2","alias":"source","bootstrap":"example.invalid:9092"},{"namespace":"synthetic-ns","workload_name":"mm2","alias":"target","bootstrap":"other.invalid:9092"}]`) == nil {
		t.Fatal("mock catalog accepted")
	}
}
