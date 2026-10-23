package discovery

import (
	"lunaorbit/backend/internal/discovery/resource"
	"strings"
	"testing"
)

func TestDirectionalTopicsWithoutExplicitClustersAreNotMM2(t *testing.T) {
	input := collection("a->b.topics=orders.*\n")
	input.Namespaces[0].Deployments[0].Spec.Template.Spec.Containers[0].Image = "synthetic/application:test"
	result, e := Build(input, nil, nil, nil, Snapshot{})
	if e != nil || len(result.Links) != 0 {
		t.Fatal("directional topics alone identified MM2")
	}
}
func TestConflictingBootstrapDeclarationsDoNotSelectFirst(t *testing.T) {
	input := collection(syntheticConfig)
	other := input.Namespaces[0].ConfigMaps[0]
	other.Metadata.Name = "other-cm"
	other.Metadata.UID = "other-uid"
	other.Data = map[string]string{"mm2.properties": strings.ReplaceAll(syntheticConfig, "mars.synthetic.test", "other.synthetic.test")}
	input.Namespaces[0].ConfigMaps = append(input.Namespaces[0].ConfigMaps, other)
	dep := &input.Namespaces[0].Deployments[0]
	dep.Spec.Template.Spec.Volumes = append(dep.Spec.Template.Spec.Volumes, resource.Volume{Name: "other", ConfigMap: &resource.KeyRef{Name: "other-cm"}})
	regs := []Registration{{ID: "first", Enabled: true, BootstrapServers: []string{"mars.synthetic.test:9092"}}, {ID: "second", Enabled: true, BootstrapServers: []string{"other.synthetic.test:9092"}}}
	snapshot, e := Build(input, regs, nil, nil, Snapshot{})
	if e != nil || len(snapshot.Links) != 1 {
		t.Fatal(e)
	}
	link := snapshot.Links[0]
	if link.State != "unknown" || link.Source.Status != "ambiguous" || len(link.Source.CandidateIDs) != 2 || len(link.Declarations) != 2 {
		t.Fatal("bootstrap conflict silently picked first declaration")
	}
}
func TestAliasIdentityCannotCrossScope(t *testing.T) {
	proof := AliasIdentity{ScopeID: "different-scope", Namespace: "synthetic-ns", WorkloadUID: "deployment-uid", Alias: "mars", ClusterID: "id", Verified: true}
	match := Match("synthetic-scope", "synthetic-ns", "deployment-uid", "mars", nil, []Registration{{ID: "registration", Enabled: true, IdentityVerified: true, DiscoveredClusterID: "id"}}, nil, []AliasIdentity{proof})
	if match.Status == "confirmed" {
		t.Fatal("identity from another scope accepted")
	}
}
