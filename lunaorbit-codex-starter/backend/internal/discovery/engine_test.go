package discovery

import (
	"encoding/json"
	"fmt"
	"lunaorbit/backend/internal/discovery/resource"
	"strings"
	"testing"
	"time"
)

func collection(text string) resource.Collection {
	dep := resource.Deployment{Metadata: resource.Metadata{Name: "synthetic-mm2", Namespace: "synthetic-ns", UID: "deployment-uid", ResourceVersion: "7", Generation: 2}}
	dep.Spec.Template.Spec.Containers = []resource.Container{{Name: "mirror", Image: "synthetic/mm2:test"}}
	dep.Spec.Template.Spec.Volumes = []resource.Volume{{Name: "config", ConfigMap: &resource.KeyRef{Name: "synthetic-cm"}}}
	dep.Status.ObservedGeneration = 2
	dep.Status.Replicas = 1
	dep.Status.ReadyReplicas = 1
	cm := resource.ConfigMap{Metadata: resource.Metadata{Name: "synthetic-cm", Namespace: "synthetic-ns", UID: "config-uid", ResourceVersion: "9"}, Data: map[string]string{"mm2.properties": text}}
	return resource.Collection{Scope: resource.Scope{ID: "synthetic-scope", Synthetic: true}, ObservedAt: time.Now().UTC(), Namespaces: []resource.NamespaceBatch{{Namespace: "synthetic-ns", Complete: true, Deployments: []resource.Deployment{dep}, ConfigMaps: []resource.ConfigMap{cm}}}}
}

const syntheticConfig = "clusters=mars,moon-prod\nmars.bootstrap.servers=mars.synthetic.test:9092\nmoon-prod.bootstrap.servers=moon.synthetic.test:9092\nmars->moon-prod.enabled=true\nmars->moon-prod.topics=orders.*,(audit|logs).*\nmars.sasl.jaas.config=synthetic-do-not-expose\nssl.key.password=synthetic-do-not-expose\n"

func TestDiscoveryEvidenceStateAndSecretBoundary(t *testing.T) {
	input := collection(syntheticConfig)
	regs := []Registration{{ID: "mars-registration", BootstrapServers: []string{"mars.synthetic.test:9092"}, Enabled: true}, {ID: "moon-registration", BootstrapServers: []string{"moon.synthetic.test:9092"}, Enabled: true}}
	snapshot, e := Build(input, regs, nil, nil, Snapshot{})
	if e != nil {
		t.Fatal(e)
	}
	if len(snapshot.Links) != 1 {
		t.Fatalf("expected link: %+v", snapshot)
	}
	link := snapshot.Links[0]
	if !link.Declared || !link.Observed || link.Verified || link.Stale || link.TopicsRaw != "orders.*,(audit|logs).*" {
		t.Fatal("truth states conflated")
	}
	if link.Target.Status != "candidate" || link.Target.RegistrationID != "" || len(link.Evidence) != 1 || link.Evidence[0].ResourceVersion != "9" {
		t.Fatal("missing candidate or evidence")
	}
	body, _ := json.Marshal(snapshot)
	if strings.Contains(string(body), "synthetic-do-not-expose") || strings.Contains(string(body), "jaas") {
		t.Fatal("snapshot leaked raw config")
	}
	for _, node := range snapshot.Nodes {
		if node.Kind == "kafka" && (node.ReplicationFactor != nil || node.RFStatus != "unknown") {
			t.Fatal("RF inferred")
		}
	}
	if strings.Contains(fmt.Sprintf("%+v", input.Namespaces[0].ConfigMaps[0]), "synthetic-do-not-expose") {
		t.Fatal("raw input printed")
	}
}

func TestInsufficientConfigAndStaleRetention(t *testing.T) {
	first, e := Build(collection(syntheticConfig), nil, nil, nil, Snapshot{})
	if e != nil {
		t.Fatal(e)
	}
	failed := collection("")
	failed.ObservedAt = first.ObservedAt.Add(time.Minute)
	failed.Namespaces[0].Complete = false
	failed.Namespaces[0].ErrorCode = "PERMISSION_DENIED"
	stale, e := Build(failed, nil, nil, nil, first)
	if e != nil {
		t.Fatal(e)
	}
	if len(stale.Links) != 1 || !stale.Stale || !stale.Links[0].Stale || stale.Links[0].State != "unknown" || !stale.Links[0].ObservedAt.Equal(first.Links[0].ObservedAt) {
		t.Fatal("failed collection erased or refreshed evidence")
	}
	empty := collection("topics=orders.*\n")
	result, e := Build(empty, nil, nil, nil, Snapshot{})
	if e != nil || len(result.Links) != 0 {
		t.Fatal("topics alone confirmed MM2")
	}
}

func TestMappingIdentityPriorityAndAmbiguity(t *testing.T) {
	regs := []Registration{{ID: "address-candidate", BootstrapServers: []string{"old.synthetic.test:9092"}, Enabled: true}, {ID: "verified-id", DiscoveredClusterID: "synthetic-cluster-id", IdentityVerified: true, Enabled: true}}
	proof := AliasIdentity{ScopeID: "synthetic-scope", Namespace: "synthetic-ns", WorkloadUID: "deployment-uid", Alias: "mars", ClusterID: "synthetic-cluster-id", Verified: true}
	m := Match("synthetic-scope", "synthetic-ns", "deployment-uid", "mars", []string{"old.synthetic.test:9092"}, regs, nil, []AliasIdentity{proof})
	if m.RegistrationID != "verified-id" || m.Method != "cluster_id" {
		t.Fatal("verified identity lost to address")
	}
	proof.Synthetic = true
	m = Match("synthetic-scope", "synthetic-ns", "deployment-uid", "mars", []string{"old.synthetic.test:9092"}, regs, nil, []AliasIdentity{proof})
	if m.Status != "candidate" || m.RegistrationID != "" {
		t.Fatal("synthetic proof accepted as real")
	}
	regs = append(regs, Registration{ID: "second-candidate", BootstrapServers: []string{"old.synthetic.test:9092"}, Enabled: true})
	m = Match("synthetic-scope", "synthetic-ns", "deployment-uid", "mars", []string{"old.synthetic.test:9092"}, regs, nil, nil)
	if m.Status != "ambiguous" || len(m.CandidateIDs) != 2 {
		t.Fatal("ambiguous registrations merged")
	}
	binding := ManualBinding{ScopeID: "synthetic-scope", Namespace: "synthetic-ns", WorkloadUID: "deployment-uid", Alias: "mars", RegistrationID: "second-candidate", ApprovedBy: "synthetic-owner", ApprovedAt: time.Now()}
	m = Match("synthetic-scope", "synthetic-ns", "deployment-uid", "mars", nil, regs, []ManualBinding{binding}, nil)
	if m.Method != "manual" || m.RegistrationID != "second-candidate" {
		t.Fatal("authorized manual binding ignored")
	}
}

func TestBusinessNotesAndHistoricalName(t *testing.T) {
	input := collection(syntheticConfig + "clusters=mars,charon,moon-prod,styx,saturn-uat\ncharon->moon-prod.enabled=true\ncharon->moon-prod.topics=orders.*\nstyx->saturn-uat.enabled=true\nstyx->saturn-uat.topics=uat.*\n")
	// Avoid a duplicate clusters key: ambiguity is tested separately by the parser.
	input.Namespaces[0].ConfigMaps[0].Data["mm2.properties"] = strings.Replace(input.Namespaces[0].ConfigMaps[0].Data["mm2.properties"], "clusters=mars,moon-prod\n", "", 1)
	input.Namespaces[0].Deployments[0].Metadata.Name = "styx2saturn-res"
	snapshot, e := Build(input, nil, nil, nil, Snapshot{})
	if e != nil || len(snapshot.Links) != 3 {
		t.Fatalf("business fixture: %v", e)
	}
	for _, link := range snapshot.Links {
		if link.SourceAlias == "styx" && link.TargetAlias != "saturn-uat" {
			t.Fatal("historical name overwrote route")
		}
		if link.TargetAlias == "moon-prod" && !strings.Contains(strings.Join(link.Notes, " "), "approved_multi_source") {
			t.Fatal("approved dual source missing")
		}
		if link.Verified {
			t.Fatal("business approval treated as data verification")
		}
	}
}
func TestLineageSemanticsProjectionAndLegacyUnknown(t *testing.T) {
	snapshot, e := Build(collection(syntheticConfig+"topics.exclude=private.*\nreplication.policy.separator=_\n"), nil, nil, nil, Snapshot{})
	if e != nil {
		t.Fatal(e)
	}
	link := snapshot.Links[0]
	if !link.SemanticsKnown || link.TopicsExcludeRaw != "private.*" || link.Separator != "_" || !link.Declarations[0].SemanticsKnown || link.Declarations[0].Separator != "_" {
		t.Fatalf("%+v", link)
	}
	var old Link
	if e := json.Unmarshal([]byte(`{"topics_raw":".*","policy":"org.apache.kafka.connect.mirror.DefaultReplicationPolicy"}`), &old); e != nil {
		t.Fatal(e)
	}
	if old.SemanticsKnown {
		t.Fatal("legacy snapshot guessed known")
	}
}
func TestInternalSeparatorFlagProjection(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		config := syntheticConfig
		if !enabled {
			config += "replication.policy.internal.topic.separator.enabled=false\n"
		}
		snapshot, e := Build(collection(config), nil, nil, nil, Snapshot{})
		if e != nil {
			t.Fatal(e)
		}
		link := snapshot.Links[0]
		if link.InternalSeparatorEnabled != enabled || link.Declarations[0].InternalSeparatorEnabled != enabled || !link.SemanticsKnown {
			t.Fatalf("%+v", link)
		}
	}
}
