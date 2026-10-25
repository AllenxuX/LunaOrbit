package discovery

import (
	"encoding/json"
	"lunaorbit/backend/internal/discovery/resource"
	"strings"
	"testing"
	"time"
)

func autoDeployment(ns, name, uid string, command []string, image, cm string) resource.Deployment {
	var d resource.Deployment
	d.Metadata = resource.Metadata{Name: name, Namespace: ns, UID: uid, ResourceVersion: "1"}
	c := resource.Container{Name: "mm2", Image: image, Command: command}
	d.Spec.Template.Spec.Containers = []resource.Container{c}
	if cm != "" {
		d.Spec.Template.Spec.Volumes = []resource.Volume{{Name: "cfg", ConfigMap: &resource.KeyRef{Name: cm}}}
	}
	return d
}

func autoCollection(cluster string) resource.Collection {
	cm := resource.ConfigMap{Metadata: resource.Metadata{Name: "mm2-config", Namespace: "luna-kafka", UID: "cm-" + cluster, ResourceVersion: "1"}, Data: map[string]string{"mm2.properties": "clusters=k1,k2\nk1.bootstrap.servers=k1:9092\nk2.bootstrap.servers=k2:9092\nk1->k2.enabled=true\nk1->k2.topics=.*"}}
	mm2 := autoDeployment("luna-kafka", "mm2", "uid-"+cluster, []string{"/opt/kafka/bin/connect-mirror-maker.sh", "/etc/mm2/mm2.properties"}, "apache/kafka:3.9.2", "mm2-config")
	odd := autoDeployment("luna-kafka", "replicator", "odd-"+cluster, nil, "example/mirrormaker:1", "")
	return resource.Collection{
		Scope:              resource.Scope{ID: "k8s-" + cluster, Cluster: cluster, Auto: true},
		ObservedAt:         time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC),
		NamespacesScanned:  3,
		DeploymentsScanned: 7,
		Namespaces: []resource.NamespaceBatch{{Namespace: "luna-kafka", Complete: true, Deployments: []resource.Deployment{mm2, odd}, ConfigMaps: []resource.ConfigMap{cm},
			Signals: map[string][]string{"uid-" + cluster: {"command:connect-mirror-maker.sh"}, "odd-" + cluster: {"image:example/mirrormaker:1"}}}},
	}
}

func TestAutoScopesWithSameNamedMM2DoNotCollide(t *testing.T) {
	a, err := Build(autoCollection("edge-a"), nil, nil, nil, Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Build(autoCollection("edge-b"), nil, nil, nil, Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Links) != 1 || len(b.Links) != 1 || a.Links[0].WorkloadName != "mm2" || a.Links[0].ID == b.Links[0].ID {
		t.Fatalf("links collide: %+v %+v", a.Links, b.Links)
	}
	if a.Links[0].KubeCluster != "edge-a" || b.Links[0].KubeCluster != "edge-b" {
		t.Fatalf("link must carry its Kubernetes cluster: %q %q", a.Links[0].KubeCluster, b.Links[0].KubeCluster)
	}
}

func TestAutoScopeClassifiesConfirmedAndSuspectedMM2(t *testing.T) {
	s, err := Build(autoCollection("edge-a"), nil, nil, nil, Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, w := range s.Workloads {
		got[w.Name] = w.Status
		if w.KubeCluster != "edge-a" || w.Namespace != "luna-kafka" || len(w.Signals) == 0 {
			t.Fatalf("workload identity %+v", w)
		}
	}
	if got["mm2"] != "confirmed" || got["replicator"] != "suspected" || len(got) != 2 {
		t.Fatalf("classification %v", got)
	}
	for _, l := range s.Links {
		if l.WorkloadName == "replicator" {
			t.Fatal("suspected MM2 must not enter the main graph")
		}
	}
	if s.Scan == nil || *s.Scan != (ScanSummary{KubeCluster: "edge-a", Namespaces: 3, Deployments: 7, NamespacesWithMM2: 1, Confirmed: 1, Suspected: 1}) {
		t.Fatalf("scan %+v", s.Scan)
	}
}

func TestLegacyScopeSnapshotHasNoAutoFields(t *testing.T) {
	col := autoCollection("in-cluster")
	col.Scope = resource.Scope{ID: "readonly-lab", Cluster: "in-cluster", Namespaces: []string{"luna-kafka"}}
	col.NamespacesScanned, col.DeploymentsScanned = 0, 0
	for i := range col.Namespaces {
		col.Namespaces[i].Signals = nil
	}
	s, err := Build(col, nil, nil, nil, Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(s)
	for _, key := range []string{`"workloads"`, `"scan"`, `"k8s_cluster"`} {
		if strings.Contains(string(raw), key) {
			t.Fatalf("legacy snapshot gained %s", key)
		}
	}
}
