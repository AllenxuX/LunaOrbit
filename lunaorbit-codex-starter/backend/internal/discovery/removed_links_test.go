package discovery

import (
	"fmt"
	"lunaorbit/backend/internal/discovery/resource"
	"strings"
	"testing"
)

// 12 MM2 Deployments exist now; 10 more were renamed away and only live in the previous snapshot.
func renamedCollection(names []string) resource.Collection {
	col := autoCollection("edge-a")
	batch := &col.Namespaces[0]
	batch.Deployments, batch.ConfigMaps, batch.Signals = nil, nil, map[string][]string{}
	for _, name := range names {
		cm := resource.ConfigMap{Metadata: resource.Metadata{Name: name + "-cm", Namespace: "luna-kafka", UID: "cm-" + name, ResourceVersion: "1"},
			Data: map[string]string{"mm2.properties": fmt.Sprintf("clusters=a%[1]s,b%[1]s\na%[1]s.bootstrap.servers=a%[1]s:9092\nb%[1]s.bootstrap.servers=b%[1]s:9092\na%[1]s->b%[1]s.enabled=true\n", strings.ReplaceAll(name, "-", ""))}}
		d := autoDeployment("luna-kafka", name, "uid-"+name, []string{"/opt/kafka/bin/connect-mirror-maker.sh", "/etc/mm2/mm2.properties"}, "apache/kafka:3.9.2", name+"-cm")
		batch.Deployments = append(batch.Deployments, d)
		batch.ConfigMaps = append(batch.ConfigMaps, cm)
		batch.Signals["uid-"+name] = []string{"command:connect-mirror-maker.sh"}
	}
	return col
}

func names(prefix string, n int) []string {
	out := []string{}
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("%s-%02d", prefix, i))
	}
	return out
}

func TestRemovedMM2AreMarkedNotCountedAndDoNotMakeTheSnapshotStale(t *testing.T) {
	old, err := Build(renamedCollection(append(names("old", 10), names("mm2", 12)...)), nil, nil, nil, Snapshot{})
	if err != nil || len(old.Links) != 22 {
		t.Fatalf("seed %v %d", err, len(old.Links))
	}
	now, err := Build(renamedCollection(names("mm2", 12)), nil, nil, nil, old)
	if err != nil {
		t.Fatal(err)
	}
	if now.Stale {
		t.Fatal("removed Deployments must not make a complete collection stale")
	}
	current, removed := 0, 0
	for _, l := range now.Links {
		if l.Removed {
			removed++
			if l.State != "unknown" {
				t.Fatalf("removed link state %s", l.State)
			}
		} else {
			current++
			if l.Stale {
				t.Fatalf("current link %s must stay fresh", l.WorkloadName)
			}
		}
	}
	if current != 12 || removed != 10 {
		t.Fatalf("current=%d removed=%d", current, removed)
	}
	workloads := 0
	for _, n := range now.Nodes {
		if n.Kind == "workload" {
			workloads++
		}
	}
	if workloads != 12 {
		t.Fatalf("graph must only draw existing MM2, got %d workloads", workloads)
	}
	for _, e := range now.Edges {
		for _, l := range now.Links {
			if l.Removed && e.LinkID == l.ID {
				t.Fatal("removed link drawn as edge")
			}
		}
	}
}

func TestFailedCollectionStillKeepsEvidenceAsStale(t *testing.T) {
	old, _ := Build(renamedCollection(names("mm2", 3)), nil, nil, nil, Snapshot{})
	col := renamedCollection(names("mm2", 2))
	col.Namespaces[0].Complete = false
	now, _ := Build(col, nil, nil, nil, old)
	if !now.Stale {
		t.Fatal("an incomplete collection must stay stale")
	}
	for _, l := range now.Links {
		if l.Removed {
			t.Fatal("cannot claim removal when the collection failed")
		}
	}
}
