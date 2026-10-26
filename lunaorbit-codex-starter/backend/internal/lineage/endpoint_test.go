package lineage

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/kafkaops"
	"strings"
	"testing"
	"time"
)

func confirmed(alias, reg string) discovery.Mapping {
	return discovery.Mapping{Alias: alias, Status: "confirmed", RegistrationID: reg, BootstrapAddresses: []string{alias + ":9092"}}
}
func unknownEnd(alias string, addrs ...string) discovery.Mapping {
	return discovery.Mapping{Alias: alias, Status: "unknown", Method: "unmatched", CandidateIDs: []string{}, BootstrapAddresses: addrs}
}
func rule(id string, src, dst discovery.Mapping) discovery.Link {
	return discovery.Link{ID: id, WorkloadName: "mm2-" + id, SourceAlias: src.Alias, TargetAlias: dst.Alias, Source: src, Target: dst, Enabled: true, SemanticsKnown: true, TopicsRaw: ".*", Policy: identityPolicy, Separator: "."}
}

func chainProvider(links ...discovery.Link) fakeProvider {
	return fakeProvider{links: []discovery.Snapshot{{ScopeID: "s", Links: links}}, inventories: map[string]kafkaops.Snapshot{
		"a": {State: "ok", Topics: []kafkaops.Topic{{Name: "orders"}}}, "b": {State: "ok", Topics: []kafkaops.Topic{{Name: "orders"}}}, "c": {State: "ok", Topics: []kafkaops.Topic{{Name: "orders"}}},
	}}
}

func TestLineageContinuesThroughUnreadableEndpoints(t *testing.T) {
	// a -> x (not registered) -> c : the chain must not stop at x.
	p := chainProvider(
		rule("L1", confirmed("a", "a"), unknownEnd("x", "kafka-x.lab:9092")),
		rule("L2", unknownEnd("x", "kafka-x.lab:9092", "kafka-x2.lab:9092"), confirmed("c", "c")),
	)
	g, err := New(p).Trace(context.Background(), Query{"a", "orders", 4, "downstream"})
	if err != nil {
		t.Fatal(err)
	}
	var x *Node
	clusters := map[string]bool{}
	for i, n := range g.Nodes {
		if n.Kind != "topic" {
			continue
		}
		clusters[n.ClusterName] = true
		if strings.HasPrefix(n.ClusterID, "endpoint-") {
			x = &g.Nodes[i]
		}
	}
	if x == nil || !clusters["C"] {
		t.Fatalf("chain stopped at the unregistered endpoint: %+v", g.Nodes)
	}
	if x.Exists != nil || x.ExistenceNote != "Topic 是否存在无法确认" || x.ClusterName != "x · kafka-x.lab:9092" {
		t.Fatalf("unreadable endpoint node %+v", x)
	}
	for _, i := range g.Issues {
		if i.Code == "UNRESOLVED_MAPPING" || i.Code == "ENDPOINT_UNRESOLVABLE" {
			t.Fatalf("no break expected: %+v", i)
		}
	}
}

func TestLineageStopsOnlyWhereTheAddressCannotBeResolved(t *testing.T) {
	p := chainProvider(rule("L1", confirmed("a", "a"), unknownEnd("ghost")))
	g, err := New(p).Trace(context.Background(), Query{"a", "orders", 4, "downstream"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, i := range g.Issues {
		if i.Code == "ENDPOINT_UNRESOLVABLE" && i.ClusterID == "a" && i.LinkID == "L1" && strings.Contains(i.Message, "ghost") {
			found = true
		}
	}
	if !found || len(g.Nodes) != 1 {
		t.Fatalf("expected a stated break at a: %+v %+v", g.Nodes, g.Issues)
	}
}

func TestLineageCanStartFromAnAutoEndpoint(t *testing.T) {
	p := chainProvider(rule("L2", unknownEnd("x", "kafka-x.lab:9092"), confirmed("c", "c")))
	root := EndpointID([]string{"kafka-x.lab:9092"})
	g, err := New(p).Trace(context.Background(), Query{root, "orders", 2, "downstream"})
	if err != nil || len(g.Nodes) < 2 {
		t.Fatalf("%v %+v", err, g.Nodes)
	}
}

func TestCatalogAndLineageAgreeOnEndpointIDs(t *testing.T) {
	for _, set := range [][]string{{"b:9092", "A:9092"}, {"kafka-x.lab:9092"}} {
		if discovery.LineageEndpointID(set) != EndpointID(set) {
			t.Fatalf("%v", set)
		}
	}
}

func TestLineageIgnoresRemovedMM2(t *testing.T) {
	old := rule("OLD", confirmed("a", "a"), confirmed("b", "b"))
	old.Removed = true
	p := chainProvider(old, rule("L2", confirmed("a", "a"), confirmed("c", "c")))
	g, err := New(p).Trace(context.Background(), Query{"a", "orders", 2, "downstream"})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range g.Nodes {
		if n.ClusterID == "b" || n.LinkID == "OLD" {
			t.Fatalf("removed MM2 traced: %+v", n)
		}
	}
	if len(g.Edges) == 0 {
		t.Fatal("current MM2 must still be traced")
	}
}

type readableEndpoints struct {
	fakeProvider
	topics map[string][]string
	reads  int
}

func (r *readableEndpoints) EndpointTopics(_ context.Context, id string) ([]string, time.Time, error) {
	r.reads++
	if t, ok := r.topics[id]; ok {
		return t, time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC), nil
	}
	return nil, time.Time{}, errors.New("unreadable")
}

func TestReadableAutoEndpointsConfirmTopicExistence(t *testing.T) {
	src, mid := unknownEnd("LAB-SRC", "lab-src:9092"), unknownEnd("LAB-MID", "lab-mid:9092")
	dst := unknownEnd("LAB-DST", "lab-dst:9092")
	base := chainProvider(rule("L1", src, mid), rule("L2", mid, dst))
	p := &readableEndpoints{fakeProvider: base, topics: map[string][]string{
		EndpointID([]string{"lab-src:9092"}): {"derivatives.test1"},
		EndpointID([]string{"lab-mid:9092"}): {"other"},
	}}
	g, err := New(p).Trace(context.Background(), Query{EndpointID([]string{"lab-src:9092"}), "derivatives.test1", 4, "downstream"})
	if err != nil {
		t.Fatal(err)
	}
	byCluster := map[string]Node{}
	for _, n := range g.Nodes {
		if n.Kind == "topic" {
			byCluster[n.ClusterName] = n
		}
	}
	srcNode, midNode, dstNode := byCluster["LAB-SRC · lab-src:9092"], byCluster["LAB-MID · lab-mid:9092"], byCluster["LAB-DST · lab-dst:9092"]
	if srcNode.Exists == nil || !*srcNode.Exists || srcNode.ExistenceNote != "" || srcNode.ObservedAt.IsZero() || srcNode.Evidence != "observed" {
		t.Fatalf("readable source must be confirmed: %+v", srcNode)
	}
	if midNode.Exists == nil || *midNode.Exists {
		t.Fatalf("readable endpoint without the topic must say it does not exist: %+v", midNode)
	}
	if dstNode.Exists != nil || dstNode.ExistenceNote != "Topic 是否存在无法确认" {
		t.Fatalf("unreadable endpoint stays unknown: %+v", dstNode)
	}
}
