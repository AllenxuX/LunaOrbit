package lineage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/information"
	"lunaorbit/backend/internal/kafkaops"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func link(name, source, target, alias, rule, policy string) discovery.Link {
	return discovery.Link{ID: name, WorkloadName: name, SourceAlias: alias, TargetAlias: target, TopicsRaw: rule, Policy: policy, Separator: ".", SemanticsKnown: true, Enabled: true, Source: discovery.Mapping{Status: "confirmed", RegistrationID: source}, Target: discovery.Mapping{Status: "confirmed", RegistrationID: target}}
}

const defaultPolicy = "org.apache.kafka.connect.mirror.DefaultReplicationPolicy"
const identityPolicy = "org.apache.kafka.connect.mirror.IdentityReplicationPolicy"

func inventoryOf(names ...string) kafkaops.Snapshot {
	s := kafkaops.Snapshot{ObservedAt: time.Now().UTC()}
	for _, n := range names {
		s.Topics = append(s.Topics, kafkaops.Topic{Name: n})
	}
	return s
}
func fixture() fakeProvider {
	return fakeProvider{inventories: map[string]kafkaops.Snapshot{"a": inventoryOf("orders"), "b": inventoryOf("source.orders", "orders"), "c": inventoryOf("target.source.orders", "source.orders")}, links: []discovery.Snapshot{{ScopeID: "scope", Links: []discovery.Link{link("ab", "a", "b", "source", "orders", defaultPolicy), link("bc", "b", "c", "target", "source.orders", defaultPolicy), link("ac", "a", "c", "source", "orders", defaultPolicy)}}}}
}
func hasTopic(g Graph, c, t string) bool {
	for _, n := range g.Nodes {
		if n.Kind == "topic" && n.ClusterID == c && n.Topic == t {
			return true
		}
	}
	return false
}
func issueCode(g Graph, code string) bool {
	for _, i := range g.Issues {
		if i.Code == code {
			return true
		}
	}
	return false
}
func TestMultiHopActualNameAndReverse(t *testing.T) {
	p := fixture()
	for _, q := range []Query{{"a", "orders", 2, "downstream"}, {"c", "target.source.orders", 2, "upstream"}} {
		g, e := New(p).Trace(context.Background(), q)
		if e != nil {
			t.Fatal(e)
		}
		if !hasTopic(g, "a", "orders") || !hasTopic(g, "b", "source.orders") || !hasTopic(g, "c", "target.source.orders") {
			t.Fatalf("wrong paths: %+v", g)
		}
		if hasTopic(g, "b", "orders") {
			t.Fatal("merged same-name inventory")
		}
		for _, edge := range g.Edges {
			if edge.LinkID != "" && edge.Evidence != "configured" {
				t.Fatal("existence promoted configured evidence")
			}
		}
	}
}
func TestExcludedCustomStaleAndConflict(t *testing.T) {
	for _, kind := range []string{"exclude", "custom", "stale", "old", "conflict", "unsupported"} {
		t.Run(kind, func(t *testing.T) {
			p := fixture()
			p.links[0].Links = p.links[0].Links[:1]
			l := &p.links[0].Links[0]
			switch kind {
			case "exclude":
				l.TopicsExcludeRaw = "orders"
			case "custom":
				l.Policy = "custom.IdentityReplicationPolicy"
			case "stale":
				l.Stale = true
			case "old":
				l.SemanticsKnown = false
			case "unsupported":
				l.TopicsRaw = "orders,(?=x)x"
			case "conflict":
				l.Declarations = []discovery.Declaration{{SemanticsKnown: true, TopicsRaw: "other"}}
			}
			g, e := New(p).Trace(context.Background(), Query{"a", "orders", 2, "both"})
			if e != nil {
				t.Fatal(e)
			}
			if len(g.Edges) != 0 {
				t.Fatalf("guessed relationship: %+v", g)
			}
			if kind != "exclude" && len(g.Issues) == 0 {
				t.Fatal("missing unknown issue")
			}
		})
	}
}
func TestDefaultCycleAndIdentity(t *testing.T) {
	p := fixture()
	p.links[0].Links = append(p.links[0].Links[:1], link("ba", "b", "a", "target", ".*", defaultPolicy))
	p.links[0].Links[1].TargetAlias = "source"
	g, e := New(p).Trace(context.Background(), Query{"a", "orders", 4, "downstream"})
	if e != nil || hasTopic(g, "a", "target.source.orders") || !issueCode(g, "REPLICATION_CYCLE") {
		t.Fatalf("cycle: %+v %v", g, e)
	}
	for i := range p.links[0].Links {
		p.links[0].Links[i].Policy = identityPolicy
	}
	g, e = New(p).Trace(context.Background(), Query{"a", "orders", 4, "downstream"})
	if e != nil || !hasTopic(g, "b", "orders") || len(g.Nodes) > 4 || !issueCode(g, "REPLICATION_CYCLE") {
		t.Fatalf("identity loop: %+v %v", g, e)
	}
}
func TestMissingTargetAndManual(t *testing.T) {
	p := fixture()
	p.inventories["b"] = inventoryOf()
	p.apps = []information.Application{{ID: "producer", Name: "producer", Relations: []information.Relation{{ClusterID: "a", Topic: "orders", Mode: "produce"}}}, {ID: "consumer", Name: "consumer", Relations: []information.Relation{{ClusterID: "b", Topic: "source.orders", Mode: "consume", ConsumerGroup: "g"}}}}
	g, e := New(p).Trace(context.Background(), Query{"a", "orders", 2, "both"})
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, n := range g.Nodes {
		if n.ClusterID == "b" && n.Topic == "source.orders" {
			found = n.Exists != nil && !*n.Exists
		}
	}
	if !found {
		t.Fatal("missing configured target not explicit")
	}
	manual := 0
	for _, e := range g.Edges {
		if e.Evidence == "manual" {
			manual++
		}
	}
	if manual != 3 {
		t.Fatalf("manual edges %d", manual)
	}
}
func TestNodeBound(t *testing.T) {
	p := fixture()
	p.links = nil
	for i := 0; i < 250; i++ {
		p.apps = append(p.apps, information.Application{ID: fmt.Sprint(i), Name: "app", Relations: []information.Relation{{ClusterID: "a", Topic: "orders", Mode: "produce"}}})
	}
	g, e := New(p).Trace(context.Background(), Query{"a", "orders", 2, "both"})
	if e != nil || !g.Truncated || len(g.Nodes) > 200 || len(g.Edges) > 400 {
		t.Fatalf("bounds: %+v %v", g, e)
	}
}
func TestHTTPContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	Mount(r.Group("/api/v1"), New(fixture()))
	for _, tc := range []struct {
		path   string
		status int
	}{{"?cluster_id=a&topic=orders", 200}, {"?cluster_id=a&topic=missing", 404}, {"?cluster_id=a&topic=orders&depth=5", 400}, {"?cluster_id=a&topic=orders&direction=invalid", 400}} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/topic-lineage"+tc.path, nil))
		if w.Code != tc.status {
			t.Fatalf("%s got %d %s", tc.path, w.Code, w.Body)
		}
		if w.Code == 200 {
			var g Graph
			if e := json.Unmarshal(w.Body.Bytes(), &g); e != nil || g.RootID == "" || g.Depth != 2 {
				t.Fatal("contract", e, w.Body)
			}
		}
	}
}
func TestConsumerEvidenceAndManualSeparation(t *testing.T) {
	p := fixture()
	p.links = nil
	p.apps = []information.Application{{ID: "app", Name: "app", Relations: []information.Relation{{ClusterID: "a", Topic: "orders", Mode: "consume", ConsumerGroup: "active"}}}}
	committed := int64(12)
	p.reads = func(c, k, r string) (kafkaops.Snapshot, error) {
		switch k {
		case "topics":
			return p.inventories[c], nil
		case "groups":
			if r == "" {
				return kafkaops.Snapshot{Groups: []kafkaops.Group{{ID: "active"}, {ID: "old"}}}, nil
			}
			return kafkaops.Snapshot{ObservedAt: time.Now().UTC(), Groups: []kafkaops.Group{{ID: r, AssignmentsKnown: true, SubscriptionsKnown: true, AssignedTopics: func() []string {
				if r == "active" {
					return []string{"orders"}
				}
				return nil
			}()}}}, nil
		case "offsets":
			return kafkaops.Snapshot{ObservedAt: time.Now().UTC(), Offsets: []kafkaops.Offset{{Group: r, Topic: "orders", Committed: &committed}}}, nil
		}
		return kafkaops.Snapshot{}, nil
	}
	g, e := New(p).Trace(context.Background(), Query{"a", "orders", 2, "both"})
	if e != nil {
		t.Fatal(e)
	}
	counts := map[string]int{}
	for _, edge := range g.Edges {
		counts[edge.Evidence]++
	}
	if counts["observed"] != 1 || counts["historical"] != 1 || counts["manual"] != 2 {
		t.Fatalf("evidence conflated: %+v", g)
	}
}
func TestGroupBudget(t *testing.T) {
	p := fixture()
	p.links = nil
	calls := 0
	p.reads = func(c, k, r string) (kafkaops.Snapshot, error) {
		calls++
		if k == "topics" {
			return p.inventories[c], nil
		}
		if k == "groups" && r == "" {
			s := kafkaops.Snapshot{}
			for i := 0; i < 25; i++ {
				s.Groups = append(s.Groups, kafkaops.Group{ID: fmt.Sprint(i)})
			}
			return s, nil
		}
		return kafkaops.Snapshot{}, nil
	}
	g, e := New(p).Trace(context.Background(), Query{"a", "orders", 2, "both"})
	if e != nil || !g.Truncated || !issueCode(g, "GROUPS_TRUNCATED") || calls > 26 {
		t.Fatalf("consumer budget %d %+v %v", calls, g, e)
	}
}
func TestPolicyInternalVetoIndependentOfExclude(t *testing.T) {
	for _, policy := range []string{defaultPolicy, identityPolicy} {
		for _, topic := range []string{"__consumer_offsets", ".hidden", "foo.internal", "foo-internal", "foo_internal"} {
			t.Run(policy+topic, func(t *testing.T) {
				p := fixture()
				p.inventories["a"] = inventoryOf(topic)
				l := link("ab", "a", "b", "source", ".*", policy)
				l.Separator = "_"
				l.InternalSeparatorEnabled = true
				l.TopicsExcludeRaw = ""
				p.links[0].Links = []discovery.Link{l}
				g, e := New(p).Trace(context.Background(), Query{"a", topic, 2, "downstream"})
				if e != nil || len(g.Edges) != 0 {
					t.Fatalf("internal topic replicated: %+v %v", g, e)
				}
			})
		}
	}
	if internalTopic("foo_internal", "_", false) {
		t.Fatal("disabled custom internal separator still vetoed")
	}
	if internalTopic("foo.replica", ".", true) {
		t.Fatal("replica wrongly treated as policy internal")
	}
}
func TestDefaultHeartbeatBypassesFilter(t *testing.T) {
	p := fixture()
	p.inventories["a"] = inventoryOf("heartbeats")
	l := link("ab", "a", "b", "source", "orders", defaultPolicy)
	l.TopicsExcludeRaw = ".*"
	p.links[0].Links = []discovery.Link{l}
	g, e := New(p).Trace(context.Background(), Query{"a", "heartbeats", 2, "downstream"})
	if e != nil || !hasTopic(g, "b", "source.heartbeats") {
		t.Fatalf("heartbeat mapping missing %+v %v", g, e)
	}
}
func TestStaleCurrentConsumerNotObserved(t *testing.T) {
	p := fixture()
	p.links = nil
	p.reads = func(c, k, r string) (kafkaops.Snapshot, error) {
		if k == "topics" {
			return p.inventories[c], nil
		}
		if k == "groups" {
			if r == "" {
				return kafkaops.Snapshot{Groups: []kafkaops.Group{{ID: "g"}}}, nil
			}
			return kafkaops.Snapshot{Stale: true, Groups: []kafkaops.Group{{ID: "g", AssignmentsKnown: true, AssignedTopics: []string{"orders"}}}}, nil
		}
		return kafkaops.Snapshot{}, nil
	}
	g, e := New(p).Trace(context.Background(), Query{"a", "orders", 2, "both"})
	if e != nil || !issueCode(g, "GROUP_EVIDENCE_UNKNOWN") {
		t.Fatalf("stale issue missing %+v %v", g, e)
	}
	for _, edge := range g.Edges {
		if edge.Evidence == "observed" {
			t.Fatal("stale membership promoted to current")
		}
	}
}
func TestScopedLinkAndVisitBudgets(t *testing.T) {
	p := fixture()
	p.links = []discovery.Snapshot{{ScopeID: "large"}}
	for i := 0; i < 5000; i++ {
		l := link(fmt.Sprint(i), "a", "b", "source", "orders", identityPolicy)
		p.links[0].Links = append(p.links[0].Links, l)
	}
	started := time.Now()
	g, e := New(p).Trace(context.Background(), Query{"a", "orders", 4, "downstream"})
	if e != nil || !g.Truncated || !issueCode(g, "LINKS_TRUNCATED") || time.Since(started) > time.Second {
		t.Fatalf("large snapshot was not bounded: elapsed=%v err=%v issues=%+v", time.Since(started), e, g.Issues)
	}
	p = fixture()
	p.links = []discovery.Snapshot{{ScopeID: "branch"}}
	for i := 0; i < 4093; i++ {
		l := link(fmt.Sprint(i), "unrelated", "unrelated-target", "source", ".*", defaultPolicy)
		p.links[0].Links = append(p.links[0].Links, l)
	}
	p.links[0].Links = append(p.links[0].Links, link("ab1", "a", "b", "one", "orders", defaultPolicy), link("ab2", "a", "b", "two", "orders", defaultPolicy), link("bc", "b", "c", "middle", ".*", defaultPolicy))
	g, e = New(p).Trace(context.Background(), Query{"a", "orders", 4, "downstream"})
	if e != nil || !g.Truncated || !issueCode(g, "TRAVERSAL_TRUNCATED") {
		t.Fatalf("visit budget missing: %v %+v", e, g.Issues)
	}
}
func TestCanceledTraversalPreservesPartialRoot(t *testing.T) {
	p := fixture()
	p.links = []discovery.Snapshot{{ScopeID: "large"}}
	for i := 0; i < 5000; i++ {
		p.links[0].Links = append(p.links[0].Links, link(fmt.Sprint(i), "a", "b", "source", "orders", defaultPolicy))
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.reads = func(c, k, r string) (kafkaops.Snapshot, error) {
		if k == "topics" {
			cancel()
			return p.inventories[c], nil
		}
		return kafkaops.Snapshot{}, ctx.Err()
	}
	started := time.Now()
	g, e := New(p).Trace(ctx, Query{"a", "orders", 4, "upstream"})
	if e != nil || !g.Truncated || !hasTopic(g, "a", "orders") || len(g.Edges) != 0 || !issueCode(g, "DEADLINE_REACHED") || time.Since(started) > time.Second {
		t.Fatalf("canceled traversal: elapsed=%v err=%v graph=%+v", time.Since(started), e, g)
	}
}

func TestConnectWorkerGroupsAreNotBusinessConsumers(t *testing.T) {
	p := fixture()
	p.links = nil
	p.reads = func(c, k, r string) (kafkaops.Snapshot, error) {
		if k == "topics" {
			return p.inventories[c], nil
		}
		if k == "groups" && r == "" {
			return kafkaops.Snapshot{Groups: []kafkaops.Group{{ID: "source-mm2"}}}, nil
		}
		if k == "groups" {
			return kafkaops.Snapshot{Groups: []kafkaops.Group{{ID: r, ProtocolType: "connect", EvidenceIssue: "unsupported_group_protocol"}}}, nil
		}
		t.Fatal("Connect worker group should not be read as business offsets")
		return kafkaops.Snapshot{}, nil
	}
	g, e := New(p).Trace(context.Background(), Query{"a", "orders", 2, "both"})
	if e != nil || issueCode(g, "GROUP_EVIDENCE_UNKNOWN") {
		t.Fatal("Connect group generated business warning", g, e)
	}
}

func TestIdentityDAGConvergenceAndReverseAreNotCycles(t *testing.T) {
	p := fixture()
	p.inventories["c"] = inventoryOf("orders")
	for i := range p.links[0].Links {
		p.links[0].Links[i].Policy = identityPolicy
		p.links[0].Links[i].TopicsRaw = "orders"
	}
	for _, direction := range []string{"both", "upstream", "downstream"} {
		root := "a"
		if direction == "upstream" {
			root = "c"
		}
		g, e := New(p).Trace(context.Background(), Query{root, "orders", 4, direction})
		if e != nil || issueCode(g, "REPLICATION_CYCLE") {
			t.Fatalf("DAG traversal mislabeled as cycle (%s): %+v %v", direction, g.Issues, e)
		}
		for _, c := range []string{"a", "b", "c"} {
			if !hasTopic(g, c, "orders") {
				t.Fatal("same-named cluster node missing", c)
			}
		}
	}
}

func TestUnknownGroupIssueNamesClusterAndGroup(t *testing.T) {
	p := fixture()
	p.links = nil
	p.reads = func(c, k, r string) (kafkaops.Snapshot, error) {
		if k == "topics" {
			return p.inventories[c], nil
		}
		if k == "groups" && r == "" {
			return kafkaops.Snapshot{Groups: []kafkaops.Group{{ID: "business-group"}}}, nil
		}
		return kafkaops.Snapshot{Groups: []kafkaops.Group{{ID: r, ProtocolType: "custom", EvidenceIssue: "unsupported_group_protocol"}}}, nil
	}
	g, e := New(p).Trace(context.Background(), Query{"a", "orders", 2, "both"})
	if e != nil {
		t.Fatal(e)
	}
	for _, i := range g.Issues {
		if i.Code == "GROUP_EVIDENCE_UNKNOWN" {
			if !strings.Contains(i.Message, "A / business-group") {
				t.Fatal("ambiguous issue", i)
			}
			return
		}
	}
	t.Fatal("real unknown issue hidden")
}
