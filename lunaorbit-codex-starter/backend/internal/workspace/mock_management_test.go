package workspace

import (
	"context"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/registry"
	"reflect"
	"testing"
	"time"
)

func TestMockResourceDeleteAndConfigsShareReadState(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	g := NewMockResources(m)
	actor := registry.Actor{ID: "synthetic", Role: "admin"}
	if e := m.CreateTopic(ctx, actor, "one", "synthetic-resource", 1, 1, nil); e != nil {
		t.Fatal(e)
	}
	value := "123456"
	r := kafkaadmin.ConfigResource{Type: "TOPIC", Name: "synthetic-resource"}
	if e := g.AlterConfigs(ctx, actor, "one", r, []kafkaadmin.ConfigChange{{Name: "retention.ms", Operation: "SET", Value: &value}}); e != nil {
		t.Fatal(e)
	}
	state, e := m.TopicConfigs(ctx, actor, "one", r.Name)
	if e != nil || state["retention.ms"] != value {
		t.Fatal("config state split", e)
	}
	if e = g.DeleteTopic(ctx, actor, "one", r.Name); e != nil {
		t.Fatal(e)
	}
	snap, e := m.BindReader("one", "").Read(ctx, "topics", "")
	if e != nil {
		t.Fatal(e)
	}
	for _, topic := range snap.Topics {
		if topic.Name == r.Name {
			t.Fatal("deleted topic remains")
		}
	}
}
func TestMockReassignmentUnionCancelAndSettle(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	g := NewMockResources(m)
	a := registry.Actor{ID: "synthetic", Role: "admin"}
	if e := m.CreateTopic(ctx, a, "one", "migration", 1, 2, nil); e != nil {
		t.Fatal(e)
	}
	original, e := g.TopicState(ctx, a, "one", "migration")
	if e != nil {
		t.Fatal(e)
	}
	old := append([]int32{}, original.Partitions[0].Replicas...)
	target := []int32{2, 3}
	if e = g.Reassign(ctx, a, "one", "migration", []kafkaadmin.ReplicaAssignment{{Partition: 0, Replicas: target}}); e != nil {
		t.Fatal(e)
	}
	active, e := g.Reassignments(ctx, a, "one", "migration")
	if e != nil || len(active) != 1 || !reflect.DeepEqual(active[0].AddingReplicas, []int32{3}) || !reflect.DeepEqual(active[0].RemovingReplicas, []int32{1}) || !reflect.DeepEqual(active[0].Replicas, []int32{1, 2, 3}) {
		t.Fatal(active, e)
	}
	during, e := g.TopicState(ctx, a, "one", "migration")
	if e != nil || !reflect.DeepEqual(during.Partitions[0].Replicas, active[0].Replicas) || !reflect.DeepEqual(during.Partitions[0].ISR, old) {
		t.Fatal(during, e)
	}
	if e = g.CancelReassignments(ctx, a, "one", "migration", []int32{0}); e != nil {
		t.Fatal(e)
	}
	after, e := g.TopicState(ctx, a, "one", "migration")
	if e != nil || !reflect.DeepEqual(after.Partitions[0].Replicas, old) {
		t.Fatal("cancel lost original", after, e)
	}
	if e = g.Reassign(ctx, a, "one", "migration", []kafkaadmin.ReplicaAssignment{{Partition: 0, Replicas: target}}); e != nil {
		t.Fatal(e)
	}
	g.mu.Lock()
	pending := g.pending["one/migration"]
	pending.until = time.Now().Add(-time.Second)
	g.pending["one/migration"] = pending
	g.mu.Unlock()
	active, e = g.Reassignments(ctx, a, "one", "migration")
	if e != nil || len(active) != 0 {
		t.Fatal(active, e)
	}
	after, e = g.TopicState(ctx, a, "one", "migration")
	if e != nil || !reflect.DeepEqual(after.Partitions[0].Replicas, target) || !reflect.DeepEqual(after.Partitions[0].ISR, target) {
		t.Fatal("settle not applied", after, e)
	}
}
func TestMockBrokerRestoreInheritsClusterDefault(t *testing.T) {
	s, a, id, _ := setup(t)
	s.LabAutoApprove = true
	ctx := context.Background()
	g := NewMockResources(NewMock())
	s.Resources = g
	defaultR := ResourceTarget{Type: "BROKER", Name: ""}
	brokerR := ResourceTarget{Type: "BROKER", Name: "1"}
	for _, v := range []struct {
		r     ResourceTarget
		value string
	}{{defaultR, "55555"}, {brokerR, "77777"}} {
		if e := g.AlterConfigs(ctx, a, id, v.r, []kafkaadmin.ConfigChange{{Name: "log.retention.ms", Operation: "SET", Value: &v.value}}); e != nil {
			t.Fatal(e)
		}
	}
	op := createResourceTest(t, s, a, id, ResourceIntent{Kind: "configs", Resource: brokerR, Changes: []kafkaadmin.ConfigChange{{Name: "log.retention.ms", Operation: "DELETE"}}})
	for _, c := range op.Plan.After.Entries {
		if c.Name == "log.retention.ms" && (c.Value == nil || *c.Value != "55555") {
			t.Fatal("preview lost cluster inheritance", c)
		}
	}
	op, e := s.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil {
		t.Fatal(e)
	}
	op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "succeeded" {
		t.Fatal(op, e)
	}
	snap, e := g.DescribeConfigs(ctx, a, id, brokerR)
	if e != nil {
		t.Fatal(e)
	}
	for _, c := range snap.Entries {
		if c.Name == "log.retention.ms" && (c.Value == nil || *c.Value != "55555" || c.Source != "DYNAMIC_DEFAULT_BROKER_CONFIG") {
			t.Fatal(c)
		}
	}
}
