package workspace

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mockTopicDefaults = map[string]string{"retention.ms": "604800000", "retention.bytes": "-1", "cleanup.policy": "delete", "min.insync.replicas": "1", "max.message.bytes": "1048588", "segment.bytes": "1073741824", "compression.type": "producer"}
var mockBrokerDefaults = map[string]string{"log.retention.ms": "604800000", "num.network.threads": "3", "message.max.bytes": "1048588", "leader.replication.throttled.rate": "9223372036854775807", "follower.replication.throttled.rate": "9223372036854775807", "delete.topic.enable": "true", "broker.id": "1"}

type mockPendingAssignment struct {
	until       time.Time
	assignments []kafkaadmin.ReplicaAssignment
	originals   map[int32][]int32
}
type MockResources struct {
	mock    *Mock
	mu      sync.Mutex
	brokers map[string]map[string]map[string]string
	pending map[string]mockPendingAssignment
}

func NewMockResources(m *Mock) *MockResources {
	return &MockResources{mock: m, brokers: map[string]map[string]map[string]string{}, pending: map[string]mockPendingAssignment{}}
}
func pointer(v string) *string { return &v }
func (m *MockResources) DescribeConfigs(_ context.Context, _ registry.Actor, id string, r kafkaadmin.ConfigResource) (kafkaadmin.ConfigSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mock.mu.Lock()
	defer m.mock.mu.Unlock()
	m.mock.seed(id)
	defaults := mockTopicDefaults
	overrides := m.mock.topics[id][r.Name]
	source := "DYNAMIC_TOPIC_CONFIG"
	if r.Type == "BROKER" {
		if r.Name != "" {
			n, e := strconv.Atoi(r.Name)
			if e != nil || n < 1 || n > 3 {
				return kafkaadmin.ConfigSnapshot{}, errors.New("NOT_FOUND")
			}
		}
		defaults = mockBrokerDefaults
		source = "DYNAMIC_BROKER_CONFIG"
		if m.brokers[id] == nil {
			m.brokers[id] = map[string]map[string]string{}
		}
		overrides = m.brokers[id][r.Name]
		if r.Name == "" {
			source = "DYNAMIC_DEFAULT_BROKER_CONFIG"
		}
	} else if r.Type != "TOPIC" || overrides == nil {
		return kafkaadmin.ConfigSnapshot{}, errors.New("NOT_FOUND")
	}
	out := kafkaadmin.ConfigSnapshot{Resource: r, Entries: []kafkaadmin.ConfigEntry{}}
	for name, base := range defaults {
		entry := kafkaadmin.ConfigEntry{Name: name, Value: pointer(base), DefaultValue: pointer(base), Source: "DEFAULT_CONFIG", Type: "STRING", Documentation: "合成配置元数据", Synonyms: []kafkaadmin.ConfigSynonym{{Name: name, Value: pointer(base), Source: "DEFAULT_CONFIG"}}}
		if strings.HasSuffix(name, ".ms") || strings.HasSuffix(name, ".bytes") || strings.HasSuffix(name, ".rate") {
			entry.Type = "LONG"
		}
		if r.Type == "BROKER" && r.Name != "" {
			if value, ok := m.brokers[id][""][name]; ok {
				entry.Value = pointer(value)
				entry.Source = "DYNAMIC_DEFAULT_BROKER_CONFIG"
				entry.Synonyms = append([]kafkaadmin.ConfigSynonym{{Name: name, Value: pointer(value), Source: "DYNAMIC_DEFAULT_BROKER_CONFIG"}}, entry.Synonyms...)
			}
		}
		if value, ok := overrides[name]; ok {
			entry.Value = pointer(value)
			entry.Source = source
		}
		if name == "broker.id" || name == "delete.topic.enable" {
			entry.ReadOnly = true
		}
		out.Entries = append(out.Entries, entry)
	}
	if r.Type == "BROKER" {
		out.Entries = append(out.Entries, kafkaadmin.ConfigEntry{Name: "listener.name.sasl_plaintext.scram-sha-256.sasl.jaas.config", Sensitive: true, ReadOnly: true, Type: "PASSWORD", Source: "STATIC_BROKER_CONFIG", Synonyms: []kafkaadmin.ConfigSynonym{}})
	}
	sort.Slice(out.Entries, func(i, j int) bool { return out.Entries[i].Name < out.Entries[j].Name })
	return out, nil
}
func (m *MockResources) ValidateConfigs(ctx context.Context, a registry.Actor, id string, r kafkaadmin.ConfigResource, changes []kafkaadmin.ConfigChange) error {
	snap, e := m.DescribeConfigs(ctx, a, id, r)
	if e != nil {
		return e
	}
	if len(changes) == 0 || len(changes) > 100 {
		return errInvalid
	}
	seen := map[string]bool{}
	for _, c := range changes {
		found := false
		for _, entry := range snap.Entries {
			if c.Name == entry.Name {
				found = true
				if entry.ReadOnly || entry.Sensitive {
					return errInvalid
				}
				if c.Operation == "SET" && (c.Value == nil || len(*c.Value) > 8192) {
					return errInvalid
				}
				if c.Operation == "DELETE" && c.Value != nil {
					return errInvalid
				}
				if c.Operation != "SET" && c.Operation != "DELETE" {
					return errInvalid
				}
				if c.Operation == "SET" && entry.Type == "LONG" {
					if _, e := strconv.ParseInt(*c.Value, 10, 64); e != nil {
						return errInvalid
					}
				}
			}
		}
		if !found || seen[c.Name] {
			return errInvalid
		}
		seen[c.Name] = true
	}
	return nil
}
func (m *MockResources) AlterConfigs(ctx context.Context, a registry.Actor, id string, r kafkaadmin.ConfigResource, changes []kafkaadmin.ConfigChange) error {
	if e := m.ValidateConfigs(ctx, a, id, r, changes); e != nil {
		return e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mock.mu.Lock()
	defer m.mock.mu.Unlock()
	target := m.mock.topics[id][r.Name]
	if r.Type == "BROKER" {
		if m.brokers[id][r.Name] == nil {
			m.brokers[id][r.Name] = map[string]string{}
		}
		target = m.brokers[id][r.Name]
	}
	for _, c := range changes {
		if c.Operation == "DELETE" {
			delete(target, c.Name)
		} else {
			target[c.Name] = *c.Value
		}
	}
	return nil
}
func (m *MockResources) DeleteTopic(_ context.Context, _ registry.Actor, id, name string) error {
	if strings.HasPrefix(name, "__") {
		return errInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pending, id+"/"+name)
	m.mock.mu.Lock()
	defer m.mock.mu.Unlock()
	m.mock.seed(id)
	delete(m.mock.topics[id], name)
	delete(m.mock.topicIDs[id], name)
	delete(m.mock.assignments[id], name)
	return nil
}
func (m *MockResources) settle(id, name string) {
	p, ok := m.pending[id+"/"+name]
	if !ok || time.Now().Before(p.until) {
		return
	}
	m.mock.mu.Lock()
	defer m.mock.mu.Unlock()
	if m.mock.assignments[id] == nil {
		m.mock.assignments[id] = map[string]map[int32][]int32{}
	}
	if m.mock.assignments[id][name] == nil {
		m.mock.assignments[id][name] = map[int32][]int32{}
	}
	for _, a := range p.assignments {
		m.mock.assignments[id][name][a.Partition] = append([]int32(nil), a.Replicas...)
	}
	delete(m.pending, id+"/"+name)
}
func (m *MockResources) TopicState(ctx context.Context, _ registry.Actor, id, name string) (*kafkaops.Topic, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.settle(id, name)
	snap, e := m.mock.BindReader(id, "").Read(ctx, "topics", name)
	if e != nil {
		return nil, e
	}
	for _, topic := range snap.Topics {
		if topic.Name == name {
			pending := m.pending[id+"/"+name]
			for i := range topic.Partitions {
				for _, assignment := range pending.assignments {
					if topic.Partitions[i].Partition == assignment.Partition {
						topic.Partitions[i].Replicas = mockAssignmentState(assignment, pending.originals[assignment.Partition]).Replicas
					}
				}
			}
			kafkaops.TopicReplication(&topic)
			return &topic, nil
		}
	}
	return nil, nil
}
func (m *MockResources) Reassign(ctx context.Context, a registry.Actor, id, name string, assignments []kafkaadmin.ReplicaAssignment) error {
	topic, e := m.TopicState(ctx, a, id, name)
	if e != nil {
		return e
	}
	if topic == nil {
		return errors.New("NOT_FOUND")
	}
	if len(assignments) == 0 {
		return errInvalid
	}
	seenPartitions := map[int32]bool{}
	for _, a := range assignments {
		if a.Partition < 0 || int(a.Partition) >= len(topic.Partitions) || len(a.Replicas) == 0 {
			return errInvalid
		}
		if seenPartitions[a.Partition] {
			return errInvalid
		}
		seenPartitions[a.Partition] = true
		seen := map[int32]bool{}
		for _, b := range a.Replicas {
			if b < 1 || b > 3 || seen[b] {
				return errInvalid
			}
			seen[b] = true
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.settle(id, name)
	pending := m.pending[id+"/"+name]
	if pending.originals == nil {
		pending.originals = map[int32][]int32{}
	}
	for _, a := range assignments {
		for _, active := range pending.assignments {
			if active.Partition == a.Partition {
				return errConflict
			}
		}
	}
	for _, a := range assignments {
		for _, p := range topic.Partitions {
			if p.Partition == a.Partition {
				pending.originals[a.Partition] = append([]int32(nil), p.Replicas...)
			}
		}
		pending.assignments = append(pending.assignments, kafkaadmin.ReplicaAssignment{Partition: a.Partition, Replicas: append([]int32(nil), a.Replicas...)})
	}
	pending.until = time.Now().Add(5 * time.Second)
	m.pending[id+"/"+name] = pending
	return nil
}
func (m *MockResources) CancelReassignments(_ context.Context, _ registry.Actor, id, name string, partitions []int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.settle(id, name)
	pending, ok := m.pending[id+"/"+name]
	if !ok {
		return errors.New("NOT_FOUND")
	}
	remaining := []kafkaadmin.ReplicaAssignment{}
	for _, a := range pending.assignments {
		remove := false
		for _, p := range partitions {
			if a.Partition == p {
				remove = true
			}
		}
		if !remove {
			remaining = append(remaining, a)
		} else {
			delete(pending.originals, a.Partition)
		}
	}
	if len(remaining) == 0 {
		delete(m.pending, id+"/"+name)
	} else {
		pending.assignments = remaining
		m.pending[id+"/"+name] = pending
	}
	return nil
}
func (m *MockResources) Reassignments(_ context.Context, _ registry.Actor, id, name string) ([]kafkaadmin.PartitionReassignment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.settle(id, name)
	out := []kafkaadmin.PartitionReassignment{}
	for _, a := range m.pending[id+"/"+name].assignments {
		out = append(out, mockAssignmentState(a, m.pending[id+"/"+name].originals[a.Partition]))
	}
	return out, nil
}
func mockAssignmentState(a kafkaadmin.ReplicaAssignment, old []int32) kafkaadmin.PartitionReassignment {
	out := kafkaadmin.PartitionReassignment{Partition: a.Partition, Replicas: append([]int32{}, old...), AddingReplicas: []int32{}, RemovingReplicas: []int32{}}
	oldSet, target := map[int32]bool{}, map[int32]bool{}
	for _, b := range old {
		oldSet[b] = true
	}
	for _, b := range a.Replicas {
		target[b] = true
		if !oldSet[b] {
			out.AddingReplicas = append(out.AddingReplicas, b)
			out.Replicas = append(out.Replicas, b)
		}
	}
	for _, b := range old {
		if !target[b] {
			out.RemovingReplicas = append(out.RemovingReplicas, b)
		}
	}
	return out
}
