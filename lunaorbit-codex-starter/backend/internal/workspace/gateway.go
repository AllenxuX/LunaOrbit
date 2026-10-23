package workspace

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"sync"
)

type Native struct {
	Registry *registry.Service
	Factory  *kafkaadmin.Factory
}

func (n *Native) Synthetic() bool { return false }
func (n *Native) ACLs(ctx context.Context, a registry.Actor, id string) (kafkaops.ACLSnapshot, error) {
	p, e := n.Registry.AuthorizedProfile(ctx, a, id)
	if e != nil {
		return kafkaops.ACLSnapshot{}, e
	}
	return n.Factory.ReadACLs(ctx, p)
}
func (n *Native) CreateACL(ctx context.Context, a registry.Actor, id string, r kafkaops.ACLRule) error {
	if e := authorizeACL(ctx, n.Registry, a, id, "acl.manage", r); e != nil {
		return e
	}
	p, e := n.Registry.AuthorizedProfile(ctx, a, id)
	if e != nil {
		return e
	}
	return n.Factory.CreateACL(ctx, p, r)
}
func (n *Native) DeleteACL(ctx context.Context, a registry.Actor, id string, r kafkaops.ACLRule) error {
	if e := authorizeACL(ctx, n.Registry, a, id, "acl.manage", r); e != nil {
		return e
	}
	p, e := n.Registry.AuthorizedProfile(ctx, a, id)
	if e != nil {
		return e
	}
	return n.Factory.DeleteACL(ctx, p, r)
}
func (n *Native) TopicConfigs(ctx context.Context, a registry.Actor, id, topic string) (map[string]string, error) {
	p, e := n.Registry.AuthorizedProfile(ctx, a, id)
	if e != nil {
		return nil, e
	}
	snapshot, e := n.Factory.Read(ctx, p, "topics", "")
	if e != nil {
		return nil, e
	}
	var found *kafkaops.Topic
	for i := range snapshot.Topics {
		if snapshot.Topics[i].Name == topic {
			found = &snapshot.Topics[i]
			break
		}
	}
	if found == nil {
		return nil, errors.New("NOT_FOUND")
	}
	out, e := n.Factory.TopicConfigs(ctx, p, topic)
	if e != nil {
		return nil, e
	}
	out["partitions"] = fmt.Sprint(len(found.Partitions))
	if found.RFMin != nil && found.RFMax != nil && *found.RFMin == *found.RFMax {
		out["replication_factor"] = fmt.Sprint(*found.RFMin)
	}
	return out, nil
}
func (n *Native) CreateTopic(ctx context.Context, a registry.Actor, id, topic string, part, rf int, ret *int64) error {
	if e := n.Registry.AuthorizeResource(ctx, a, id, "topic.manage", "topic:"+topic); e != nil {
		return e
	}
	p, e := n.Registry.AuthorizedProfile(ctx, a, id)
	if e != nil {
		return e
	}
	return n.Factory.CreateTopic(ctx, p, topic, part, rf, ret)
}
func (n *Native) ExpandTopic(ctx context.Context, a registry.Actor, id, topic string, part int) error {
	if e := n.Registry.AuthorizeResource(ctx, a, id, "topic.manage", "topic:"+topic); e != nil {
		return e
	}
	p, e := n.Registry.AuthorizedProfile(ctx, a, id)
	if e != nil {
		return e
	}
	return n.Factory.ExpandTopic(ctx, p, topic, part)
}
func (n *Native) AlterTopicRetention(ctx context.Context, a registry.Actor, id, topic string, ret int64) error {
	if e := n.Registry.AuthorizeResource(ctx, a, id, "topic.manage", "topic:"+topic); e != nil {
		return e
	}
	p, e := n.Registry.AuthorizedProfile(ctx, a, id)
	if e != nil {
		return e
	}
	return n.Factory.AlterTopicRetention(ctx, p, topic, ret)
}

type Mock struct {
	mu          sync.Mutex
	rules       map[string][]kafkaops.ACLRule
	topics      map[string]map[string]map[string]string
	topicIDs    map[string]map[string]string
	assignments map[string]map[string]map[int32][]int32
}

func NewMock() *Mock {
	return &Mock{rules: map[string][]kafkaops.ACLRule{}, topics: map[string]map[string]map[string]string{}, topicIDs: map[string]map[string]string{}, assignments: map[string]map[string]map[int32][]int32{}}
}
func (m *Mock) Synthetic() bool { return true }
func (m *Mock) seed(id string) {
	if m.topics[id] == nil {
		m.topics[id] = map[string]map[string]string{
			"events": {"partitions": "1", "replication_factor": "1", "retention.ms": "604800000"},
			"orders": {"partitions": "2", "replication_factor": "3", "retention.ms": "604800000"},
		}
	}
}
func (m *Mock) ACLs(_ context.Context, _ registry.Actor, id string) (kafkaops.ACLSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := append([]kafkaops.ACLRule{}, m.rules[id]...)
	return kafkaops.ACLSnapshot{Items: v, Synthetic: true, State: "available"}, nil
}
func (m *Mock) CreateACL(_ context.Context, _ registry.Actor, id string, r kafkaops.ACLRule) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range m.rules[id] {
		if key(v) == key(r) {
			return nil
		}
	}
	m.rules[id] = append(m.rules[id], r)
	return nil
}
func (m *Mock) DeleteACL(_ context.Context, _ registry.Actor, id string, r kafkaops.ACLRule) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := []kafkaops.ACLRule{}
	for _, old := range m.rules[id] {
		if key(old) != key(r) {
			v = append(v, old)
		}
	}
	m.rules[id] = v
	return nil
}
func (m *Mock) TopicConfigs(_ context.Context, _ registry.Actor, id, topic string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seed(id)
	v, ok := m.topics[id][topic]
	if !ok {
		return nil, errors.New("NOT_FOUND")
	}
	out := map[string]string{}
	for k, x := range mockTopicDefaults {
		out[k] = x
	}
	for k, x := range v {
		out[k] = x
	}
	return out, nil
}
func (m *Mock) CreateTopic(_ context.Context, _ registry.Actor, id, topic string, part, rf int, ret *int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seed(id)
	if m.topics[id] == nil {
		m.topics[id] = map[string]map[string]string{}
	}
	if m.topics[id][topic] != nil {
		return errConflict
	}
	v := map[string]string{"partitions": fmt.Sprint(part), "replication_factor": fmt.Sprint(rf), "retention.ms": "604800000"}
	if ret != nil {
		v["retention.ms"] = fmt.Sprint(*ret)
	}
	m.topics[id][topic] = v
	if m.topicIDs[id] == nil {
		m.topicIDs[id] = map[string]string{}
	}
	m.topicIDs[id][topic] = uuid.NewString()
	return nil
}
func (m *Mock) ExpandTopic(_ context.Context, _ registry.Actor, id, topic string, n int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seed(id)
	v := m.topics[id][topic]
	if v == nil {
		return errors.New("NOT_FOUND")
	}
	v["partitions"] = fmt.Sprint(n)
	return nil
}
func (m *Mock) AlterTopicRetention(_ context.Context, _ registry.Actor, id, topic string, n int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seed(id)
	v := m.topics[id][topic]
	if v == nil {
		return errors.New("NOT_FOUND")
	}
	v["retention.ms"] = fmt.Sprint(n)
	return nil
}
