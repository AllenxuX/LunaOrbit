package workspace

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
)

type NativeResources struct{ Native *Native }

func (n *NativeResources) profile(ctx context.Context, a registry.Actor, id string, r kafkaadmin.ConfigResource, write bool) (kafkaadmin.Profile, error) {
	action := "read"
	if write {
		action = "topic.manage"
	}
	var e error
	if r.Type == "BROKER" {
		brokerAction := "topic.manage"
		if a.Role == "information" && !write {
			brokerAction = "read"
		}
		e = n.Native.Registry.AuthorizeUnscopedResource(ctx, a, id, brokerAction)
	} else {
		e = n.Native.Registry.AuthorizeResource(ctx, a, id, action, "topic:"+r.Name)
	}
	if e != nil {
		return kafkaadmin.Profile{}, e
	}
	return n.Native.Registry.AuthorizedProfile(ctx, a, id)
}
func (n *NativeResources) DescribeConfigs(ctx context.Context, a registry.Actor, id string, r kafkaadmin.ConfigResource) (kafkaadmin.ConfigSnapshot, error) {
	p, e := n.profile(ctx, a, id, r, false)
	if e != nil {
		return kafkaadmin.ConfigSnapshot{}, e
	}
	return n.Native.Factory.DescribeResourceConfigs(ctx, p, r)
}
func (n *NativeResources) ValidateConfigs(ctx context.Context, a registry.Actor, id string, r kafkaadmin.ConfigResource, c []kafkaadmin.ConfigChange) error {
	p, e := n.profile(ctx, a, id, r, true)
	if e != nil {
		return e
	}
	return n.Native.Factory.ValidateResourceConfigs(ctx, p, r, c)
}
func (n *NativeResources) AlterConfigs(ctx context.Context, a registry.Actor, id string, r kafkaadmin.ConfigResource, c []kafkaadmin.ConfigChange) error {
	p, e := n.profile(ctx, a, id, r, true)
	if e != nil {
		return e
	}
	return n.Native.Factory.AlterResourceConfigs(ctx, p, r, c)
}
func (n *NativeResources) DeleteTopic(ctx context.Context, a registry.Actor, id, name string) error {
	p, e := n.profile(ctx, a, id, kafkaadmin.ConfigResource{Type: "TOPIC", Name: name}, true)
	if e != nil {
		return e
	}
	return n.Native.Factory.DeleteTopic(ctx, p, name)
}
func (n *NativeResources) DeleteTopicIdentified(ctx context.Context, a registry.Actor, id, name, identity string) error {
	p, e := n.profile(ctx, a, id, kafkaadmin.ConfigResource{Type: "TOPIC", Name: name}, true)
	if e != nil {
		return e
	}
	return n.Native.Factory.DeleteTopicIdentified(ctx, p, name, identity)
}
func (n *NativeResources) Reassign(ctx context.Context, a registry.Actor, id, name string, assignments []kafkaadmin.ReplicaAssignment) error {
	p, e := n.profile(ctx, a, id, kafkaadmin.ConfigResource{Type: "TOPIC", Name: name}, true)
	if e != nil {
		return e
	}
	return n.Native.Factory.ReassignPartitions(ctx, p, name, assignments)
}
func (n *NativeResources) CancelReassignments(ctx context.Context, a registry.Actor, id, name string, partitions []int32) error {
	p, e := n.profile(ctx, a, id, kafkaadmin.ConfigResource{Type: "TOPIC", Name: name}, true)
	if e != nil {
		return e
	}
	return n.Native.Factory.CancelReassignments(ctx, p, name, partitions)
}
func (n *NativeResources) Reassignments(ctx context.Context, a registry.Actor, id, name string) ([]kafkaadmin.PartitionReassignment, error) {
	p, e := n.profile(ctx, a, id, kafkaadmin.ConfigResource{Type: "TOPIC", Name: name}, false)
	if e != nil {
		return nil, e
	}
	return n.Native.Factory.ListReassignments(ctx, p, name)
}
func (n *NativeResources) TopicState(ctx context.Context, a registry.Actor, id, name string) (*kafkaops.Topic, error) {
	p, e := n.profile(ctx, a, id, kafkaadmin.ConfigResource{Type: "TOPIC", Name: name}, false)
	if e != nil {
		return nil, e
	}
	snapshot, e := n.Native.Factory.Read(ctx, p, "topics", name)
	if e != nil {
		var op *kafkaops.Error
		if errors.As(e, &op) && op.Code == "NOT_FOUND" {
			return nil, nil
		}
		return nil, e
	}
	for _, topic := range snapshot.Topics {
		if topic.Name == name {
			return &topic, nil
		}
	}
	return nil, nil
}
