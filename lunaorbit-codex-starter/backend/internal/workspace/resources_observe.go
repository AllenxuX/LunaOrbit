package workspace

import (
	"context"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"reflect"
)

func validateResourceAssignments(in ResourceIntent, topic *kafkaops.Topic, ongoing []kafkaadmin.PartitionReassignment) error {
	parts := map[int32]bool{}
	for _, p := range topic.Partitions {
		if p.ErrorCode != "" {
			return errInvalid
		}
		parts[p.Partition] = true
	}
	active := map[int32]bool{}
	for _, p := range ongoing {
		active[p.Partition] = true
	}
	seen := map[int32]bool{}
	for _, a := range in.Assignments {
		if a.Partition < 0 || !parts[a.Partition] || active[a.Partition] || seen[a.Partition] || len(a.Replicas) == 0 || len(a.Replicas) > 20 {
			return errInvalid
		}
		seen[a.Partition] = true
		brokers := map[int32]bool{}
		for _, b := range a.Replicas {
			if b < 0 || brokers[b] {
				return errInvalid
			}
			brokers[b] = true
		}
	}
	for _, p := range in.Partitions {
		if !parts[p] || !active[p] {
			return errInvalid
		}
	}
	return nil
}
func sameReplicas(a, b []int32) bool { return reflect.DeepEqual(a, b) }
func cancellationReplicas(p kafkaadmin.PartitionReassignment) []int32 {
	added := map[int32]bool{}
	for _, b := range p.AddingReplicas {
		added[b] = true
	}
	out := []int32{}
	for _, b := range p.Replicas {
		if !added[b] {
			out = append(out, b)
		}
	}
	return out
}
func (s *Service) observeResource(ctx context.Context, a registry.Actor, id string, op ResourceOperation) (string, string) {
	in := op.Plan.Input
	if in.Kind == "configs" {
		if in.Resource.Type == "TOPIC" && op.Plan.Topic != nil && op.Plan.Topic.TopicID != "" {
			topic, e := s.Resources.TopicState(ctx, a, id, in.Resource.Name)
			if e != nil {
				return "unknown", resourceErrorCode(e)
			}
			if topic == nil || topic.TopicID != op.Plan.Topic.TopicID {
				return "unknown", "RESOURCE_IDENTITY_CHANGED"
			}
		}
		snap, e := s.Resources.DescribeConfigs(ctx, a, id, in.Resource)
		if e != nil {
			return "unknown", resourceErrorCode(e)
		}
		entries := map[string]kafkaadmin.ConfigEntry{}
		for _, c := range snap.Entries {
			entries[c.Name] = c
		}
		matched := 0
		for _, change := range in.Changes {
			c, ok := entries[change.Name]
			if !ok || c.Sensitive || secretResourceConfig(c.Name) {
				return "unknown", "OBSERVATION_INCOMPLETE"
			}
			if change.Operation == "SET" && c.Value != nil && change.Value != nil && *c.Value == *change.Value && c.Source == resourceOverrideSource(in.Resource) {
				matched++
			}
			if change.Operation == "DELETE" {
				override := resourceOverrideSource(in.Resource)
				if c.Source != "" && c.Source != "UNKNOWN_CONFIG" && c.Source != override {
					matched++
				}
			}
		}
		if matched == len(in.Changes) {
			return "succeeded", ""
		}
		if matched > 0 {
			return "partial_failure", "OBSERVED_PARTIAL"
		}
		return "observing", ""
	}
	topic, e := s.Resources.TopicState(ctx, a, id, in.Resource.Name)
	if e != nil {
		return "unknown", resourceErrorCode(e)
	}
	if topic != nil && op.Plan.Topic != nil && op.Plan.Topic.TopicID != "" && topic.TopicID != op.Plan.Topic.TopicID {
		return "unknown", "RESOURCE_IDENTITY_CHANGED"
	}
	if in.Kind == "topic_delete" {
		if topic == nil {
			return "succeeded", ""
		}
		return "observing", ""
	}
	if topic == nil || topic.ErrorCode != "" {
		return "unknown", "OBSERVATION_INCOMPLETE"
	}
	active, e := s.Resources.Reassignments(ctx, a, id, in.Resource.Name)
	if e != nil {
		return "unknown", resourceErrorCode(e)
	}
	pending := map[int32]bool{}
	for _, v := range active {
		pending[v.Partition] = true
	}
	actual := map[int32]kafkaops.Partition{}
	for _, p := range topic.Partitions {
		actual[p.Partition] = p
	}
	desired := in.Assignments
	if in.Kind == "reassign_cancel" {
		desired = []kafkaadmin.ReplicaAssignment{}
		for _, pid := range in.Partitions {
			found := false
			for _, before := range op.Plan.Assignments {
				if before.Partition == pid {
					desired = append(desired, kafkaadmin.ReplicaAssignment{Partition: pid, Replicas: cancellationReplicas(before)})
					found = true
				}
			}
			if !found {
				return "unknown", "OBSERVATION_INCOMPLETE"
			}
		}
	}
	matched := 0
	for _, v := range desired {
		p, ok := actual[v.Partition]
		if !ok || p.ErrorCode != "" {
			return "unknown", "OBSERVATION_INCOMPLETE"
		}
		if !pending[v.Partition] && sameReplicas(v.Replicas, p.Replicas) && replicasInSync(p) {
			matched++
		}
	}
	for _, v := range desired {
		if pending[v.Partition] {
			return "observing", ""
		}
	}
	if matched == len(desired) {
		return "succeeded", ""
	}
	if matched > 0 {
		return "partial_failure", "OBSERVED_PARTIAL"
	}
	return "observing", ""
}
func replicasInSync(p kafkaops.Partition) bool {
	isr := map[int32]bool{}
	for _, b := range p.ISR {
		isr[b] = true
	}
	for _, b := range p.Replicas {
		if !isr[b] {
			return false
		}
	}
	return len(p.Replicas) > 0
}
func resourceOverrideSource(r ResourceTarget) string {
	if r.Type == "TOPIC" {
		return "DYNAMIC_TOPIC_CONFIG"
	}
	if r.Name == "" {
		return "DYNAMIC_DEFAULT_BROKER_CONFIG"
	}
	return "DYNAMIC_BROKER_CONFIG"
}
