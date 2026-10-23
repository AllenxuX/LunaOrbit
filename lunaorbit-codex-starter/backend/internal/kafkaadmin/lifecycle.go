package kafkaadmin

import (
	"context"
	"encoding/hex"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"lunaorbit/backend/internal/kafkaops"
	"strconv"
	"strings"
)

type ReplicaAssignment struct {
	Partition int32   `json:"partition"`
	Replicas  []int32 `json:"replicas"`
}
type PartitionReassignment struct {
	Partition        int32   `json:"partition"`
	Replicas         []int32 `json:"replicas"`
	AddingReplicas   []int32 `json:"adding_replicas"`
	RemovingReplicas []int32 `json:"removing_replicas"`
}

func mutationTopic(name string) bool { return validTopic(name) && !strings.HasPrefix(name, "__") }
func topicMetadata(ctx context.Context, cl *kgo.Client, name string) (*kmsg.MetadataResponse, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}
	resp, e := req.RequestWith(ctx, cl)
	if e != nil {
		return nil, &kafkaops.Error{Code: operationCode(e)}
	}
	if len(resp.Topics) != 1 || resp.Topics[0].Topic == nil || *resp.Topics[0].Topic != name {
		return nil, malformedManagement()
	}
	if e = managementError(resp.Topics[0].ErrorCode); e != nil {
		return nil, e
	}
	return resp, nil
}
func (f *Factory) DeleteTopic(ctx context.Context, p Profile, name string) error {
	return f.DeleteTopicIdentified(ctx, p, name, "")
}
func (f *Factory) DeleteTopicIdentified(ctx context.Context, p Profile, name, expectedID string) error {
	if !mutationTopic(name) {
		return invalidManagement()
	}
	return f.managementClient(ctx, p, func(ctx context.Context, cl *kgo.Client) error {
		m, e := topicMetadata(ctx, cl, name)
		if e != nil {
			return e
		}
		if m.Topics[0].IsInternal {
			return invalidManagement()
		}
		identity := m.Topics[0].TopicID
		if expectedID != "" && (identity == ([16]byte{}) || hex.EncodeToString(identity[:]) != expectedID) {
			return &kafkaops.Error{Code: "CONFLICT"}
		}
		for _, broker := range m.Brokers {
			snapshot, err := describeResource(ctx, cl.Broker(int(broker.NodeID)), ConfigResource{Type: "BROKER", Name: strconv.FormatInt(int64(broker.NodeID), 10)})
			if err != nil {
				return err
			}
			for _, entry := range snapshot.Entries {
				if entry.Name == "delete.topic.enable" && entry.Value != nil && strings.EqualFold(*entry.Value, "false") {
					return &kafkaops.Error{Code: "TOPIC_DELETION_DISABLED"}
				}
			}
		}
		req := kmsg.NewPtrDeleteTopicsRequest()
		req.TopicNames = []string{name}
		req.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &name}}
		if identity != ([16]byte{}) {
			req.Topics = []kmsg.DeleteTopicsRequestTopic{{TopicID: identity}}
		}
		resp, e := req.RequestWith(ctx, cl)
		if e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
		if len(resp.Topics) != 1 {
			return malformedManagement()
		}
		if identity != ([16]byte{}) && resp.Version >= 6 {
			if resp.Topics[0].TopicID != identity {
				return malformedManagement()
			}
		} else if resp.Topics[0].Topic == nil || *resp.Topics[0].Topic != name {
			return malformedManagement()
		}
		if resp.Topics[0].ErrorCode == 73 {
			return &kafkaops.Error{Code: "TOPIC_DELETION_DISABLED"}
		}
		return managementError(resp.Topics[0].ErrorCode)
	})
}
func validateAssignments(a []ReplicaAssignment, cancel bool) error {
	if len(a) == 0 || len(a) > 100000 {
		return invalidManagement()
	}
	seen := map[int32]bool{}
	for _, v := range a {
		if v.Partition < 0 || seen[v.Partition] || (!cancel && len(v.Replicas) == 0) {
			return invalidManagement()
		}
		seen[v.Partition] = true
		ids := map[int32]bool{}
		for _, id := range v.Replicas {
			if id < 0 || ids[id] {
				return invalidManagement()
			}
			ids[id] = true
		}
	}
	return nil
}
func (f *Factory) reassign(ctx context.Context, p Profile, name string, a []ReplicaAssignment, cancel bool) error {
	if !mutationTopic(name) {
		return invalidManagement()
	}
	if e := validateAssignments(a, cancel); e != nil {
		return e
	}
	return f.managementClient(ctx, p, func(ctx context.Context, cl *kgo.Client) error {
		m, e := topicMetadata(ctx, cl, name)
		if e != nil {
			return e
		}
		if m.Topics[0].IsInternal {
			return invalidManagement()
		}
		brokers := map[int32]bool{}
		for _, b := range m.Brokers {
			brokers[b.NodeID] = true
		}
		parts := map[int32]bool{}
		for _, v := range m.Topics[0].Partitions {
			if e = managementError(v.ErrorCode); e != nil {
				return e
			}
			parts[v.Partition] = true
		}
		topic := kmsg.AlterPartitionAssignmentsRequestTopic{Topic: name}
		want := map[int32]bool{}
		for _, v := range a {
			if !parts[v.Partition] {
				return invalidManagement()
			}
			want[v.Partition] = true
			for _, id := range v.Replicas {
				if !brokers[id] {
					return invalidManagement()
				}
			}
			replicas := v.Replicas
			if cancel {
				replicas = nil
			}
			topic.Partitions = append(topic.Partitions, kmsg.AlterPartitionAssignmentsRequestTopicPartition{Partition: v.Partition, Replicas: replicas})
		}
		req := kmsg.NewPtrAlterPartitionAssignmentsRequest()
		req.Topics = []kmsg.AlterPartitionAssignmentsRequestTopic{topic}
		resp, e := req.RequestWith(ctx, cl)
		if e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
		if e = managementError(resp.ErrorCode); e != nil {
			return e
		}
		if len(resp.Topics) != 1 || resp.Topics[0].Topic != name || len(resp.Topics[0].Partitions) != len(a) {
			return malformedManagement()
		}
		for _, v := range resp.Topics[0].Partitions {
			if !want[v.Partition] {
				return malformedManagement()
			}
			delete(want, v.Partition)
			if e = managementError(v.ErrorCode); e != nil {
				return e
			}
		}
		return nil
	})
}
func (f *Factory) ReassignPartitions(ctx context.Context, p Profile, name string, a []ReplicaAssignment) error {
	return f.reassign(ctx, p, name, a, false)
}
func (f *Factory) CancelReassignments(ctx context.Context, p Profile, name string, parts []int32) error {
	a := make([]ReplicaAssignment, len(parts))
	for i, n := range parts {
		a[i].Partition = n
	}
	return f.reassign(ctx, p, name, a, true)
}
func reassignmentQuery(name string, partitions []kmsg.MetadataResponseTopicPartition) *kmsg.ListPartitionReassignmentsRequest {
	req := kmsg.NewPtrListPartitionReassignmentsRequest()
	query := kmsg.ListPartitionReassignmentsRequestTopic{Topic: name}
	for _, partition := range partitions {
		query.Partitions = append(query.Partitions, partition.Partition)
	}
	req.Topics = []kmsg.ListPartitionReassignmentsRequestTopic{query}
	return req
}
func (f *Factory) ListReassignments(ctx context.Context, p Profile, name string) (out []PartitionReassignment, err error) {
	if !validTopic(name) {
		return nil, invalidManagement()
	}
	out = []PartitionReassignment{}
	err = f.managementClient(ctx, p, func(ctx context.Context, cl *kgo.Client) error {
		metadata, e := topicMetadata(ctx, cl, name)
		if e != nil {
			return e
		}
		// Partitions is non-nullable: an empty array selects no partitions,
		// rather than all partitions. Enumerate actual metadata explicitly.
		req := reassignmentQuery(name, metadata.Topics[0].Partitions)
		resp, e := req.RequestWith(ctx, cl)
		if e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
		if e = managementError(resp.ErrorCode); e != nil {
			return e
		}
		seen := map[int32]bool{}
		for _, t := range resp.Topics {
			if t.Topic != name {
				return malformedManagement()
			}
			for _, v := range t.Partitions {
				if v.Partition < 0 || seen[v.Partition] {
					return malformedManagement()
				}
				seen[v.Partition] = true
				out = append(out, PartitionReassignment{Partition: v.Partition, Replicas: v.Replicas, AddingReplicas: v.AddingReplicas, RemovingReplicas: v.RemovingReplicas})
			}
		}
		return nil
	})
	return
}
