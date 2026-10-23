package workspace

import (
	"context"
	"github.com/google/uuid"
	"sort"
	"strconv"

	"lunaorbit/backend/internal/kafkaops"
)

type mockWorkspaceReader struct {
	mock     *Mock
	id       string
	scenario string
}

// BindReader shares the synthetic workspace's topic state with resource queries.
// Failure/version scenarios and non-topic resources retain the legacy fixture.
func (m *Mock) BindReader(id, scenario string) kafkaops.Reader {
	return mockWorkspaceReader{mock: m, id: id, scenario: scenario}
}

func (r mockWorkspaceReader) Read(ctx context.Context, kind, resource string) (kafkaops.Snapshot, error) {
	snapshot, err := (kafkaops.MockReader{Scenario: r.scenario}).Read(ctx, kind, resource)
	if err != nil || kind != "topics" {
		return snapshot, err
	}
	r.mock.mu.Lock()
	defer r.mock.mu.Unlock()
	r.mock.seed(r.id)
	baseline := make(map[string]kafkaops.Topic, len(snapshot.Topics))
	for _, topic := range snapshot.Topics {
		baseline[topic.Name] = topic
	}
	snapshot.Topics = make([]kafkaops.Topic, 0, len(r.mock.topics[r.id]))
	for name, config := range r.mock.topics[r.id] {
		if resource != "" && resource != name {
			continue
		}
		count, countErr := strconv.Atoi(config["partitions"])
		rf, rfErr := strconv.Atoi(config["replication_factor"])
		if countErr != nil || rfErr != nil || count < 1 || count > 100000 || rf < 1 || rf > 32767 {
			return kafkaops.Snapshot{}, &kafkaops.Error{Code: "INVALID_ARGUMENT"}
		}
		topic := baseline[name]
		topic.Name = name
		if r.mock.topicIDs[r.id] == nil {
			r.mock.topicIDs[r.id] = map[string]string{}
		}
		if r.mock.topicIDs[r.id][name] == "" {
			r.mock.topicIDs[r.id][name] = uuid.NewString()
		}
		topic.TopicID = r.mock.topicIDs[r.id][name]
		// Existing fixture partitions retain their observed ISR/leader values.
		// Only newly introduced partitions receive generated synthetic assignments.
		if len(topic.Partitions) > count {
			topic.Partitions = topic.Partitions[:count]
		}
		for len(topic.Partitions) < count {
			partition := int32(len(topic.Partitions))
			replicas := make([]int32, rf)
			for i := range replicas {
				replicas[i] = int32((int(partition)+i)%rf + 1)
			}
			topic.Partitions = append(topic.Partitions, kafkaops.Partition{Partition: partition, Leader: replicas[0], Replicas: replicas, ISR: append([]int32(nil), replicas...)})
		}
		for i := range topic.Partitions {
			p := &topic.Partitions[i]
			if replicas, ok := r.mock.assignments[r.id][name][p.Partition]; ok {
				p.Replicas = append([]int32(nil), replicas...)
				p.ISR = append([]int32(nil), replicas...)
				p.Leader = replicas[0]
			}
		}
		kafkaops.TopicReplication(&topic)
		snapshot.Topics = append(snapshot.Topics, topic)
	}
	sort.Slice(snapshot.Topics, func(i, j int) bool { return snapshot.Topics[i].Name < snapshot.Topics[j].Name })
	return snapshot, nil
}
