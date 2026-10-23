package kafkaops

import (
	"context"
	"time"
)

// MockReader never performs network I/O. Scenario selects a synthetic protocol fixture.
type MockReader struct{ Scenario string }

func CalculateLag(committed, end *int64) *int64 {
	if committed == nil || end == nil || *committed < 0 || *end < *committed {
		return nil
	}
	v := *end - *committed
	return &v
}
func TopicReplication(t *Topic) {
	t.RFMin = nil
	t.RFMax = nil
	if t.ErrorCode != "" {
		return
	}
	for _, p := range t.Partitions {
		if p.ErrorCode != "" || len(p.Replicas) == 0 {
			t.RFMin = nil
			t.RFMax = nil
			return
		}
		n := len(p.Replicas)
		if t.RFMin == nil || n < *t.RFMin {
			v := n
			t.RFMin = &v
		}
		if t.RFMax == nil || n > *t.RFMax {
			v := n
			t.RFMax = &v
		}
	}
}
func (m MockReader) Read(ctx context.Context, kind, resource string) (Snapshot, error) {
	select {
	case <-ctx.Done():
		return Snapshot{}, &Error{Code: "TIMEOUT"}
	default:
	}
	codes := map[string]string{"authorization_denied": "AUTHORIZATION_DENIED", "broker_unreachable": "BROKER_UNREACHABLE", "unsupported_api": "UNSUPPORTED_API", "timeout": "TIMEOUT"}
	if c := codes[m.Scenario]; c != "" {
		return Snapshot{}, &Error{Code: c}
	}
	switch m.Scenario {
	case "", "success", "kafka28", "kafka36", "kafka37", "kafka392":
	default:
		return Snapshot{}, &Error{Code: "INVALID_ARGUMENT"}
	}
	s := Snapshot{Synthetic: true, ObservedAt: time.Now().UTC(), State: "synthetic", Reason: "Synthetic protocol fixture; no Kafka connection", Brokers: []Broker{{ID: 1, Host: "broker-1.synthetic.test", Port: 9092}, {ID: 2, Host: "broker-2.synthetic.test", Port: 9092}, {ID: 3, Host: "broker-3.synthetic.test", Port: 9092}}, Topics: []Topic{{Name: "events", Partitions: []Partition{{Partition: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}}, {Name: "orders", Partitions: []Partition{{Partition: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2}}, {Partition: 1, Leader: 2, Replicas: []int32{2, 3, 1}, ISR: []int32{2, 3, 1}}}}}, Groups: []Group{{ID: "payments", State: "Stable", ProtocolType: "consumer"}, {ID: "analytics", State: "Empty", ProtocolType: "consumer"}}, Capabilities: []Capability{{Feature: "METADATA", Supported: true, Reason: "synthetic supported response"}, {Feature: "GROUP_LAG", Supported: m.Scenario != "kafka28", Reason: "synthetic API capability fixture"}}}
	for i := range s.Topics {
		TopicReplication(&s.Topics[i])
	}
	c, e := int64(100), int64(125)
	s.Offsets = []Offset{{Group: resource, Topic: "orders", Partition: 0, Committed: &c, LogEnd: &e, Lag: CalculateLag(&c, &e)}, {Group: resource, Topic: "orders", Partition: 1, ErrorCode: "OFFSET_UNKNOWN"}}
	return s, nil
}
