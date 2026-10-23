package kafkaops

import (
	"context"
	"testing"
)

func TestMockPartitionRFAndUnknownOffsets(t *testing.T) {
	s, e := (MockReader{}).Read(context.Background(), "topics", "")
	if e != nil || len(s.Topics) != 2 {
		t.Fatal(e)
	}
	for _, v := range s.Topics {
		if v.RFMin == nil || *v.RFMin != len(v.Partitions[0].Replicas) {
			t.Fatal("RF must use actual partitions")
		}
	}
	s, e = (MockReader{}).Read(context.Background(), "offsets", "payments")
	if e != nil || s.Offsets[1].Lag != nil {
		t.Fatal("unknown offset needs null lag")
	}
	a, b := int64(20), int64(10)
	if CalculateLag(&a, &b) != nil {
		t.Fatal("negative lag")
	}
}
func TestReplicationUnknown(t *testing.T) {
	for _, topic := range []Topic{{ErrorCode: "AUTHORIZATION_DENIED", Partitions: []Partition{{Replicas: []int32{1}}}}, {Partitions: []Partition{{Replicas: nil}}}, {Partitions: []Partition{{Replicas: []int32{1}}, {Replicas: []int32{1}, ErrorCode: "UNKNOWN"}}}} {
		TopicReplication(&topic)
		if topic.RFMin != nil || topic.RFMax != nil {
			t.Fatal("unknown replica metadata must remain null")
		}
	}
}
