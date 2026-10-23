package workspace

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"testing"
)

func TestMockReaderReflectsTopicMutationsAndIsolation(t *testing.T) {
	m := NewMock()
	ctx := context.Background()
	actor := registry.Actor{}
	if e := m.CreateTopic(ctx, actor, "one", "new-events", 2, 2, nil); e != nil {
		t.Fatal(e)
	}
	if e := m.ExpandTopic(ctx, actor, "one", "orders", 4); e != nil {
		t.Fatal(e)
	}
	for _, resource := range []string{"", "new-events"} {
		s, e := m.BindReader("one", "success").Read(ctx, "topics", resource)
		if e != nil {
			t.Fatal(e)
		}
		if !s.Synthetic || s.State != "synthetic" {
			t.Fatal("lost synthetic markers", s)
		}
		found := false
		for _, topic := range s.Topics {
			if topic.Name == "new-events" {
				found = true
				if len(topic.Partitions) != 2 || topic.RFMin == nil || *topic.RFMin != 2 {
					t.Fatal(topic)
				}
			}
			if topic.Name == "orders" {
				if len(topic.Partitions) != 4 || len(topic.Partitions[0].ISR) != 2 {
					t.Fatal("lost baseline or expansion", topic)
				}
			}
		}
		if !found {
			t.Fatal("missing created topic")
		}
	}
	s, e := m.BindReader("two", "success").Read(ctx, "topics", "")
	if e != nil {
		t.Fatal(e)
	}
	if len(s.Topics) != 2 {
		t.Fatal("cross-cluster state", s.Topics)
	}
	for _, topic := range s.Topics {
		if topic.Name == "orders" && len(topic.Partitions) != 2 {
			t.Fatal("cross-cluster expansion")
		}
	}
}
func TestMockReaderPreservesFailureAndNonTopicSemantics(t *testing.T) {
	m := NewMock()
	ctx := context.Background()
	for _, scenario := range []string{"authorization_denied", "timeout", "unsupported_api", "broker_unreachable"} {
		_, e := m.BindReader("one", scenario).Read(ctx, "topics", "")
		var typed *kafkaops.Error
		if !errors.As(e, &typed) {
			t.Fatal(scenario, e)
		}
	}
	s, e := m.BindReader("one", "kafka28").Read(ctx, "offsets", "payments")
	if e != nil || len(s.Offsets) != 2 || s.Offsets[1].Lag != nil {
		t.Fatal(s, e)
	}
	for _, c := range s.Capabilities {
		if c.Feature == "GROUP_LAG" && c.Supported {
			t.Fatal("lost version scenario")
		}
	}
}
