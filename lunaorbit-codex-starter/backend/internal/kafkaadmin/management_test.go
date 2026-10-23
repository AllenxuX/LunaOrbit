package kafkaadmin

import (
	"context"
	"testing"
)

func TestManagementRejectsInvalidBeforeNetwork(t *testing.T) {
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}})
	f, _ := NewFactory(p, nil)
	for _, name := range []string{"", ".", "..", "bad/name", "bad name"} {
		if err := f.CreateTopic(context.Background(), Profile{}, name, 1, 1, nil); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	for _, v := range []int64{-1, 0, 31536000001} {
		if err := f.AlterTopicRetention(context.Background(), Profile{}, "synthetic", v); err == nil {
			t.Fatalf("accepted %d", v)
		}
	}
}
