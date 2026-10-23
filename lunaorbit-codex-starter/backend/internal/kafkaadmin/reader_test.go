package kafkaadmin

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/kafkaops"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestReaderMetadata(t *testing.T) {
	address, close := mockBroker(t, "")
	defer close()
	_, port, _ := net.SplitHostPort(address)
	n, _ := strconv.Atoi(port)
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{n}})
	f, _ := NewFactory(p, nil)
	s, e := f.Read(context.Background(), Profile{BootstrapServers: []string{address}, SecurityProtocol: "PLAINTEXT"}, "brokers", "")
	if e != nil || len(s.Brokers) != 1 || s.Synthetic || s.ObservedAt.IsZero() {
		t.Fatal(s, e)
	}
}
func TestReaderGroupsTopicsAndOffsetsProtocol(t *testing.T) {
	address, close := mockReaderBroker(t, "")
	defer close()
	_, port, _ := net.SplitHostPort(address)
	n, _ := strconv.Atoi(port)
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{n}})
	f, _ := NewFactory(p, nil)
	profile := Profile{BootstrapServers: []string{address}, SecurityProtocol: "PLAINTEXT"}
	topics, e := f.Read(context.Background(), profile, "topics", "")
	if e != nil || len(topics.Topics) != 1 || topics.Topics[0].RFMin == nil || *topics.Topics[0].RFMin != 1 {
		t.Fatal(topics, e)
	}
	groups, e := f.Read(context.Background(), profile, "groups", "")
	if e != nil || len(groups.Groups) != 1 || groups.Groups[0].ID != "synthetic-group" {
		t.Fatal(groups, e)
	}
	offsets, e := f.Read(context.Background(), profile, "offsets", "synthetic-group")
	if e != nil || len(offsets.Offsets) != 3 {
		t.Fatal(offsets, e)
	}
	for _, o := range offsets.Offsets {
		switch o.Partition {
		case 0:
			if o.Lag == nil || *o.Lag != 10 {
				t.Fatal(o)
			}
		case 1:
			if o.Committed != nil || o.Lag != nil {
				t.Fatal(o)
			}
		case 2:
			if o.Lag != nil {
				t.Fatal(o)
			}
		}
	}
}
func TestReaderSafeFailure(t *testing.T) {
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{9092}})
	f, _ := NewFactory(p, nil)
	for _, tc := range []struct {
		kind    string
		profile Profile
		want    string
	}{{"write", Profile{}, "UNSUPPORTED_API"}, {"offsets", Profile{}, "INVALID_ARGUMENT"}, {"brokers", Profile{SecurityProtocol: "PLAINTEXT", BootstrapServers: []string{"10.0.0.1:9092"}}, "NETWORK_DENIED"}} {
		_, e := f.Read(context.Background(), tc.profile, tc.kind, "")
		var safe *kafkaops.Error
		if !errors.As(e, &safe) || safe.Code != tc.want {
			t.Fatal(e)
		}
	}
}

// Opt-in dedicated Kafka lab only: numeric loopback prevents accidental remote access.
func TestDedicatedLoopbackKafkaLab(t *testing.T) {
	address := os.Getenv("LUNAORBIT_KAFKA_TEST_ADDR")
	if address == "" {
		t.Skip("dedicated Kafka lab is not configured")
	}
	host, port, e := net.SplitHostPort(address)
	if e != nil {
		t.Fatal("invalid dedicated lab address")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		t.Fatal("dedicated lab must use a numeric loopback address")
	}
	n, e := strconv.Atoi(port)
	if e != nil {
		t.Fatal("invalid dedicated lab port")
	}
	p, e := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{ip.String() + func() string {
		if ip.To4() != nil {
			return "/32"
		}
		return "/128"
	}()}, AllowedPorts: []int{n}})
	if e != nil {
		t.Fatal(e)
	}
	f, _ := NewFactory(p, nil)
	s, e := f.Read(context.Background(), Profile{BootstrapServers: []string{address}, SecurityProtocol: "PLAINTEXT"}, "brokers", "")
	if e != nil || len(s.Brokers) == 0 {
		t.Fatal(s, e)
	}
	topic := os.Getenv("LUNAORBIT_KAFKA_TEST_TOPIC")
	if topic == "" {
		return
	}
	if !strings.HasPrefix(topic, "lunaorbit-phase05-") {
		t.Fatal("synthetic topic required")
	}
	profile := Profile{BootstrapServers: []string{address}, SecurityProtocol: "PLAINTEXT"}
	topics, e := f.Read(context.Background(), profile, "topics", topic)
	if e != nil || len(topics.Topics) != 1 || topics.Topics[0].RFMin == nil || *topics.Topics[0].RFMin != 1 || len(topics.Topics[0].Partitions[0].ISR) != 1 {
		t.Fatal("topic RF/ISR lab failed", e)
	}
	group := os.Getenv("LUNAORBIT_KAFKA_TEST_GROUP")
	if !strings.HasPrefix(group, "lunaorbit-phase05-") {
		t.Fatal("synthetic group required")
	}
	groups, e := f.Read(context.Background(), profile, "groups", "")
	found := false
	for _, g := range groups.Groups {
		if g.ID == group {
			found = true
		}
	}
	if e != nil || !found {
		t.Fatal("group lab failed", e)
	}
	offsets, e := f.Read(context.Background(), profile, "offsets", group)
	if e != nil || len(offsets.Offsets) != 1 {
		t.Fatal("offset lab failed", e)
	}
	o := offsets.Offsets[0]
	if o.Topic != topic || o.Committed == nil || *o.Committed != 1 || o.LogEnd == nil || *o.LogEnd != 3 || o.Lag == nil || *o.Lag != 2 {
		t.Fatal("offset/lag lab values incorrect", o)
	}
}
func TestReaderGroupDetailsSeparatesHistoricalOffsets(t *testing.T) {
	address, close := mockReaderBroker(t, "")
	defer close()
	_, port, _ := net.SplitHostPort(address)
	n, _ := strconv.Atoi(port)
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{n}})
	f, _ := NewFactory(p, nil)
	s, e := f.Read(context.Background(), Profile{BootstrapServers: []string{address}, SecurityProtocol: "PLAINTEXT"}, "groups", "synthetic-group")
	if e != nil || len(s.Groups) != 1 || !s.Groups[0].AssignmentsKnown || len(s.Groups[0].AssignedTopics) != 1 || s.Groups[0].AssignedTopics[0] != "current-topic" || len(s.Offsets) != 0 {
		t.Fatalf("%+v %v", s, e)
	}
}
