package kafkaadmin

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
)

type fixedResolver map[string][]net.IPAddr

func (f fixedResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	if v, ok := f[host]; ok {
		return v, nil
	}
	return nil, &net.DNSError{Err: "no such host", Name: host}
}

func autoPolicy(t *testing.T) *NetworkPolicy {
	t.Helper()
	p, err := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"10.9.0.0/16", "127.0.0.1/32"}, AllowedHosts: []string{"127.0.0.1"}, AllowedPorts: []int{19092}, Timeout: 2 * time.Second, MaxBrokers: 8, MaxConcurrent: 2,
		Resolver: fixedResolver{"kafka-k9.lab": {{IP: net.ParseIP("10.9.0.9")}}, "evil.lab": {{IP: net.ParseIP("192.168.1.1")}}, "kafka-0.k9.lab": {{IP: net.ParseIP("10.9.3.3")}}}})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAutoEndpointTargetsAreLimitedToMM2ConfiguredAddresses(t *testing.T) {
	p := autoPolicy(t)
	allow := p.autoTarget([]string{"kafka-k9.lab:9092", "kafka-k1.lab:9093"}, map[string]string{"kafka-k1.lab": "10.9.0.1"})
	ctx := context.Background()
	for addr, want := range map[string]string{
		"kafka-k9.lab:9092":   "10.9.0.9:9092", // configured, DNS inside CIDR
		"kafka-k1.lab:9093":   "10.9.0.1:9093", // configured, hostAliases
		"kafka-0.k9.lab:9092": "10.9.3.3:9092", // advertised broker on a configured port, inside CIDR
	} {
		if got, err := allow(ctx, addr); err != nil || got != want {
			t.Fatalf("%s -> %q %v", addr, got, err)
		}
	}
	for _, addr := range []string{"kafka-k9.lab:22", "evil.lab:9092", "10.0.0.1:9092", "unknown.lab:9092"} {
		if _, err := allow(ctx, addr); err == nil {
			t.Fatalf("%s must be refused", addr)
		}
	}
}

func TestReadOnlyEndpointReaderListsTopicsAndPartitions(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "orders", "payments"), kfake.ClusterID("cid-test"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := EndpointReader{Dial: plainDial}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cid, err := r.Probe(ctx, c.ListenAddrs(), nil, nil)
	if err != nil || cid != "cid-test" {
		t.Fatalf("probe %q %v", cid, err)
	}
	topics, err := r.Topics(ctx, c.ListenAddrs(), nil, nil)
	if err != nil || len(topics) != 2 || topics[0].Name != "orders" || topics[0].Partitions != 3 {
		t.Fatalf("topics %+v %v", topics, err)
	}
	detail, err := r.Topic(ctx, c.ListenAddrs(), nil, nil, "orders")
	if err != nil || len(detail.Partitions) != 3 || detail.Partitions[0].Replicas != 1 || detail.Partitions[0].ISR != 1 {
		t.Fatalf("detail %+v %v", detail, err)
	}
	if _, err := r.Groups(ctx, c.ListenAddrs(), nil, nil); err != nil {
		t.Fatalf("groups %v", err)
	}
}
