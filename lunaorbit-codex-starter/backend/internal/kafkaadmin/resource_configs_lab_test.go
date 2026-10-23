package kafkaadmin

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"os"
	"strings"
	"testing"
	"time"
)

// Explicit, read/validate-only diagnostic for the dedicated synthetic namespace.
func TestResourceConfigLabValidation(t *testing.T) {
	host := os.Getenv("RESOURCE_LAB_SOURCE_HOST")
	if os.Getenv("RESOURCE_LAB_TEST") != "1" {
		t.Skip("explicit synthetic lab required")
	}
	if !strings.HasSuffix(host, ".lunaorbit-integration.svc.cluster.local") {
		t.Fatal("synthetic namespace required")
	}
	policy, e := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"10.0.0.0/8"}, AllowedHosts: []string{host, "kafka-source-extra.lunaorbit-integration.svc.cluster.local"}, AllowedPorts: []int{9092}, Timeout: 15 * time.Second})
	if e != nil {
		t.Fatal(e)
	}
	f, _ := NewFactory(policy, nil)
	p := Profile{BootstrapServers: []string{host + ":9092"}, SecurityProtocol: "PLAINTEXT"}
	for i := 0; i < 3; i++ {
		e = f.managementClient(context.Background(), p, func(ctx context.Context, cl *kgo.Client) error {
			start := time.Now()
			m, e := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
			if e != nil {
				return e
			}
			t.Log("metadata", time.Since(start), "controller", m.ControllerID)
			r := ConfigResource{Type: "BROKER"}
			target, e := configTarget(ctx, cl, r)
			if e != nil {
				return e
			}
			start = time.Now()
			_, e = describeResolvedResource(ctx, cl, target, r)
			t.Log("describe", time.Since(start), e)
			if e != nil {
				return e
			}
			value := "604800001"
			start = time.Now()
			e = alterResource(ctx, target, r, []ConfigChange{{Name: "log.retention.ms", Operation: "SET", Value: &value}}, true)
			t.Log("validate-only", time.Since(start), e)
			return e
		})
		if e != nil {
			t.Fatal(e)
		}
	}
}
