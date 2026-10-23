package kafkaadmin

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"lunaorbit/backend/internal/kafkaops"
	"net"
	"os"
	"strconv"
	"testing"
)

func TestACLProtocolResponses(t *testing.T) {
	r := kmsg.NewPtrDescribeACLsResponse()
	r.ErrorCode = kerr.SecurityDisabled.Code
	_, e := aclSnapshot(r, "registration")
	var op *kafkaops.Error
	if !errors.As(e, &op) || op.Code != "AUTHORIZER_UNVERIFIED" {
		t.Fatal(e)
	}
	r.ErrorCode = 0
	r.Resources = []kmsg.DescribeACLsResponseResource{{ResourceType: 2, ResourceName: "events", ResourcePatternType: 3, ACLs: []kmsg.DescribeACLsResponseResourceACL{{Principal: "User:explicit-service", Host: "*", Operation: 3, PermissionType: 3}}}}
	b := r.AppendTo(nil)
	decoded := kmsg.NewPtrDescribeACLsResponse()
	if e = decoded.ReadFrom(b); e != nil {
		t.Fatal(e)
	}
	s, e := aclSnapshot(decoded, "registration")
	if e != nil || s.Synthetic || s.State != "pending" || len(s.Items) != 1 || s.Items[0].Principal != "User:explicit-service" || s.Items[0].SourceRegistrationID != "registration" {
		t.Fatal(s, e)
	}
}
func TestACLReadSocketSecurityDisabled(t *testing.T) {
	address, close := mockACLBroker(t, "")
	defer close()
	_, port, _ := net.SplitHostPort(address)
	n, _ := strconv.Atoi(port)
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{n}})
	f, _ := NewFactory(p, nil)
	_, e := f.ReadACLs(context.Background(), Profile{RegistrationID: "fixture", BootstrapServers: []string{address}, SecurityProtocol: "PLAINTEXT"})
	var op *kafkaops.Error
	if !errors.As(e, &op) || op.Code != "AUTHORIZER_UNVERIFIED" {
		t.Fatal(e)
	}
}
func TestACLRealNoAuthorizerOptIn(t *testing.T) {
	address := os.Getenv("LUNAORBIT_ACL_KAFKA_BOOTSTRAP")
	if address == "" {
		t.Skip("LUNAORBIT_ACL_KAFKA_BOOTSTRAP not configured")
	}
	host, port, e := net.SplitHostPort(address)
	if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		t.Fatal("only loopback Kafka allowed")
	}
	n, _ := strconv.Atoi(port)
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{n}})
	f, _ := NewFactory(p, nil)
	_, e = f.ReadACLs(context.Background(), Profile{RegistrationID: "dedicated-local-kafka", BootstrapServers: []string{address}, SecurityProtocol: "PLAINTEXT"})
	var op *kafkaops.Error
	if !errors.As(e, &op) || op.Code != "AUTHORIZER_UNVERIFIED" {
		t.Fatal(e)
	}
}
