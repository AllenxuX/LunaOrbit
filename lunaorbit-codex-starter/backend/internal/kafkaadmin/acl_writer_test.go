package kafkaadmin

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/kafkaops"
	"net"
	"strconv"
	"testing"
)

func TestACLCreationMapping(t *testing.T) {
	for typ, ops := range kafkaops.ACLCapabilities() {
		for _, op := range ops {
			name, pattern := "fixture", "PREFIXED"
			if typ == "CLUSTER" {
				name, pattern = "kafka-cluster", "LITERAL"
			}
			c, e := aclCreation(kafkaops.ACLRule{Principal: "User:synthetic", Host: "*", ResourceType: typ, ResourceName: name, PatternType: pattern, Operation: op, Permission: "ALLOW"})
			if e != nil || c.ResourceType < 2 || c.Operation < 2 {
				t.Fatalf("%s: %+v %v", op, c, e)
			}
		}
	}
}
func TestACLCreateNoAuthorizerNeverSucceeds(t *testing.T) {
	address, close := mockACLBroker(t, "")
	defer close()
	_, port, _ := net.SplitHostPort(address)
	n, _ := strconv.Atoi(port)
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{n}})
	f, _ := NewFactory(p, nil)
	e := f.CreateACL(context.Background(), Profile{RegistrationID: "fixture", BootstrapServers: []string{address}, SecurityProtocol: "PLAINTEXT"}, kafkaops.ACLRule{Principal: "User:fixture", Host: "*", ResourceType: "TOPIC", ResourceName: "events", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"})
	var op *kafkaops.Error
	if !errors.As(e, &op) || op.Code != "AUTHORIZER_UNVERIFIED" {
		t.Fatal(e)
	}
}
