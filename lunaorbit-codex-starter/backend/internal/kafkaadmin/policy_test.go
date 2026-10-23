package kafkaadmin

import (
	"context"
	"net"
	"testing"
)

type resolverFunc func(context.Context, string) ([]net.IPAddr, error)

func (f resolverFunc) LookupIPAddr(c context.Context, h string) ([]net.IPAddr, error) { return f(c, h) }
func TestPolicyDenyDefaultAndMixedDNS(t *testing.T) {
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{})
	if _, e := p.resolve(context.Background(), "localhost:9092"); CodeOf(e) != "network_denied" {
		t.Fatal(e)
	}
	p, _ = NewNetworkPolicy(NetworkPolicyConfig{AllowedHosts: []string{"broker.test"}, AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{9092}, Resolver: resolverFunc(func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}, {IP: net.ParseIP("10.0.0.1")}}, nil
	})})
	if _, e := p.resolve(context.Background(), "broker.test:9092"); CodeOf(e) != "network_denied" {
		t.Fatal(e)
	}
}
func TestPolicyPinsResolvedIP(t *testing.T) {
	calls := 0
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{9092}, Resolver: resolverFunc(func(context.Context, string) ([]net.IPAddr, error) {
		calls++
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
	})})
	ips, e := p.resolve(context.Background(), "broker.test:9092")
	if e != nil || ips[0] != "127.0.0.1:9092" || calls != 1 {
		t.Fatal(ips, e, calls)
	}
}
