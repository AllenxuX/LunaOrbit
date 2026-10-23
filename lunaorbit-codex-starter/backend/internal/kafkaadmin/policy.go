// Package kafkaadmin implements bounded, read-only Kafka connection probes.
package kafkaadmin

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

type safeError string

func (e safeError) Error() string { return string(e) }
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if e, ok := err.(safeError); ok {
		return string(e)
	}
	return "broker_unreachable"
}

type Resolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}
type NetworkPolicyConfig struct {
	AllowedCIDRs  []string
	AllowedHosts  []string
	AllowedPorts  []int
	Resolver      Resolver
	Timeout       time.Duration
	MaxBrokers    int
	MaxConcurrent int
}
type NetworkPolicy struct {
	prefixes   []netip.Prefix
	hosts      map[string]bool
	ports      map[int]bool
	resolver   Resolver
	timeout    time.Duration
	maxBrokers int
	slots      chan struct{}
}

// ValidateSeed checks the same DNS, host, CIDR and port policy used by dialers
// without opening a connection.
func (p *NetworkPolicy) ValidateSeed(ctx context.Context, address string) error {
	if p == nil {
		return safeError("network_denied")
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	_, err := p.resolve(ctx, address)
	return err
}

// NewNetworkPolicy defaults to deny-all. Host allowlists narrow CIDR permission;
// every DNS answer must fall inside an allowed CIDR, even for allowed hostnames.
func NewNetworkPolicy(c NetworkPolicyConfig) (*NetworkPolicy, error) {
	p := &NetworkPolicy{hosts: map[string]bool{}, ports: map[int]bool{}, resolver: c.Resolver, timeout: c.Timeout, maxBrokers: c.MaxBrokers}
	if p.resolver == nil {
		p.resolver = net.DefaultResolver
	}
	if p.timeout <= 0 {
		p.timeout = 8 * time.Second
	}
	if p.timeout > 30*time.Second {
		return nil, safeError("invalid_configuration")
	}
	if p.maxBrokers <= 0 {
		p.maxBrokers = 32
	}
	if p.maxBrokers > 128 {
		return nil, safeError("invalid_configuration")
	}
	if c.MaxConcurrent <= 0 {
		c.MaxConcurrent = 4
	}
	if c.MaxConcurrent > 32 {
		return nil, safeError("invalid_configuration")
	}
	p.slots = make(chan struct{}, c.MaxConcurrent)
	for _, s := range c.AllowedCIDRs {
		x, e := netip.ParsePrefix(s)
		if e != nil {
			return nil, safeError("invalid_configuration")
		}
		p.prefixes = append(p.prefixes, x)
	}
	for _, h := range c.AllowedHosts {
		if h == "" || strings.ContainsAny(h, "/:@* ") {
			return nil, safeError("invalid_configuration")
		}
		p.hosts[strings.ToLower(h)] = true
	}
	for _, n := range c.AllowedPorts {
		if n < 1 || n > 65535 {
			return nil, safeError("invalid_configuration")
		}
		p.ports[n] = true
	}
	return p, nil
}
func (p *NetworkPolicy) resolve(ctx context.Context, address string) ([]string, error) {
	host, port, e := net.SplitHostPort(address)
	if e != nil || host == "" || strings.ContainsAny(host, "/@% ") {
		return nil, safeError("network_denied")
	}
	n, e := strconv.Atoi(port)
	if e != nil || !p.ports[n] {
		return nil, safeError("network_denied")
	}
	if len(p.hosts) > 0 && !p.hosts[strings.ToLower(host)] {
		return nil, safeError("network_denied")
	}
	addresses, e := p.resolver.LookupIPAddr(ctx, host)
	if e != nil || len(addresses) == 0 {
		return nil, safeError("broker_unreachable")
	}
	result := make([]string, 0, len(addresses))
	for _, a := range addresses {
		ip, ok := netip.AddrFromSlice(a.IP)
		if !ok {
			return nil, safeError("network_denied")
		}
		ip = ip.Unmap()
		allowed := false
		for _, cidr := range p.prefixes {
			if cidr.Contains(ip) {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, safeError("network_denied")
		}
		result = append(result, net.JoinHostPort(ip.String(), port))
	}
	return result, nil
}
