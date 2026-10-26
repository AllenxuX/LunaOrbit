package kafkaadmin

import (
	"context"
	"crypto/tls"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
)

// autoTarget is the outbound rule for auto-discovered endpoints (minimal change to the
// KAFKA_LAB_ALLOWED_* policy): a target is allowed only if it is one of the MM2-configured
// bootstrap addresses, or a broker advertised on one of those configured ports; in both
// cases every resolved IP (hostAliases first, then DNS) must be inside KAFKA_LAB_ALLOWED_CIDRS.
// The static host and port lists keep governing registered clusters only.
func (p *NetworkPolicy) autoTarget(configured []string, hostIPs map[string]string) func(context.Context, string) (string, error) {
	exact, ports := map[string]bool{}, map[string]bool{}
	for _, a := range configured {
		host, port, e := net.SplitHostPort(strings.TrimSpace(a))
		if e != nil {
			continue
		}
		exact[net.JoinHostPort(strings.ToLower(host), port)] = true
		ports[port] = true
	}
	inCIDR := func(ip netip.Addr) bool {
		for _, c := range p.prefixes {
			if c.Contains(ip.Unmap()) {
				return true
			}
		}
		return false
	}
	return func(ctx context.Context, address string) (string, error) {
		host, port, e := net.SplitHostPort(address)
		if e != nil || host == "" {
			return "", safeError("network_denied")
		}
		host = strings.ToLower(host)
		if n, e := strconv.Atoi(port); e != nil || n < 1 || n > 65535 || (!exact[net.JoinHostPort(host, port)] && !ports[port]) {
			return "", safeError("network_denied")
		}
		var ips []netip.Addr
		if v, ok := hostIPs[host]; ok {
			if ip, e := netip.ParseAddr(v); e == nil {
				ips = append(ips, ip)
			}
		} else if ip, e := netip.ParseAddr(host); e == nil {
			ips = append(ips, ip)
		} else {
			answers, e := p.resolver.LookupIPAddr(ctx, host)
			if e != nil || len(answers) == 0 {
				return "", safeError("address_unresolvable")
			}
			for _, a := range answers {
				if ip, ok := netip.AddrFromSlice(a.IP); ok {
					ips = append(ips, ip)
				}
			}
		}
		if len(ips) == 0 {
			return "", safeError("address_unresolvable")
		}
		for _, ip := range ips {
			if !inCIDR(ip) {
				return "", safeError("network_denied")
			}
		}
		return net.JoinHostPort(ips[0].Unmap().String(), port), nil
	}
}

// AutoEndpointDialer dials only targets allowed by autoTarget, with the policy timeout.
func (p *NetworkPolicy) AutoEndpointDialer(configured []string, hostIPs map[string]string) Dialer {
	allow := p.autoTarget(configured, hostIPs)
	return func(ctx context.Context, _ string, address string) (net.Conn, error) {
		target, err := allow(ctx, address)
		if err != nil {
			return nil, err
		}
		conn, err := (&net.Dialer{Timeout: p.timeout}).DialContext(ctx, "tcp", target)
		if err != nil {
			return nil, classify(err)
		}
		return conn, nil
	}
}

// EndpointReader performs the read-only queries for auto-discovered endpoints. Every
// client it builds goes through NewReadOnlyClient, so only allowlisted requests are sent.
type EndpointReader struct {
	Dial Dialer
}

// TopicSummary is one topic of an endpoint.
type TopicSummary struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	Internal   bool   `json:"internal"`
}

// PartitionSummary counts replicas and in-sync replicas of one partition.
type PartitionSummary struct {
	Partition int32 `json:"partition"`
	Leader    int32 `json:"leader"`
	Replicas  int   `json:"replicas"`
	ISR       int   `json:"isr"`
}

// TopicDetail is the partition / replica / ISR summary of one topic.
type TopicDetail struct {
	Name       string             `json:"name"`
	Partitions []PartitionSummary `json:"partitions"`
}

func (r EndpointReader) with(ctx context.Context, seeds []string, mech sasl.Mechanism, tlsCfg *tls.Config, fn func(*kgo.Client) error) error {
	cl, err := NewReadOnlyClient(seeds, mech, tlsCfg, r.Dial)
	if err != nil {
		return safeError("invalid_configuration")
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	return classify(fn(cl))
}

func metadata(ctx context.Context, cl *kgo.Client) (*kmsg.MetadataResponse, error) {
	req := kmsg.NewPtrMetadataRequest()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	for _, t := range resp.Topics {
		if err := kerrOf(t.ErrorCode); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

// Probe connects read-only and returns the cluster ID.
func (r EndpointReader) Probe(ctx context.Context, seeds []string, mech sasl.Mechanism, tlsCfg *tls.Config) (string, error) {
	id := ""
	err := r.with(ctx, seeds, mech, tlsCfg, func(cl *kgo.Client) error {
		resp, err := metadata(ctx, cl)
		if err == nil && resp.ClusterID != nil {
			id = *resp.ClusterID
		}
		return err
	})
	return id, err
}

// Topics lists all topics, sorted by name.
func (r EndpointReader) Topics(ctx context.Context, seeds []string, mech sasl.Mechanism, tlsCfg *tls.Config) ([]TopicSummary, error) {
	out := []TopicSummary{}
	err := r.with(ctx, seeds, mech, tlsCfg, func(cl *kgo.Client) error {
		resp, err := metadata(ctx, cl)
		if err != nil {
			return err
		}
		for _, t := range resp.Topics {
			if t.Topic != nil {
				out = append(out, TopicSummary{Name: *t.Topic, Partitions: len(t.Partitions), Internal: t.IsInternal})
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, err
}

// Topic summarises partitions, replicas and ISR of one topic.
func (r EndpointReader) Topic(ctx context.Context, seeds []string, mech sasl.Mechanism, tlsCfg *tls.Config, name string) (TopicDetail, error) {
	d := TopicDetail{Name: name, Partitions: []PartitionSummary{}}
	err := r.with(ctx, seeds, mech, tlsCfg, func(cl *kgo.Client) error {
		req := kmsg.NewPtrMetadataRequest()
		t := kmsg.NewMetadataRequestTopic()
		t.Topic = kmsg.StringPtr(name)
		req.Topics = append(req.Topics, t)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			return err
		}
		for _, t := range resp.Topics {
			if err := kerrOf(t.ErrorCode); err != nil {
				return err
			}
			for _, p := range t.Partitions {
				d.Partitions = append(d.Partitions, PartitionSummary{Partition: p.Partition, Leader: p.Leader, Replicas: len(p.Replicas), ISR: len(p.ISR)})
			}
		}
		return nil
	})
	sort.Slice(d.Partitions, func(i, j int) bool { return d.Partitions[i].Partition < d.Partitions[j].Partition })
	return d, err
}

// Groups lists consumer group IDs.
func (r EndpointReader) Groups(ctx context.Context, seeds []string, mech sasl.Mechanism, tlsCfg *tls.Config) ([]string, error) {
	out := []string{}
	err := r.with(ctx, seeds, mech, tlsCfg, func(cl *kgo.Client) error {
		resp, err := kmsg.NewPtrListGroupsRequest().RequestWith(ctx, cl)
		if err != nil {
			return err
		}
		if err := kerrOf(resp.ErrorCode); err != nil {
			return err
		}
		for _, g := range resp.Groups {
			out = append(out, g.Group)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

func kerrOf(code int16) error { return kerr.ErrorForCode(code) }
