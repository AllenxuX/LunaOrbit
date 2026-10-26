package main

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"time"

	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/discovery/mm2auth"
	"lunaorbit/backend/internal/kafkaadmin"

	"github.com/twmb/franz-go/pkg/sasl"
)

// configureMM2Credentials applies KAFKA_MM2_CREDENTIALS (auth B exception). Only the exact
// value "true" turns it on; then MM2 credentials are read into memory and auto endpoints are
// probed and read through the read-only allowlist client, dialing only MM2-configured
// addresses (and brokers advertised on those ports) inside KAFKA_LAB_ALLOWED_CIDRS.
func configureMM2Credentials(service *discovery.Service, env func(string) string) (bool, error) {
	if env("KAFKA_MM2_CREDENTIALS") != "true" {
		return false, nil
	}
	cidrs := []string{}
	for _, c := range strings.Split(env("KAFKA_LAB_ALLOWED_CIDRS"), ",") {
		if c = strings.TrimSpace(c); c != "" {
			cidrs = append(cidrs, c)
		}
	}
	if len(cidrs) == 0 || len(cidrs) > 16 {
		return false, errors.New("KAFKA_MM2_CREDENTIALS requires bounded KAFKA_LAB_ALLOWED_CIDRS")
	}
	policy, err := kafkaadmin.NewNetworkPolicy(kafkaadmin.NetworkPolicyConfig{AllowedCIDRs: cidrs, AllowedPorts: []int{9092}, MaxConcurrent: 2, MaxBrokers: 16, Timeout: 5 * time.Second})
	if err != nil {
		return false, errors.New("invalid MM2 endpoint network policy")
	}
	service.EnableMM2Credentials(mm2auth.NewStore())
	service.SetEndpointAccess(mm2Access{policy: policy})
	return true, nil
}

type mm2Access struct{ policy *kafkaadmin.NetworkPolicy }

func (a mm2Access) args(ep discovery.AutoEndpoint, cred mm2auth.Credential) (kafkaadmin.EndpointReader, sasl.Mechanism, *tls.Config, error) {
	var mech sasl.Mechanism
	if cred.UsesSASL() {
		m, err := cred.SASL()
		if err != nil {
			return kafkaadmin.EndpointReader{}, nil, nil, err
		}
		mech = m
	}
	var tc *tls.Config
	if cred.UsesTLS() {
		tc = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return kafkaadmin.EndpointReader{Dial: a.policy.AutoEndpointDialer(ep.Addresses, ep.HostIPs)}, mech, tc, nil
}

func reasonOf(err error) string {
	switch code := kafkaadmin.CodeOf(err); code {
	case "timeout":
		return "timeout"
	case "tls_failed":
		return "tls"
	case "authentication_failed":
		return "auth"
	case "authorization_denied":
		return "authorization"
	case "address_unresolvable":
		return "unresolvable"
	case "network_denied":
		return "blocked"
	default:
		return "refused"
	}
}

func (a mm2Access) Probe(ctx context.Context, ep discovery.AutoEndpoint, cred mm2auth.Credential) discovery.ProbeResult {
	r, mech, tc, err := a.args(ep, cred)
	now := time.Now().UTC()
	if err != nil {
		return discovery.ProbeResult{Access: discovery.AccessCredentialsOff, CheckedAt: now}
	}
	id, err := r.Probe(ctx, ep.Addresses, mech, tc)
	if err != nil {
		return discovery.ProbeResult{Access: discovery.AccessFailed, Reason: reasonOf(err), CheckedAt: now}
	}
	return discovery.ProbeResult{Access: discovery.AccessReadable, ClusterID: id, CheckedAt: now}
}

func (a mm2Access) Topics(ctx context.Context, ep discovery.AutoEndpoint, cred mm2auth.Credential) ([]discovery.TopicSummary, error) {
	r, mech, tc, err := a.args(ep, cred)
	if err != nil {
		return nil, err
	}
	rows, err := r.Topics(ctx, ep.Addresses, mech, tc)
	out := make([]discovery.TopicSummary, 0, len(rows))
	for _, t := range rows {
		out = append(out, discovery.TopicSummary{Name: t.Name, Partitions: t.Partitions, Internal: t.Internal})
	}
	return out, err
}

func (a mm2Access) Topic(ctx context.Context, ep discovery.AutoEndpoint, cred mm2auth.Credential, name string) (any, error) {
	r, mech, tc, err := a.args(ep, cred)
	if err != nil {
		return nil, err
	}
	return r.Topic(ctx, ep.Addresses, mech, tc, name)
}

func (a mm2Access) Groups(ctx context.Context, ep discovery.AutoEndpoint, cred mm2auth.Credential) ([]string, error) {
	r, mech, tc, err := a.args(ep, cred)
	if err != nil {
		return nil, err
	}
	return r.Groups(ctx, ep.Addresses, mech, tc)
}
