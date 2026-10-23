package registry

import (
	"context"
	"lunaorbit/backend/internal/kafkaadmin"
)

// FranzGateway binds a caller-authorized registration snapshot to the ID-only
// gateway. Development HTTP mode never selects it; a real application must first
// provide production identity, persistent SecretStore and network policy.
type FranzGateway struct {
	Factory      kafkaadmin.Prober
	Registration Registration
}

func (g FranzGateway) Probe(ctx context.Context, id string) (ProbeResult, error) {
	if id != g.Registration.ID || g.Factory == nil {
		return ProbeResult{}, failure("NOT_FOUND")
	}
	r := g.Registration
	result, e := g.Factory.Probe(ctx, kafkaadmin.Profile{RegistrationID: r.ID, BootstrapServers: r.BootstrapServers, SecurityProtocol: r.SecurityProtocol, SASLMechanism: r.SASLMechanism, UsernameSecretRef: r.UsernameRef, PasswordSecretRef: r.PasswordRef, TLSCASecretRef: r.TLSCARef, TLSCertSecretRef: r.TLSCertRef, TLSKeySecretRef: r.TLSKeyRef, TLSServerName: r.TLSServerName})
	if e != nil {
		codes := map[string]string{"network_denied": "NetworkDenied", "invalid_configuration": "InvalidConfiguration", "broker_unreachable": "BrokerUnreachable", "advertised_unreachable": "AdvertisedBrokerUnreachable", "tls_failed": "TLSHandshakeFailed", "authentication_failed": "AuthenticationFailed", "authorization_denied": "AuthorizationDenied", "unsupported_api": "UnsupportedAPI", "timeout": "Timeout", "broker_limit": "BrokerLimit", "cluster_mismatch": "ClusterMismatch"}
		code := codes[kafkaadmin.CodeOf(e)]
		if code == "" {
			code = "ProbeFailed"
		}
		return ProbeResult{}, failure(code)
	}
	out := ProbeResult{ClusterID: result.ClusterID}
	for _, cap := range result.Capabilities {
		out.Capabilities = append(out.Capabilities, Capability{Feature: cap.Feature, Supported: cap.Supported, Reason: cap.Reason})
	}
	for _, broker := range result.Brokers {
		out.Brokers = append(out.Brokers, Broker{ID: broker.ID, Host: broker.Host, Port: broker.Port})
	}
	return out, nil
}
