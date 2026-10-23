package kafkaadmin

import (
	"context"
	"crypto/tls"
	"github.com/twmb/franz-go/pkg/kerr"
	"testing"
)

type memorySecrets map[string][]byte

func (m memorySecrets) Get(_ context.Context, k string) ([]byte, error) { return m[k], nil }
func TestFactorySecurity(t *testing.T) {
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{})
	f, _ := NewFactory(p, memorySecrets{"u": []byte("synthetic-user"), "p": []byte("synthetic-password")})
	for _, protocol := range []string{"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"} {
		for _, mechanism := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
			cfg, sasl, e := f.security(context.Background(), Profile{SecurityProtocol: protocol, SASLMechanism: mechanism, UsernameSecretRef: "u", PasswordSecretRef: "p"})
			if e != nil {
				t.Fatal(e)
			}
			want := protocol == "SSL" || protocol == "SASL_SSL"
			if (cfg != nil) != want {
				t.Fatal(protocol)
			}
			if cfg != nil && (cfg.InsecureSkipVerify || cfg.MinVersion < tls.VersionTLS12) {
				t.Fatal("unsafe TLS")
			}
			if (sasl != nil) != (protocol == "SASL_SSL" || protocol == "SASL_PLAINTEXT") {
				t.Fatal("sasl")
			}
		}
	}
	if _, _, e := f.security(context.Background(), Profile{SecurityProtocol: "SSL", TLSCASecretRef: "p"}); CodeOf(e) != "tls_failed" {
		t.Fatal(e)
	}
}
func TestErrorClasses(t *testing.T) {
	for _, c := range []struct {
		e    error
		code string
	}{{kerr.SaslAuthenticationFailed, "authentication_failed"}, {kerr.ClusterAuthorizationFailed, "authorization_denied"}, {kerr.UnsupportedVersion, "unsupported_api"}, {context.DeadlineExceeded, "timeout"}} {
		if CodeOf(classify(c.e)) != c.code {
			t.Fatal(c)
		}
	}
}
func TestProbeRejectsEverySeedBeforeDial(t *testing.T) {
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{9092}})
	f, _ := NewFactory(p, nil)
	_, e := f.Probe(context.Background(), Profile{SecurityProtocol: "PLAINTEXT", BootstrapServers: []string{"127.0.0.1:9092", "10.0.0.1:9092"}})
	if CodeOf(e) != "network_denied" {
		t.Fatal(e)
	}
}
func TestAdvertisedClassificationPreservesSecurityErrors(t *testing.T) {
	for _, code := range []string{"tls_failed", "authentication_failed", "authorization_denied", "unsupported_api", "broker_limit", "cluster_mismatch"} {
		if got := CodeOf(advertisedError(safeError(code))); got != code {
			t.Fatalf("%s became %s", code, got)
		}
	}
	for _, code := range []string{"network_denied", "broker_unreachable", "timeout"} {
		if got := CodeOf(advertisedError(safeError(code))); got != "advertised_unreachable" {
			t.Fatalf("%s became %s", code, got)
		}
	}
}
