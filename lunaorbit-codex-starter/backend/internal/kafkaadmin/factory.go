package kafkaadmin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"net"
	"strconv"
)

type SecretReader interface {
	Get(context.Context, string) ([]byte, error)
}
type Profile struct {
	RegistrationID                                                                                                                          string
	BootstrapServers                                                                                                                        []string
	SecurityProtocol, SASLMechanism, UsernameSecretRef, PasswordSecretRef, TLSCASecretRef, TLSCertSecretRef, TLSKeySecretRef, TLSServerName string
}
type Capability struct {
	Feature   string
	Supported bool
	Reason    string
}
type Broker struct {
	ID   int32
	Host string
	Port int32
}
type Result struct {
	ClusterID    string
	Capabilities []Capability
	Brokers      []Broker
}
type Prober interface {
	Probe(context.Context, Profile) (Result, error)
}
type Factory struct {
	policy  *NetworkPolicy
	secrets SecretReader
}

func NewFactory(policy *NetworkPolicy, secrets SecretReader) (*Factory, error) {
	if policy == nil {
		return nil, safeError("invalid_configuration")
	}
	return &Factory{policy, secrets}, nil
}
func (f *Factory) secret(ctx context.Context, ref string) ([]byte, error) {
	if f.secrets == nil || ref == "" {
		return nil, safeError("invalid_configuration")
	}
	b, e := f.secrets.Get(ctx, ref)
	if e != nil || len(b) == 0 {
		return nil, safeError("invalid_configuration")
	}
	return b, nil
}
func (f *Factory) security(ctx context.Context, p Profile) (*tls.Config, sasl.Mechanism, error) {
	var tc *tls.Config
	var mechanism sasl.Mechanism
	switch p.SecurityProtocol {
	case "SSL", "SASL_SSL":
		tc = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: p.TLSServerName}
	case "PLAINTEXT", "SASL_PLAINTEXT":
	default:
		return nil, nil, safeError("invalid_configuration")
	}
	if tc != nil {
		if p.TLSCASecretRef != "" {
			pem, e := f.secret(ctx, p.TLSCASecretRef)
			if e != nil {
				return nil, nil, e
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, nil, safeError("tls_failed")
			}
			tc.RootCAs = pool
		}
		if p.TLSCertSecretRef != "" || p.TLSKeySecretRef != "" {
			cert, e := f.secret(ctx, p.TLSCertSecretRef)
			if e != nil {
				return nil, nil, e
			}
			key, e := f.secret(ctx, p.TLSKeySecretRef)
			if e != nil {
				return nil, nil, e
			}
			pair, e := tls.X509KeyPair(cert, key)
			if e != nil {
				return nil, nil, safeError("tls_failed")
			}
			tc.Certificates = []tls.Certificate{pair}
		}
	}
	if p.SecurityProtocol == "SASL_SSL" || p.SecurityProtocol == "SASL_PLAINTEXT" {
		u, e := f.secret(ctx, p.UsernameSecretRef)
		if e != nil {
			return nil, nil, e
		}
		pw, e := f.secret(ctx, p.PasswordSecretRef)
		if e != nil {
			return nil, nil, e
		}
		switch p.SASLMechanism {
		case "PLAIN":
			mechanism = plain.Auth{User: string(u), Pass: string(pw)}.AsMechanism()
		case "SCRAM-SHA-256":
			mechanism = scram.Auth{User: string(u), Pass: string(pw)}.AsSha256Mechanism()
		case "SCRAM-SHA-512":
			mechanism = scram.Auth{User: string(u), Pass: string(pw)}.AsSha512Mechanism()
		default:
			return nil, nil, safeError("invalid_configuration")
		}
	}
	return tc, mechanism, nil
}
func classify(e error) error {
	if e == nil {
		return nil
	}
	var se safeError
	if errors.As(e, &se) {
		return se
	}
	if errors.Is(e, context.DeadlineExceeded) || errors.Is(e, context.Canceled) {
		return safeError("timeout")
	}
	var ne net.Error
	if errors.As(e, &ne) && ne.Timeout() {
		return safeError("timeout")
	}
	var auth *kerr.Error
	if errors.As(e, &auth) {
		switch auth.Code {
		case 29, 30, 31, 53, 65:
			return safeError("authorization_denied")
		case 33, 34, 58:
			return safeError("authentication_failed")
		case 35:
			return safeError("unsupported_api")
		}
	}
	return safeError("broker_unreachable")
}
func (f *Factory) Probe(ctx context.Context, p Profile) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, f.policy.timeout)
	defer cancel()
	select {
	case f.policy.slots <- struct{}{}:
		defer func() { <-f.policy.slots }()
	case <-ctx.Done():
		return Result{}, safeError("timeout")
	}
	if len(p.BootstrapServers) == 0 || len(p.BootstrapServers) > f.policy.maxBrokers {
		return Result{}, safeError("invalid_configuration")
	}
	// Resolve and validate every seed before constructing any client. The dialer
	// resolves again on each connect, checks all answers, then dials a numeric IP.
	for _, seed := range p.BootstrapServers {
		if _, e := f.policy.resolve(ctx, seed); e != nil {
			return Result{}, e
		}
	}
	opts, e := f.clientOptions(ctx, p)
	if e != nil {
		return Result{}, e
	}
	probe := func(seed string) (*kmsg.MetadataResponse, error) {
		o := append([]kgo.Opt{}, opts...)
		o = append(o, kgo.SeedBrokers(seed))
		client, e := kgo.NewClient(o...)
		if e != nil {
			return nil, safeError("invalid_configuration")
		}
		defer client.Close()
		request := kmsg.NewPtrMetadataRequest()
		request.Topics = []kmsg.MetadataRequestTopic{}
		response, e := request.RequestWith(ctx, client)
		if e != nil {
			return nil, classify(e)
		}
		if response.ErrorCode != 0 {
			return nil, classify(kerr.ErrorForCode(response.ErrorCode))
		}
		if len(response.Brokers) == 0 || len(response.Brokers) > f.policy.maxBrokers {
			return nil, safeError("broker_limit")
		}
		for _, b := range response.Brokers {
			if _, e := f.policy.resolve(ctx, net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port)))); e != nil {
				return nil, advertisedError(e)
			}
		}
		return response, nil
	}
	result := Result{}
	seen := map[string]bool{}
	collect := func(metadata *kmsg.MetadataResponse) error {
		if metadata.ClusterID != nil {
			if result.ClusterID != "" && result.ClusterID != *metadata.ClusterID {
				return safeError("cluster_mismatch")
			}
			result.ClusterID = *metadata.ClusterID
		}
		for _, b := range metadata.Brokers {
			address := net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port)))
			if !seen[address] {
				if len(result.Brokers) >= f.policy.maxBrokers {
					return safeError("broker_limit")
				}
				seen[address] = true
				result.Brokers = append(result.Brokers, Broker{b.NodeID, b.Host, b.Port})
			}
		}
		return nil
	}
	for _, seed := range p.BootstrapServers {
		metadata, e := probe(seed)
		if e != nil {
			return Result{}, e
		}
		if e = collect(metadata); e != nil {
			return Result{}, e
		}
	}
	// Include new brokers observed from any seed or advertised broker, bounded by
	// the total broker budget and the overall context deadline.
	for i := 0; i < len(result.Brokers); i++ {
		b := result.Brokers[i]
		metadata, e := probe(net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port))))
		if e != nil {
			return Result{}, advertisedError(e)
		}
		if e = collect(metadata); e != nil {
			return Result{}, e
		}
	}
	result.Capabilities = []Capability{{Feature: "METADATA", Supported: true, Reason: "observed"}}
	for _, feature := range []string{"ACL_READ", "ACL_WRITE", "TOPIC_CREATE", "TOPIC_ALTER_CONFIG", "GROUP_LAG", "DESCRIBE_QUORUM"} {
		result.Capabilities = append(result.Capabilities, Capability{Feature: feature, Reason: "not_probed"})
	}
	return result, nil
}

// SASL challenge errors may include untrusted server text. Replace them at the
// mechanism boundary, before they can reach a client error or logger.
type safeMechanism struct{ sasl.Mechanism }

func (m safeMechanism) Authenticate(ctx context.Context, host string) (sasl.Session, []byte, error) {
	session, b, e := m.Mechanism.Authenticate(ctx, host)
	if e != nil {
		return nil, nil, safeError("authentication_failed")
	}
	return safeSession{session}, b, nil
}

type safeSession struct{ sasl.Session }

func (s safeSession) Challenge(b []byte) (bool, []byte, error) {
	done, response, e := s.Session.Challenge(b)
	if e != nil {
		return false, nil, safeError("authentication_failed")
	}
	return done, response, nil
}

// advertisedError identifies network failures after broker discovery while
// retaining authentication, TLS, authorization and API-specific diagnoses.
func advertisedError(err error) error {
	switch CodeOf(err) {
	case "network_denied", "broker_unreachable", "timeout":
		return safeError("advertised_unreachable")
	default:
		return err
	}
}
