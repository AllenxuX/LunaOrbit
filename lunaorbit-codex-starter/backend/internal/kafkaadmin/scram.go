package kafkaadmin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"golang.org/x/crypto/pbkdf2"
	"lunaorbit/backend/internal/kafkaops"
)

type SCRAMUser struct {
	Username   string `json:"username"`
	Mechanism  string `json:"mechanism"`
	Iterations int32  `json:"iterations"`
}

var scramUsername = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,128}$`)

func validSCRAMCredentials(username, password string) bool {
	return scramUsername.MatchString(username) && len(password) > 0 && len(password) <= 4096 && !strings.ContainsRune(password, 0)
}

func (f *Factory) ListSCRAM(ctx context.Context, p Profile) ([]SCRAMUser, error) {
	out := []SCRAMUser{}
	err := f.managementClient(ctx, p, func(ctx context.Context, cl *kgo.Client) error {
		req := kmsg.NewPtrDescribeUserSCRAMCredentialsRequest()
		r, e := req.RequestWith(ctx, cl)
		if e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
		if e = managementError(r.ErrorCode); e != nil {
			return e
		}
		for _, u := range r.Results {
			if e = managementError(u.ErrorCode); e != nil {
				return e
			}
			for _, c := range u.CredentialInfos {
				mechanism := "UNKNOWN"
				if c.Mechanism == 1 {
					mechanism = "SCRAM-SHA-256"
				}
				if c.Mechanism == 2 {
					mechanism = "SCRAM-SHA-512"
				}
				out = append(out, SCRAMUser{u.User, mechanism, c.Iterations})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Username == out[j].Username {
			return out[i].Mechanism < out[j].Mechanism
		}
		return out[i].Username < out[j].Username
	})
	return out, nil
}

// CreateSCRAM checks immediately before upserting. Kafka has no create-only CAS:
// external administrators can still race this check; callers must use unique names.
func (f *Factory) CreateSCRAM(ctx context.Context, p Profile, username, password string) error {
	if !validSCRAMCredentials(username, password) {
		return invalidManagement()
	}
	return f.managementClient(ctx, p, func(ctx context.Context, cl *kgo.Client) error {
		describe := kmsg.NewPtrDescribeUserSCRAMCredentialsRequest()
		describe.Users = []kmsg.DescribeUserSCRAMCredentialsRequestUser{{Name: username}}
		r, e := describe.RequestWith(ctx, cl)
		if e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
		if e = managementError(r.ErrorCode); e != nil {
			return e
		}
		if len(r.Results) != 1 || r.Results[0].User != username {
			return malformedManagement()
		}
		user := r.Results[0]
		if user.ErrorCode == 0 {
			return &kafkaops.Error{Code: "CONFLICT"}
		}
		if user.ErrorCode != 91 {
			return managementError(user.ErrorCode)
		}
		salt := make([]byte, 32)
		if _, e = rand.Read(salt); e != nil {
			return malformedManagement()
		}
		req := kmsg.NewPtrAlterUserSCRAMCredentialsRequest()
		req.Upsertions = []kmsg.AlterUserSCRAMCredentialsRequestUpsertion{{Name: username, Mechanism: 1, Iterations: 8192, Salt: salt, SaltedPassword: pbkdf2.Key([]byte(password), salt, 8192, 32, sha256.New)}}
		result, e := req.RequestWith(ctx, cl)
		if e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
		if len(result.Results) != 1 || result.Results[0].User != username {
			return malformedManagement()
		}
		return managementError(result.Results[0].ErrorCode)
	})
}

// VerifySCRAM keeps supplied credentials only in this short-lived client. The
// management identity discovers and validates listeners; the supplied identity
// performs an authenticated ApiVersions request, requiring no topic/cluster ACL.
func (f *Factory) VerifySCRAM(ctx context.Context, p Profile, username, password string) error {
	return f.verifySCRAMListener(ctx, p, p, username, password, true)
}

// VerifySCRAMListener supports a separate, server-owned SASL listener profile.
// Its bootstrap list must contain every endpoint to authenticate; management
// discovery cannot infer port mappings between different listener names.
// Caller-provided browser endpoints must never be used to construct this profile.
func (f *Factory) VerifySCRAMListener(ctx context.Context, management, listener Profile, username, password string) error {
	return f.verifySCRAMListener(ctx, management, listener, username, password, false)
}

func (f *Factory) verifySCRAMListener(ctx context.Context, management, p Profile, username, password string, includeDiscovered bool) error {
	if !validSCRAMCredentials(username, password) || (p.SecurityProtocol != "SASL_SSL" && p.SecurityProtocol != "SASL_PLAINTEXT") {
		return invalidManagement()
	}
	ctx, cancel := context.WithTimeout(ctx, f.policy.timeout)
	defer cancel()
	target, err := f.Probe(ctx, management)
	if err != nil {
		return &kafkaops.Error{Code: operationCode(err)}
	}
	select {
	case f.policy.slots <- struct{}{}:
		defer func() { <-f.policy.slots }()
	case <-ctx.Done():
		return &kafkaops.Error{Code: "TIMEOUT"}
	}
	// Obtain only TLS and policy dial options; never resolve or replace credential
	// references in the shared secrets provider.
	transport := p
	if p.SecurityProtocol == "SASL_SSL" {
		transport.SecurityProtocol = "SSL"
	} else {
		transport.SecurityProtocol = "PLAINTEXT"
	}
	opts, err := f.clientOptions(ctx, transport)
	if err != nil {
		return &kafkaops.Error{Code: operationCode(err)}
	}
	addresses := append([]string(nil), p.BootstrapServers...)
	if includeDiscovered {
		for _, b := range target.Brokers {
			addresses = append(addresses, net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port))))
		}
	}
	if len(addresses) == 0 || len(addresses) > f.policy.maxBrokers+len(management.BootstrapServers) {
		return invalidManagement()
	}
	// Validate all configured SASL targets before sending credentials to any one.
	for _, address := range addresses {
		if _, e := f.policy.resolve(ctx, address); e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
	}
	seen := map[string]bool{}
	for _, address := range addresses {
		if seen[address] {
			continue
		}
		seen[address] = true
		options := append([]kgo.Opt{}, opts...)
		options = append(options, kgo.SeedBrokers(address), kgo.SASL(safeMechanism{scram.Auth{User: username, Pass: password}.AsSha256Mechanism()}))
		cl, e := kgo.NewClient(options...)
		if e != nil {
			return invalidManagement()
		}
		r, e := kmsg.NewPtrApiVersionsRequest().RequestWith(ctx, cl)
		cl.Close()
		if e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
		if e = managementError(r.ErrorCode); e != nil {
			return e
		}
	}
	return nil
}
