package registry

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func syntheticInput() CreateInput {
	u, p := "synthetic-user", "synthetic-password"
	return CreateInput{DisplayName: "Synthetic cluster", BootstrapServers: []string{"broker.synthetic.test:9092"}, Enabled: true, CredentialInput: CredentialInput{SecurityProtocol: "SASL_SSL", SASLMechanism: "SCRAM-SHA-256", Username: &u, Password: &p}}
}

func TestRegistrationSecretsAndOwnership(t *testing.T) {
	service := NewService(NewMemoryStore(), NewMemorySecrets())
	defer service.Close()
	actor := Actor{ID: "synthetic-owner", Role: "admin"}
	row, err := service.Create(context.Background(), actor, syntheticInput())
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != "unverified" || !row.CredentialsConfigured {
		t.Fatal("registration marked healthy or missing credentials")
	}
	body, _ := json.Marshal(row)
	for _, secret := range []string{"synthetic-password", "synthetic-user", "password_ref", "secret_ref"} {
		if strings.Contains(string(body), secret) {
			t.Fatal("DTO exposed secret")
		}
	}
	if _, err := service.Get(context.Background(), Actor{ID: "other", Role: "admin"}, row.ID); ErrorCode(err) != "NOT_FOUND" {
		t.Fatal("cross-user access allowed")
	}
	if _, err := service.Create(context.Background(), Actor{ID: "viewer", Role: "viewer"}, syntheticInput()); ErrorCode(err) != "FORBIDDEN" {
		t.Fatal("viewer mutation allowed")
	}
	rows, err := service.List(context.Background(), Actor{ID: "other", Role: "admin"})
	if err != nil || len(rows) != 0 {
		t.Fatal("cross-user list leaked")
	}
}

func TestCredentialRevisionAndInvalidation(t *testing.T) {
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	defer s.Close()
	actor := Actor{ID: "owner", Role: "admin"}
	row, err := s.Create(context.Background(), actor, syntheticInput())
	if err != nil {
		t.Fatal(err)
	}
	newPassword := "synthetic-replacement"
	changed, err := s.ReplaceCredentials(context.Background(), actor, row.ID, CredentialUpdate{Revision: row.Revision, CredentialInput: CredentialInput{SecurityProtocol: "SASL_SSL", SASLMechanism: "SCRAM-SHA-256", Password: &newPassword}})
	if err != nil || changed.CredentialRevision != 2 || changed.Status != "unverified" {
		t.Fatalf("rotation failed: %v", err)
	}
	if _, err = s.ReplaceCredentials(context.Background(), actor, row.ID, CredentialUpdate{Revision: row.Revision, CredentialInput: syntheticInput().CredentialInput}); ErrorCode(err) != "CONFLICT" {
		t.Fatal("stale update accepted")
	}
}

func TestMockProbeScenariosAndRateLimit(t *testing.T) {
	for _, tc := range []struct{ scenario, code string }{{"success", ""}, {"authentication_failed", "AuthenticationFailed"}, {"tls_failed", "TLSHandshakeFailed"}, {"broker_unreachable", "BrokerUnreachable"}, {"advertised_unreachable", "AdvertisedBrokerUnreachable"}, {"authorization_denied", "AuthorizationDenied"}, {"unsupported_api", "UnsupportedAPI"}, {"timeout", "Timeout"}, {"kafka37", ""}, {"kafka392", ""}} {
		t.Run(tc.scenario, func(t *testing.T) {
			s := NewService(NewMemoryStore(), NewMemorySecrets())
			defer s.Close()
			actor := Actor{ID: "owner", Role: "admin"}
			row, err := s.Create(context.Background(), actor, syntheticInput())
			if err != nil {
				t.Fatal(err)
			}
			job, err := s.StartProbe(context.Background(), actor, row.ID, tc.scenario)
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Second)
			for job.Status != "done" && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
				job, err = s.ProbeStatus(context.Background(), actor, row.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			if job.Status != "done" || job.ErrorCode != tc.code {
				t.Fatalf("unexpected probe result: %+v", job)
			}
			if _, err = s.StartProbe(context.Background(), actor, row.ID, tc.scenario); ErrorCode(err) != "RATE_LIMITED" {
				t.Fatal("probe rate limit missing")
			}
		})
	}
}

func TestDisabledAndDeletedRegistrationsCannotProbe(t *testing.T) {
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	defer s.Close()
	actor := Actor{ID: "owner", Role: "admin"}
	input := syntheticInput()
	input.Enabled = false
	row, err := s.Create(context.Background(), actor, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.StartProbe(context.Background(), actor, row.ID, "success"); ErrorCode(err) != "DISABLED" {
		t.Fatal("disabled probe allowed")
	}
	if err = s.Delete(context.Background(), actor, row.ID, row.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Get(context.Background(), actor, row.ID); ErrorCode(err) != "NOT_FOUND" {
		t.Fatal("deleted record visible")
	}
}

type waitingGateway struct{ started chan struct{} }

func (g waitingGateway) Probe(ctx context.Context, id string) (ProbeResult, error) {
	close(g.started)
	<-ctx.Done()
	return ProbeResult{}, failure("Timeout")
}
func TestStoppedProbePersistsFailure(t *testing.T) {
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	actor := Actor{ID: "owner", Role: "admin"}
	started := make(chan struct{})
	s.GatewayFactory = func(Registration, string) KafkaAdminGateway { return waitingGateway{started: started} }
	row, e := s.Create(context.Background(), actor, syntheticInput())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.StartProbe(context.Background(), actor, row.ID, ""); e != nil {
		t.Fatal(e)
	}
	<-started
	s.Close()
	row, e = s.Get(context.Background(), actor, row.ID)
	if e != nil || row.Status != "failed" || row.LastProbeErrorCode != "Timeout" {
		t.Fatalf("failed probe not persisted: %+v %v", row, e)
	}
}
