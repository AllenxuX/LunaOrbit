package registry

import (
	"context"
	"testing"
)

func TestInformationActorRegistryOnly(t *testing.T) {
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	defer s.Close()
	a := Actor{ID: "shared-lab", Role: "information"}
	if _, err := s.List(context.Background(), a); err != nil {
		t.Fatalf("shared registry reads: %v", err)
	}
	for _, action := range []string{"account.manage", "credentials.read", "topic.manage", "acl.manage", "team.manage", "applications.manage"} {
		if roleAction(a.Role, action) {
			t.Fatalf("information actor permitted %s", action)
		}
	}
}
func TestInformationIgnoresHistoricResourceGrants(t *testing.T) {
	ctx := context.Background()
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	defer s.Close()
	owner := Actor{ID: "private", Role: "admin"}
	shared := Actor{ID: "shared", Role: "information"}
	row, e := s.Create(ctx, owner, CreateInput{DisplayName: "private", BootstrapServers: []string{"localhost:9092"}, Enabled: true, CredentialInput: CredentialInput{SecurityProtocol: "PLAINTEXT"}})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.CreateGrant(ctx, owner, row.ID, GrantInput{SubjectType: "user", SubjectID: shared.ID, Role: "admin"}); e != nil {
		t.Fatal(e)
	}
	rows, e := s.List(ctx, shared)
	if e != nil || len(rows) != 0 {
		t.Fatalf("historic grant merged private resource: %v %v", rows, e)
	}
	if _, e = s.Get(ctx, shared, row.ID); ErrorCode(e) != "NOT_FOUND" {
		t.Fatalf("private grant permitted get: %v", e)
	}
	if _, e = s.ReplaceCredentials(ctx, shared, row.ID, CredentialUpdate{}); ErrorCode(e) != "NOT_FOUND" {
		t.Fatalf("credentials allowed: %v", e)
	}
}
func TestInformationConnectionSecretsAreWriteOnly(t *testing.T) {
	ctx := context.Background()
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	defer s.Close()
	a := Actor{ID: "shared", Role: "information"}
	username, password := "collector", "private-connection-secret"
	row, e := s.Create(ctx, a, CreateInput{DisplayName: "SASL", BootstrapServers: []string{"localhost:9092"}, Enabled: true, CredentialInput: CredentialInput{SecurityProtocol: "SASL_PLAINTEXT", SASLMechanism: "SCRAM-SHA-256", Username: &username, Password: &password}})
	if e != nil {
		t.Fatalf("connection setup denied: %v", e)
	}
	if !row.CredentialsConfigured || row.UsernameMasked == username {
		t.Fatalf("connection DTO %#v", row)
	}
	replacement := "replacement-secret"
	row, e = s.ReplaceCredentials(ctx, a, row.ID, CredentialUpdate{Revision: row.Revision, CredentialInput: CredentialInput{SecurityProtocol: "SASL_PLAINTEXT", SASLMechanism: "SCRAM-SHA-256", Username: &username, Password: &replacement}})
	if e != nil {
		t.Fatalf("connection reconfiguration denied: %v", e)
	}
	if !row.CredentialsConfigured {
		t.Fatal("connection secret lost")
	}
	if e = s.AuthorizeResource(ctx, a, row.ID, "credentials.read", ""); ErrorCode(e) != "FORBIDDEN" {
		t.Fatalf("secret read permitted: %v", e)
	}
}
