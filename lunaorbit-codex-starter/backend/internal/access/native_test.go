package access

import (
	"context"
	"encoding/json"
	"errors"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"testing"
)

func TestLabMutationResponsesAdvertiseNextAction(t *testing.T) {
	s, a, ordinary := setup(t)
	ctx := context.Background()
	p, e := kafkaadmin.NewNetworkPolicy(kafkaadmin.NetworkPolicyConfig{AllowedHosts: []string{"127.0.0.1", "127.0.0.2"}, AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{9092}})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Registry.UseReadOnlyKafka(p); e != nil {
		t.Fatal(e)
	}
	entries := []registry.CreateInput{}
	for _, seed := range []string{"127.0.0.1:9092", "127.0.0.2:9092"} {
		entries = append(entries, registry.CreateInput{DisplayName: "Synthetic lab", BootstrapServers: []string{seed}, Enabled: true, CredentialInput: registry.CredentialInput{SecurityProtocol: "PLAINTEXT"}})
	}
	raw, _ := json.Marshal(entries)
	if e = s.Registry.ConfigureLabCatalog(ctx, string(raw), p); e != nil {
		t.Fatal(e)
	}
	rows, e := s.Registry.SeedLabCatalog(ctx, a)
	if e != nil || len(rows) != 2 {
		t.Fatal(e)
	}
	s.LabWrites = true
	s.LabSelfApproval = true
	s.Writer = writerFunc(func(context.Context, registry.Actor, string, kafkaops.ACLRule) error { return nil })
	r, e := s.Submit(ctx, a, rows[0].ID, Input{Rule: rule()})
	if e != nil || !r.CanApprove || r.CanExecute {
		t.Fatalf("submit flags: %+v %v", r, e)
	}
	r, e = s.Approve(ctx, a, r.ID, r.Revision)
	if e != nil || r.CanApprove || !r.CanExecute {
		t.Fatalf("approve flags: %+v %v", r, e)
	}
	outside, e := s.Submit(ctx, a, ordinary, Input{Rule: rule()})
	if e != nil || outside.CanApprove || outside.CanExecute {
		t.Fatalf("ordinary registration flags: %+v %v", outside, e)
	}
}

type writerFunc func(context.Context, registry.Actor, string, kafkaops.ACLRule) error

func (f writerFunc) CreateACL(c context.Context, a registry.Actor, id string, r kafkaops.ACLRule) error {
	return f(c, a, id, r)
}
func TestExecutionRequiresApprovalAllowlistAndAuditsFailure(t *testing.T) {
	s, a, id := setup(t)
	ctx := context.Background()
	r, e := s.Submit(ctx, a, id, Input{Rule: rule()})
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	s.Writer = writerFunc(func(context.Context, registry.Actor, string, kafkaops.ACLRule) error {
		calls++
		return errors.New("AUTHORIZER_UNVERIFIED")
	})
	if _, e = s.Execute(ctx, a, r.ID, r.Revision); e == nil {
		t.Fatal("disabled execution")
	}
	s.WriteRegistrations = map[string]bool{id: true}
	if _, e = s.Execute(ctx, a, r.ID, r.Revision); e == nil {
		t.Fatal("unapproved execution")
	}
	s.ApprovalAuthorizer = approveFunc(func(context.Context, registry.Actor, Request) error { return nil })
	r, e = s.Approve(ctx, registry.Actor{ID: "approver", Role: "admin"}, r.ID, r.Revision)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Execute(ctx, a, r.ID, r.Revision); e == nil {
		t.Fatal("broker failure reported success")
	}
	stored, _ := s.Store.Get(ctx, r.ID)
	if stored.Status != "execution_failed" || calls != 1 {
		t.Fatalf("%+v calls=%d", stored, calls)
	}
	if _, e = s.Execute(ctx, a, r.ID, stored.Revision); e == nil || calls != 1 {
		t.Fatal("replayed write")
	}
}

func TestExecutionAuditFailurePreventsBrokerWrite(t *testing.T) {
	s, a, id := setup(t)
	ctx := context.Background()
	r, e := s.Submit(ctx, a, id, Input{Rule: rule()})
	if e != nil {
		t.Fatal(e)
	}
	s.ApprovalAuthorizer = approveFunc(func(context.Context, registry.Actor, Request) error { return nil })
	r, e = s.Approve(ctx, registry.Actor{ID: "approver", Role: "admin"}, r.ID, r.Revision)
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	s.Writer = writerFunc(func(context.Context, registry.Actor, string, kafkaops.ACLRule) error { calls++; return nil })
	s.WriteRegistrations = map[string]bool{id: true}
	s.Store.(*MemoryStore).FailAudit = true
	if _, e = s.Execute(ctx, a, r.ID, r.Revision); !errors.Is(e, ErrStorage) || calls != 0 {
		t.Fatalf("audit failure: %v calls=%d", e, calls)
	}
	s.Store.(*MemoryStore).FailAudit = false
	r, e = s.Execute(ctx, a, r.ID, r.Revision)
	if e != nil || r.Status != "executed" || calls != 1 {
		t.Fatalf("%+v %v calls=%d", r, e, calls)
	}
}
