package access

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"sync"
	"testing"
)

type approveFunc func(context.Context, registry.Actor, Request) error

func (f approveFunc) AuthorizeApproval(c context.Context, a registry.Actor, r Request) error {
	return f(c, a, r)
}
func setup(t *testing.T) (*Service, registry.Actor, string) {
	t.Helper()
	reg := registry.NewService(registry.NewMemoryStore(), registry.NewMemorySecrets())
	t.Cleanup(reg.Close)
	a := registry.Actor{ID: "owner", Role: "admin"}
	r, e := reg.Create(context.Background(), a, registry.CreateInput{DisplayName: "synthetic", BootstrapServers: []string{"broker.synthetic.test:9092"}, Enabled: true, CredentialInput: registry.CredentialInput{SecurityProtocol: "PLAINTEXT"}})
	if e != nil {
		t.Fatal(e)
	}
	return NewService(reg, NewMemoryStore()), a, r.ID
}
func rule() kafkaops.ACLRule {
	return kafkaops.ACLRule{Principal: "User:explicit-service", Host: "*", ResourceType: "TOPIC", ResourceName: "events", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"}
}
func TestScopesPreviewApprovalCASAndRollback(t *testing.T) {
	s, a, id := setup(t)
	ctx := context.Background()
	for _, actor := range []registry.Actor{{}, {ID: a.ID, Role: "viewer"}, {ID: a.ID, Role: "operator"}, {ID: "other", Role: "admin"}} {
		if _, e := s.Read(ctx, actor, id, ""); e == nil {
			t.Fatal("unauthorized read")
		}
	}
	v, e := s.Read(ctx, a, id, "")
	if e != nil || !v.Synthetic || v.State != "pending" || v.Items[0].Principal == "User:"+a.ID {
		t.Fatal("principal inference or effective ACL")
	}
	p, e := s.Submit(ctx, a, id, Input{Rule: rule(), Preview: true})
	if e != nil || p.Status != "preview" {
		t.Fatal(e)
	}
	rows, _ := s.List(ctx, a, id)
	if len(rows) != 0 {
		t.Fatal("preview persisted")
	}
	r, e := s.Submit(ctx, a, id, Input{Rule: rule()})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Approve(ctx, registry.Actor{ID: "approver", Role: "admin"}, r.ID, 1); !errors.Is(e, ErrNotFound) {
		t.Fatal("approval default not deny")
	}
	s.ApprovalAuthorizer = approveFunc(func(_ context.Context, a registry.Actor, r Request) error {
		if a.ID != "approver" && a.ID != r.Applicant {
			return ErrNotFound
		}
		return nil
	})
	if _, e = s.Approve(ctx, a, r.ID, 1); e == nil || e.Error() != "SELF_APPROVAL_DENIED" {
		t.Fatal("self approval")
	}
	store := s.Store.(*MemoryStore)
	store.FailAudit = true
	if _, e = s.Approve(ctx, registry.Actor{ID: "approver", Role: "admin"}, r.ID, 1); !errors.Is(e, ErrStorage) {
		t.Fatal(e)
	}
	old, _ := store.Get(ctx, r.ID)
	if old.Revision != 1 || len(store.Audits) != 1 {
		t.Fatal("audit rollback")
	}
	store.FailAudit = false
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := s.Approve(ctx, registry.Actor{ID: "approver", Role: "admin"}, r.ID, 1)
			results <- e
		}()
	}
	wg.Wait()
	close(results)
	success := 0
	for e := range results {
		if e == nil {
			success++
		} else if !errors.Is(e, ErrConflict) {
			t.Fatal(e)
		}
	}
	if success != 1 || len(store.Audits) != 2 {
		t.Fatal("CAS failed")
	}
	done, _ := store.Get(ctx, r.ID)
	if done.Status != "approved_not_executed" {
		t.Fatal("unexpected execution")
	}
}
func TestValidationAndSelfApproval(t *testing.T) {
	s := NewService(nil, NewMemoryStore())
	if s.ApprovalAuthorizer != nil {
		t.Fatal("approval must default deny")
	}
	for _, r := range []kafkaops.ACLRule{{Principal: "alice"}, {Principal: "User:alice", Host: "*", ResourceType: "TOPIC", ResourceName: "events", PatternType: "REGEX", Operation: "READ", Permission: "ALLOW"}} {
		if validate(r) == nil {
			t.Fatal("unsafe rule accepted")
		}
	}
}
func TestAccessRejectsDisabledRegistration(t *testing.T) {
	s, a, id := setup(t)
	row, e := s.Registry.Get(context.Background(), a, id)
	if e != nil {
		t.Fatal(e)
	}
	off := false
	if _, e = s.Registry.Patch(context.Background(), a, id, registry.PatchInput{Revision: row.Revision, Enabled: &off}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Read(context.Background(), a, id, ""); e == nil || e.Error() != "DISABLED" {
		t.Fatal("disabled ACL read allowed", e)
	}
	if _, e = s.Submit(context.Background(), a, id, Input{Rule: rule()}); e == nil || e.Error() != "DISABLED" {
		t.Fatal("disabled ACL application allowed", e)
	}
}
