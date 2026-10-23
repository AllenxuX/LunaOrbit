package access

import (
	"context"
	"errors"
	"fmt"
	"lunaorbit/backend/internal/registry"
	"testing"
	"time"
)

func assertImmutableRequest(t *testing.T, store Store, base Request) {
	t.Helper()
	mutations := []struct {
		name  string
		apply func(*Request)
	}{{"owner", func(r *Request) { r.OwnerID = "forged-owner" }}, {"registration", func(r *Request) { r.RegistrationID = "forged-registration" }}, {"applicant", func(r *Request) { r.Applicant = "forged-applicant" }}, {"rule", func(r *Request) { r.Rule.Principal = "User:unapproved-principal" }}, {"created_at", func(r *Request) { r.CreatedAt = r.CreatedAt.Add(time.Second) }}}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			changed := base
			changed.Revision = base.Revision + 1
			mutation.apply(&changed)
			if e := store.Save(context.Background(), changed, base.Revision, "independent-approver", "acl.approve"); !errors.Is(e, ErrConflict) {
				t.Fatalf("immutable field accepted: %s", mutation.name)
			}
			current, e := store.Get(context.Background(), base.ID)
			if e != nil || current.Revision != base.Revision || current.OwnerID != base.OwnerID || current.RegistrationID != base.RegistrationID || current.Applicant != base.Applicant || current.Rule != base.Rule || !current.CreatedAt.Equal(base.CreatedAt) {
				t.Fatal("rejected update changed request")
			}
		})
	}
}
func TestMemoryStoreImmutableRequestIdentity(t *testing.T) {
	store := NewMemoryStore()
	base := Request{ID: "immutable", OwnerID: "owner", RegistrationID: "registration", Applicant: "owner", Revision: 1, Rule: rule(), Status: "pending_approval", CreatedAt: time.Now().UTC()}
	if e := store.Save(context.Background(), base, 0, "owner", "acl.request"); e != nil {
		t.Fatal(e)
	}
	assertImmutableRequest(t, store, base)
	if len(store.Audits) != 1 {
		t.Fatal("rejected updates appended audit")
	}
}
func inboxFlood(t *testing.T) (*Service, int) {
	t.Helper()
	service := NewService(nil, NewMemoryStore())
	service.ApprovalAuthorizer = StaticApprovals{"approver": []string{"authorized-registration"}}
	store := service.Store
	for i := 0; i < 1004; i++ {
		registration := "outside-registration"
		if i >= 1001 {
			registration = "authorized-registration"
		}
		r := Request{ID: fmt.Sprintf("pending-%04d", i), OwnerID: "owner", RegistrationID: registration, Applicant: "owner", Revision: 1, Rule: rule(), Status: "pending_approval", CreatedAt: time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC)}
		if e := store.Save(context.Background(), r, 0, "owner", "acl.request"); e != nil {
			t.Fatal(e)
		}
	}
	return service, 1004
}
func TestPendingDoesNotTruncateBeforeAuthorization(t *testing.T) {
	service, count := inboxFlood(t)
	pending, e := service.Store.Pending(context.Background())
	if e != nil || len(pending) != count {
		t.Fatal("pending results truncated before authorizing", len(pending), e)
	}
	rows, e := service.Inbox(context.Background(), registry.Actor{ID: "approver", Role: "admin"})
	if e != nil || len(rows) != 3 {
		t.Fatal("authorized requests starved by outside scope", len(rows), e)
	}
	for i, r := range rows {
		if r.RegistrationID != "authorized-registration" || i > 0 && rows[i-1].CreatedAt.Before(r.CreatedAt) {
			t.Fatal("inbox scope or order")
		}
	}
}
