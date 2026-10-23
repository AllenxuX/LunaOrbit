package registry

import (
	"context"
	"lunaorbit/backend/internal/kafkaops"
	"testing"
)

func TestSharedGrantIsolationAndRevoke(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	s := NewService(st, nil)
	defer s.Close()
	owner := Actor{ID: "owner", Role: "admin"}
	other := Actor{ID: "other", Role: "viewer"}
	r := Registration{ID: "cluster", CreatedBy: owner.ID, Revision: 1, Enabled: true}
	if e := st.Save(ctx, r, 0, "create"); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Get(ctx, other, r.ID); ErrorCode(e) != "NOT_FOUND" {
		t.Fatalf("isolation %v", e)
	}
	g, e := s.CreateGrant(ctx, owner, r.ID, GrantInput{SubjectType: "user", SubjectID: other.ID, Role: "operator", TopicPrefix: "team.", GroupPrefix: "team-"})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.AuthorizeResource(ctx, other, r.ID, "topic.manage", "topic:team.orders"); e != nil {
		t.Fatal(e)
	}
	if e = s.AuthorizeResource(ctx, other, r.ID, "topic.manage", "topic:private.orders"); ErrorCode(e) != "FORBIDDEN" {
		t.Fatal(e)
	}
	if e = s.AuthorizeResource(ctx, other, r.ID, "acl.manage", "topic:team.orders"); ErrorCode(e) != "FORBIDDEN" {
		t.Fatal(e)
	}
	if e = s.RevokeGrant(ctx, owner, r.ID, g.ID, g.Revision); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Get(ctx, other, r.ID); ErrorCode(e) != "NOT_FOUND" {
		t.Fatal(e)
	}
}
func TestGrantRolesAndTrustedGroup(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	s := NewService(st, NewMemorySecrets())
	defer s.Close()
	owner := Actor{ID: "owner", Role: "admin"}
	r := Registration{ID: "cluster", CreatedBy: "owner", Revision: 1, Enabled: true, SecurityProtocol: "PLAINTEXT"}
	if e := st.Save(ctx, r, 0, "create"); e != nil {
		t.Fatal(e)
	}
	_, e := s.CreateGrant(ctx, owner, r.ID, GrantInput{SubjectType: "group", SubjectID: "engineering", Role: "security-admin", TopicPrefix: "app.", GroupPrefix: "app-"})
	if e != nil {
		t.Fatal(e)
	}
	outsider := Actor{ID: "other", Role: "admin"}
	if e = s.AuthorizeResource(ctx, outsider, r.ID, "read", ""); ErrorCode(e) != "NOT_FOUND" {
		t.Fatalf("global admin bypass: %v", e)
	}
	trusted := Actor{ID: "other", Role: "viewer", Groups: []string{"engineering"}}
	if e = s.AuthorizeResource(ctx, trusted, r.ID, "acl.manage", "topic:app.orders"); e != nil {
		t.Fatal(e)
	}
	if e = s.AuthorizeResource(ctx, trusted, r.ID, "acl.manage", "topic:external"); ErrorCode(e) != "FORBIDDEN" {
		t.Fatal(e)
	}
	if e = s.AuthorizeResource(ctx, trusted, r.ID, "topic.manage", "topic:app.orders"); ErrorCode(e) != "FORBIDDEN" {
		t.Fatal(e)
	}
	viewer := Actor{ID: "viewer", Role: "admin"}
	_, e = s.CreateGrant(ctx, owner, r.ID, GrantInput{SubjectType: "user", SubjectID: viewer.ID, Role: "viewer"})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.AuthorizeResource(ctx, viewer, r.ID, "topic.manage", "topic:app.orders"); ErrorCode(e) != "FORBIDDEN" {
		t.Fatalf("platform role overrides grant: %v", e)
	}
	if got, e := s.ResourceOwner(ctx, trusted, r.ID); e != nil || got != "owner" {
		t.Fatalf("owner %s %v", got, e)
	}
	if _, e = s.AuthorizedProfile(ctx, trusted, r.ID); e != nil {
		t.Fatal(e)
	}
	rows, e := s.List(ctx, trusted)
	if e != nil || len(rows) != 1 {
		t.Fatalf("list %v %v", rows, e)
	}
}
func TestGrantRevocationAuditActualActorAndRevision(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	s := NewService(st, NewMemorySecrets())
	defer s.Close()
	owner := Actor{ID: "owner", Role: "admin"}
	st.Save(ctx, Registration{ID: "cluster", CreatedBy: "owner", Revision: 1}, 0, "create")
	_, e := s.CreateGrant(ctx, owner, "cluster", GrantInput{SubjectType: "user", SubjectID: "delegate", Role: "admin"})
	if e != nil {
		t.Fatal(e)
	}
	delegate := Actor{ID: "delegate", Role: "viewer"}
	g, e := s.CreateGrant(ctx, delegate, "cluster", GrantInput{SubjectType: "user", SubjectID: "reader", Role: "viewer"})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.RevokeGrant(ctx, delegate, "cluster", g.ID, 9); ErrorCode(e) != "CONFLICT" {
		t.Fatal(e)
	}
	if e = s.RevokeGrant(ctx, delegate, "cluster", g.ID, g.Revision); e != nil {
		t.Fatal(e)
	}
	last := st.audits[len(st.audits)-1]
	if last.Actor != "delegate" || last.Action != "kafka.team.revoke" {
		t.Fatalf("audit %+v", last)
	}
}
func TestSharedNativeReadFiltersScopesBeforeAndAfterLookup(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	s := NewService(st, NewMemorySecrets())
	defer s.Close()
	owner := Actor{ID: "owner", Role: "admin"}
	a := Actor{ID: "viewer", Role: "viewer"}
	st.Save(ctx, Registration{ID: "cluster", CreatedBy: "owner", Revision: 1, Enabled: true, SecurityProtocol: "PLAINTEXT"}, 0, "create")
	if _, e := s.CreateGrant(ctx, owner, "cluster", GrantInput{SubjectType: "user", SubjectID: a.ID, Role: "viewer", TopicPrefix: "team.", GroupPrefix: "team-"}); e != nil {
		t.Fatal(e)
	}
	called := false
	s.OperationsFactory = func(Registration, string) kafkaops.Reader { return scopedReader{&called} }
	if _, e := s.ReadOperations(ctx, a, "cluster", "topics", "private", " "); ErrorCode(e) != "FORBIDDEN" || called {
		t.Fatalf("scope queried hidden topic: %v %v", called, e)
	}
	snap, e := s.ReadOperations(ctx, a, "cluster", "topics", "", "")
	if e != nil {
		t.Fatal(e)
	}
	if len(snap.Topics) != 1 || snap.Topics[0].Name != "team.orders" || len(snap.Groups) != 1 || len(snap.Offsets) != 1 {
		t.Fatalf("scope leaked %+v", snap)
	}
}

type scopedReader struct{ called *bool }

func (r scopedReader) Read(context.Context, string, string) (kafkaops.Snapshot, error) {
	*r.called = true
	return kafkaops.Snapshot{Topics: []kafkaops.Topic{{Name: "team.orders"}, {Name: "private"}}, Groups: []kafkaops.Group{{ID: "team-consumer"}, {ID: "private"}}, Offsets: []kafkaops.Offset{{Topic: "team.orders", Group: "team-consumer"}, {Topic: "private", Group: "team-consumer"}, {Topic: "team.orders", Group: "private"}}}, nil
}
func TestApplicationMetadataRequiresUnscopedGrant(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	s := NewService(st, NewMemorySecrets())
	defer s.Close()
	owner := Actor{ID: "owner", Role: "admin"}
	scoped := Actor{ID: "scoped", Role: "admin"}
	unscoped := Actor{ID: "unscoped", Role: "viewer"}
	st.Save(ctx, Registration{ID: "cluster", CreatedBy: "owner", Revision: 1}, 0, "create")
	for _, v := range []GrantInput{{SubjectType: "user", SubjectID: scoped.ID, Role: "operator", GroupPrefix: "team-"}, {SubjectType: "user", SubjectID: unscoped.ID, Role: "operator"}} {
		if _, e := s.CreateGrant(ctx, owner, "cluster", v); e != nil {
			t.Fatal(e)
		}
	}
	if e := s.AuthorizeResource(ctx, scoped, "cluster", "applications.manage", "application:*"); ErrorCode(e) != "FORBIDDEN" {
		t.Fatalf("scoped metadata mutation allowed: %v", e)
	}
	if e := s.AuthorizeResource(ctx, scoped, "cluster", "read", "application:hidden"); ErrorCode(e) != "FORBIDDEN" {
		t.Fatalf("scoped metadata read allowed: %v", e)
	}
	if e := s.AuthorizeUnscopedResource(ctx, scoped, "cluster", "applications.manage"); ErrorCode(e) != "FORBIDDEN" {
		t.Fatalf("cluster metadata escape: %v", e)
	}
	if e := s.AuthorizeResource(ctx, unscoped, "cluster", "applications.manage", "application:*"); e != nil {
		t.Fatal(e)
	}
	if e := s.AuthorizeResource(ctx, scoped, "cluster", "topic.manage", "topic:team.orders"); e != nil {
		t.Fatalf("topic scope changed: %v", e)
	}
}
func TestDuplicateActiveGrantConflictsAndSingleRevokeRemovesAccess(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	s := NewService(st, NewMemorySecrets())
	defer s.Close()
	owner := Actor{ID: "owner", Role: "admin"}
	user := Actor{ID: "user", Role: "viewer"}
	st.Save(ctx, Registration{ID: "cluster", CreatedBy: owner.ID, Revision: 1}, 0, "create")
	input := GrantInput{SubjectType: "user", SubjectID: user.ID, Role: "viewer", TopicPrefix: "team.", GroupPrefix: "team-"}
	g, e := s.CreateGrant(ctx, owner, "cluster", input)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if _, e = s.CreateGrant(ctx, owner, "cluster", input); ErrorCode(e) != "CONFLICT" {
			t.Fatalf("duplicate grant accepted: %v", e)
		}
	}
	if len(st.audits) != 2 {
		t.Fatalf("duplicate produced audit: %+v", st.audits)
	}
	if e = s.RevokeGrant(ctx, owner, "cluster", g.ID, g.Revision); e != nil {
		t.Fatal(e)
	}
	if e = s.AuthorizeResource(ctx, user, "cluster", "read", ""); ErrorCode(e) != "NOT_FOUND" {
		t.Fatalf("hidden duplicate retained access: %v", e)
	}
	if _, e = s.CreateGrant(ctx, owner, "cluster", input); e != nil {
		t.Fatalf("re-grant after revocation failed: %v", e)
	}
}
func TestConfiguredLabTargetSurvivesLedgerResetAndRequiresAuthorization(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	s := NewService(st, NewMemorySecrets())
	defer s.Close()
	s.readOnlyNative = true
	s.labCatalog = []CreateInput{{BootstrapServers: []string{"one.synthetic.test:9092"}, CredentialInput: CredentialInput{SecurityProtocol: "PLAINTEXT"}}, {BootstrapServers: []string{"two.synthetic.test:9092"}, CredentialInput: CredentialInput{SecurityProtocol: "PLAINTEXT"}}}
	owner := Actor{ID: "owner", Role: "admin"}
	user := Actor{ID: "user", Role: "viewer"}
	r := Registration{ID: "cluster", CreatedBy: owner.ID, Revision: 1, Enabled: true, SecurityProtocol: "PLAINTEXT", BootstrapServers: []string{"one.synthetic.test:9092"}}
	st.Save(ctx, r, 0, "create")
	s.CreateGrant(ctx, owner, r.ID, GrantInput{SubjectType: "user", SubjectID: user.ID, Role: "operator"})
	if e := s.AuthorizeConfiguredLabTarget(ctx, user, r.ID); e != nil {
		t.Fatalf("ledger-free authorized target: %v", e)
	}
	if e := s.AuthorizeConfiguredLabTarget(ctx, Actor{ID: "outsider", Role: "admin"}, r.ID); ErrorCode(e) != "NOT_FOUND" {
		t.Fatalf("global admin bypass: %v", e)
	}
	r.Revision++
	r.BootstrapServers = []string{"outside.synthetic.test:9092"}
	st.Save(ctx, r, 1, "update")
	if e := s.AuthorizeConfiguredLabTarget(ctx, user, r.ID); ErrorCode(e) != "NOT_FOUND" {
		t.Fatalf("noncatalog target accepted: %v", e)
	}
}
