package workspace

import (
	"context"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"testing"
)

func setup(t *testing.T) (*Service, registry.Actor, string, Application) {
	t.Helper()
	r := registry.NewService(registry.NewMemoryStore(), registry.NewMemorySecrets())
	t.Cleanup(r.Close)
	a := registry.Actor{ID: "owner", Role: "admin"}
	v, e := r.Create(context.Background(), a, registry.CreateInput{DisplayName: "synthetic", BootstrapServers: []string{"broker.synthetic.test:9092"}, Enabled: true, CredentialInput: registry.CredentialInput{SecurityProtocol: "PLAINTEXT"}})
	if e != nil {
		t.Fatal(e)
	}
	s := NewService(r, NewMemoryStore(), NewMock())
	s.WriteEnabled = func(context.Context, registry.Actor, string) error { return nil }
	app, e := s.SaveApplication(context.Background(), a, v.ID, Application{Name: "Synthetic app", Principal: "User:synthetic-app", Owner: "test"})
	if e != nil {
		t.Fatal(e)
	}
	return s, a, v.ID, app
}
func request(t *testing.T, s *Service, a registry.Actor, id string, in Input) Request {
	t.Helper()
	p, e := s.Preview(context.Background(), a, id, in)
	if e != nil {
		t.Fatal(e)
	}
	in.Fingerprint = p.Fingerprint
	labAutoApprove := s.LabAutoApprove
	s.LabAutoApprove = false
	r, e := s.CreateRequest(context.Background(), a, id, in)
	s.LabAutoApprove = labAutoApprove
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestAuthorityRevisionSnapshotAndExactRevoke(t *testing.T) {
	s, a, id, app := setup(t)
	ctx := context.Background()
	in := Input{Kind: "grant", ApplicationID: app.ID, Mode: "produce", Topics: []Resource{{"events", "LITERAL"}}}
	if _, e := s.Preview(ctx, registry.Actor{ID: "other", Role: "admin"}, id, in); e == nil {
		t.Fatal("cross owner")
	}
	r := request(t, s, a, id, in)
	if _, e := s.Approve(ctx, a, id, r.ID, r.Revision); e == nil {
		t.Fatal("self approval")
	}
	s.LabAutoApprove = true
	r, e := s.Approve(ctx, a, id, r.ID, r.Revision)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Execute(ctx, a, id, r.ID, 1); e == nil {
		t.Fatal("stale revision")
	}
	r, e = s.Execute(ctx, a, id, r.ID, r.Revision)
	if e != nil || r.Status != "executed" {
		t.Fatal(r, e)
	}
	if _, e = s.Execute(ctx, a, id, r.ID, r.Revision); e == nil {
		t.Fatal("replay")
	}
	external := kafkaops.ACLRule{Principal: app.Principal, Host: "*", ResourceType: "TOPIC", ResourceName: "external", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"}
	m := s.Gateway.(*Mock)
	_ = m.CreateACL(ctx, a, id, external)
	r = request(t, s, a, id, Input{Kind: "revoke", ApplicationID: app.ID, SelectedRules: r.Plan.Additions})
	r, e = s.Approve(ctx, a, id, r.ID, r.Revision)
	if e != nil {
		t.Fatal(e)
	}
	r, e = s.Execute(ctx, a, id, r.ID, r.Revision)
	if e != nil || r.Status != "executed" {
		t.Fatal(r, e)
	}
	snap, _ := m.ACLs(ctx, a, id)
	if len(snap.Items) != 1 || key(snap.Items[0]) != key(external) {
		t.Fatal("external rule removed")
	}
}
func TestTopicLifecycleAndDrift(t *testing.T) {
	s, a, id, _ := setup(t)
	s.LabAutoApprove = true
	ctx := context.Background()
	ret := int64(60000)
	for _, in := range []Input{{Kind: "topic_create", Topic: "new-topic", Partitions: 2, ReplicationFactor: 1, RetentionMS: &ret}, {Kind: "topic_expand", Topic: "new-topic", Partitions: 4}, {Kind: "topic_retention", Topic: "new-topic", RetentionMS: &ret}} {
		r := request(t, s, a, id, in)
		r, e := s.Approve(ctx, a, id, r.ID, r.Revision)
		if e != nil {
			t.Fatal(e)
		}
		r, e = s.Execute(ctx, a, id, r.ID, r.Revision)
		if e != nil || r.Status != "executed" {
			t.Fatal(r, e)
		}
	}
	r := request(t, s, a, id, Input{Kind: "topic_expand", Topic: "new-topic", Partitions: 6})
	r, e := s.Approve(ctx, a, id, r.ID, r.Revision)
	if e != nil {
		t.Fatal(e)
	}
	_ = s.Gateway.ExpandTopic(ctx, a, id, "new-topic", 5)
	if _, e = s.Execute(ctx, a, id, r.ID, r.Revision); e == nil {
		t.Fatal("snapshot drift")
	}
}

type partialGateway struct {
	*Mock
	calls int
}

func (m *partialGateway) CreateACL(ctx context.Context, a registry.Actor, id string, r kafkaops.ACLRule) error {
	m.calls++
	if m.calls == 2 {
		return &kafkaops.Error{Code: "AUTHORIZATION_DENIED"}
	}
	return m.Mock.CreateACL(ctx, a, id, r)
}
func TestPartialFailureIsDurableAndNotRetried(t *testing.T) {
	s, a, id, app := setup(t)
	s.LabAutoApprove = true
	s.Gateway = &partialGateway{Mock: s.Gateway.(*Mock)}
	ctx := context.Background()
	r := request(t, s, a, id, Input{Kind: "grant", ApplicationID: app.ID, Mode: "produce", Topics: []Resource{{"events", "LITERAL"}}})
	r, e := s.Approve(ctx, a, id, r.ID, r.Revision)
	if e != nil {
		t.Fatal(e)
	}
	r, e = s.Execute(ctx, a, id, r.ID, r.Revision)
	if e != nil || r.Status != "partial_failure" || len(r.Results) != 2 || r.Results[0].State != "applied" {
		t.Fatal(r, e)
	}
	st, _ := s.Store.View(ctx, id)
	if st.Requests[r.ID].Status != "partial_failure" {
		t.Fatal("result not durable")
	}
	if _, e = s.Execute(ctx, a, id, r.ID, r.Revision); e == nil {
		t.Fatal("partial replay")
	}
}

func TestWorkspaceAdmissionCapacityKeepsExistingRequestExecutable(t *testing.T) {
	s, a, id, app := setup(t)
	ctx := context.Background()
	s.LabAutoApprove = true
	r := request(t, s, a, id, Input{Kind: "grant", ApplicationID: app.ID, Mode: "produce", Topics: []Resource{{"bounded", "LITERAL"}}})
	if e := s.Store.Update(ctx, id, func(st *State) error {
		for len(st.Audit) < maxAdmissionAudit {
			st.Audit = append(st.Audit, Audit{})
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if _, e := s.SaveApplication(ctx, a, id, Application{Name: "New", Principal: "User:new"}); e != errCapacity {
		t.Fatal("audit admission cap", e)
	}
	r, e := s.Approve(ctx, a, id, r.ID, r.Revision)
	if e != nil {
		t.Fatal("saved request approval blocked", e)
	}
	r, e = s.Execute(ctx, a, id, r.ID, r.Revision)
	if e != nil || r.Status != "executed" {
		t.Fatal("saved request execution blocked", e)
	}
}

func TestLabAutoApprovalRequiresFlagAndWriteGate(t *testing.T) {
	s, a, id, app := setup(t)
	ctx := context.Background()
	in := Input{Kind: "grant", ApplicationID: app.ID, Mode: "produce", Topics: []Resource{{"lab", "LITERAL"}}}
	p, e := s.Preview(ctx, a, id, in)
	if e != nil {
		t.Fatal(e)
	}
	in.Fingerprint = p.Fingerprint
	for _, tc := range []struct {
		flag, allow bool
		want        string
	}{{false, true, "pending_approval"}, {true, false, "pending_approval"}, {true, true, "approved"}} {
		s.LabAutoApprove = tc.flag
		s.WriteEnabled = func(context.Context, registry.Actor, string) error {
			if tc.allow {
				return nil
			}
			return errConflict
		}
		r, e := s.CreateRequest(ctx, a, id, in)
		if e != nil || r.Status != tc.want {
			t.Fatal(tc, r, e)
		}
		if r.Status == "approved" && r.ApprovedBy != a.ID {
			t.Fatal("missing explicit lab approver")
		}
	}
}

func TestApplicationPrincipalImmutable(t *testing.T) {
	s, a, id, app := setup(t)
	ctx := context.Background()
	app.Principal = "User:changed"
	if _, e := s.SaveApplication(ctx, a, id, app); e != errConflict {
		t.Fatal("principal changed", e)
	}
	st, e := s.Store.View(ctx, id)
	if e != nil || st.Applications[app.ID].Principal != "User:synthetic-app" {
		t.Fatal("original identity lost", e)
	}
}
