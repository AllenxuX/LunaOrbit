package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"net/http/httptest"
	"strings"
	"testing"
)

type accountFake struct {
	ids     []AccountIdentity
	creates int
}

func TestAccountImportWithoutApplicationAndHTTPApplicationFilter(t *testing.T) {
	s, a, id, app := setup(t)
	f := &accountFake{ids: []AccountIdentity{{"existing-one", "SCRAM-SHA-256"}, {"existing-two", "SCRAM-SHA-256"}}}
	s.AccountGateway = f
	s.AccountKey = []byte(strings.Repeat("k", 32))
	ctx := context.Background()
	_, e := s.SaveAccount(ctx, a, id, AccountInput{Mode: "reference", ApplicationID: app.ID, Username: "existing-one"})
	if e != nil {
		t.Fatal(e)
	}
	second, e := s.SaveAccount(ctx, a, id, AccountInput{Mode: "import", Username: "existing-two", Password: "synthetic-input-only"})
	if e != nil || len(second.ApplicationIDs) != 0 || f.creates != 0 {
		t.Fatal("password input must not require an application or change Kafka credentials", e)
	}
	r := gin.New()
	Mount(r.Group("/api/v1"), s, func(*gin.Context) registry.Actor { return a })
	recorder := httptest.NewRecorder()
	r.ServeHTTP(recorder, httptest.NewRequest("GET", "/api/v1/kafka/clusters/"+id+"/workspace/accounts?application_id="+app.ID+"&page_size=1", nil))
	var response struct {
		Items []Account `json:"items"`
		Total int       `json:"total"`
	}
	if e := json.Unmarshal(recorder.Body.Bytes(), &response); e != nil {
		t.Fatal(e)
	}
	// The setup fixture also contributes a legacy account associated with this application.
	if recorder.Code != 200 || response.Total != 2 || len(response.Items) != 1 || len(response.Items[0].ApplicationIDs) != 1 || response.Items[0].ApplicationIDs[0] != app.ID || strings.Contains(recorder.Body.String(), "existing-two") || strings.Contains(recorder.Body.String(), "synthetic-input-only") {
		t.Fatal("application filtering must precede pagination and never return secrets", recorder.Code)
	}
}

func (f *accountFake) List(context.Context, registry.Actor, string) ([]AccountIdentity, error) {
	return f.ids, nil
}
func (f *accountFake) Create(_ context.Context, _ registry.Actor, _ string, u, p string) error {
	f.creates++
	f.ids = append(f.ids, AccountIdentity{u, "SCRAM-SHA-256"})
	return nil
}
func (f *accountFake) Verify(context.Context, registry.Actor, string, string, string) error {
	return nil
}
func TestAccountEncryptedImportAndReveal(t *testing.T) {
	s, a, id, app := setup(t)
	f := &accountFake{ids: []AccountIdentity{{"existing", "SCRAM-SHA-256"}}}
	s.AccountGateway = f
	s.AccountKey = []byte(strings.Repeat("k", 32))
	v, e := s.SaveAccount(context.Background(), a, id, AccountInput{Mode: "import", ApplicationID: app.ID, Username: "existing", Password: "synthetic-private-value"})
	if e != nil {
		t.Fatal(e)
	}
	if f.creates != 0 {
		t.Fatal("import wrote credentials")
	}
	st, _ := s.Store.View(context.Background(), id)
	b, _ := json.Marshal(st)
	if strings.Contains(string(b), "synthetic-private-value") {
		t.Fatal("plaintext persisted")
	}
	password, e := s.RevealAccount(context.Background(), a, id, v.ID, v.Revision)
	if e != nil || password != "synthetic-private-value" {
		t.Fatal(e)
	}
	s.AccountKey[0] = 1
	if _, e = s.RevealAccount(context.Background(), a, id, v.ID, v.Revision); e == nil {
		t.Fatal("wrong key decrypted")
	}
}
func TestAccountApprovalAndReplay(t *testing.T) {
	s, a, id, _ := setup(t)
	f := &accountFake{}
	s.AccountGateway = f
	s.AccountKey = []byte(strings.Repeat("k", 32))
	v, e := s.SaveAccount(context.Background(), a, id, AccountInput{RequestID: uuid.NewString(), Mode: "create"})
	if e != nil {
		t.Fatal(e)
	}
	if v.Operation.Status != "pending_approval" || f.creates != 0 {
		t.Fatal(v)
	}
	if _, e = s.ApproveAccount(context.Background(), a, id, v.ID, v.Revision); e == nil {
		t.Fatal("self approval")
	}
	s.LabAutoApprove = true
	v, e = s.ApproveAccount(context.Background(), a, id, v.ID, v.Revision)
	if e != nil {
		t.Fatal(e)
	}
	v, e = s.ExecuteAccount(context.Background(), a, id, v.ID, v.Revision)
	if e != nil || v.Operation.Status != "executed" {
		t.Fatal(v, e)
	}
	if _, e = s.ExecuteAccount(context.Background(), a, id, v.ID, v.Revision); e == nil || f.creates != 1 {
		t.Fatal("replayed")
	}
}

func TestAccountConcurrentExecutionClaimsOneWriter(t *testing.T) {
	s, a, id, _ := setup(t)
	f := &accountFake{}
	s.AccountGateway = f
	s.AccountKey = []byte(strings.Repeat("k", 32))
	s.LabAutoApprove = true
	v, e := s.SaveAccount(context.Background(), a, id, AccountInput{RequestID: uuid.NewString(), Mode: "create"})
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, e := s.ExecuteAccount(context.Background(), a, id, v.ID, v.Revision); done <- e }()
	}
	success := 0
	for i := 0; i < 2; i++ {
		if <-done == nil {
			success++
		}
	}
	if success != 1 || f.creates != 1 {
		t.Fatal(success, f.creates)
	}
}
func TestScopedAccountManagementAndRevealDenied(t *testing.T) {
	s, a, id, _ := setup(t)
	ctx := context.Background()
	viewer := registry.Actor{ID: "scope-account", Role: "viewer"}
	if _, e := s.Registry.CreateGrant(ctx, a, id, registry.GrantInput{SubjectType: "user", SubjectID: viewer.ID, Role: "admin", TopicPrefix: "team.", GroupPrefix: "team-"}); e != nil {
		t.Fatal(e)
	}
	s.AccountGateway = &accountFake{}
	s.AccountKey = []byte(strings.Repeat("k", 32))
	if _, e := s.SaveAccount(ctx, viewer, id, AccountInput{RequestID: uuid.NewString(), Mode: "create"}); e == nil {
		t.Fatal("scoped create allowed")
	}
	if _, e := s.ListAccounts(ctx, viewer, id); e == nil {
		t.Fatal("scoped discovery allowed")
	}
	if _, e := s.RevealAccount(ctx, viewer, id, "unknown", 1); e == nil {
		t.Fatal("scoped reveal allowed")
	}
}

type accountUncertain struct{ accountFake }

func (f *accountUncertain) Create(ctx context.Context, a registry.Actor, id, u, p string) error {
	_ = f.accountFake.Create(ctx, a, id, u, p)
	return context.DeadlineExceeded
}
func TestAccountUncertainWriteRequiresReviewWithoutReplay(t *testing.T) {
	s, a, id, _ := setup(t)
	f := &accountUncertain{}
	s.AccountGateway = f
	s.AccountKey = []byte(strings.Repeat("k", 32))
	s.LabAutoApprove = true
	v, e := s.SaveAccount(context.Background(), a, id, AccountInput{RequestID: uuid.NewString(), Mode: "create"})
	if e != nil {
		t.Fatal(e)
	}
	v, e = s.ExecuteAccount(context.Background(), a, id, v.ID, v.Revision)
	if e != nil || v.Operation.Status != "review_required" || v.Operation.Code != "WRITE_UNCONFIRMED" {
		t.Fatal(v, e)
	}
	if _, e = s.ExecuteAccount(context.Background(), a, id, v.ID, v.Revision); e == nil || f.creates != 1 {
		t.Fatal("uncertain write replayed")
	}
}

func TestAccountProvisionIncludesImmutableACLIntent(t *testing.T) {
	s, a, id, app := setup(t)
	s.AccountGateway = &accountFake{}
	s.AccountKey = []byte(strings.Repeat("k", 32))
	s.LabAutoApprove = true
	v, e := s.SaveAccount(context.Background(), a, id, AccountInput{RequestID: uuid.NewString(), Mode: "create", ApplicationID: app.ID, AccessMode: "consume", Topics: []Resource{{"team.events", "PREFIXED"}}, Group: &Resource{"team-", "PREFIXED"}})
	if e != nil {
		t.Fatal(e)
	}
	if len(v.Operation.Rules) != 3 || v.Operation.Fingerprint == "" {
		t.Fatal(v.Operation)
	}
	v, e = s.ExecuteAccount(context.Background(), a, id, v.ID, v.Revision)
	if e != nil || v.Operation.Status != "executed" || len(v.Operation.Results) != 5 {
		t.Fatal(v, e)
	}
	snap, _ := s.Gateway.ACLs(context.Background(), a, id)
	if len(snap.Items) != 3 {
		t.Fatal(snap)
	}
}

type accountACLPartial struct {
	*Mock
	writes int
}

func (g *accountACLPartial) CreateACL(ctx context.Context, a registry.Actor, id string, r kafkaops.ACLRule) error {
	g.writes++
	if g.writes == 2 {
		return context.DeadlineExceeded
	}
	return g.Mock.CreateACL(ctx, a, id, r)
}
func TestAccountACLPartialFailureRetainsSteps(t *testing.T) {
	s, a, id, app := setup(t)
	g := &accountACLPartial{Mock: NewMock()}
	s.Gateway = g
	s.AccountGateway = &accountFake{}
	s.AccountKey = []byte(strings.Repeat("k", 32))
	s.LabAutoApprove = true
	v, e := s.SaveAccount(context.Background(), a, id, AccountInput{RequestID: uuid.NewString(), Mode: "create", ApplicationID: app.ID, AccessMode: "produce", Topics: []Resource{{"events", "LITERAL"}}})
	if e != nil {
		t.Fatal(e)
	}
	v, e = s.ExecuteAccount(context.Background(), a, id, v.ID, v.Revision)
	if e != nil || v.Operation.Status != "partial_failure" || v.Observation == "verified" || len(v.Operation.Results) != 4 {
		t.Fatal(v, e)
	}
	if _, e = s.ExecuteAccount(context.Background(), a, id, v.ID, v.Revision); e == nil || g.writes != 2 {
		t.Fatal("partial operation replay")
	}
}

func TestAccountAccessIntentRejectsInvalidConsumerAndScopedActor(t *testing.T) {
	s, a, id, _ := setup(t)
	s.AccountGateway = &accountFake{}
	s.AccountKey = []byte(strings.Repeat("k", 32))
	ctx := context.Background()
	if _, e := s.SaveAccount(ctx, a, id, AccountInput{RequestID: uuid.NewString(), Mode: "create", AccessMode: "consume", Topics: []Resource{{"events", "LITERAL"}}}); e == nil {
		t.Fatal("consumer without group")
	}
	scoped := registry.Actor{ID: "scoped-access", Role: "viewer"}
	if _, e := s.Registry.CreateGrant(ctx, a, id, registry.GrantInput{SubjectType: "user", SubjectID: scoped.ID, Role: "admin", TopicPrefix: "team.", GroupPrefix: "team-"}); e != nil {
		t.Fatal(e)
	}
	if _, e := s.SaveAccount(ctx, scoped, id, AccountInput{RequestID: uuid.NewString(), Mode: "create", AccessMode: "produce", Topics: []Resource{{"team.events", "LITERAL"}}}); e == nil {
		t.Fatal("scoped account intent")
	}
}

func TestAccountZeroKeyDisabledAndCapacityBound(t *testing.T) {
	s, a, id, _ := setup(t)
	s.AccountGateway = &accountFake{}
	s.AccountKey = make([]byte, 32)
	if _, e := s.SaveAccount(context.Background(), a, id, AccountInput{RequestID: uuid.NewString(), Mode: "create"}); e == nil {
		t.Fatal("zero key accepted")
	}
	s.AccountKey = []byte(strings.Repeat("k", 32))
	if e := s.Store.Update(context.Background(), id, func(st *State) error {
		for i := 0; i < 128; i++ {
			key := fmt.Sprint("capacity-", i)
			st.Accounts[key] = Account{ID: key, Username: key}
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if _, e := s.SaveAccount(context.Background(), a, id, AccountInput{RequestID: uuid.NewString(), Mode: "create"}); e != errCapacity {
		t.Fatal("129th account", e)
	}
}

func TestAccountCreateIdempotentAcrossServices(t *testing.T) {
	s, a, id, _ := setup(t)
	s.AccountKey = []byte(strings.Repeat("k", 32))
	s.AccountGateway = &accountFake{}
	other := NewService(s.Registry, s.Store, s.Gateway)
	other.AccountKey = s.AccountKey
	other.AccountGateway = s.AccountGateway
	in := AccountInput{RequestID: uuid.NewString(), Mode: "create", AccessMode: "produce", Topics: []Resource{{"events", "LITERAL"}}}
	results := make(chan Account, 2)
	errs := make(chan error, 2)
	for _, svc := range []*Service{s, other} {
		go func(svc *Service) { v, e := svc.SaveAccount(context.Background(), a, id, in); results <- v; errs <- e }(svc)
	}
	first, second := <-results, <-results
	if e := <-errs; e != nil {
		t.Fatal(e)
	}
	if e := <-errs; e != nil {
		t.Fatal(e)
	}
	if first.ID != second.ID || first.Username != second.Username {
		t.Fatal("duplicate account")
	}
	st, _ := s.Store.View(context.Background(), id)
	if len(st.AccountSecrets) != 1 {
		t.Fatal("duplicate secret")
	}
	in.Topics = []Resource{{"other.events", "LITERAL"}}
	if _, e := s.SaveAccount(context.Background(), a, id, in); e != errConflict {
		t.Fatal("changed intent reused", e)
	}
	if _, e := s.SaveAccount(context.Background(), a, id, AccountInput{Mode: "create"}); e != errInvalid {
		t.Fatal("missing request ID", e)
	}
}

func TestAccountDiscoveryDeduplicatesMechanismsAndRejects512Imports(t *testing.T) {
	s, a, id, _ := setup(t)
	s.AccountGateway = &accountFake{ids: []AccountIdentity{{"dual", "SCRAM-SHA-512"}, {"dual", "SCRAM-SHA-256"}, {"only512", "SCRAM-SHA-512"}}}
	s.AccountKey = []byte(strings.Repeat("k", 32))
	items, e := s.ListAccounts(context.Background(), a, id)
	if e != nil {
		t.Fatal(e)
	}
	count := 0
	for _, v := range items {
		if v.Username == "dual" {
			count++
			if v.Mechanism != "SCRAM-SHA-256" {
				t.Fatal(v)
			}
		}
	}
	if count != 1 {
		t.Fatal("duplicate dual identity", count)
	}
	for _, mode := range []string{"reference", "import"} {
		in := AccountInput{Mode: mode, Username: "only512"}
		if mode == "import" {
			in.Password = "synthetic"
		}
		if _, e := s.SaveAccount(context.Background(), a, id, in); e == nil || e.Error() != "UNSUPPORTED_API" {
			t.Fatal(mode, e)
		}
	}
	if _, e := s.SaveAccount(context.Background(), a, id, AccountInput{Mode: "reference", Username: "dual"}); e != nil {
		t.Fatal(e)
	}
}
func TestDeleteApplicationUnlinksAccountsPreservingCredentials(t *testing.T) {
	s, a, id, app := setup(t)
	s.AccountGateway = &accountFake{ids: []AccountIdentity{{"shared", "SCRAM-SHA-256"}}}
	s.AccountKey = []byte(strings.Repeat("k", 32))
	v, e := s.SaveAccount(context.Background(), a, id, AccountInput{Mode: "import", Username: "shared", Password: "synthetic-password", ApplicationID: app.ID})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.DeleteApplication(context.Background(), a, id, app.ID, app.Revision); e != nil {
		t.Fatal(e)
	}
	st, e := s.Store.View(context.Background(), id)
	if e != nil {
		t.Fatal(e)
	}
	got, ok := st.Accounts[v.ID]
	if !ok || len(got.ApplicationIDs) != 0 || got.Revision != v.Revision+1 {
		t.Fatal(got)
	}
	password, e := s.RevealAccount(context.Background(), a, id, v.ID, got.Revision)
	if e != nil || password != "synthetic-password" {
		t.Fatal("credential removed", e)
	}
}

func TestApplicationCreateRequestIDReusesOriginalWithoutAudit(t *testing.T) {
	s, a, id, _ := setup(t)
	in := Application{RequestID: uuid.NewString(), Name: "Idempotent app", Owner: "Synthetic owner"}
	first, e := s.SaveApplication(context.Background(), a, id, in)
	if e != nil {
		t.Fatal(e)
	}
	before, _ := s.Store.View(context.Background(), id)
	second, e := s.SaveApplication(context.Background(), a, id, in)
	if e != nil || first.ID != second.ID || first.Revision != second.Revision {
		t.Fatal(first, second, e)
	}
	after, _ := s.Store.View(context.Background(), id)
	if len(before.Audit) != len(after.Audit) || len(before.Applications) != len(after.Applications) {
		t.Fatal("duplicate creation/audit")
	}
	in.Description = "changed intent"
	if _, e = s.SaveApplication(context.Background(), a, id, in); e != errConflict {
		t.Fatal("request ID intent collision", e)
	}
}
