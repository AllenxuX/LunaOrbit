package changes

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"lunaorbit/backend/internal/registry"
	"regexp"
	"sort"
	"time"
)

type Service struct {
	EnableWrites       bool
	LabSelfApproval    bool
	Store              Store
	Vault              *Vault
	Adapter            Adapter
	SourceAuthorizer   SourceAuthorizer
	ApprovalAuthorizer StaticApprovals
	catalog            map[string]Target
	slots              chan struct{}
}

func NewService(store Store, vault *Vault, catalog []Target, adapter Adapter) *Service {
	s := &Service{Store: store, Vault: vault, Adapter: adapter, catalog: map[string]Target{}, slots: make(chan struct{}, 2)}
	if len(catalog) <= 32 {
		for _, t := range catalog {
			if t.ID != "" && t.ConfigMapUID != "" && t.DeploymentUID != "" && t.TopicRuleKey != "" {
				s.catalog[t.ID] = t
			}
		}
	}
	return s
}
func (s *Service) target(ctx context.Context, a registry.Actor, id string) (Target, error) {
	t, ok := s.catalog[id]
	if !ok {
		return t, ErrNotFound
	}
	if a.ID == "" {
		return t, errors.New("UNAUTHENTICATED")
	}
	if s.SourceAuthorizer == nil || s.SourceAuthorizer(ctx, a, t) != nil {
		return t, ErrNotFound
	}
	return t, nil
}
func (s *Service) limited(ctx context.Context) (context.Context, func(), error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	select {
	case s.slots <- struct{}{}:
		return ctx, func() { <-s.slots; cancel() }, nil
	case <-ctx.Done():
		cancel()
		return nil, func() {}, errors.New("TIMEOUT")
	}
}
func (s *Service) Targets(ctx context.Context, a registry.Actor) ([]Target, error) {
	if a.ID == "" {
		return nil, errors.New("UNAUTHENTICATED")
	}
	out := []Target{}
	ctx, done, e := s.limited(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	if s.Adapter == nil {
		return out, nil
	}
	for _, t := range s.catalog {
		if _, e := s.target(ctx, a, t.ID); e != nil {
			continue
		}
		doc, e := s.Adapter.ReadConfig(ctx, t)
		if e != nil || doc.UID != t.ConfigMapUID {
			continue
		}
		t.ResourceVersion = doc.ResourceVersion
		t.Synthetic = s.Adapter.Synthetic()
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

var topicSyntax = regexp.MustCompile(`^[a-zA-Z0-9_.*^$|?+()\[\]{},:\\-]{1,256}$`)

func digest(r Request) string {
	b, _ := json.Marshal([]string{r.OwnerID, r.ID, r.TargetID, r.TopicRule, r.ExpectedResourceVersion, r.ConfigMapUID, r.WorkloadUID})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func aad(r Request) []byte {
	b, _ := json.Marshal([]string{r.OwnerID, r.TargetID, r.ID, r.ConfigMapUID, r.WorkloadUID, r.ExpectedResourceVersion})
	return b
}
func (s *Service) Submit(ctx context.Context, a registry.Actor, in Input) (Request, error) {
	var r Request
	if a.ID == "" {
		return r, errors.New("UNAUTHENTICATED")
	}
	if a.Role != "admin" && a.Role != "operator" {
		return r, errors.New("FORBIDDEN")
	}
	t, e := s.target(ctx, a, in.TargetID)
	if e != nil {
		return r, e
	}
	if !topicSyntax.MatchString(in.TopicRule) || len(in.ExpectedResourceVersion) > 128 || in.ExpectedResourceVersion == "" {
		return r, errors.New("INVALID_ARGUMENT")
	}
	if _, e = regexp.Compile(in.TopicRule); e != nil {
		return r, errors.New("INVALID_ARGUMENT")
	}
	ctx, done, e := s.limited(ctx)
	if e != nil {
		return r, e
	}
	defer done()
	if s.Adapter == nil {
		return r, errors.New("ADAPTER_UNAVAILABLE")
	}
	doc, e := s.Adapter.ReadConfig(ctx, t)
	if ctx.Err() != nil {
		return r, errors.New("TIMEOUT")
	}
	if e != nil {
		return r, errors.New("ADAPTER_UNAVAILABLE")
	}
	if doc.ResourceVersion != in.ExpectedResourceVersion || doc.UID != t.ConfigMapUID {
		return r, ErrConflict
	}
	var b [16]byte
	if _, e = rand.Read(b[:]); e != nil {
		return r, ErrStorage
	}
	r = Request{ID: hex.EncodeToString(b[:]), OwnerID: a.ID, Applicant: a.ID, TargetID: t.ID, TopicRule: in.TopicRule, ExpectedResourceVersion: doc.ResourceVersion, CurrentResourceVersion: doc.ResourceVersion, ConfigMapUID: doc.UID, WorkloadUID: t.DeploymentUID, Revision: 1, Status: "pending_approval", Synthetic: s.Adapter.Synthetic(), CreatedAt: time.Now().UTC(), Reason: "Approval required before execution"}
	if in.Preview {
		r.Status = "preview"
		return r, nil
	}
	e = s.Store.Save(ctx, r, 0, a.ID, "change.request", nil)
	return s.decorate(ctx, a, r), e
}
func (s *Service) decorate(ctx context.Context, a registry.Actor, r Request) Request {
	r.CanApprove = r.Status == "pending_approval" && (r.Applicant != a.ID && s.ApprovalAuthorizer.Allows(a, r.TargetID) || r.Applicant == a.ID && s.labSelf(a))
	_, e := s.target(ctx, a, r.TargetID)
	r.CanExecute = r.OwnerID == a.ID && (a.Role == "admin" || a.Role == "operator") && e == nil && r.Status == "approved_not_executed" && s.Adapter != nil && (s.Adapter.Synthetic() || s.EnableWrites)
	return r
}
func (s *Service) visible(ctx context.Context, a registry.Actor, r Request) bool {
	if a.ID == "" {
		return false
	}
	if s.ApprovalAuthorizer.Allows(a, r.TargetID) && r.Applicant != a.ID {
		return true
	}
	if r.OwnerID != a.ID {
		return false
	}
	_, e := s.target(ctx, a, r.TargetID)
	return e == nil
}
func (s *Service) Get(ctx context.Context, a registry.Actor, id string) (Request, error) {
	r, e := s.Store.Get(ctx, id)
	if e != nil {
		return r, e
	}
	if !s.visible(ctx, a, r) {
		return Request{}, ErrNotFound
	}
	return s.decorate(ctx, a, r), nil
}
func (s *Service) List(ctx context.Context, a registry.Actor) ([]Request, error) {
	if a.ID == "" {
		return nil, errors.New("UNAUTHENTICATED")
	}
	rows, e := s.Store.List(ctx)
	if e != nil {
		return nil, e
	}
	out := []Request{}
	for _, r := range rows {
		if s.visible(ctx, a, r) {
			out = append(out, s.decorate(ctx, a, r))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}
func (s *Service) Approve(ctx context.Context, a registry.Actor, id string, revision int64) (Request, error) {
	r, e := s.Store.Get(ctx, id)
	if e != nil {
		return Request{}, e
	}
	if !s.ApprovalAuthorizer.Allows(a, r.TargetID) && !(r.Applicant == a.ID && s.labSelf(a)) {
		return Request{}, ErrNotFound
	}
	if r.Applicant == a.ID && !s.labSelf(a) {
		return Request{}, errors.New("SELF_APPROVAL_DENIED")
	}
	if r.Revision != revision || r.Status != "pending_approval" {
		return Request{}, ErrConflict
	}
	if _, ok := s.catalog[r.TargetID]; !ok {
		return Request{}, ErrNotFound
	}
	r.Approver = a.ID
	r.ApprovalHash = digest(r)
	r.Status = "approved_not_executed"
	r.Revision++
	r.Reason = "Approved; no action executed"
	e = s.Store.Save(ctx, r, revision, a.ID, "change.approve", nil)
	return s.decorate(ctx, a, r), e
}
func (s *Service) Execute(ctx context.Context, a registry.Actor, id string, revision int64, scenario string) (Request, error) {
	r, e := s.Get(ctx, a, id)
	if e != nil {
		return Request{}, e
	}
	if r.Revision != revision {
		return Request{}, ErrConflict
	}
	if r.Status == "executing" || r.Status == "succeeded" || r.Status == "manual_review" || r.Status == "conflict" {
		return Request{}, ErrConflict
	}
	if !r.CanExecute {
		return Request{}, errors.New("FORBIDDEN")
	}
	if r.Revision != revision {
		return Request{}, ErrConflict
	}
	switch scenario {
	case "", "success", "conflict", "rollout_failed", "metrics_missing", "timeout":
	default:
		return Request{}, errors.New("INVALID_ARGUMENT")
	}
	if r.ApprovalHash == "" || r.ApprovalHash != digest(r) || (r.Approver == r.Applicant && !s.labSelf(a)) {
		return Request{}, ErrConflict
	}
	t, e := s.target(ctx, a, r.TargetID)
	if e != nil {
		return Request{}, e
	}
	if !s.Adapter.Synthetic() && !s.EnableWrites {
		return Request{}, errors.New("WRITES_DISABLED")
	}
	ctx, done, e := s.limited(ctx)
	if e != nil {
		return Request{}, e
	}
	defer done()
	ctx = context.WithValue(ctx, scenarioKey{}, scenario)
	r.Status = "executing"
	r.CanExecute = false
	r.CanApprove = false
	r.Revision++
	if e = s.Store.Save(ctx, r, revision, a.ID, "change.execute.start", nil); e != nil {
		return Request{}, e
	}
	return s.execute(ctx, a, t, r)
}
func (s *Service) finish(a registry.Actor, r Request, status, reason string) (Request, error) {
	old := r.Revision
	r.Revision++
	r.Status = status
	r.Reason = reason
	r.CanExecute = false
	r.CanApprove = false
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if e := s.Store.Save(ctx, r, old, a.ID, "change.execute.finish", nil); e != nil {
		return Request{}, e
	}
	return r, nil
}
func (s *Service) execute(ctx context.Context, a registry.Actor, t Target, r Request) (Request, error) {
	if _, e := s.target(ctx, a, t.ID); e != nil {
		return s.finish(a, r, "manual_review", "Source scope no longer authorized; no write attempted")
	}
	doc, e := s.Adapter.ReadConfig(ctx, t)
	if e != nil || ctx.Err() != nil {
		return s.finish(a, r, "manual_review", "Config read failed or timed out; no automatic rollback")
	}
	if doc.UID != r.ConfigMapUID || doc.ResourceVersion != r.ExpectedResourceVersion || t.DeploymentUID != r.WorkloadUID {
		return s.finish(a, r, "conflict", "Resource identity or version changed; no write attempted")
	}
	dep, e := s.Adapter.ReadDeployment(ctx, t)
	if e != nil || dep.UID != r.WorkloadUID || dep.ResourceVersion == "" {
		return s.finish(a, r, "conflict", "Deployment identity or version changed; no write attempted")
	}
	backup, e := s.Vault.Seal(doc.Raw, aad(r))
	if e != nil {
		return s.finish(a, r, "manual_review", "Encrypted backup unavailable; no write attempted")
	}
	old := r.Revision
	r.Revision++
	r.Reason = "Encrypted backup saved before update"
	if e = s.Store.Save(ctx, r, old, a.ID, "change.backup", backup); e != nil {
		return Request{}, e
	}
	updated, e := s.Adapter.UpdateConfig(ctx, t, doc, r.TopicRule)
	if e != nil {
		if errors.Is(e, ErrConflict) {
			return s.finish(a, r, "conflict", "Resource version conflict; update stopped")
		}
		return s.finish(a, r, "manual_review", "Config update failed; no automatic rollback")
	}
	if updated.UID != r.ConfigMapUID || updated.ResourceVersion == "" || updated.ResourceVersion == doc.ResourceVersion {
		return s.finish(a, r, "manual_review", "Updated resource identity or version was not confirmed")
	}
	r.CurrentResourceVersion = updated.ResourceVersion
	previousGeneration := dep.Generation
	dep, e = s.Adapter.Restart(ctx, t, dep, time.Now().UTC())
	if e != nil {
		return s.finish(a, r, "manual_review", "Restart failed; no automatic rollback")
	}
	if dep.UID != r.WorkloadUID || dep.Generation <= previousGeneration || dep.RestartedAt.IsZero() {
		return s.finish(a, r, "manual_review", "Restart identity or generation was not confirmed")
	}
	evidence, e := s.Adapter.Verify(ctx, t, dep)
	r.Evidence = evidence
	if e != nil || ctx.Err() != nil || evidence.Generation != dep.Generation || evidence.Generation <= 0 || evidence.ObservedGeneration < evidence.Generation || !evidence.Ready || evidence.ReplicationHealthy == nil || !*evidence.ReplicationHealthy {
		return s.finish(a, r, "manual_review", "Rollout or replication evidence incomplete; manual review required")
	}
	return s.finish(a, r, "succeeded", "Synthetic rollout and replication evidence verified")
}
func safeReason(e error) string {
	if e == nil {
		return ""
	}
	switch e.Error() {
	case "NOT_FOUND", "CONFLICT", "STORAGE_UNAVAILABLE", "FORBIDDEN", "UNAUTHENTICATED", "SELF_APPROVAL_DENIED", "INVALID_ARGUMENT", "TIMEOUT", "ADAPTER_UNAVAILABLE", "WRITES_DISABLED":
		return e.Error()
	}
	return "STORAGE_UNAVAILABLE"
}

func (s *Service) labSelf(a registry.Actor) bool {
	_, lab := s.Adapter.(*LabAdapter)
	return lab && s.EnableWrites && s.LabSelfApproval && a.Role == "admin"
}
