package access

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"strings"
	"time"
)

var ErrNotFound = errors.New("NOT_FOUND")
var ErrConflict = errors.New("CONFLICT")
var ErrStorage = errors.New("STORAGE_UNAVAILABLE")

type Request struct {
	CanExecute     bool             `json:"can_execute"`
	CanApprove     bool             `json:"can_approve"`
	ID             string           `json:"id"`
	OwnerID        string           `json:"-"`
	RegistrationID string           `json:"source_registration_id"`
	Applicant      string           `json:"applicant"`
	Approver       string           `json:"approver,omitempty"`
	Revision       int64            `json:"revision"`
	Rule           kafkaops.ACLRule `json:"rule"`
	Status         string           `json:"status"`
	CreatedAt      time.Time        `json:"created_at"`
}
type Input struct {
	Rule    kafkaops.ACLRule `json:"rule"`
	Preview bool             `json:"preview"`
}
type Store interface {
	Pending(context.Context) ([]Request, error)
	Get(context.Context, string) (Request, error)
	List(context.Context, string, string) ([]Request, error)
	Save(context.Context, Request, int64, string, string) error
}

// ApprovalAuthorizer must independently verify the approver's platform resource scope.
// It never receives an owner ID from request JSON. Nil means all approvals are denied.
type ApprovalAuthorizer interface {
	AuthorizeApproval(context.Context, registry.Actor, Request) error
}
type Service struct {
	LabWrites          bool
	LabSelfApproval    bool
	Native             *Native
	Writer             ACLWriter
	WriteRegistrations map[string]bool
	Registry           *registry.Service
	Store              Store
	Reader             kafkaops.ACLReader
	ApprovalAuthorizer ApprovalAuthorizer
}

func NewService(r *registry.Service, store Store) *Service {
	return &Service{Registry: r, Store: store, Reader: MockReader{}}
}
func allowed(a registry.Actor) bool {
	return a.ID != "" && (a.Role == "admin" || a.Role == "security-admin")
}
func (s *Service) scope(ctx context.Context, a registry.Actor, id string) error {
	if !allowed(a) {
		return errors.New("FORBIDDEN")
	}
	if s.Registry == nil {
		return ErrNotFound
	}
	row, e := s.Registry.Get(ctx, registry.Actor{ID: a.ID, Role: "admin"}, id)
	if e == nil && !row.Enabled {
		return errors.New("DISABLED")
	}
	return e
}
func validate(r kafkaops.ACLRule) error {
	has := func(v string, vs ...string) bool {
		for _, x := range vs {
			if v == x {
				return true
			}
		}
		return false
	}
	if !strings.HasPrefix(r.Principal, "User:") || len(r.Principal) <= 5 || len(r.Principal) > 256 || strings.ContainsAny(r.Principal+r.Host+r.ResourceName, "\x00\r\n") || len(r.Host) > 253 || r.Host == "" || r.ResourceName == "" || len(r.ResourceName) > 249 || !has(r.ResourceType, "TOPIC", "GROUP", "CLUSTER", "TRANSACTIONAL_ID") || !has(r.PatternType, "LITERAL", "PREFIXED") || !has(r.Operation, "READ", "WRITE", "DESCRIBE", "CREATE", "DELETE", "ALTER", "DESCRIBE_CONFIGS", "ALTER_CONFIGS", "IDEMPOTENT_WRITE", "ALL") || !has(r.Permission, "ALLOW", "DENY") {
		return errors.New("INVALID_ARGUMENT")
	}
	return nil
}
func (s *Service) Read(ctx context.Context, a registry.Actor, id, scenario string) (kafkaops.ACLSnapshot, error) {
	if e := s.scope(ctx, a, id); e != nil {
		return kafkaops.ACLSnapshot{}, e
	}
	if s.Reader == nil {
		return kafkaops.ACLSnapshot{}, errors.New("UNSUPPORTED_API")
	}
	var v kafkaops.ACLSnapshot
	var e error
	if s.Native != nil {
		v, e = s.Native.Read(ctx, a, id)
	} else {
		v, e = s.Reader.ReadACLs(ctx, id, scenario)
	}
	if e != nil {
		return kafkaops.ACLSnapshot{}, e
	}
	for i := range v.Items {
		v.Items[i].SourceRegistrationID = id
	}
	v.State = "pending"
	v.Reason = "Kafka authorizer and explicit Kafka principal mapping have not been verified"
	return v, nil
}
func (s *Service) Submit(ctx context.Context, a registry.Actor, id string, in Input) (Request, error) {
	if e := s.scope(ctx, a, id); e != nil {
		return Request{}, e
	}
	if e := validate(in.Rule); e != nil {
		return Request{}, e
	}
	in.Rule.SourceRegistrationID = id
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return Request{}, ErrStorage
	}
	r := Request{ID: hex.EncodeToString(b[:]), OwnerID: a.ID, RegistrationID: id, Applicant: a.ID, Revision: 1, Rule: in.Rule, Status: "pending_approval", CreatedAt: time.Now().UTC()}
	if in.Preview {
		r.Status = "preview"
		return r, nil
	}
	e := s.Store.Save(ctx, r, 0, a.ID, "acl.request")
	if e == nil {
		r = s.decorateRequest(ctx, a, r)
	}
	return r, e
}
func (s *Service) List(ctx context.Context, a registry.Actor, id string) ([]Request, error) {
	if e := s.scope(ctx, a, id); e != nil {
		return nil, e
	}
	rows, e := s.Store.List(ctx, a.ID, id)
	for i := range rows {
		rows[i] = s.decorateRequest(ctx, a, rows[i])
	}
	return rows, e
}
func (s *Service) Approve(ctx context.Context, a registry.Actor, id string, revision int64) (Request, error) {
	if a.ID == "" || a.Role != "admin" {
		return Request{}, errors.New("FORBIDDEN")
	}
	r, e := s.Store.Get(ctx, id)
	if e != nil {
		return Request{}, e
	}
	labSelf := s.LabSelfApproval && a.ID == r.Applicant && s.labEligible(ctx, a, r.RegistrationID)
	if s.ApprovalAuthorizer == nil && !labSelf {
		return Request{}, ErrNotFound
	}
	if !labSelf {
		if e = s.ApprovalAuthorizer.AuthorizeApproval(ctx, a, r); e != nil {
			return Request{}, ErrNotFound
		}
	}
	if r.Applicant == a.ID && !labSelf {
		return Request{}, errors.New("SELF_APPROVAL_DENIED")
	}
	if r.Revision != revision || r.Status != "pending_approval" {
		return Request{}, ErrConflict
	}
	r.Revision++
	r.Approver = a.ID
	r.Status = "approved_not_executed"
	e = s.Store.Save(ctx, r, revision, a.ID, "acl.approve")
	if e == nil {
		r = s.decorateRequest(ctx, a, r)
	}
	return r, e
}

func (s *Service) decorateRequest(ctx context.Context, a registry.Actor, r Request) Request {
	r.CanExecute = s.canExecute(ctx, a, r)
	r.CanApprove = s.LabSelfApproval && r.OwnerID == a.ID && r.Applicant == a.ID && r.Status == "pending_approval" && s.labEligible(ctx, a, r.RegistrationID)
	return r
}

type MockReader struct{}

func (MockReader) ReadACLs(ctx context.Context, id, scenario string) (kafkaops.ACLSnapshot, error) {
	select {
	case <-ctx.Done():
		return kafkaops.ACLSnapshot{}, errors.New("TIMEOUT")
	default:
	}
	switch scenario {
	case "", "success", "no_authorizer":
	case "authorization_denied":
		return kafkaops.ACLSnapshot{}, errors.New("AUTHORIZATION_DENIED")
	case "unsupported_api":
		return kafkaops.ACLSnapshot{}, errors.New("UNSUPPORTED_API")
	default:
		return kafkaops.ACLSnapshot{}, errors.New("INVALID_ARGUMENT")
	}
	return kafkaops.ACLSnapshot{Items: []kafkaops.ACLRule{{Principal: "User:synthetic-service", Host: "*", ResourceType: "TOPIC", ResourceName: "events", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW", SourceRegistrationID: id}}, Synthetic: true, State: "pending", ObservedAt: time.Now().UTC()}, nil
}
