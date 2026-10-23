package access

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"time"
)

type ACLWriter interface {
	CreateACL(context.Context, registry.Actor, string, kafkaops.ACLRule) error
}
type Native struct {
	Registry *registry.Service
	Factory  *kafkaadmin.Factory
}

func (s *Service) labEligible(ctx context.Context, a registry.Actor, id string) bool {
	return s.LabWrites && s.Registry != nil && s.Registry.AuthorizeLabACL(ctx, a, id) == nil
}
func (s *Service) canExecute(ctx context.Context, a registry.Actor, r Request) bool {
	return a.Role == "admin" && a.ID == r.OwnerID && s.Writer != nil && (s.WriteRegistrations[r.RegistrationID] || s.labEligible(ctx, a, r.RegistrationID)) && r.Status == "approved_not_executed" && r.Approver != "" && (r.Approver != r.Applicant || s.LabSelfApproval && s.labEligible(ctx, a, r.RegistrationID))
}

func (n *Native) Read(ctx context.Context, a registry.Actor, id string) (kafkaops.ACLSnapshot, error) {
	p, e := n.Registry.AuthorizedProfile(ctx, a, id)
	if e != nil {
		return kafkaops.ACLSnapshot{}, e
	}
	return n.Factory.ReadACLs(ctx, p)
}
func (n *Native) CreateACL(ctx context.Context, a registry.Actor, id string, r kafkaops.ACLRule) error {
	p, e := n.Registry.AuthorizedProfile(ctx, a, id)
	if e != nil {
		return e
	}
	return n.Factory.CreateACL(ctx, p, r)
}

// Execute claims and audits the request before calling Kafka. Ambiguous failures
// are terminal and require a new approved request, preventing automatic replay.
func (s *Service) Execute(ctx context.Context, a registry.Actor, id string, revision int64) (Request, error) {
	if a.Role != "admin" || a.ID == "" {
		return Request{}, errors.New("FORBIDDEN")
	}
	r, e := s.Store.Get(ctx, id)
	if e != nil {
		return Request{}, e
	}
	if e = s.scope(ctx, a, r.RegistrationID); e != nil {
		return Request{}, e
	}
	if !(s.WriteRegistrations[r.RegistrationID] || s.labEligible(ctx, a, r.RegistrationID)) || s.Writer == nil {
		return Request{}, errors.New("DISABLED")
	}
	if r.Revision != revision || !s.canExecute(ctx, a, r) {
		return Request{}, ErrConflict
	}
	if e = validate(r.Rule); e != nil {
		return Request{}, e
	}
	r.Revision++
	r.Status = "executing"
	if e = s.Store.Save(ctx, r, revision, a.ID, "acl.execute.start"); e != nil {
		return Request{}, e
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	e = s.Writer.CreateACL(bounded, a, r.RegistrationID, r.Rule)
	expected := r.Revision
	r.Revision++
	r.Status = "executed"
	if e != nil {
		r.Status = "execution_failed"
	}
	auditCtx, auditCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer auditCancel()
	if saveErr := s.Store.Save(auditCtx, r, expected, a.ID, "acl.execute."+r.Status); saveErr != nil {
		return Request{}, saveErr
	}
	return r, e
}
