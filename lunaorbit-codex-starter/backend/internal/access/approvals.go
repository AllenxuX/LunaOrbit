package access

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/registry"
	"sort"
)

// StaticApprovals is a server-owned subject-to-registration allowlist.
type StaticApprovals map[string][]string

func (s StaticApprovals) AuthorizeApproval(ctx context.Context, a registry.Actor, r Request) error {
	if a.ID == "" || a.Role != "admin" {
		return ErrNotFound
	}
	for _, id := range s[a.ID] {
		if id == r.RegistrationID {
			return nil
		}
	}
	return ErrNotFound
}
func (s *Service) Inbox(ctx context.Context, a registry.Actor) ([]Request, error) {
	if a.ID == "" || a.Role != "admin" {
		return nil, errors.New("FORBIDDEN")
	}
	out := []Request{}
	if s.ApprovalAuthorizer == nil {
		return out, nil
	}
	rows, e := s.Store.Pending(ctx)
	if e != nil {
		return nil, e
	}
	for _, r := range rows {
		if r.Applicant != a.ID && s.ApprovalAuthorizer.AuthorizeApproval(ctx, a, r) == nil {
			r.CanApprove = true
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out, nil
}
