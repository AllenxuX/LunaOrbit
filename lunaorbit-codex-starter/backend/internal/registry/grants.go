package registry

import (
	"context"
	"strings"
	"time"
)

// Groups are populated exclusively by the authenticated server identity provider.
type GrantInput struct {
	SubjectType string `json:"subject_type"`
	SubjectID   string `json:"subject_id"`
	Role        string `json:"role"`
	TopicPrefix string `json:"topic_prefix"`
	GroupPrefix string `json:"group_prefix"`
}
type Grant struct {
	ID        string `json:"id"`
	ClusterID string `json:"cluster_id"`
	GrantInput
	Revision  int64      `json:"revision"`
	CreatedBy string     `json:"created_by"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}
type grantStore interface {
	GetByID(context.Context, string) (Registration, error)
	Grants(context.Context, string) ([]Grant, error)
	AllGrants(context.Context) ([]Grant, error)
	SaveGrant(context.Context, Grant, int64, string, string) error
}

func matchesGrant(a Actor, g Grant) bool {
	if g.RevokedAt != nil {
		return false
	}
	if g.SubjectType == "user" {
		return g.SubjectID == a.ID
	}
	for _, v := range a.Groups {
		if v == g.SubjectID {
			return true
		}
	}
	return false
}
func roleAction(role, action string) bool {
	switch action {
	case "account.manage", "credentials.read":
		return role == "admin"
	case "read":
		return role == "information" || role == "viewer" || role == "operator" || role == "security-admin" || role == "admin"
	case "topic.manage", "applications.manage":
		return role == "operator" || role == "admin"
	case "acl.manage":
		return role == "security-admin" || role == "admin"
	case "audit.read":
		return role == "security-admin" || role == "admin"
	case "team.manage":
		return role == "admin"
	}
	return false
}
func scopeMatch(g Grant, name string) bool {
	if strings.HasPrefix(name, "application:") || name == "cluster:*" {
		return g.TopicPrefix == "" && g.GroupPrefix == ""
	}
	if name == "" {
		return true
	}
	if strings.HasPrefix(name, "group:") {
		return strings.HasPrefix(strings.TrimPrefix(name, "group:"), g.GroupPrefix)
	}
	if strings.Contains(name, ":") && !strings.HasPrefix(name, "topic:") {
		return false
	}
	return strings.HasPrefix(strings.TrimPrefix(name, "topic:"), g.TopicPrefix)
}
func (s *Service) resource(ctx context.Context, a Actor, id string) (Registration, []Grant, error) {
	if a.ID == "" {
		return Registration{}, nil, failure("FORBIDDEN")
	}
	r, e := s.store.Get(ctx, a.ID, id)
	if e == nil {
		return r, nil, nil
	}
	if a.Role == "information" {
		return Registration{}, nil, failure("NOT_FOUND")
	}
	if e != ErrNotFound {
		return Registration{}, nil, serviceStoreError(e)
	}
	gs, ok := s.store.(grantStore)
	if !ok {
		return Registration{}, nil, failure("NOT_FOUND")
	}
	all, e := gs.Grants(ctx, id)
	if e != nil {
		return Registration{}, nil, serviceStoreError(e)
	}
	valid := []Grant{}
	for _, g := range all {
		if matchesGrant(a, g) {
			valid = append(valid, g)
		}
	}
	if len(valid) == 0 {
		return Registration{}, nil, failure("NOT_FOUND")
	}
	r, e = gs.GetByID(ctx, id)
	if e != nil {
		return Registration{}, nil, serviceStoreError(e)
	}
	return r, valid, nil
}
func (s *Service) AuthorizeResource(ctx context.Context, a Actor, id, action, name string) error {
	r, gs, e := s.resource(ctx, a, id)
	if e != nil {
		return e
	}
	if r.CreatedBy == a.ID {
		if roleAction(a.Role, action) {
			return nil
		}
	} else {
		for _, g := range gs {
			if roleAction(g.Role, action) && scopeMatch(g, name) {
				return nil
			}
		}
	}
	return failure("FORBIDDEN")
}
func (s *Service) ResourceOwner(ctx context.Context, a Actor, id string) (string, error) {
	if e := s.AuthorizeResource(ctx, a, id, "read", ""); e != nil {
		return "", e
	}
	r, _, e := s.resource(ctx, a, id)
	return r.CreatedBy, e
}
func (s *Service) ListGrants(ctx context.Context, a Actor, id string) ([]Grant, error) {
	if e := s.AuthorizeResource(ctx, a, id, "team.manage", ""); e != nil {
		return nil, e
	}
	gs, ok := s.store.(grantStore)
	if !ok {
		return nil, failure("STORAGE_UNAVAILABLE")
	}
	out, e := gs.Grants(ctx, id)
	if e != nil {
		return nil, serviceStoreError(e)
	}
	return out, nil
}
func (s *Service) CreateGrant(ctx context.Context, a Actor, id string, in GrantInput) (Grant, error) {
	if e := s.AuthorizeResource(ctx, a, id, "team.manage", ""); e != nil {
		return Grant{}, e
	}
	if (in.SubjectType != "user" && in.SubjectType != "group") || strings.TrimSpace(in.SubjectID) == "" || len(in.SubjectID) > 512 || !roleAction(in.Role, "read") || len(in.TopicPrefix) > 249 || len(in.GroupPrefix) > 249 || strings.ContainsAny(in.SubjectID+in.TopicPrefix+in.GroupPrefix, "\x00\r\n") {
		return Grant{}, failure("INVALID_ARGUMENT")
	}
	// Scoped administrators cannot delegate beyond their own namespace.
	r, own, e := s.resource(ctx, a, id)
	if e != nil {
		return Grant{}, e
	}
	if r.CreatedBy != a.ID {
		allowed := false
		for _, g := range own {
			if g.Role == "admin" && strings.HasPrefix(in.TopicPrefix, g.TopicPrefix) && strings.HasPrefix(in.GroupPrefix, g.GroupPrefix) {
				allowed = true
			}
		}
		if !allowed {
			return Grant{}, failure("FORBIDDEN")
		}
	}
	gs, ok := s.store.(grantStore)
	if !ok {
		return Grant{}, failure("STORAGE_UNAVAILABLE")
	}
	g := Grant{ID: randomID(), ClusterID: id, GrantInput: in, Revision: 1, CreatedBy: a.ID, CreatedAt: time.Now().UTC()}
	if e = gs.SaveGrant(ctx, g, 0, a.ID, "kafka.team.grant"); e != nil {
		return Grant{}, serviceStoreError(e)
	}
	return g, nil
}
func (s *Service) RevokeGrant(ctx context.Context, a Actor, id, gid string, revision int64) error {
	rows, e := s.ListGrants(ctx, a, id)
	if e != nil {
		return e
	}
	for _, g := range rows {
		if g.ID == gid {
			if g.Revision != revision || g.RevokedAt != nil {
				return failure("CONFLICT")
			}
			r, own, e := s.resource(ctx, a, id)
			if e != nil {
				return e
			}
			if r.CreatedBy != a.ID {
				allowed := false
				for _, p := range own {
					if p.Role == "admin" && strings.HasPrefix(g.TopicPrefix, p.TopicPrefix) && strings.HasPrefix(g.GroupPrefix, p.GroupPrefix) {
						allowed = true
					}
				}
				if !allowed {
					return failure("FORBIDDEN")
				}
			}
			now := time.Now().UTC()
			g.RevokedAt = &now
			g.Revision++
			return serviceStoreErrorNil(s.store.(grantStore).SaveGrant(ctx, g, revision, a.ID, "kafka.team.revoke"))
		}
	}
	return failure("NOT_FOUND")
}
func serviceStoreErrorNil(e error) error {
	if e == nil {
		return nil
	}
	return serviceStoreError(e)
}

// AuthorizeUnscopedResource checks cluster-wide metadata that has no Topic or group namespace.
func (s *Service) AuthorizeUnscopedResource(ctx context.Context, a Actor, id, action string) error {
	return s.AuthorizeResource(ctx, a, id, action, "cluster:*")
}
