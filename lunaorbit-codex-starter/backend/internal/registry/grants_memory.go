package registry

import "context"

func (s *MemoryStore) GetByID(ctx context.Context, id string) (Registration, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rows[id]
	if !ok || r.DeletedAt != nil {
		return Registration{}, ErrNotFound
	}
	return cloneRegistration(r), ctx.Err()
}
func (s *MemoryStore) Grants(ctx context.Context, id string) ([]Grant, error) {
	all, e := s.AllGrants(ctx)
	out := []Grant{}
	for _, g := range all {
		if g.ClusterID == id {
			out = append(out, g)
		}
	}
	return out, e
}
func (s *MemoryStore) AllGrants(ctx context.Context) ([]Grant, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []Grant{}
	for _, g := range s.grants {
		if g.RevokedAt != nil {
			v := *g.RevokedAt
			g.RevokedAt = &v
		}
		out = append(out, g)
	}
	return out, ctx.Err()
}
func (s *MemoryStore) SaveGrant(ctx context.Context, g Grant, expected int64, actor, action string) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.grants == nil {
		s.grants = map[string]Grant{}
	}
	old, ok := s.grants[g.ID]
	if g.Revision != expected+1 || expected == 0 && ok || expected > 0 && (!ok || old.Revision != expected || old.ClusterID != g.ClusterID || old.RevokedAt != nil) {
		return ErrConflict
	}
	if r, ok := s.rows[g.ClusterID]; !ok || r.DeletedAt != nil {
		return ErrNotFound
	}
	if expected == 0 {
		for _, active := range s.grants {
			if active.RevokedAt == nil && active.ClusterID == g.ClusterID && active.GrantInput == g.GrantInput {
				return ErrConflict
			}
		}
	}
	s.grants[g.ID] = g
	s.audits = append(s.audits, auditEntry{actor, action, g.ClusterID, "success"})
	return nil
}
