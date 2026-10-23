package registry

import (
	"context"
	"sort"
	"sync"
)

type auditEntry struct{ Actor, Action, Target, Outcome string }
type MemoryStore struct {
	mu     sync.RWMutex
	rows   map[string]Registration
	grants map[string]Grant
	audits []auditEntry
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{rows: make(map[string]Registration)} }
func cloneRegistration(r Registration) Registration {
	r.BootstrapServers = append([]string(nil), r.BootstrapServers...)
	r.Capabilities = append([]Capability(nil), r.Capabilities...)
	r.Brokers = append([]Broker(nil), r.Brokers...)
	if r.DeletedAt != nil {
		v := *r.DeletedAt
		r.DeletedAt = &v
	}
	if r.LastProbeAt != nil {
		v := *r.LastProbeAt
		r.LastProbeAt = &v
	}
	return r
}
func (s *MemoryStore) Get(ctx context.Context, owner, id string) (Registration, error) {
	if err := ctx.Err(); err != nil {
		return Registration{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rows[id]
	if !ok || r.CreatedBy != owner || r.DeletedAt != nil {
		return Registration{}, ErrNotFound
	}
	return cloneRegistration(r), nil
}
func (s *MemoryStore) List(ctx context.Context, owner string) ([]Registration, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []Registration{}
	for _, r := range s.rows {
		if r.CreatedBy == owner && r.DeletedAt == nil {
			out = append(out, cloneRegistration(r))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (s *MemoryStore) Save(ctx context.Context, r Registration, expected int64, action string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if expected < 0 || r.Revision != expected+1 || r.ID == "" || r.CreatedBy == "" {
		return ErrConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.rows[r.ID]
	if expected == 0 {
		if exists {
			return ErrConflict
		}
	} else if !exists || old.Revision != expected || old.CreatedBy != r.CreatedBy || old.DeletedAt != nil {
		return ErrConflict
	}
	s.rows[r.ID] = cloneRegistration(r)
	s.audits = append(s.audits, auditEntry{r.CreatedBy, action, r.ID, "success"})
	return nil
}
func (s *MemoryStore) RecordAudit(ctx context.Context, actor, action, target, outcome string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if outcome != "success" && outcome != "failure" && outcome != "denied" {
		return ErrStorage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audits = append(s.audits, auditEntry{actor, action, target, outcome})
	return nil
}
