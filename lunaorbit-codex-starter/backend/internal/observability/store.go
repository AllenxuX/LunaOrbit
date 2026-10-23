package observability

import (
	"context"
	"sort"
	"sync"
)

type Store interface {
	List(context.Context, string, string) ([]Confirmation, error)
	Put(context.Context, string, Confirmation) error
}
type Audit struct{ Actor, Action, Target string }
type MemoryStore struct {
	mu        sync.Mutex
	rows      map[string]Confirmation
	Audits    []Audit
	FailAudit bool
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{rows: map[string]Confirmation{}} }
func (s *MemoryStore) List(ctx context.Context, owner, scope string) ([]Confirmation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return nil, fail("UNAVAILABLE")
	}
	out := []Confirmation{}
	for _, c := range s.rows {
		if c.ConfirmedBy == owner && c.ScopeID == scope {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func validConfirmation(owner string, c Confirmation) bool {
	return owner != "" && c.ConfirmedBy == owner && c.ID != "" && c.ScopeID != "" && c.LinkID != "" && !c.ConfirmedAt.IsZero() && topicName.MatchString(c.Topic) && c.Topic != "." && c.Topic != ".." && len(c.Description) <= 256 && c.Kind == "manual_business_confirmation"
}
func (s *MemoryStore) Put(ctx context.Context, owner string, c Confirmation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil || s.FailAudit || len(s.Audits) >= 4096 {
		return fail("UNAVAILABLE")
	}
	if !validConfirmation(owner, c) {
		return fail("INVALID_ARGUMENT")
	}
	key := owner + "\x00" + c.ScopeID + "\x00" + c.ID
	if _, exists := s.rows[key]; !exists && len(s.rows) >= 4096 {
		return fail("CAPACITY_EXCEEDED")
	}
	s.rows[key] = c
	s.Audits = append(s.Audits, Audit{Actor: owner, Action: "lineage.business.confirm", Target: c.ID})
	return nil
}
