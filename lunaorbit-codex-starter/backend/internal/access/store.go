package access

import (
	"context"
	"sync"
)

type Audit struct{ Actor, Action, RequestID string }
type MemoryStore struct {
	mu        sync.Mutex
	rows      map[string]Request
	Audits    []Audit
	FailAudit bool
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{rows: map[string]Request{}} }
func (s *MemoryStore) Get(ctx context.Context, id string) (Request, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return Request{}, ErrNotFound
	}
	return r, nil
}
func (s *MemoryStore) List(ctx context.Context, owner, id string) ([]Request, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := []Request{}
	for _, r := range s.rows {
		if r.OwnerID == owner && r.RegistrationID == id {
			v = append(v, r)
		}
	}
	return v, nil
}
func (s *MemoryStore) Save(ctx context.Context, r Request, expected int64, actor, action string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := ctx.Err(); e != nil {
		return e
	}
	if expected == 0 && len(s.rows) >= 10000 {
		return ErrStorage
	}
	old, ok := s.rows[r.ID]
	if expected == 0 && ok || expected != 0 && (!ok || old.Revision != expected || !sameRequestIdentity(old, r)) || r.Revision != expected+1 {
		return ErrConflict
	}
	if s.FailAudit {
		return ErrStorage
	}
	s.rows[r.ID] = r
	s.Audits = append(s.Audits, Audit{actor, action, r.ID})
	return nil
}
func (s *MemoryStore) Pending(ctx context.Context) ([]Request, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Request{}
	for _, r := range s.rows {
		if r.Status == "pending_approval" {
			out = append(out, r)
			if len(out) == 10000 {
				break
			}
		}
	}
	return out, nil
}

// Authorization-relevant request data is immutable after initial submission.
func sameRequestIdentity(old, next Request) bool {
	return old.ID == next.ID && old.OwnerID == next.OwnerID && old.RegistrationID == next.RegistrationID && old.Applicant == next.Applicant && old.Rule == next.Rule && old.CreatedAt.Equal(next.CreatedAt)
}
