package changes

import (
	"context"
	"sync"
)

type Audit struct {
	Actor, Action, RequestID string
	Revision                 int64
}
type MemoryStore struct {
	mu        sync.Mutex
	rows      map[string]Request
	backups   map[string][]byte
	Audits    []Audit
	FailAudit bool
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{rows: map[string]Request{}, backups: map[string][]byte{}}
}
func (s *MemoryStore) Get(ctx context.Context, id string) (Request, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return r, ErrNotFound
	}
	return r, nil
}
func (s *MemoryStore) List(ctx context.Context) ([]Request, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Request{}
	for _, r := range s.rows {
		out = append(out, r)
	}
	return out, nil
}
func (s *MemoryStore) Backup(ctx context.Context, id string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.backups[id]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), b...), nil
}
func (s *MemoryStore) Save(ctx context.Context, r Request, expected int64, actor, action string, backup []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return ErrStorage
	}
	old, ok := s.rows[r.ID]
	if expected == 0 && ok || expected != 0 && (!ok || old.Revision != expected || old.OwnerID != r.OwnerID || old.TargetID != r.TargetID || old.ConfigMapUID != r.ConfigMapUID) || r.Revision != expected+1 {
		return ErrConflict
	}
	if expected == 0 && len(s.rows) >= maxObjects {
		return ErrStorage
	}
	if s.FailAudit {
		return ErrStorage
	}
	if backup != nil {
		total := 0
		for _, b := range s.backups {
			total += len(b)
		}
		if _, ok := s.backups[r.ID]; ok || len(backup) > maxSnapshot || total+len(backup) > maxSnapshotTotal {
			return ErrStorage
		}
	}
	r.CanApprove = false
	r.CanExecute = false
	s.rows[r.ID] = r
	if backup != nil {
		s.backups[r.ID] = append([]byte(nil), backup...)
	}
	s.Audits = append(s.Audits, Audit{actor, action, r.ID, r.Revision})
	return nil
}
