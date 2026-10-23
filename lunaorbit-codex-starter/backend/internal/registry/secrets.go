package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
)

// MemorySecrets is explicitly volatile and only for development/tests, never a KMS.
type MemorySecrets struct {
	mu     sync.Mutex
	values map[string][]byte
}

func NewMemorySecrets() *MemorySecrets { return &MemorySecrets{values: make(map[string][]byte)} }
func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(b[:])
}
func (s *MemorySecrets) Put(ctx context.Context, value []byte) (string, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.values) >= 4096 || len(value) > 65536 {
		return "", errors.New("secret capacity exceeded")
	}
	id := randomID()
	if id == "" {
		return "", errors.New("secret generation failed")
	}
	s.values[id] = append([]byte(nil), value...)
	return id, nil
}
func (s *MemorySecrets) Get(ctx context.Context, id string) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.values[id]
	if !ok {
		return nil, errors.New("secret unavailable")
	}
	return append([]byte(nil), v...), nil
}
func (s *MemorySecrets) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.values[id])
	delete(s.values, id)
	return nil
}
