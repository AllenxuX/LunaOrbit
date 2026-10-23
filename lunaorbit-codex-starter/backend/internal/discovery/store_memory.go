package discovery

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
)

type snapshotKey struct{ owner, scope string }
type bindingKey struct{ owner, scope, namespace, uid, alias string }
type MemoryStore struct {
	mu        sync.RWMutex
	snapshots map[snapshotKey]Snapshot
	bindings  map[bindingKey]ManualBinding
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{snapshots: map[snapshotKey]Snapshot{}, bindings: map[bindingKey]ManualBinding{}}
}
func validSnapshot(owner string, v Snapshot) bool {
	return strings.TrimSpace(owner) != "" && strings.TrimSpace(v.ScopeID) != ""
}
func validBinding(owner string, b ManualBinding) bool {
	for _, v := range []string{owner, b.ScopeID, b.Namespace, b.WorkloadUID, b.Alias, b.RegistrationID} {
		if strings.TrimSpace(v) == "" {
			return false
		}
	}
	return b.ApprovedBy == owner && !b.ApprovedAt.IsZero()
}

// Call while holding the snapshot lock through the binding write. A service
// preflight check alone can race with discovery marking the workload stale.
func bindingObserved(snapshot Snapshot, b ManualBinding) bool {
	if snapshot.ScopeID != b.ScopeID {
		return false
	}
	for _, link := range snapshot.Links {
		if !link.Stale && link.Namespace == b.Namespace && link.WorkloadUID == b.WorkloadUID && (link.SourceAlias == b.Alias || link.TargetAlias == b.Alias) {
			return true
		}
	}
	return false
}
func cloneSnapshot(v Snapshot) Snapshot {
	raw, _ := json.Marshal(v)
	var out Snapshot
	_ = json.Unmarshal(raw, &out)
	return out
}
func (s *MemoryStore) Load(ctx context.Context, owner, scope string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.snapshots[snapshotKey{owner, scope}]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	return cloneSnapshot(v), nil
}
func (s *MemoryStore) Save(ctx context.Context, owner string, v Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validSnapshot(owner, v) {
		return ErrConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := snapshotKey{owner, v.ScopeID}
	if old, ok := s.snapshots[key]; ok && (v.ObservedAt.Before(old.ObservedAt) || v.AttemptedAt.Before(old.AttemptedAt)) {
		return ErrConflict
	}
	s.snapshots[key] = cloneSnapshot(v)
	return nil
}
func (s *MemoryStore) ListBindings(ctx context.Context, owner, scope string) ([]ManualBinding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []ManualBinding{}
	for k, b := range s.bindings {
		if k.owner == owner && k.scope == scope {
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.WorkloadUID != b.WorkloadUID {
			return a.WorkloadUID < b.WorkloadUID
		}
		return a.Alias < b.Alias
	})
	return out, nil
}
func (s *MemoryStore) PutBinding(ctx context.Context, owner string, b ManualBinding) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validBinding(owner, b) {
		return ErrConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !bindingObserved(s.snapshots[snapshotKey{owner, b.ScopeID}], b) {
		return ErrNotFound
	}
	s.bindings[bindingKey{owner, b.ScopeID, b.Namespace, b.WorkloadUID, b.Alias}] = b
	return nil
}
