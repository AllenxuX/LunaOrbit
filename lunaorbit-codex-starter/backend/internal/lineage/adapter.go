package lineage

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/information"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"sync"
	"time"
)

type cacheEntry struct {
	value kafkaops.Snapshot
	until time.Time
}
type pendingRead struct {
	done  chan struct{}
	value kafkaops.Snapshot
	err   error
}
type Adapter struct {
	Registry  *registry.Service
	Discovery *discovery.Service
	Records   information.Store
	Workspace string
	mu        sync.Mutex
	cache     map[string]cacheEntry
	pending   map[string]*pendingRead
}

func (a *Adapter) actor() registry.Actor { return registry.Actor{ID: a.Workspace, Role: "information"} }
func (a *Adapter) Clusters(ctx context.Context) (map[string]string, error) {
	rows, e := a.Registry.List(ctx, a.actor())
	out := map[string]string{}
	for _, r := range rows {
		out[r.ID] = r.DisplayName
	}
	return out, e
}
func (a *Adapter) Snapshots(ctx context.Context) ([]discovery.Snapshot, error) {
	out := []discovery.Snapshot{}
	if a.Discovery == nil {
		return out, nil
	}
	actor := discovery.Actor{ID: a.Workspace, Role: "information"}
	scopes, e := a.Discovery.Scopes(actor)
	if e != nil {
		return out, e
	}
	var failures []error
	for _, scope := range scopes {
		snap, e := a.Discovery.Snapshot(ctx, actor, scope.ID)
		if e != nil {
			failures = append(failures, e)
			continue
		}
		out = append(out, snap)
	}
	return out, errors.Join(failures...)
}
func (a *Adapter) Applications(ctx context.Context) ([]information.Application, bool, error) {
	out := []information.Application{}
	if a.Records == nil {
		return out, false, nil
	}
	for page := 1; page <= 10; page++ {
		p, e := a.Records.List(ctx, a.Workspace, page, 100, "")
		if e != nil {
			return out, false, e
		}
		out = append(out, p.Items...)
		if len(out) >= p.Total || len(p.Items) < 100 {
			return out, false, nil
		}
	}
	return out, true, nil
}
func (a *Adapter) Read(ctx context.Context, c, k, r string) (kafkaops.Snapshot, error) {
	key := id("read", c, k, r)
	a.mu.Lock()
	if v, ok := a.cache[key]; ok && time.Now().Before(v.until) {
		a.mu.Unlock()
		return v.value, nil
	}
	if pending, ok := a.pending[key]; ok {
		a.mu.Unlock()
		select {
		case <-pending.done:
			return pending.value, pending.err
		case <-ctx.Done():
			return kafkaops.Snapshot{}, ctx.Err()
		}
	}
	if a.pending == nil {
		a.pending = map[string]*pendingRead{}
	}
	pending := &pendingRead{done: make(chan struct{})}
	a.pending[key] = pending
	a.mu.Unlock()
	v, e := a.Registry.ReadOperations(ctx, a.actor(), c, k, r, "")
	defer func() {
		a.mu.Lock()
		pending.value = v
		pending.err = e
		delete(a.pending, key)
		close(pending.done)
		a.mu.Unlock()
	}()
	if e == nil && !v.Stale && v.State != "partial" && v.State != "unavailable" {
		a.mu.Lock()
		if a.cache == nil {
			a.cache = map[string]cacheEntry{}
		}
		if len(a.cache) >= 256 {
			for key, row := range a.cache {
				if time.Now().After(row.until) {
					delete(a.cache, key)
				}
			}
			if len(a.cache) >= 256 {
				for key := range a.cache {
					delete(a.cache, key)
					break
				}
			}
		}
		a.cache[key] = cacheEntry{v, time.Now().Add(time.Minute)}
		a.mu.Unlock()
	}
	return v, e
}
