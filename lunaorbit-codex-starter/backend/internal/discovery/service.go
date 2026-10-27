package discovery

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"lunaorbit/backend/internal/discovery/collector"
	"lunaorbit/backend/internal/discovery/mm2auth"
	"lunaorbit/backend/internal/discovery/resource"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

type Actor struct{ ID, Role string }
type Error struct{ Code string }

func (e *Error) Error() string { return "discovery: " + e.Code }
func fail(code string) error   { return &Error{Code: code} }
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	var safe *Error
	if errors.As(err, &safe) {
		return safe.Code
	}
	if errors.Is(err, ErrNotFound) {
		return "NOT_FOUND"
	}
	if errors.Is(err, ErrConflict) {
		return "CONFLICT"
	}
	return "STORAGE_UNAVAILABLE"
}

type RegistrationLookup func(context.Context, string) ([]Registration, error)
type SourceFactory func(resource.Scope, string) (collector.Source, func(), error)
type Job struct {
	ID         string     `json:"job_id"`
	Status     string     `json:"status"`
	ErrorCode  string     `json:"error_code,omitempty"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}
type Service struct {
	store       Store
	lookup      RegistrationLookup
	factory     SourceFactory
	scopes      map[string]resource.Scope
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	jobs        map[string]Job
	slots       chan struct{}
	workers     sync.WaitGroup
	labBindings []LabBinding
	// scopeMu guards the scope registry, which auto mode changes at runtime.
	scopeMu  sync.RWMutex
	removed  map[string]time.Time
	kiteErr  map[string]string
	autoMode bool
	// mm2Creds is non-nil only when KAFKA_MM2_CREDENTIALS is on; credentials stay in memory.
	mm2Creds  *mm2auth.Store
	endpoints map[string][]AutoEndpoint
	probes    map[string]ProbeResult
	access    EndpointAccess
}

// ClusterEntry is one cluster from Kite's cluster list (name only; Kite has
// no stable cluster identifier and omits clusters the key cannot access).
type ClusterEntry struct{ Name, Error string }

// ScopeInfo is the public scope listing. Auto-only fields are omitted for
// static scopes. Namespace metadata describes the actual collection boundary.
type ScopeInfo struct {
	Namespaces    []string   `json:"namespaces"`
	NamespaceMode string     `json:"namespace_mode"`
	ID            string     `json:"id"`
	Synthetic     bool       `json:"synthetic"`
	KubeCluster   string     `json:"k8s_cluster,omitempty"`
	Auto          bool       `json:"auto,omitempty"`
	Removed       bool       `json:"removed,omitempty"`
	RemovedAt     *time.Time `json:"removed_at,omitempty"`
	KiteError     string     `json:"kite_error,omitempty"`
}

func NewService(store Store, scopes []resource.Scope, factory SourceFactory, lookup RegistrationLookup) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	byID := map[string]resource.Scope{}
	for _, scope := range scopes {
		byID[scope.ID] = scope
	}
	return &Service{store: store, lookup: lookup, factory: factory, scopes: byID, ctx: ctx, cancel: cancel, jobs: map[string]Job{}, slots: make(chan struct{}, 2), removed: map[string]time.Time{}, kiteErr: map[string]string{}}
}
func NewMockService(store Store, lookup RegistrationLookup) (*Service, error) {
	_, scope, closeMock, e := collector.NewMock("success")
	if e != nil {
		return nil, fail("INITIALIZATION_FAILED")
	}
	closeMock()
	return NewService(store, []resource.Scope{scope}, func(expected resource.Scope, scenario string) (collector.Source, func(), error) {
		source, actual, closeSource, e := collector.NewMock(scenario)
		if e != nil {
			return nil, closeSource, e
		}
		if !sameScope(actual, expected) {
			closeSource()
			return nil, func() {}, fail("SCOPE_MISMATCH")
		}
		return source, closeSource, nil
	}, lookup), nil
}
func (s *Service) Close() { s.mu.Lock(); s.closed = true; s.cancel(); s.mu.Unlock(); s.workers.Wait() }
func (s *Service) authorize(actor Actor, write bool) error {
	if actor.ID == "" || (actor.Role != "information" && actor.Role != "admin" && actor.Role != "operator" && actor.Role != "viewer") || (write && actor.Role == "viewer") {
		return fail("FORBIDDEN")
	}
	return nil
}
func (s *Service) scope(id string) (resource.Scope, bool) {
	s.scopeMu.RLock()
	defer s.scopeMu.RUnlock()
	scope, ok := s.scopes[id]
	return scope, ok
}
func (s *Service) isRemoved(id string) bool {
	s.scopeMu.RLock()
	defer s.scopeMu.RUnlock()
	_, gone := s.removed[id]
	return gone
}

// EnableAutoScopes switches lab bindings to cluster-qualified entries.
// EnableMM2Credentials turns on reading MM2 client credentials into the given in-memory store.
func (s *Service) EnableMM2Credentials(store *mm2auth.Store) {
	s.scopeMu.Lock()
	s.mm2Creds = store
	s.scopeMu.Unlock()
}

// Endpoints returns the auto-discovered endpoints of every scope (no credential material).
func (s *Service) Endpoints() []AutoEndpoint {
	s.scopeMu.RLock()
	defer s.scopeMu.RUnlock()
	ids := make([]string, 0, len(s.endpoints))
	for id := range s.endpoints {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := []AutoEndpoint{}
	for _, id := range ids {
		out = append(out, s.endpoints[id]...)
	}
	return out
}

func (s *Service) EnableAutoScopes() { s.scopeMu.Lock(); s.autoMode = true; s.scopeMu.Unlock() }

// AutoScopeID derives a stable, URL-safe scope id from a Kite cluster name.
func AutoScopeID(name string) string {
	var b strings.Builder
	dash := false
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			dash = false
		} else if !dash && b.Len() > 0 {
			b.WriteByte('-')
			dash = true
		}
		if b.Len() >= 40 {
			break
		}
	}
	slug := strings.Trim(b.String(), "-")
	sum := sha256.Sum256([]byte(name))
	if slug == "" {
		return "k8s-" + hex.EncodeToString(sum[:4])
	}
	return "k8s-" + slug + "-" + hex.EncodeToString(sum[:4])
}

// SyncClusters reconciles auto scopes with Kite's current cluster list: new
// names get a scope, missing names are marked removed (history and bindings
// are kept), returning names become active again. Static scopes are untouched.
func (s *Service) SyncClusters(list []ClusterEntry, now time.Time) {
	s.scopeMu.Lock()
	defer s.scopeMu.Unlock()
	present := map[string]bool{}
	for _, entry := range list {
		if entry.Name == "" {
			continue
		}
		id := AutoScopeID(entry.Name)
		if old, ok := s.scopes[id]; ok && !old.Auto {
			continue
		}
		present[id] = true
		if _, ok := s.scopes[id]; !ok {
			s.scopes[id] = resource.Scope{ID: id, Cluster: entry.Name, Auto: true}
		}
		delete(s.removed, id)
		if entry.Error != "" {
			s.kiteErr[id] = entry.Error
		} else {
			delete(s.kiteErr, id)
		}
	}
	for id, scope := range s.scopes {
		if scope.Auto && !present[id] {
			if _, gone := s.removed[id]; !gone {
				s.removed[id] = now.UTC()
			}
		}
	}
}
func (s *Service) markRemoved(id string, now time.Time) {
	s.scopeMu.Lock()
	defer s.scopeMu.Unlock()
	if _, gone := s.removed[id]; !gone {
		s.removed[id] = now.UTC()
	}
}

// WatchClusters polls Kite's cluster list until the service closes. A failed
// poll changes nothing, so a transient Kite error never marks clusters removed.
func (s *Service) WatchClusters(interval time.Duration, list func(context.Context) ([]ClusterEntry, error)) {
	poll := func() {
		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		defer cancel()
		if rows, e := list(ctx); e == nil {
			s.SyncClusters(rows, time.Now())
		}
	}
	poll()
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				poll()
			}
		}
	}()
}

// DiscoverAll starts one collection for every active (not removed) scope. It is used once
// at startup so the in-memory endpoint catalog is complete without a manual refresh.
// Failures are only logged; it never blocks startup. Returns how many runs were started.
func (s *Service) DiscoverAll(actor Actor) int {
	scopes, e := s.Scopes(actor)
	if e != nil {
		slog.Warn("startup_discovery_skipped", "code", "SCOPES_UNAVAILABLE")
		return 0
	}
	started := 0
	for _, scope := range scopes {
		if scope.Removed || scope.Synthetic {
			continue
		}
		if _, e := s.Start(s.ctx, actor, scope.ID, "success"); e != nil {
			slog.Warn("startup_discovery_failed", "scope", scope.ID)
			continue
		}
		started++
	}
	slog.Info("startup_discovery_started", "scopes", started)
	return started
}

func (s *Service) Scopes(actor Actor) ([]ScopeInfo, error) {
	if e := s.authorize(actor, false); e != nil {
		return nil, e
	}
	s.scopeMu.RLock()
	defer s.scopeMu.RUnlock()
	result := make([]ScopeInfo, 0, len(s.scopes))
	for _, scope := range s.scopes {
		info := ScopeInfo{ID: scope.ID, Synthetic: scope.Synthetic, Namespaces: append([]string{}, scope.Namespaces...), NamespaceMode: "allowlist"}
		if scope.Auto {
			info.NamespaceMode = "all_accessible"
			info.KubeCluster, info.Auto, info.KiteError = scope.Cluster, true, s.kiteErr[scope.ID]
			if at, gone := s.removed[scope.ID]; gone {
				at := at
				info.Removed, info.RemovedAt = true, &at
			}
		}
		result = append(result, info)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}
func (s *Service) Snapshot(ctx context.Context, actor Actor, scope string) (Snapshot, error) {
	if e := s.authorize(actor, false); e != nil {
		return Snapshot{}, e
	}
	if _, ok := s.scope(scope); !ok {
		return Snapshot{}, ErrNotFound
	}
	snapshot, e := s.store.Load(ctx, actor.ID, scope)
	if e != nil {
		return Snapshot{}, e
	}
	return snapshot, nil
}
func newID() string {
	var bytes [16]byte
	if _, e := rand.Read(bytes[:]); e != nil {
		return ""
	}
	return hex.EncodeToString(bytes[:])
}
func (s *Service) Start(ctx context.Context, actor Actor, scopeID, scenario string) (Job, error) {
	if e := s.authorize(actor, true); e != nil {
		return Job{}, e
	}
	scope, ok := s.scope(scopeID)
	if !ok {
		return Job{}, ErrNotFound
	}
	if s.isRemoved(scopeID) {
		return Job{}, fail("SCOPE_REMOVED")
	}
	if scenario == "" {
		scenario = "success"
	}
	if scenario != "success" && scenario != "partial_failure" && scenario != "unavailable" {
		return Job{}, fail("INVALID_ARGUMENT")
	}
	if !scope.Synthetic && scenario != "success" {
		return Job{}, fail("INVALID_ARGUMENT")
	}
	if ctx.Err() != nil {
		return Job{}, fail("CANCELED")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Job{}, fail("SERVICE_STOPPED")
	}
	key := actor.ID + "\x00" + scopeID
	if old, ok := s.jobs[key]; ok && (old.Status != "done" || time.Since(old.StartedAt) < 2*time.Second) {
		return Job{}, fail("RATE_LIMITED")
	}
	if _, exists := s.jobs[key]; !exists && len(s.jobs) >= 128 {
		for id, old := range s.jobs {
			if old.Status == "done" && time.Since(old.StartedAt) > 5*time.Minute {
				delete(s.jobs, id)
			}
		}
		if len(s.jobs) >= 128 {
			return Job{}, fail("RATE_LIMITED")
		}
	}
	select {
	case s.slots <- struct{}{}:
	default:
		return Job{}, fail("RATE_LIMITED")
	}
	id := newID()
	if id == "" {
		<-s.slots
		return Job{}, fail("INTERNAL_ERROR")
	}
	job := Job{ID: id, Status: "pending", StartedAt: time.Now().UTC()}
	s.jobs[key] = job
	s.workers.Add(1)
	go s.run(actor, scope, scenario, key)
	return job, nil
}
func (s *Service) Latest(ctx context.Context, actor Actor, scope string) (Job, error) {
	if e := s.authorize(actor, false); e != nil {
		return Job{}, e
	}
	if _, ok := s.scope(scope); !ok {
		return Job{}, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[actor.ID+"\x00"+scope]
	if !ok {
		return Job{}, ErrNotFound
	}
	return job, nil
}
func (s *Service) run(actor Actor, scope resource.Scope, scenario, key string) {
	defer s.workers.Done()
	defer func() { <-s.slots }()
	s.mu.Lock()
	job := s.jobs[key]
	job.Status = "running"
	s.jobs[key] = job
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	code := ""
	defer func() {
		if recover() != nil {
			code = "DISCOVERY_FAILED"
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		job := s.jobs[key]
		job.Status = "done"
		job.ErrorCode = code
		now := time.Now().UTC()
		job.FinishedAt = &now
		s.jobs[key] = job
	}()
	previous, e := s.store.Load(ctx, actor.ID, scope.ID)
	if e != nil && !errors.Is(e, ErrNotFound) {
		code = "STORAGE_UNAVAILABLE"
		return
	}
	col := resource.Collection{Scope: scope, ObservedAt: time.Now().UTC()}
	source, closeSource, e := s.factory(scope, scenario)
	if closeSource != nil {
		defer closeSource()
	}
	if e != nil {
		col.ErrorCode = "COLLECTION_FAILED"
	} else {
		col, e = source.Collect(ctx, scope)
		if e != nil {
			failure := "COLLECTION_FAILED"
			if scope.Auto && col.ErrorCode != "" {
				failure = col.ErrorCode
			}
			col = resource.Collection{Scope: scope, ObservedAt: time.Now().UTC(), ErrorCode: failure}
			if scope.Auto && failure == "NOT_FOUND" {
				// Kite answers 404 for a cluster that is no longer registered.
				s.markRemoved(scope.ID, time.Now())
			}
		}
	}
	if ctx.Err() != nil {
		col = resource.Collection{Scope: scope, ObservedAt: time.Now().UTC(), ErrorCode: "TIMEOUT"}
	}
	if !sameScope(col.Scope, scope) {
		col = resource.Collection{Scope: scope, ObservedAt: time.Now().UTC(), ErrorCode: "SCOPE_MISMATCH"}
	}
	col.Scope = scope
	registrations, e := s.lookup(ctx, actor.ID)
	if e != nil {
		col.ErrorCode = "REGISTRY_UNAVAILABLE"
		registrations = nil
	}
	bindings, e := s.store.ListBindings(ctx, actor.ID, scope.ID)
	if e != nil {
		col.ErrorCode = "STORAGE_UNAVAILABLE"
		bindings = nil
	}
	bindings = s.resolveLabBindings(col, registrations, bindings)
	snapshot, e := Build(col, registrations, bindings, nil, previous)
	if e == nil && col.ErrorCode == "" {
		s.scopeMu.RLock()
		creds := s.mm2Creds
		s.scopeMu.RUnlock()
		eps := extractEndpoints(col, creds)
		s.scopeMu.Lock()
		if s.endpoints == nil {
			s.endpoints = map[string][]AutoEndpoint{}
		}
		s.endpoints[scope.ID] = eps
		s.scopeMu.Unlock()
		s.probeEndpoints(ctx, eps)
	}
	if e != nil {
		code = "DISCOVERY_LIMIT_OR_INVALID_INPUT"
		col = resource.Collection{Scope: scope, ObservedAt: time.Now().UTC(), ErrorCode: code}
		snapshot, _ = Build(col, nil, nil, nil, previous)
	}
	if col.ErrorCode != "" {
		code = col.ErrorCode
	} else if snapshot.Stale {
		code = "PARTIAL_COLLECTION"
	}
	raw, e := json.Marshal(snapshot)
	if e != nil || len(raw) > 8<<20 {
		code = "SNAPSHOT_TOO_LARGE"
		return
	}
	persistCtx, persistCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer persistCancel()
	if e = s.store.Save(persistCtx, actor.ID, snapshot); e != nil {
		code = CodeOf(e)
	}
}

func (s *Service) Bind(ctx context.Context, actor Actor, scope string, input ManualBinding) (ManualBinding, error) {
	if actor.ID == "" || (actor.Role != "admin" && actor.Role != "information") {
		return ManualBinding{}, fail("FORBIDDEN")
	}
	if _, ok := s.scope(scope); !ok {
		return ManualBinding{}, ErrNotFound
	}
	if s.isRemoved(scope) {
		return ManualBinding{}, fail("SCOPE_REMOVED")
	}
	if !safeName.MatchString(input.Namespace) || !safeName.MatchString(input.WorkloadUID) || !safeName.MatchString(input.Alias) || len(input.RegistrationID) > 128 {
		return ManualBinding{}, fail("INVALID_ARGUMENT")
	}
	snapshot, e := s.store.Load(ctx, actor.ID, scope)
	if e != nil {
		return ManualBinding{}, e
	}
	observed := false
	for _, link := range snapshot.Links {
		if !link.Stale && link.Namespace == input.Namespace && link.WorkloadUID == input.WorkloadUID && (link.SourceAlias == input.Alias || link.TargetAlias == input.Alias) {
			observed = true
		}
	}
	if !observed {
		return ManualBinding{}, ErrNotFound
	}
	rows, e := s.lookup(ctx, actor.ID)
	if e != nil {
		return ManualBinding{}, fail("REGISTRY_UNAVAILABLE")
	}
	allowed := false
	for _, row := range rows {
		if row.ID == input.RegistrationID && row.Enabled {
			allowed = true
		}
	}
	if !allowed {
		return ManualBinding{}, ErrNotFound
	}
	binding := ManualBinding{ScopeID: scope, Namespace: input.Namespace, WorkloadUID: input.WorkloadUID, Alias: input.Alias, RegistrationID: input.RegistrationID, ApprovedBy: actor.ID, ApprovedAt: time.Now().UTC()}
	if e = s.store.PutBinding(ctx, actor.ID, binding); e != nil {
		return ManualBinding{}, e
	}
	return binding, nil
}

func sameScope(a, b resource.Scope) bool {
	return a.ID == b.ID && a.Cluster == b.Cluster && a.Synthetic == b.Synthetic && slices.Equal(a.Namespaces, b.Namespaces)
}
