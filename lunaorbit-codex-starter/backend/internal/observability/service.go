package observability

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"lunaorbit/backend/internal/discovery"
	"regexp"
	"sort"
	"time"
)

type Actor struct{ ID, Role string }
type SnapshotReader interface {
	Snapshot(context.Context, discovery.Actor, string) (discovery.Snapshot, error)
}
type ServiceConfig struct {
	TTL                 time.Duration
	ConfirmationLinkIDs map[string][]string
}
type Service struct {
	discovery SnapshotReader
	provider  MetricProvider
	store     Store
	ttl       time.Duration
	allow     map[string]map[string]bool
	now       func() time.Time
}

func NewService(reader SnapshotReader, provider MetricProvider, store Store, cfg ServiceConfig) *Service {
	if cfg.TTL <= 0 {
		cfg.TTL = time.Minute
	}
	allowed := map[string]map[string]bool{}
	for scope, links := range cfg.ConfirmationLinkIDs {
		allowed[scope] = map[string]bool{}
		for _, link := range links {
			allowed[scope][link] = true
		}
	}
	return &Service{discovery: reader, provider: provider, store: store, ttl: cfg.TTL, allow: allowed, now: time.Now}
}
func validActor(a Actor) bool {
	return a.ID != "" && (a.Role == "admin" || a.Role == "operator" || a.Role == "viewer" || a.Role == "security-admin")
}
func (s *Service) snapshot(ctx context.Context, a Actor, scope string) (discovery.Snapshot, error) {
	if !validActor(a) {
		return discovery.Snapshot{}, fail("FORBIDDEN")
	}
	if s.discovery == nil || scope == "" || len(scope) > 128 {
		return discovery.Snapshot{}, fail("NOT_FOUND")
	}
	role := a.Role
	if role == "security-admin" {
		role = "viewer"
	}
	snap, e := s.discovery.Snapshot(ctx, discovery.Actor{ID: a.ID, Role: role}, scope)
	if e != nil {
		code := discovery.CodeOf(e)
		if code != "FORBIDDEN" && code != "NOT_FOUND" {
			code = "UNAVAILABLE"
		}
		return snap, fail(code)
	}
	if snap.ScopeID != scope {
		return discovery.Snapshot{}, fail("NOT_FOUND")
	}
	if len(snap.Links) > 128 || len(snap.Nodes) > 1024 || len(snap.Edges) > 2048 {
		return discovery.Snapshot{}, fail("CAPACITY_EXCEEDED")
	}
	return snap, nil
}
func active(link discovery.Link) bool {
	return link.Enabled && link.Declared && link.Observed && !link.Stale && (link.State == "declared" || link.State == "observed" || link.State == "verified")
}

type Metrics struct {
	Items      []Sample  `json:"items"`
	Synthetic  bool      `json:"synthetic"`
	ObservedAt time.Time `json:"observed_at"`
	Stale      bool      `json:"stale"`
	State      string    `json:"state"`
}

func (s *Service) Read(ctx context.Context, a Actor, scope, scenario string) (Metrics, error) {
	out := Metrics{Items: []Sample{}, ObservedAt: s.now().UTC(), State: "unknown"}
	if scenario == "" {
		scenario = "success"
	}
	if scenario != "success" && scenario != "stale" && scenario != "unavailable" && scenario != "no_data" {
		return out, fail("INVALID_ARGUMENT")
	}
	snap, e := s.snapshot(ctx, a, scope)
	if e != nil {
		return out, e
	}
	if !snap.Synthetic && scenario != "success" {
		return out, fail("INVALID_ARGUMENT")
	}
	out.Synthetic = snap.Synthetic
	out.Stale = snap.Stale
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	observed := false
	for _, link := range snap.Links {
		samples := []Sample{}
		if active(link) && !snap.Stale && s.provider != nil {
			samples, e = s.provider.Read(ctx, scope, link.ID, scenario)
		} else {
			e = fail("LINK_INACTIVE")
		}
		byMetric := map[string]Sample{}
		if e == nil {
			for _, sample := range samples {
				if sample.LinkID == link.ID && (sample.Metric == LagMetric || sample.Metric == RecordsMetric) {
					if _, duplicate := byMetric[sample.Metric]; duplicate {
						byMetric[sample.Metric] = unknown(link.ID, sample.Metric, "unavailable", "AMBIGUOUS_SAMPLES", snap.Synthetic, s.now())
						continue
					}
					if sample.Source != "mock" && sample.Source != "prometheus_httpapi" {
						sample = unknown(link.ID, sample.Metric, "unavailable", "INVALID_SOURCE", snap.Synthetic, s.now())
					}
					unit := "seconds"
					if sample.Metric == RecordsMetric {
						unit = "records"
					}
					if sample.Unit != unit {
						sample = unknown(link.ID, sample.Metric, "unavailable", "INVALID_UNIT", snap.Synthetic, s.now())
					}
					if sample.Value != nil {
						sample = normalize(sample, s.now(), s.ttl)
					} else {
						sample.State = "unknown"
						sample.ExpiresAt = time.Time{}
					}
					byMetric[sample.Metric] = sample
				}
			}
		}
		for _, metric := range []string{LagMetric, RecordsMetric} {
			sample, ok := byMetric[metric]
			if !ok {
				reason := "NO_DATA"
				if e != nil {
					reason = CodeOf(e)
				}
				sample = unknown(link.ID, metric, "unavailable", reason, snap.Synthetic, s.now())
			}
			out.Items = append(out.Items, sample)
			out.Synthetic = out.Synthetic || sample.Synthetic
			out.Stale = out.Stale || sample.Stale
			observed = observed || sample.State == "observed"
		}
	}
	if observed {
		out.State = "observed"
	}
	if out.Stale {
		out.State = "stale"
	}
	return out, nil
}

type ImpactItem struct {
	NodeID string `json:"node_id"`
	LinkID string `json:"link_id"`
	State  string `json:"state"`
	Reason string `json:"reason"`
}
type Impact struct {
	Reason    string       `json:"reason"`
	NodeID    string       `json:"node_id"`
	Items     []ImpactItem `json:"items"`
	Synthetic bool         `json:"synthetic"`
	State     string       `json:"state"`
	Verified  bool         `json:"verified"`
}

func (s *Service) Impact(ctx context.Context, a Actor, scope, node string) (Impact, error) {
	out := Impact{NodeID: node, Items: []ImpactItem{}, State: "candidate", Reason: "POTENTIAL_DOWNSTREAM_DECLARATIONS_ONLY"}
	snap, e := s.snapshot(ctx, a, scope)
	if e != nil {
		return out, e
	}
	nodes := map[string]bool{}
	for _, n := range snap.Nodes {
		nodes[n.ID] = true
	}
	if !nodes[node] {
		return out, fail("NOT_FOUND")
	}
	out.Synthetic = snap.Synthetic
	if snap.Stale {
		out.State = "unknown"
		out.Reason = "SNAPSHOT_STALE"
		return out, nil
	}
	links := map[string]bool{}
	for _, link := range snap.Links {
		links[link.ID] = active(link)
	}
	adj := map[string][]discovery.Edge{}
	for _, edge := range snap.Edges {
		if !edge.Stale && links[edge.LinkID] && nodes[edge.Source] && nodes[edge.Target] && (edge.Relation == "declared_input" || edge.Relation == "declared_output") {
			adj[edge.Source] = append(adj[edge.Source], edge)
		}
	}
	seen := map[string]bool{node: true}
	queue := []string{node}
	for len(queue) > 0 {
		source := queue[0]
		queue = queue[1:]
		for _, edge := range adj[source] {
			if seen[edge.Target] {
				continue
			}
			seen[edge.Target] = true
			queue = append(queue, edge.Target)
			out.Items = append(out.Items, ImpactItem{NodeID: edge.Target, LinkID: edge.LinkID, State: "candidate", Reason: "POTENTIAL_DOWNSTREAM_DECLARATION"})
		}
	}
	sort.Slice(out.Items, func(i, j int) bool { return out.Items[i].NodeID < out.Items[j].NodeID })
	return out, nil
}

type Confirmation struct {
	ID          string    `json:"id"`
	ScopeID     string    `json:"scope_id"`
	LinkID      string    `json:"link_id"`
	Topic       string    `json:"topic"`
	Description string    `json:"description,omitempty"`
	ConfirmedBy string    `json:"confirmed_by"`
	ConfirmedAt time.Time `json:"confirmed_at"`
	Kind        string    `json:"kind"`
	Active      bool      `json:"active"`
}
type ConfirmationInput struct {
	LinkID      string `json:"link_id"`
	Topic       string `json:"topic"`
	Description string `json:"description,omitempty"`
}

var topicName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

func confirmationActive(snap discovery.Snapshot, linkID string) bool {
	if snap.Stale {
		return false
	}
	for _, link := range snap.Links {
		if link.ID == linkID {
			return active(link)
		}
	}
	return false
}
func (s *Service) Confirm(ctx context.Context, a Actor, scope string, in ConfirmationInput) (Confirmation, error) {
	if a.Role != "admin" || a.ID == "" {
		return Confirmation{}, fail("FORBIDDEN")
	}
	if !topicName.MatchString(in.Topic) || in.Topic == "." || in.Topic == ".." || len(in.Description) > 256 || in.LinkID == "" || len(in.LinkID) > 128 {
		return Confirmation{}, fail("INVALID_ARGUMENT")
	}
	snap, e := s.snapshot(ctx, a, scope)
	if e != nil {
		return Confirmation{}, e
	}
	if !confirmationActive(snap, in.LinkID) {
		return Confirmation{}, fail("LINK_INACTIVE")
	}
	if len(s.allow) > 0 && !s.allow[scope][in.LinkID] {
		return Confirmation{}, fail("FORBIDDEN")
	}
	if s.store == nil {
		return Confirmation{}, fail("UNAVAILABLE")
	}
	hash := sha256.Sum256([]byte(a.ID + "\x00" + scope + "\x00" + in.LinkID + "\x00" + in.Topic))
	c := Confirmation{ID: hex.EncodeToString(hash[:16]), ScopeID: scope, LinkID: in.LinkID, Topic: in.Topic, Description: in.Description, ConfirmedBy: a.ID, ConfirmedAt: s.now().UTC(), Kind: "manual_business_confirmation", Active: true}
	if e = s.store.Put(ctx, a.ID, c); e != nil {
		return Confirmation{}, e
	}
	return c, nil
}
func (s *Service) Confirmations(ctx context.Context, a Actor, scope string) ([]Confirmation, error) {
	snap, e := s.snapshot(ctx, a, scope)
	if e != nil {
		return nil, e
	}
	if s.store == nil {
		return nil, fail("UNAVAILABLE")
	}
	rows, e := s.store.List(ctx, a.ID, scope)
	if e != nil {
		return nil, e
	}
	for i := range rows {
		rows[i].Active = confirmationActive(snap, rows[i].LinkID)
	}
	return rows, nil
}
