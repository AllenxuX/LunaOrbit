package observability

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/discovery"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type snapshotFixture struct {
	owner    string
	snapshot discovery.Snapshot
}

func (f *snapshotFixture) Snapshot(ctx context.Context, a discovery.Actor, scope string) (discovery.Snapshot, error) {
	if a.ID != f.owner || scope != f.snapshot.ScopeID {
		return discovery.Snapshot{}, discovery.ErrNotFound
	}
	return f.snapshot, nil
}
func testSnapshot() *snapshotFixture {
	return &snapshotFixture{owner: "owner-a", snapshot: discovery.Snapshot{ScopeID: "scope", Synthetic: true, ObservedAt: time.Now(), Links: []discovery.Link{{ID: "active", Enabled: true, Declared: true, Observed: true, State: "declared"}, {ID: "unknown", Enabled: true, Declared: true, Observed: true, State: "unknown"}, {ID: "disabled", Enabled: false, Declared: true, Observed: true, State: "disabled"}}, Nodes: []discovery.Node{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}}, Edges: []discovery.Edge{{ID: "a-b", Source: "a", Target: "b", LinkID: "active", Relation: "declared_input"}, {ID: "b-c", Source: "b", Target: "c", LinkID: "active", Relation: "declared_output"}, {ID: "c-a", Source: "c", Target: "a", LinkID: "active", Relation: "declared_output"}, {ID: "c-d", Source: "c", Target: "d", LinkID: "unknown", Relation: "declared_output"}, {ID: "b-d", Source: "b", Target: "d", LinkID: "disabled", Relation: "declared_output"}}}}
}
func TestImpactCyclesUnknownDisabledAndOwnerIsolation(t *testing.T) {
	f := testSnapshot()
	service := NewService(f, NewMockProvider(time.Minute), NewMemoryStore(), ServiceConfig{})
	a := Actor{ID: f.owner, Role: "viewer"}
	impact, e := service.Impact(context.Background(), a, "scope", "a")
	if e != nil || len(impact.Items) != 2 || impact.Verified || impact.State != "candidate" {
		t.Fatal(impact, e)
	}
	for _, item := range impact.Items {
		if item.NodeID == "d" || item.State != "candidate" {
			t.Fatal(item)
		}
	}
	if _, e = service.Impact(context.Background(), Actor{ID: "owner-b", Role: "admin"}, "scope", "a"); CodeOf(e) != "NOT_FOUND" {
		t.Fatal(e)
	}
	f.snapshot.Stale = true
	impact, e = service.Impact(context.Background(), a, "scope", "a")
	if e != nil || len(impact.Items) != 0 {
		t.Fatal(impact, e)
	}
}
func TestLineageManualConfirmationNeverPromotesVerification(t *testing.T) {
	f := testSnapshot()
	store := NewMemoryStore()
	service := NewService(f, NewMockProvider(time.Minute), store, ServiceConfig{})
	admin := Actor{ID: f.owner, Role: "admin"}
	ctx := context.Background()
	input := ConfirmationInput{LinkID: "active", Topic: "business-events", Description: "Synthetic business acknowledgement"}
	c, e := service.Confirm(ctx, admin, "scope", input)
	if e != nil || c.Kind != "manual_business_confirmation" || c.ConfirmedBy != f.owner {
		t.Fatal(c, e)
	}
	if f.snapshot.Links[0].Verified {
		t.Fatal("business lineage promoted runtime verification")
	}
	rows, e := service.Confirmations(ctx, admin, "scope")
	if e != nil || len(rows) != 1 || len(store.Audits) != 1 || store.Audits[0].Actor != f.owner {
		t.Fatal(rows, e)
	}
	if _, e = service.Confirm(ctx, Actor{ID: f.owner, Role: "operator"}, "scope", input); CodeOf(e) != "FORBIDDEN" {
		t.Fatal(e)
	}
	input.LinkID = "disabled"
	if _, e = service.Confirm(ctx, admin, "scope", input); CodeOf(e) != "LINK_INACTIVE" {
		t.Fatal(e)
	}
	input.LinkID = "unknown"
	if _, e = service.Confirm(ctx, admin, "scope", input); CodeOf(e) != "LINK_INACTIVE" {
		t.Fatal(e)
	}
	if _, e = service.Confirmations(ctx, Actor{ID: "owner-b", Role: "admin"}, "scope"); CodeOf(e) != "NOT_FOUND" {
		t.Fatal(e)
	}
	f.snapshot.Links[0].Stale = true
	rows, e = service.Confirmations(ctx, admin, "scope")
	if e != nil || rows[0].Active {
		t.Fatal(rows, e)
	}
}
func TestLineageAuditFailureIsAtomic(t *testing.T) {
	f := testSnapshot()
	store := NewMemoryStore()
	store.FailAudit = true
	service := NewService(f, NewMockProvider(time.Minute), store, ServiceConfig{})
	_, e := service.Confirm(context.Background(), Actor{ID: f.owner, Role: "admin"}, "scope", ConfirmationInput{LinkID: "active", Topic: "events"})
	if CodeOf(e) != "UNAVAILABLE" {
		t.Fatal(e)
	}
	rows, _ := store.List(context.Background(), f.owner, "scope")
	if len(rows) != 0 {
		t.Fatal("confirmation saved without audit")
	}
}
func TestMetricInactiveLinksStayUnknown(t *testing.T) {
	f := testSnapshot()
	service := NewService(f, NewMockProvider(time.Minute), NewMemoryStore(), ServiceConfig{})
	rows, e := service.Read(context.Background(), Actor{ID: f.owner, Role: "viewer"}, "scope", "success")
	if e != nil || len(rows.Items) != 6 || !rows.Synthetic {
		t.Fatal(rows, e)
	}
	for _, sample := range rows.Items {
		if sample.State == "healthy" || sample.LinkID != "active" && sample.Value != nil {
			t.Fatal(sample)
		}
	}
	if _, e = service.Read(context.Background(), Actor{ID: "other", Role: "admin"}, "scope", "success"); CodeOf(e) != "NOT_FOUND" {
		t.Fatal(e)
	}
}
func TestObservabilityHTTPAuthorizationAndStrictBody(t *testing.T) {
	f := testSnapshot()
	service := NewService(f, NewMockProvider(time.Minute), NewMemoryStore(), ServiceConfig{})
	gin.SetMode(gin.TestMode)
	router := gin.New()
	role := "viewer"
	Mount(router.Group("/api/v1"), service, func(*gin.Context) Actor { return Actor{ID: f.owner, Role: role} })
	request := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://127.0.0.1"+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}
	if w := request("GET", "/api/v1/observability/scopes/no-such-scope", ""); w.Code != 404 {
		t.Fatal(w.Code)
	}
	if w := request("POST", "/api/v1/observability/scopes/scope/lineage-confirmations", `{"link_id":"active","topic":"events"}`); w.Code != 403 {
		t.Fatal(w.Code)
	}
	role = "admin"
	if w := request("POST", "/api/v1/observability/scopes/scope/lineage-confirmations", `{"link_id":"active","topic":"events","confirmed_by":"spoofed"}`); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if w := request("POST", "/api/v1/observability/scopes/scope/lineage-confirmations", `{"link_id":"active","topic":"events"}`); w.Code != 201 || !strings.Contains(w.Body.String(), "manual_business_confirmation") {
		t.Fatal(w.Code, w.Body.String())
	}
}

type badMetricProvider struct{}

func (badMetricProvider) Read(context.Context, string, string, string) ([]Sample, error) {
	value := 1.
	return []Sample{{LinkID: "active", Metric: LagMetric, Value: &value, Unit: "milliseconds", Source: "prometheus_httpapi", SampledAt: time.Now()}}, nil
}
func TestMetricUnitCannotBeMisrepresented(t *testing.T) {
	service := NewService(testSnapshot(), badMetricProvider{}, NewMemoryStore(), ServiceConfig{})
	rows, e := service.Read(context.Background(), Actor{ID: "owner-a", Role: "viewer"}, "scope", "success")
	if e != nil {
		t.Fatal(e)
	}
	for _, sample := range rows.Items {
		if sample.Metric == LagMetric && sample.Unit != "seconds" {
			t.Fatal("wrong lag unit")
		}
		if sample.Value != nil {
			t.Fatal("invalid unit retained numeric sample")
		}
	}
}
func TestImpactStaleReturnsUnknownReason(t *testing.T) {
	f := testSnapshot()
	f.snapshot.Stale = true
	s := NewService(f, NewMockProvider(time.Minute), NewMemoryStore(), ServiceConfig{})
	v, e := s.Impact(context.Background(), Actor{ID: f.owner, Role: "viewer"}, "scope", "a")
	if e != nil || v.State != "unknown" || v.Reason != "SNAPSHOT_STALE" {
		t.Fatal("stale impact looked calculated", v, e)
	}
}
func TestMetricExpiryUsesSampleTimeAndUnknownHasNoExpiry(t *testing.T) {
	now := time.Now().UTC()
	v := 1.
	sample := normalize(Sample{Value: &v, SampledAt: now.Add(-10 * time.Second)}, now, time.Minute)
	if !sample.ExpiresAt.Equal(sample.SampledAt.Add(time.Minute)) {
		t.Fatal("expiry must follow source sample")
	}
	bad := normalize(Sample{Value: &v, SampledAt: now.Add(time.Second), ExpiresAt: now.Add(time.Hour)}, now, time.Minute)
	if !bad.ExpiresAt.IsZero() {
		t.Fatal("invalid future value retained expiry")
	}
	s := NewService(testSnapshot(), badMetricProvider{}, NewMemoryStore(), ServiceConfig{})
	rows, e := s.Read(context.Background(), Actor{ID: "owner-a", Role: "viewer"}, "scope", "success")
	if e != nil {
		t.Fatal(e)
	}
	for _, s := range rows.Items {
		if s.Value == nil && !s.ExpiresAt.IsZero() {
			t.Fatal("unknown value has expiry")
		}
	}
}
func TestMemoryLineageCapacityMatchesPostgresPolicy(t *testing.T) {
	s := NewMemoryStore()
	now := time.Now().UTC()
	existing := Confirmation{ID: "existing", ScopeID: "scope", LinkID: "active", Topic: "events", ConfirmedBy: "owner", ConfirmedAt: now, Kind: "manual_business_confirmation", Description: "original"}
	if e := s.Put(context.Background(), "owner", existing); e != nil {
		t.Fatal(e)
	}
	for i := 1; i < 4096; i++ {
		c := existing
		c.ID = fmt.Sprintf("fixture-%d", i)
		c.Topic = c.ID
		s.rows["owner\x00scope\x00"+c.ID] = c
	}
	next := existing
	next.ID = "overflow"
	next.Topic = "overflow"
	if CodeOf(s.Put(context.Background(), "owner", next)) != "CAPACITY_EXCEEDED" {
		t.Fatal("memory object cap")
	}
	existing.Description = "updated"
	if e := s.Put(context.Background(), "owner", existing); e != nil {
		t.Fatal("upsert disallowed below audit cap")
	}
	s.Audits = make([]Audit, 4096)
	existing.Description = "must-not-persist"
	if CodeOf(s.Put(context.Background(), "owner", existing)) != "UNAVAILABLE" {
		t.Fatal("memory audit cap")
	}
	rows, e := s.List(context.Background(), "owner", "scope")
	if e != nil || len(rows) != 4096 {
		t.Fatal("bounded list lost")
	}
	for _, r := range rows {
		if r.ID == "existing" && r.Description != "updated" {
			t.Fatal("audit refusal changed prior record")
		}
	}
}
