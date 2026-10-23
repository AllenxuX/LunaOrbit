package workspace

import (
	"context"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestScopedPlanVisibilityExistingRulesAndMetadataWrites(t *testing.T) {
	s, owner, id, app := setup(t)
	ctx := context.Background()
	viewer := registry.Actor{ID: "scoped", Role: "viewer"}
	_, e := s.Registry.CreateGrant(ctx, owner, id, registry.GrantInput{SubjectType: "user", SubjectID: viewer.ID, Role: "admin", TopicPrefix: "team.", GroupPrefix: "team-"})
	if e != nil {
		t.Fatal(e)
	}
	private := request(t, s, owner, id, Input{Kind: "grant", ApplicationID: app.ID, Mode: "produce", Topics: []Resource{{"private.events", "LITERAL"}}})
	_ = request(t, s, owner, id, Input{Kind: "grant", ApplicationID: app.ID, Mode: "produce", Topics: []Resource{{"team.events", "LITERAL"}}})
	r := gin.New()
	Mount(r.Group("/api/v1"), s, func(*gin.Context) registry.Actor { return viewer })
	for _, path := range []string{"/requests", "/audit"} {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/kafka/clusters/"+id+"/workspace"+path, nil))
		if rec.Code != 200 || strings.Contains(rec.Body.String(), private.ID) || strings.Contains(rec.Body.String(), "private.events") {
			t.Fatal("scoped list leak", path, rec.Code, rec.Body.String())
		}
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/kafka/clusters/"+id+"/workspace/requests/"+private.ID, nil))
	if rec.Code != 403 {
		t.Fatal("scoped detail leak", rec.Code)
	}
	plan := Plan{Input: Input{Kind: "grant"}, Existing: []kafkaops.ACLRule{{ResourceType: "TOPIC", ResourceName: "private.events"}}}
	if e = s.authorizePlan(ctx, viewer, id, plan); e == nil {
		t.Fatal("existing-only scope bypass")
	}
	if _, e = s.SaveApplication(ctx, viewer, id, Application{Name: "forbidden", Principal: "User:forbidden"}); e == nil {
		t.Fatal("scoped application create")
	}
	if e = s.DeleteApplication(ctx, viewer, id, app.ID, app.Revision); e == nil {
		t.Fatal("scoped application delete")
	}
	native := Native{Registry: s.Registry}
	rule := kafkaops.ACLRule{ResourceType: "TOPIC", ResourceName: "private.events"}
	for _, write := range []func() error{func() error { return native.CreateACL(ctx, viewer, id, rule) }, func() error { return native.DeleteACL(ctx, viewer, id, rule) }, func() error { return native.CreateTopic(ctx, viewer, id, "private.events", 1, 1, nil) }, func() error { return native.ExpandTopic(ctx, viewer, id, "private.events", 2) }, func() error { return native.AlterTopicRetention(ctx, viewer, id, "private.events", 60000) }} {
		if e = write(); e == nil {
			t.Fatal("native write bypassed concrete scope")
		}
	}
}

type revokingGateway struct {
	*Mock
	after  func()
	writes int
}

func (g *revokingGateway) CreateACL(ctx context.Context, a registry.Actor, id string, r kafkaops.ACLRule) error {
	g.writes++
	e := g.Mock.CreateACL(ctx, a, id, r)
	if g.writes == 1 {
		g.after()
	}
	return e
}
func TestRevokedAuthorityStopsLaterRules(t *testing.T) {
	s, owner, id, app := setup(t)
	ctx := context.Background()
	actor := registry.Actor{ID: "executor", Role: "viewer"}
	grant, e := s.Registry.CreateGrant(ctx, owner, id, registry.GrantInput{SubjectType: "user", SubjectID: actor.ID, Role: "admin", TopicPrefix: "team.", GroupPrefix: "team-"})
	if e != nil {
		t.Fatal(e)
	}
	gateway := &revokingGateway{Mock: NewMock()}
	gateway.after = func() {
		if e := s.Registry.RevokeGrant(ctx, owner, id, grant.ID, grant.Revision); e != nil {
			t.Fatal(e)
		}
	}
	s.Gateway = gateway
	r := request(t, s, actor, id, Input{Kind: "grant", ApplicationID: app.ID, Mode: "produce", Topics: []Resource{{"team.events", "LITERAL"}}})
	r, e = s.Approve(ctx, owner, id, r.ID, r.Revision)
	if e != nil {
		t.Fatal(e)
	}
	r, e = s.Execute(ctx, actor, id, r.ID, r.Revision)
	if e != nil || r.Status != "partial_failure" || gateway.writes != 1 {
		t.Fatal("later writes continued after revoke", gateway.writes, r.Status, e)
	}
}
