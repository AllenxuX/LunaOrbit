package workspace

import (
	"context"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"testing"
)

func customRule() kafkaops.ACLRule {
	return kafkaops.ACLRule{Principal: "User:external", Host: "10.0.0.1", ResourceType: "TOPIC", ResourceName: "team.events", PatternType: "PREFIXED", Operation: "WRITE", Permission: "DENY"}
}
func TestNativeCustomACLPlan(t *testing.T) {
	s, a, id, _ := setup(t)
	ctx := context.Background()
	r := customRule()
	p, e := s.Preview(ctx, a, id, Input{Kind: "acl_custom", Principal: r.Principal, DesiredRules: []kafkaops.ACLRule{r}})
	if e != nil || len(p.Additions) != 1 {
		t.Fatalf("native plan: %+v %v", p, e)
	}
	_ = s.Gateway.CreateACL(ctx, a, id, r)
	fresh, e := s.Preview(ctx, a, id, p.Input)
	if e != nil || fresh.Fingerprint == p.Fingerprint || len(fresh.Existing) != 1 {
		t.Fatal("freshness", fresh, e)
	}
	p, e = s.Preview(ctx, a, id, Input{Kind: "acl_custom", Principal: r.Principal, SelectedRules: []kafkaops.ACLRule{r}})
	if e != nil || len(p.Removals) != 1 {
		t.Fatal("exact revoke", p, e)
	}
	r.Operation = "CLUSTER_ACTION"
	if _, e = s.Preview(ctx, a, id, Input{Kind: "acl_custom", Principal: r.Principal, DesiredRules: []kafkaops.ACLRule{r}}); e == nil {
		t.Fatal("invalid operation accepted")
	}
}
func TestCustomScopeAndVisibility(t *testing.T) {
	s, a, id, _ := setup(t)
	ctx := context.Background()
	scoped := registry.Actor{ID: "scoped", Role: "viewer"}
	_, e := s.Registry.CreateGrant(ctx, a, id, registry.GrantInput{SubjectType: "user", SubjectID: scoped.ID, Role: "admin", TopicPrefix: "team.", GroupPrefix: "team-"})
	if e != nil {
		t.Fatal(e)
	}
	r := customRule()
	if _, e = s.Preview(ctx, scoped, id, Input{Kind: "acl_custom", Principal: r.Principal, DesiredRules: []kafkaops.ACLRule{r}}); e != nil {
		t.Fatal(e)
	}
	for _, bad := range []kafkaops.ACLRule{{Principal: r.Principal, Host: "*", ResourceType: "CLUSTER", ResourceName: "kafka-cluster", PatternType: "LITERAL", Operation: "ALL", Permission: "ALLOW"}, {Principal: r.Principal, Host: "*", ResourceType: "TOPIC", ResourceName: "*", PatternType: "LITERAL", Operation: "ALL", Permission: "ALLOW"}} {
		_ = s.Gateway.CreateACL(ctx, a, id, bad)
		if _, e = s.Preview(ctx, scoped, id, Input{Kind: "acl_custom", Principal: r.Principal, DesiredRules: []kafkaops.ACLRule{bad}}); e == nil {
			t.Fatal("scope bypass")
		}
	}
	_ = s.Gateway.CreateACL(ctx, a, id, r)
	snap, e := s.ListACLRules(ctx, scoped, id, "")
	if e != nil || len(snap.Items) != 1 {
		t.Fatal("list scope leak", snap, e)
	}
}

func TestCustomExecutionFreshnessAndExactDeletion(t *testing.T) {
	s, a, id, _ := setup(t)
	ctx := context.Background()
	r := customRule()
	_ = s.Gateway.CreateACL(ctx, a, id, r)
	keep := r
	keep.Host = "10.0.0.2"
	_ = s.Gateway.CreateACL(ctx, a, id, keep)
	req := request(t, s, a, id, Input{Kind: "acl_custom", Principal: r.Principal, SelectedRules: []kafkaops.ACLRule{r}})
	if len(req.Plan.Retained) != 1 {
		t.Fatal("unselected rule not retained", req.Plan)
	}
	s.LabAutoApprove = true
	approved, e := s.Approve(ctx, a, id, req.ID, req.Revision)
	if e != nil {
		t.Fatal(e)
	}
	drift := r
	drift.Operation = "DESCRIBE"
	_ = s.Gateway.CreateACL(ctx, a, id, drift)
	if _, e = s.Execute(ctx, a, id, approved.ID, approved.Revision); e == nil {
		t.Fatal("stale native plan executed")
	}
	_ = s.Gateway.DeleteACL(ctx, a, id, drift)
	result, e := s.Execute(ctx, a, id, approved.ID, approved.Revision)
	if e != nil || result.Status != "executed" {
		t.Fatal(result, e)
	}
	snap, e := s.Gateway.ACLs(ctx, a, id)
	if e != nil || len(snap.Items) != 1 || snap.Items[0].Host != keep.Host {
		t.Fatal("exact deletion changed retained ACL", snap, e)
	}
}

type driftingACL struct {
	Gateway
	after  func()
	writes int
}

func (g *driftingACL) CreateACL(ctx context.Context, a registry.Actor, id string, r kafkaops.ACLRule) error {
	e := g.Gateway.CreateACL(ctx, a, id, r)
	g.writes++
	if g.writes == 1 && g.after != nil {
		g.after()
	}
	return e
}
func TestCustomExecutionChecksDriftBeforeEveryRule(t *testing.T) {
	s, a, id, _ := setup(t)
	ctx := context.Background()
	base := s.Gateway
	r := customRule()
	second := r
	second.Operation = "DESCRIBE"
	drift := r
	drift.Host = "10.0.0.99"
	g := &driftingACL{Gateway: base, after: func() { _ = base.CreateACL(ctx, a, id, drift) }}
	s.Gateway = g
	req := request(t, s, a, id, Input{Kind: "acl_custom", Principal: r.Principal, DesiredRules: []kafkaops.ACLRule{r, second}})
	s.LabAutoApprove = true
	approved, e := s.Approve(ctx, a, id, req.ID, req.Revision)
	if e != nil {
		t.Fatal(e)
	}
	result, e := s.Execute(ctx, a, id, approved.ID, approved.Revision)
	if e != nil {
		t.Fatal(e)
	}
	if g.writes != 1 || result.Status != "partial_failure" {
		t.Fatal("external drift failed to block subsequent rule", g.writes, result)
	}
}
