package registry

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"os"
	"testing"
	"time"
)

// Runs only against the same guarded, already-migrated dedicated loopback DB
// used by the registry integration suite. It never installs schema or touches
// registrations outside the unique synthetic test IDs.
func TestPostgresGrantsIntegration(t *testing.T) {
	raw := os.Getenv("REGISTRY_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("set REGISTRY_TEST_DATABASE_URL to a dedicated loopback test database migrated through 008")
	}
	if e := safeTestDatabaseURL(raw); e != nil {
		t.Fatal(e)
	}
	cfg, e := pgx.ParseConfig(raw)
	if e != nil {
		t.Fatal("invalid test database configuration")
	}
	db1 := stdlib.OpenDB(*cfg)
	defer db1.Close()
	db2 := stdlib.OpenDB(*cfg)
	defer db2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	st1, st2 := NewPostgresStore(db1), NewPostgresStore(db2)
	s1, s2 := NewService(st1, NewMemorySecrets()), NewService(st2, NewMemorySecrets())
	defer s1.Close()
	defer s2.Close()
	id := fmt.Sprintf("synthetic-grants-%d", time.Now().UnixNano())
	owner := Actor{ID: id + "-owner", Role: "admin"}
	delegate := Actor{ID: id + "-delegate", Role: "viewer"}
	reader := Actor{ID: id + "-reader", Role: "viewer"}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		for _, table := range []string{"kafka_cluster_grants"} {
			_, _ = db1.ExecContext(cleanup, "DELETE FROM "+table+" WHERE cluster_id=$1", id)
		}
		for _, table := range []string{"kafka_cluster_capabilities", "kafka_cluster_credentials", "kafka_cluster_probe_status"} {
			_, _ = db1.ExecContext(cleanup, "DELETE FROM "+table+" WHERE cluster_registration_id=$1", id)
		}
		_, _ = db1.ExecContext(cleanup, `DELETE FROM kafka_cluster_registrations WHERE id=$1`, id)
		_, _ = db1.ExecContext(cleanup, `DELETE FROM audit_events WHERE target_id=$1`, id)
	}()
	row := Registration{ID: id, CreatedBy: owner.ID, Revision: 1, Enabled: true, SecurityProtocol: "PLAINTEXT", DisplayName: "Synthetic shared resource", BootstrapServers: []string{"broker.synthetic.test:9092"}}
	if e = st1.Save(ctx, row, 0, "kafka.register"); e != nil {
		t.Fatal(e)
	}
	if _, e = s2.Get(ctx, delegate, id); ErrorCode(e) != "NOT_FOUND" {
		t.Fatalf("owner isolation: %v", e)
	}
	if _, e = s1.CreateGrant(ctx, owner, id, GrantInput{SubjectType: "user", SubjectID: delegate.ID, Role: "admin", TopicPrefix: "team.", GroupPrefix: "team-"}); e != nil {
		t.Fatal(e)
	}
	g, e := s2.CreateGrant(ctx, delegate, id, GrantInput{SubjectType: "user", SubjectID: reader.ID, Role: "operator", TopicPrefix: "team.orders.", GroupPrefix: "team-orders-"})
	if e != nil {
		t.Fatal(e)
	}
	if _, duplicateErr := s1.CreateGrant(ctx, delegate, id, g.GrantInput); ErrorCode(duplicateErr) != "CONFLICT" {
		t.Fatalf("cross-instance duplicate accepted: %v", duplicateErr)
	}
	got, e := s1.Get(ctx, reader, id)
	if e != nil || got.DisplayName != row.DisplayName {
		t.Fatalf("shared read: %+v %v", got, e)
	}
	if e = s1.AuthorizeResource(ctx, reader, id, "topic.manage", "topic:team.orders.created"); e != nil {
		t.Fatal(e)
	}
	for _, v := range []struct{ action, name string }{{"topic.manage", "topic:private.orders"}, {"acl.manage", "topic:team.orders.created"}, {"read", "group:private"}} {
		if e = s1.AuthorizeResource(ctx, reader, id, v.action, v.name); ErrorCode(e) != "FORBIDDEN" {
			t.Fatalf("scope/role escape: %v", e)
		}
	}
	if actual, e := s1.ResourceOwner(ctx, reader, id); e != nil || actual != owner.ID {
		t.Fatalf("owner identity: %q %v", actual, e)
	}
	if profile, e := s1.AuthorizedProfile(ctx, reader, id); e != nil || profile.RegistrationID != id {
		t.Fatalf("profile: %+v %v", profile, e)
	}
	var actor string
	if e = db1.QueryRowContext(ctx, `SELECT actor_id FROM audit_events WHERE target_id=$1 AND action='kafka.team.grant' ORDER BY id DESC LIMIT 1`, id).Scan(&actor); e != nil || actor != delegate.ID {
		t.Fatalf("grant audit actual actor: %q %v", actor, e)
	}
	if e = s1.RevokeGrant(ctx, delegate, id, g.ID, g.Revision+1); ErrorCode(e) != "CONFLICT" {
		t.Fatalf("stale CAS: %v", e)
	}
	if _, e = s2.Get(ctx, reader, id); e != nil {
		t.Fatalf("stale revoke changed state: %v", e)
	}
	if e = s1.RevokeGrant(ctx, delegate, id, g.ID, g.Revision); e != nil {
		t.Fatal(e)
	}
	if _, e = s2.Get(ctx, reader, id); ErrorCode(e) != "NOT_FOUND" {
		t.Fatalf("revocation not immediate across instances: %v", e)
	}
	if e = s2.RevokeGrant(ctx, delegate, id, g.ID, g.Revision); ErrorCode(e) != "CONFLICT" {
		t.Fatalf("repeated CAS: %v", e)
	}
	if e = db2.QueryRowContext(ctx, `SELECT actor_id FROM audit_events WHERE target_id=$1 AND action='kafka.team.revoke'`, id).Scan(&actor); e != nil || actor != delegate.ID {
		t.Fatalf("revoke audit actual actor: %q %v", actor, e)
	}
}
