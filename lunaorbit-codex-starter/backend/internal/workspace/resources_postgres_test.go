package workspace

import (
	"context"
	"github.com/google/uuid"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/registry"
	"os"
	"strings"
	"testing"
	"time"
)

type postgresResourceGateway struct {
	*MockResources
	calls int
}

func (g *postgresResourceGateway) DescribeConfigs(ctx context.Context, a registry.Actor, id string, r kafkaadmin.ConfigResource) (ConfigSnapshot, error) {
	snap, e := g.MockResources.DescribeConfigs(ctx, a, id, r)
	if e == nil {
		snap.Entries = append(snap.Entries, kafkaadmin.ConfigEntry{Name: "synthetic.credential", Sensitive: true, Value: pointer("sensitive-pg-canary"), DefaultValue: pointer("sensitive-pg-canary"), Documentation: "sensitive-pg-canary", Synonyms: []kafkaadmin.ConfigSynonym{{Name: "synthetic.credential", Value: pointer("sensitive-pg-canary")}}})
	}
	return snap, e
}
func (g *postgresResourceGateway) AlterConfigs(ctx context.Context, a registry.Actor, id string, r kafkaadmin.ConfigResource, c []kafkaadmin.ConfigChange) error {
	g.calls++
	return g.MockResources.AlterConfigs(ctx, a, id, r, c)
}
func (g *postgresResourceGateway) DeleteTopic(ctx context.Context, a registry.Actor, id, name string) error {
	g.calls++
	return g.MockResources.DeleteTopic(ctx, a, id, name)
}

// Requires the existing migrated dedicated development DB. The registry and
// workspace share one connection so SQL calls inside update callbacks deadlock
// and fail the bounded context instead of silently passing.
func TestResourcesPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("WORKSPACE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("WORKSPACE_TEST_DATABASE_URL unset")
	}
	db, e := registry.OpenDevelopmentDatabase(dsn)
	if e != nil {
		t.Fatal("dedicated development database unavailable")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if _, e = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS kafka_workspaces(registration_id text PRIMARY KEY,body jsonb NOT NULL,revision bigint NOT NULL DEFAULT 1,updated_at timestamptz NOT NULL DEFAULT now())`); e != nil {
		t.Fatal(e)
	}
	r := registry.NewService(registry.NewPostgresStore(db), registry.NewMemorySecrets())
	defer r.Close()
	a := registry.Actor{ID: "resource-pg-" + uuid.NewString(), Role: "admin"}
	reg, e := r.Create(ctx, a, registry.CreateInput{DisplayName: "Synthetic resources PG", BootstrapServers: []string{"broker.synthetic.test:9092"}, Enabled: true, CredentialInput: registry.CredentialInput{SecurityProtocol: "PLAINTEXT"}})
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		safe, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		for _, query := range []string{"DELETE FROM kafka_workspaces WHERE registration_id=$1", "DELETE FROM kafka_cluster_grants WHERE cluster_registration_id=$1", "DELETE FROM kafka_cluster_capabilities WHERE cluster_registration_id=$1", "DELETE FROM kafka_cluster_credentials WHERE cluster_registration_id=$1", "DELETE FROM kafka_cluster_probe_status WHERE cluster_registration_id=$1", "DELETE FROM kafka_cluster_registrations WHERE id=$1", "DELETE FROM audit_events WHERE target_id=$1"} {
			_, _ = db.ExecContext(safe, query, reg.ID)
		}
	}()
	mock := NewMock()
	gateway := &postgresResourceGateway{MockResources: NewMockResources(mock)}
	s := NewService(r, NewPostgresStore(db), mock)
	s.Resources = gateway
	s.LabAutoApprove = true
	s.WriteEnabled = func(ctx context.Context, a registry.Actor, id string) error {
		return r.AuthorizeUnscopedResource(ctx, a, id, "topic.manage")
	}
	create := func(in ResourceIntent) ResourceOperation {
		t.Helper()
		plan, err := s.PreviewResource(ctx, a, reg.ID, in)
		if err != nil {
			t.Fatal(err)
		}
		op, err := s.CreateResourceOperation(ctx, a, reg.ID, in, plan.Fingerprint, uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		return op
	}
	topic := "pg-resource-delete"
	if e = mock.CreateTopic(ctx, a, reg.ID, topic, 1, 1, nil); e != nil {
		t.Fatal(e)
	}
	in := ResourceIntent{Kind: "topic_delete", Resource: ResourceTarget{Type: "TOPIC", Name: topic}, ConfirmationName: topic}
	op := create(in)
	op, e = s.ExecuteResourceOperation(ctx, a, reg.ID, op.ID, op.Revision)
	if e != nil || op.Status != "observing" {
		t.Fatal(op, e)
	}
	submittedCalls := gateway.calls
	persisted, e := NewPostgresStore(db).View(ctx, reg.ID)
	if e != nil || persisted.ResourceOperations[op.ID].Status != "observing" {
		t.Fatal("observing not durable", e)
	}
	restarted := NewService(r, NewPostgresStore(db), mock)
	restarted.Resources = gateway
	restarted.WriteEnabled = s.WriteEnabled
	restarted.LabAutoApprove = true
	if _, e = restarted.ExecuteResourceOperation(ctx, a, reg.ID, op.ID, op.Revision); e != errConflict || gateway.calls != submittedCalls {
		t.Fatal("replayed durable submission", e)
	}
	if _, e = restarted.RefreshResourceOperation(ctx, a, reg.ID, op.ID, op.Revision-1); e != errConflict {
		t.Fatal("stale refresh revision", e)
	}
	op, e = restarted.RefreshResourceOperation(ctx, a, reg.ID, op.ID, op.Revision)
	if e != nil || op.Status != "succeeded" || gateway.calls != submittedCalls {
		t.Fatal(op, e)
	}
	repeated, e := restarted.CreateResourceOperation(ctx, a, reg.ID, in, op.Plan.Fingerprint, op.RequestID)
	if e != nil || repeated.ID != op.ID {
		t.Fatal("persistent idempotency", e)
	}
	broker := ResourceTarget{Type: "BROKER", Name: "1"}
	defaults := ResourceTarget{Type: "BROKER", Name: ""}
	if e = gateway.AlterConfigs(ctx, a, reg.ID, defaults, []kafkaadmin.ConfigChange{{Name: "log.retention.ms", Operation: "SET", Value: pointer("66666")}}); e != nil {
		t.Fatal(e)
	}
	setIntent := ResourceIntent{Kind: "configs", Resource: broker, Changes: []kafkaadmin.ConfigChange{{Name: "log.retention.ms", Operation: "SET", Value: pointer("88888")}}}
	op = create(setIntent)
	// External configuration drift invalidates the saved actual-state fingerprint.
	if e = gateway.AlterConfigs(ctx, a, reg.ID, broker, []kafkaadmin.ConfigChange{{Name: "log.retention.ms", Operation: "SET", Value: pointer("77777")}}); e != nil {
		t.Fatal(e)
	}
	before := gateway.calls
	if _, e = s.ExecuteResourceOperation(ctx, a, reg.ID, op.ID, op.Revision); e != errConflict || gateway.calls != before {
		t.Fatal("stale config fingerprint accepted", e)
	}
	op = create(setIntent)
	op, e = s.ExecuteResourceOperation(ctx, a, reg.ID, op.ID, op.Revision)
	if e != nil {
		t.Fatal(e)
	}
	op, e = s.RefreshResourceOperation(ctx, a, reg.ID, op.ID, op.Revision)
	if e != nil || op.Status != "succeeded" {
		t.Fatal(op, e)
	}
	op = create(ResourceIntent{Kind: "configs", Resource: broker, Changes: []kafkaadmin.ConfigChange{{Name: "log.retention.ms", Operation: "DELETE"}}})
	op, e = s.ExecuteResourceOperation(ctx, a, reg.ID, op.ID, op.Revision)
	if e != nil {
		t.Fatal(e)
	}
	op, e = restarted.RefreshResourceOperation(ctx, a, reg.ID, op.ID, op.Revision)
	if e != nil || op.Status != "succeeded" {
		t.Fatal(op, e)
	}
	snap, e := gateway.DescribeConfigs(ctx, a, reg.ID, broker)
	if e != nil {
		t.Fatal(e)
	}
	for _, c := range snap.Entries {
		if c.Name == "log.retention.ms" && (c.Value == nil || *c.Value != "66666" || c.Source != "DYNAMIC_DEFAULT_BROKER_CONFIG") {
			t.Fatal("restore lost cluster inheritance", c)
		}
	}
	var body string
	if e = db.QueryRowContext(ctx, "SELECT body::text FROM kafka_workspaces WHERE registration_id=$1", reg.ID).Scan(&body); e != nil {
		t.Fatal(e)
	}
	if strings.Contains(body, "sensitive-pg-canary") {
		t.Fatal("sensitive snapshot persisted")
	}
}
