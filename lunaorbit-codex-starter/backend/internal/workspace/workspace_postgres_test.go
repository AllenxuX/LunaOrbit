package workspace

import (
	"context"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This optional integration suite only accepts the existing dedicated loopback
// databases through OpenDevelopmentDatabase. It never reads production DSNs.
func TestWorkspacePostgresPersistenceAndConcurrency(t *testing.T) {
	dsn := os.Getenv("WORKSPACE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("WORKSPACE_TEST_DATABASE_URL unset")
	}
	db, e := registry.OpenDevelopmentDatabase(dsn)
	if e != nil {
		t.Fatal("dedicated development database unavailable")
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	migration := `CREATE TABLE IF NOT EXISTS kafka_workspaces(registration_id text PRIMARY KEY,body jsonb NOT NULL,revision bigint NOT NULL DEFAULT 1,updated_at timestamptz NOT NULL DEFAULT now())`
	// Applying twice verifies normal migration reruns preserve the table.
	for i := 0; i < 2; i++ {
		if _, e = db.ExecContext(ctx, migration); e != nil {
			t.Fatal("workspace migration failed")
		}
	}
	s, a, id, _ := setup(t)
	s.Store = NewPostgresStore(db)
	s.LabAutoApprove = true
	t.Cleanup(func() {
		safe, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := db.ExecContext(safe, "DELETE FROM kafka_workspaces WHERE registration_id=$1", id); err != nil {
			t.Error("test row cleanup failed")
		}
	})
	app, e := s.SaveApplication(ctx, a, id, Application{Name: "Postgres synthetic", Principal: "User:postgres-synthetic"})
	if e != nil {
		t.Fatal(e)
	}
	stale := app
	app.Description = "revised"
	app, e = s.SaveApplication(ctx, a, id, app)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.SaveApplication(ctx, a, id, stale); e == nil {
		t.Fatal("stale application revision accepted")
	}
	gateway := &countingGateway{Mock: NewMock()}
	s.Gateway = gateway
	r := request(t, s, a, id, Input{Kind: "grant", ApplicationID: app.ID, Mode: "produce", Topics: []Resource{{"pg-events", "LITERAL"}}})
	r, e = s.Approve(ctx, a, id, r.ID, r.Revision)
	if e != nil {
		t.Fatal(e)
	}
	// Two service/store instances compete on the same row. Exactly one claims it.
	other := NewService(s.Registry, NewPostgresStore(db), gateway)
	other.WriteEnabled = s.WriteEnabled
	other.LabAutoApprove = true
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, service := range []*Service{s, other} {
		wg.Add(1)
		go func(service *Service) {
			defer wg.Done()
			<-start
			_, err := service.Execute(ctx, a, id, r.ID, r.Revision)
			results <- err
		}(service)
	}
	close(start)
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 || gateway.calls.Load() != 2 {
		t.Fatalf("executor successes %d, writes %d", successes, gateway.calls.Load())
	}
	persisted, e := NewPostgresStore(db).View(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	saved := persisted.Requests[r.ID]
	if saved.Status != "executed" || len(saved.Results) != 2 || len(persisted.Audit) < 6 || persisted.Applications[app.ID].Revision != app.Revision {
		t.Fatal("state or audit did not survive store recreation")
	}
	partial := &partialGateway{Mock: gateway.Mock}
	s.Gateway = partial
	r = request(t, s, a, id, Input{Kind: "grant", ApplicationID: app.ID, Mode: "produce", Topics: []Resource{{"pg-partial", "LITERAL"}}})
	r, e = s.Approve(ctx, a, id, r.ID, r.Revision)
	if e != nil {
		t.Fatal(e)
	}
	r, e = s.Execute(ctx, a, id, r.ID, r.Revision)
	if e != nil || r.Status != "partial_failure" {
		t.Fatal("partial execution not reported", e)
	}
	persisted, e = NewPostgresStore(db).View(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	saved = persisted.Requests[r.ID]
	if saved.Status != "partial_failure" || len(saved.Results) != 2 || saved.Results[0].State != "applied" || saved.Results[1].State != "failed" {
		t.Fatal("partial execution results not durable")
	}
}

type countingGateway struct {
	*Mock
	calls atomic.Int64
}

func (g *countingGateway) CreateACL(ctx context.Context, a registry.Actor, id string, r kafkaops.ACLRule) error {
	g.calls.Add(1)
	return g.Mock.CreateACL(ctx, a, id, r)
}

func TestWorkspacePostgresApproveSingleConnection(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reg := registry.NewService(registry.NewPostgresStore(db), registry.NewMemorySecrets())
	defer reg.Close()
	owner := registry.Actor{ID: "workspace-pg-owner", Role: "admin"}
	approver := registry.Actor{ID: "workspace-pg-approver", Role: "viewer"}
	registration, e := reg.Create(ctx, owner, registry.CreateInput{DisplayName: "single connection synthetic", BootstrapServers: []string{"broker.synthetic.test:9092"}, Enabled: true, CredentialInput: registry.CredentialInput{SecurityProtocol: "PLAINTEXT"}})
	if e != nil {
		t.Fatal("test registry creation failed", e)
	}
	id := registration.ID
	defer func() {
		safe, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		for _, query := range []string{"DELETE FROM kafka_workspaces WHERE registration_id=$1", "DELETE FROM kafka_cluster_grants WHERE cluster_id=$1", "DELETE FROM kafka_cluster_capabilities WHERE cluster_registration_id=$1", "DELETE FROM kafka_cluster_probe_status WHERE cluster_registration_id=$1", "DELETE FROM kafka_cluster_credentials WHERE cluster_registration_id=$1", "DELETE FROM audit_events WHERE target_id=$1", "DELETE FROM kafka_cluster_registrations WHERE id=$1"} {
			if _, err := db.ExecContext(safe, query, id); err != nil {
				t.Error("test fixture cleanup failed")
			}
		}
	}()
	if _, e = reg.CreateGrant(ctx, owner, id, registry.GrantInput{SubjectType: "user", SubjectID: approver.ID, Role: "security-admin", TopicPrefix: "team.", GroupPrefix: "team-"}); e != nil {
		t.Fatal(e)
	}
	s := NewService(reg, NewPostgresStore(db), NewMock())
	app, e := s.SaveApplication(ctx, owner, id, Application{Name: "single connection", Principal: "User:single-connection"})
	if e != nil {
		t.Fatal(e)
	}
	r := request(t, s, owner, id, Input{Kind: "grant", ApplicationID: app.ID, Mode: "produce", Topics: []Resource{{"team.events", "LITERAL"}}})
	bounded, done := context.WithTimeout(ctx, 3*time.Second)
	defer done()
	r, e = s.Approve(bounded, approver, id, r.ID, r.Revision)
	if e != nil || r.Status != "approved" {
		t.Fatal("approval must not query registry while holding sole workspace transaction connection", e)
	}
}
