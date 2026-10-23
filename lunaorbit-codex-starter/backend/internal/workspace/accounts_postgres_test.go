package workspace

import (
	"context"
	"github.com/google/uuid"
	"lunaorbit/backend/internal/registry"
	"os"
	"strings"
	"testing"
	"time"
)

// Uses the dedicated, already migrated development DB only. One shared SQL
// connection makes registry authorization inside a workspace transaction fail.
func TestAccountsPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("WORKSPACE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("WORKSPACE_TEST_DATABASE_URL unset")
	}
	db, e := registry.OpenDevelopmentDatabase(dsn)
	if e != nil {
		t.Fatal("development database unavailable")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, e = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS kafka_workspaces(registration_id text PRIMARY KEY,body jsonb NOT NULL,revision bigint NOT NULL DEFAULT 1,updated_at timestamptz NOT NULL DEFAULT now())`); e != nil {
		t.Fatal(e)
	}
	r := registry.NewService(registry.NewPostgresStore(db), registry.NewMemorySecrets())
	defer r.Close()
	a := registry.Actor{ID: "account-pg-" + time.Now().Format("150405.000000000"), Role: "admin"}
	reg, e := r.Create(ctx, a, registry.CreateInput{DisplayName: "Synthetic account PG", BootstrapServers: []string{"broker.synthetic.test:9092"}, Enabled: true, CredentialInput: registry.CredentialInput{SecurityProtocol: "PLAINTEXT"}})
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		safe, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		for _, table := range []string{"kafka_workspaces"} {
			_, _ = db.ExecContext(safe, "DELETE FROM "+table+" WHERE registration_id=$1", reg.ID)
		}
		for _, table := range []string{"kafka_cluster_capabilities", "kafka_cluster_credentials", "kafka_cluster_probe_status"} {
			_, _ = db.ExecContext(safe, "DELETE FROM "+table+" WHERE cluster_registration_id=$1", reg.ID)
		}
		_, _ = db.ExecContext(safe, "DELETE FROM kafka_cluster_registrations WHERE id=$1", reg.ID)
		_, _ = db.ExecContext(safe, "DELETE FROM audit_events WHERE target_id=$1", reg.ID)
	}()
	s := NewService(r, NewPostgresStore(db), NewMock())
	s.AccountKey = []byte(strings.Repeat("p", 32))
	s.AccountGateway = &accountFake{}
	s.WriteEnabled = func(ctx context.Context, a registry.Actor, id string) error {
		return r.AuthorizeUnscopedResource(ctx, a, id, "account.manage")
	}
	app, e := s.SaveApplication(ctx, a, reg.ID, Application{Name: "Account PG"})
	if e != nil {
		t.Fatal(e)
	}
	in := AccountInput{RequestID: uuid.NewString(), Mode: "create", ApplicationID: app.ID, AccessMode: "produce", Topics: []Resource{{"pg.events", "LITERAL"}}}
	v, e := s.SaveAccount(ctx, a, reg.ID, in)
	if e != nil {
		t.Fatal(e)
	}
	s.LabAutoApprove = true
	v, e = s.ApproveAccount(ctx, a, reg.ID, v.ID, v.Revision)
	if e != nil {
		t.Fatal(e)
	}
	v, e = s.ExecuteAccount(ctx, a, reg.ID, v.ID, v.Revision)
	if e != nil || v.Operation.Status != "executed" {
		t.Fatal(v, e)
	}
	password, e := s.RevealAccount(ctx, a, reg.ID, v.ID, v.Revision)
	if e != nil || password == "" {
		t.Fatal(e)
	}
	var body string
	if e = db.QueryRowContext(ctx, "SELECT body::text FROM kafka_workspaces WHERE registration_id=$1", reg.ID).Scan(&body); e != nil {
		t.Fatal(e)
	}
	if strings.Contains(body, password) {
		t.Fatal("plaintext stored")
	}
	restarted := NewService(r, NewPostgresStore(db), NewMock())
	restarted.AccountKey = s.AccountKey
	restarted.AccountGateway = s.AccountGateway
	repeated, e := restarted.SaveAccount(ctx, a, reg.ID, in)
	if e != nil || repeated.ID != v.ID || repeated.Revision != v.Revision {
		t.Fatal("persistent request deduplication failed", e)
	}
	got, e := restarted.RevealAccount(ctx, a, reg.ID, v.ID, v.Revision)
	if e != nil || got != password {
		t.Fatal("restart decrypt failed", e)
	}
}
