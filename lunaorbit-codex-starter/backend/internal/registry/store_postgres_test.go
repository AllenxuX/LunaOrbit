package registry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func safeTestDatabaseURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid test database URL")
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return fmt.Errorf("URL scheme required")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("loopback host required")
	}
	name := strings.TrimPrefix(u.Path, "/")
	if name != "lunaorbit_phase00_test" && name != "lunaorbit_phase02_test" {
		return fmt.Errorf("dedicated phase00/phase02 test database required")
	}
	for key, values := range u.Query() {
		if (key != "sslmode" && key != "connect_timeout") || len(values) != 1 {
			return fmt.Errorf("connection overrides forbidden")
		}
	}
	cfg, err := pgx.ParseConfig(raw)
	if err != nil {
		return fmt.Errorf("invalid connection configuration")
	}
	if cfg.Database != name || cfg.Host != u.Hostname() || len(cfg.Fallbacks) > 0 || len(cfg.RuntimeParams) > 0 {
		return fmt.Errorf("effective connection overrides forbidden; use sslmode=disable")
	}
	return nil
}

func TestTestDatabaseURLGuard(t *testing.T) {
	for _, raw := range []string{"postgres://localhost/production?sslmode=disable", "postgres://192.0.2.1/lunaorbit_phase02_test?sslmode=disable", "postgres://localhost/lunaorbit_phase02_test?host=192.0.2.1", "postgres://localhost/lunaorbit_phase02_test?service=other", "postgres://localhost/lunaorbit_phase02_test?options=-csearch_path=public", "postgres://localhost/lunaorbit_phase02_test?dbname=production"} {
		if safeTestDatabaseURL(raw) == nil {
			t.Errorf("accepted unsafe URL")
		}
	}
}

func TestPostgresStoreIntegration(t *testing.T) {
	raw := os.Getenv("LUNAORBIT_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("set LUNAORBIT_TEST_DATABASE_URL to an already-migrated dedicated loopback test database")
	}
	if err := safeTestDatabaseURL(raw); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(raw)
	if err != nil {
		t.Fatal("invalid config")
	}
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := NewPostgresStore(db)
	id := fmt.Sprintf("synthetic-store-%d", time.Now().UnixNano())
	owner := id + "-owner"
	defer func() {
		for _, table := range []string{"kafka_cluster_capabilities", "kafka_cluster_credentials", "kafka_cluster_probe_status"} {
			_, _ = db.ExecContext(context.Background(), "DELETE FROM "+table+" WHERE cluster_registration_id=$1", id)
		}
		_, _ = db.ExecContext(context.Background(), `DELETE FROM kafka_cluster_registrations WHERE id=$1`, id)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM audit_events WHERE target_id=$1`, id)
	}()
	r := Registration{ID: id, CreatedBy: owner, Revision: 1, SecurityProtocol: "SASL_SSL", SASLMechanism: "PLAIN", UsernameRef: "synthetic-user-ref", PasswordRef: "synthetic-password-ref", BootstrapServers: []string{"localhost:9092"}, CredentialRevision: 1, Capabilities: []Capability{{Feature: "metadata", Supported: true}}, Brokers: []Broker{{ID: 1, Host: "localhost", Port: 9092}}}
	if err = s.Save(ctx, r, 0, "registry.create"); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, owner, id)
	if err != nil || got.PasswordRef != r.PasswordRef || len(got.Brokers) != 1 {
		t.Fatal(got, err)
	}
	if _, err = s.Get(ctx, "other", id); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err = s.Save(ctx, r, 0, "registry.create"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	r.Revision = 2
	r.DisplayName = "must roll back"
	if err = s.Save(ctx, r, 1, strings.Repeat("x", 129)); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	got, _ = s.Get(ctx, owner, id)
	if got.Revision != 1 || got.DisplayName != "" {
		t.Fatal("failed audit did not roll back")
	}
	r.CreatedBy = "other"
	if err = s.Save(ctx, r, 1, "registry.update"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	r.CreatedBy = owner
	now := time.Now()
	r.DeletedAt = &now
	if err = s.Save(ctx, r, 1, "registry.delete"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Get(ctx, owner, id); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	var count int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM audit_events WHERE target_id=$1`, id).Scan(&count); err != nil || count != 2 {
		t.Fatal(count, err)
	}
}

var _ Store = (*MemoryStore)(nil)
var _ Store = (*PostgresStore)(nil)
