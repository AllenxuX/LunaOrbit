package discovery

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func discoveryTestConfig(raw string) (*pgx.ConnConfig, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid test URL")
	}
	ip := net.ParseIP(u.Hostname())
	if (u.Scheme != "postgres" && u.Scheme != "postgresql") || ip == nil || !ip.IsLoopback() || u.Path != "/lunaorbit_phase02_test" {
		return nil, fmt.Errorf("numeric loopback dedicated phase02 test database required")
	}
	for key, values := range u.Query() {
		if (key != "sslmode" && key != "connect_timeout") || len(values) != 1 {
			return nil, fmt.Errorf("connection overrides forbidden")
		}
	}
	cfg, err := pgx.ParseConfig(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid connection config")
	}
	if cfg.Host != u.Hostname() || cfg.Database != "lunaorbit_phase02_test" || len(cfg.Fallbacks) > 0 || len(cfg.RuntimeParams) > 0 {
		return nil, fmt.Errorf("effective connection overrides forbidden")
	}
	return cfg, nil
}
func TestDiscoveryDatabaseGuard(t *testing.T) {
	for _, raw := range []string{"postgres://localhost/lunaorbit_phase02_test?sslmode=disable", "postgres://192.0.2.1/lunaorbit_phase02_test?sslmode=disable", "postgres://127.0.0.1/production?sslmode=disable", "postgres://127.0.0.1/lunaorbit_phase02_test?host=192.0.2.1", "postgres://127.0.0.1/lunaorbit_phase02_test?options=-csearch_path=public"} {
		if _, err := discoveryTestConfig(raw); err == nil {
			t.Fatal("unsafe database URL accepted")
		}
	}
}
func TestPostgresDiscoveryStoreIntegration(t *testing.T) {
	raw := os.Getenv("LUNAORBIT_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("requires explicitly configured, migrated numeric-loopback lunaorbit_phase02_test")
	}
	cfg, err := discoveryTestConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	owner := fmt.Sprintf("synthetic-discovery-%d", time.Now().UnixNano())
	scope := owner
	id := owner + "-registration"
	defer func() {
		cleanup, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		for _, q := range []string{`DELETE FROM mm2_cluster_alias_bindings WHERE owner_id=$1`, `DELETE FROM mm2_discovery_snapshots WHERE owner_id=$1`, `DELETE FROM audit_events WHERE actor_id=$1`, `DELETE FROM kafka_cluster_registrations WHERE created_by=$1`} {
			_, _ = db.ExecContext(cleanup, q, owner)
		}
	}()
	_, err = db.ExecContext(ctx, `INSERT INTO kafka_cluster_registrations(id,created_by,revision,payload) VALUES($1,$2,1,jsonb_build_object('ID',$1::text,'CreatedBy',$2::text,'Revision',1,'Enabled',true))`, id, owner)
	if err != nil {
		t.Fatal(err)
	}
	s := NewPostgresStore(db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	snap := Snapshot{ScopeID: scope, ObservedAt: now, AttemptedAt: now, Links: []Link{{Namespace: "synthetic", WorkloadUID: "uid", SourceAlias: "alias"}}}
	if err = s.Save(ctx, owner, snap); err != nil {
		t.Fatal(err)
	}
	if got, e := s.Load(ctx, owner, scope); e != nil || !got.ObservedAt.Equal(now) {
		t.Fatal(got, e)
	}
	if _, err = s.Load(ctx, "other", scope); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	snap.AttemptedAt = now.Add(-time.Second)
	if err = s.Save(ctx, owner, snap); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	b := ManualBinding{ScopeID: scope, Namespace: "synthetic", WorkloadUID: "uid", Alias: "alias", RegistrationID: id, ApprovedBy: owner, ApprovedAt: now}
	if err = s.PutBinding(ctx, owner, b); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListBindings(ctx, owner, scope)
	if err != nil || len(got) != 1 {
		t.Fatal(got, err)
	}
	b.ApprovedBy = "other"
	if err = s.PutBinding(ctx, "other", b); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross-owner registration accepted", err)
	}
	_, err = db.ExecContext(ctx, `UPDATE kafka_cluster_registrations SET payload=jsonb_set(payload,'{Enabled}','false') WHERE id=$1`, id)
	if err != nil {
		t.Fatal(err)
	}
	b.ApprovedBy = owner
	if err = s.PutBinding(ctx, owner, b); !errors.Is(err, ErrNotFound) {
		t.Fatal("disabled registration accepted", err)
	}
	_, err = db.ExecContext(ctx, `UPDATE kafka_cluster_registrations SET payload=jsonb_set(payload,'{Enabled}','true') WHERE id=$1`, id)
	if err != nil {
		t.Fatal(err)
	}
	snap.AttemptedAt = now.Add(time.Second)
	snap.Links[0].Stale = true
	if err = s.Save(ctx, owner, snap); err != nil {
		t.Fatal(err)
	}
	if err = s.PutBinding(ctx, owner, b); !errors.Is(err, ErrNotFound) {
		t.Fatal("stale workload binding accepted", err)
	}
	// The audit constraint forces a transaction failure after snapshot upsert.
	oversized := strings.Repeat("x", 129)
	bad := Snapshot{ScopeID: scope, ObservedAt: now, AttemptedAt: now}
	if err = s.Save(ctx, oversized, bad); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	if _, err = s.Load(ctx, oversized, scope); !errors.Is(err, ErrNotFound) {
		t.Fatal("audit failure left snapshot", err)
	}
}

var _ Store = (*MemoryStore)(nil)
var _ Store = (*PostgresStore)(nil)
