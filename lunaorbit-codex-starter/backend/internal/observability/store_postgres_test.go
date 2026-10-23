package observability

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/registry"
	"net"
	"net/url"
	"os"
	"testing"
	"time"
)

func safeTestDatabase(raw string) (*pgx.ConnConfig, error) {
	u, e := url.Parse(raw)
	if e != nil {
		return nil, fmt.Errorf("invalid database config")
	}
	ip := net.ParseIP(u.Hostname())
	if (u.Scheme != "postgres" && u.Scheme != "postgresql") || ip == nil || !ip.IsLoopback() || u.Path != "/lunaorbit_phase02_test" {
		return nil, fmt.Errorf("numeric loopback dedicated test database required")
	}
	for key, values := range u.Query() {
		if (key != "sslmode" && key != "connect_timeout") || len(values) != 1 {
			return nil, fmt.Errorf("connection overrides forbidden")
		}
	}
	cfg, e := pgx.ParseConfig(raw)
	if e != nil || cfg.Host != u.Hostname() || cfg.Database != "lunaorbit_phase02_test" || len(cfg.Fallbacks) > 0 || len(cfg.RuntimeParams) > 0 {
		return nil, fmt.Errorf("effective connection overrides forbidden")
	}
	return cfg, nil
}
func TestLineageDatabaseGuard(t *testing.T) {
	for _, raw := range []string{"postgres://localhost/lunaorbit_phase02_test?sslmode=disable", "postgres://192.0.2.1/lunaorbit_phase02_test?sslmode=disable", "postgres://127.0.0.1/production?sslmode=disable", "postgres://127.0.0.1/lunaorbit_phase02_test?host=192.0.2.1"} {
		if _, e := safeTestDatabase(raw); e == nil {
			t.Fatal("unsafe test database accepted")
		}
	}
}
func TestPostgresLineageOwnershipAndAtomicAudit(t *testing.T) {
	raw := os.Getenv("LUNAORBIT_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("requires migrated dedicated numeric-loopback test database")
	}
	cfg, e := safeTestDatabase(raw)
	if e != nil {
		t.Fatal("unsafe test database configuration")
	}
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	owner := fmt.Sprintf("obs-test-%d", time.Now().UnixNano())
	defer func() {
		db.ExecContext(context.Background(), `DELETE FROM lineage_confirmations WHERE owner_id=$1`, owner)
		db.ExecContext(context.Background(), `DELETE FROM mm2_discovery_snapshots WHERE owner_id=$1`, owner)
		db.ExecContext(context.Background(), `DELETE FROM audit_events WHERE actor_id=$1`, owner)
	}()
	fixture := testSnapshot()
	fixture.owner = owner
	if e = discovery.NewPostgresStore(db).Save(ctx, owner, fixture.snapshot); e != nil {
		t.Fatal("could not save dedicated discovery fixture")
	}
	store := NewPostgresStore(db)
	service := NewService(fixture, NewMockProvider(time.Minute), store, ServiceConfig{})
	confirmation, e := service.Confirm(ctx, Actor{ID: owner, Role: "admin"}, "scope", ConfirmationInput{LinkID: "active", Topic: "synthetic-events"})
	if e != nil {
		t.Fatal(e)
	}
	rows, e := store.List(ctx, owner, "scope")
	if e != nil || len(rows) != 1 || rows[0].ConfirmedBy != owner {
		t.Fatal("confirmation did not persist")
	}
	other, e := store.List(ctx, owner+"-other", "scope")
	if e != nil || len(other) != 0 {
		t.Fatal("cross-owner confirmation leak")
	}
	var audits int
	if db.QueryRowContext(ctx, `SELECT count(*) FROM audit_events WHERE actor_id=$1 AND action='lineage.business.confirm' AND target_id=$2`, owner, confirmation.ID).Scan(&audits) != nil || audits != 1 {
		t.Fatal("confirmation audit missing")
	}
	fixture.snapshot.Links[0].Stale = true
	if e = discovery.NewPostgresStore(db).Save(ctx, owner, fixture.snapshot); e != nil {
		t.Fatal("could not save stale dedicated fixture")
	}
	confirmation.Topic = "another-synthetic-topic"
	confirmation.ID = "synthetic-another-id"
	if CodeOf(store.Put(ctx, owner, confirmation)) != "LINK_INACTIVE" {
		t.Fatal("database failed to revalidate active link")
	}
	rows, e = store.List(ctx, owner, "scope")
	if e != nil || len(rows) != 1 {
		t.Fatal("rejected confirmation mutated database")
	}
}
func TestPostgresLineageCapacityBoundaries(t *testing.T) {
	raw := os.Getenv("LUNAORBIT_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("requires migrated dedicated numeric-loopback test database")
	}
	db, e := registry.OpenDevelopmentDatabase(raw)
	if e != nil {
		t.Fatal("unsafe or unavailable dedicated database")
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	owner := fmt.Sprintf("obs-capacity-%d", time.Now().UnixNano())
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		db.ExecContext(cleanup, `DELETE FROM lineage_confirmations WHERE owner_id=$1`, owner)
		db.ExecContext(cleanup, `DELETE FROM mm2_discovery_snapshots WHERE owner_id=$1`, owner)
		db.ExecContext(cleanup, `DELETE FROM audit_events WHERE actor_id=$1`, owner)
	}()
	f := testSnapshot()
	f.owner = owner
	if e = discovery.NewPostgresStore(db).Save(ctx, owner, f.snapshot); e != nil {
		t.Fatal("fixture discovery failed")
	}
	store := NewPostgresStore(db)
	s := NewService(f, NewMockProvider(time.Minute), store, ServiceConfig{})
	existing, e := s.Confirm(ctx, Actor{ID: owner, Role: "admin"}, "scope", ConfirmationInput{LinkID: "active", Topic: "existing-capacity-topic", Description: "original"})
	if e != nil {
		t.Fatal(e)
	}
	var count int
	if e = db.QueryRowContext(ctx, `SELECT count(*) FROM lineage_confirmations`).Scan(&count); e != nil {
		t.Fatal("count failed")
	}
	if count >= 4096 {
		t.Fatal("capacity fixture requires fewer than 4096 pre-existing rows")
	}
	if _, e = db.ExecContext(ctx, `INSERT INTO lineage_confirmations(owner_id,id,scope_id,link_id,topic,description,confirmed_by,confirmed_at) SELECT $1,'capacity-'||g,'scope','active','capacity-topic-'||g,'',$1,now() FROM generate_series(1,$2::int) g`, owner, 4096-count); e != nil {
		t.Fatal("capacity fixtures failed")
	}
	next := existing
	next.ID = "capacity-overflow"
	next.Topic = "overflow-topic"
	if CodeOf(store.Put(ctx, owner, next)) != "CAPACITY_EXCEEDED" {
		t.Fatal("object cap not enforced")
	}
	existing.Description = "allowed-existing-update"
	if e = store.Put(ctx, owner, existing); e != nil {
		t.Fatal("existing upsert should be allowed below audit cap", e)
	}
	if e = db.QueryRowContext(ctx, `SELECT count(*) FROM audit_events WHERE action='lineage.business.confirm'`).Scan(&count); e != nil || count >= 4096 {
		t.Fatal("audit capacity fixture unavailable")
	}
	if _, e = db.ExecContext(ctx, `INSERT INTO audit_events(actor_id,action,target_id,outcome) SELECT $1,'lineage.business.confirm','capacity-audit-'||g,'success' FROM generate_series(1,$2::int) g`, owner, 4096-count); e != nil {
		t.Fatal("audit fixtures failed")
	}
	existing.Description = "must-not-persist"
	if CodeOf(store.Put(ctx, owner, existing)) != "UNAVAILABLE" {
		t.Fatal("audit capacity must reject upsert")
	}
	var description string
	if e = db.QueryRowContext(ctx, `SELECT description FROM lineage_confirmations WHERE owner_id=$1 AND id=$2`, owner, existing.ID).Scan(&description); e != nil || description != "allowed-existing-update" {
		t.Fatal("rejected upsert changed existing record")
	}
	rows, e := store.List(ctx, owner, "scope")
	if e != nil || len(rows) > 4096 {
		t.Fatal("capacity made list unusable", e)
	}
	if e = db.QueryRowContext(ctx, `SELECT count(*) FROM audit_events WHERE action='lineage.business.confirm'`).Scan(&count); e != nil || count != 4096 {
		t.Fatal("rejected write changed audit capacity")
	}
}
