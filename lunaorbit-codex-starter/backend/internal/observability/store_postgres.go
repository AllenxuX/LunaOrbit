package observability

import (
	"context"
	"database/sql"
	"encoding/json"
	"lunaorbit/backend/internal/discovery"
)

type PostgresStore struct{ db *sql.DB }

func NewPostgresStore(db *sql.DB) *PostgresStore { return &PostgresStore{db: db} }
func (s *PostgresStore) List(ctx context.Context, owner, scope string) ([]Confirmation, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT id,scope_id,link_id,topic,description,confirmed_by,confirmed_at FROM lineage_confirmations WHERE owner_id=$1 AND scope_id=$2 ORDER BY id LIMIT 4097`, owner, scope)
	if e != nil {
		return nil, fail("UNAVAILABLE")
	}
	defer rows.Close()
	out := []Confirmation{}
	for rows.Next() {
		var c Confirmation
		if rows.Scan(&c.ID, &c.ScopeID, &c.LinkID, &c.Topic, &c.Description, &c.ConfirmedBy, &c.ConfirmedAt) != nil {
			return nil, fail("UNAVAILABLE")
		}
		c.Kind = "manual_business_confirmation"
		out = append(out, c)
		if len(out) > 4096 {
			return nil, fail("CAPACITY_EXCEEDED")
		}
	}
	if rows.Err() != nil {
		return nil, fail("UNAVAILABLE")
	}
	return out, nil
}
func (s *PostgresStore) Put(ctx context.Context, owner string, c Confirmation) error {
	if !validConfirmation(owner, c) {
		return fail("INVALID_ARGUMENT")
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return fail("UNAVAILABLE")
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(71008)"); e != nil {
		return fail("UNAVAILABLE")
	}
	var audits int
	if e = tx.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT 1 FROM audit_events WHERE action='lineage.business.confirm' LIMIT 4096) bounded").Scan(&audits); e != nil || audits >= 4096 {
		return fail("UNAVAILABLE")
	}
	var count int
	var exists bool
	if e = tx.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT 1 FROM lineage_confirmations LIMIT 4096) bounded").Scan(&count); e != nil {
		return fail("UNAVAILABLE")
	}
	if e = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM lineage_confirmations WHERE owner_id=$1 AND scope_id=$2 AND link_id=$3 AND topic=$4)", owner, c.ScopeID, c.LinkID, c.Topic).Scan(&exists); e != nil {
		return fail("UNAVAILABLE")
	}
	if !exists && count >= 4096 {
		return fail("CAPACITY_EXCEEDED")
	}
	var raw []byte
	if tx.QueryRowContext(ctx, `SELECT payload FROM mm2_discovery_snapshots WHERE owner_id=$1 AND scope_id=$2 FOR SHARE`, owner, c.ScopeID).Scan(&raw) != nil {
		return fail("NOT_FOUND")
	}
	var snap discovery.Snapshot
	if json.Unmarshal(raw, &snap) != nil || !confirmationActive(snap, c.LinkID) {
		return fail("LINK_INACTIVE")
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO lineage_confirmations(owner_id,id,scope_id,link_id,topic,description,confirmed_by,confirmed_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(owner_id,scope_id,link_id,topic) DO UPDATE SET description=EXCLUDED.description,confirmed_at=EXCLUDED.confirmed_at`, owner, c.ID, c.ScopeID, c.LinkID, c.Topic, c.Description, c.ConfirmedBy, c.ConfirmedAt); e != nil {
		return fail("UNAVAILABLE")
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO audit_events(actor_id,action,target_id,outcome) VALUES($1,'lineage.business.confirm',$2,'success')`, owner, c.ID); e != nil {
		return fail("UNAVAILABLE")
	}
	if tx.Commit() != nil {
		return fail("UNAVAILABLE")
	}
	return nil
}
