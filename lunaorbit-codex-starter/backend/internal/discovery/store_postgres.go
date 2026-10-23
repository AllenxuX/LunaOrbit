package discovery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type PostgresStore struct{ db *sql.DB }

func NewPostgresStore(db *sql.DB) *PostgresStore { return &PostgresStore{db: db} }
func discoveryStorageError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrStorage
}
func (s *PostgresStore) Load(ctx context.Context, owner, scope string) (Snapshot, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload FROM mm2_discovery_snapshots WHERE owner_id=$1 AND scope_id=$2`, owner, scope).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, ErrNotFound
	}
	if err != nil {
		return Snapshot{}, discoveryStorageError(err)
	}
	var out Snapshot
	if json.Unmarshal(raw, &out) != nil || out.ScopeID != scope {
		return Snapshot{}, ErrStorage
	}
	return out, nil
}
func (s *PostgresStore) Save(ctx context.Context, owner string, v Snapshot) error {
	if !validSnapshot(owner, v) {
		return ErrConflict
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return ErrStorage
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return discoveryStorageError(err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO mm2_discovery_snapshots(owner_id,scope_id,observed_at,attempted_at,payload) VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT(owner_id,scope_id) DO UPDATE SET observed_at=EXCLUDED.observed_at,attempted_at=EXCLUDED.attempted_at,payload=EXCLUDED.payload WHERE mm2_discovery_snapshots.observed_at<=EXCLUDED.observed_at AND mm2_discovery_snapshots.attempted_at<=EXCLUDED.attempted_at`, owner, v.ScopeID, v.ObservedAt, v.AttemptedAt, string(raw))
	if err != nil {
		return discoveryStorageError(err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return discoveryStorageError(err)
	}
	if count != 1 {
		return ErrConflict
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO audit_events(actor_id,action,target_id,outcome) VALUES($1,'discovery.snapshot.save',$2,'success')`, owner, v.ScopeID); err != nil {
		return discoveryStorageError(err)
	}
	return discoveryStorageError(tx.Commit())
}
func (s *PostgresStore) ListBindings(ctx context.Context, owner, scope string) ([]ManualBinding, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT scope_id,namespace,workload_uid,alias,registration_id,approved_by,approved_at FROM mm2_cluster_alias_bindings WHERE owner_id=$1 AND scope_id=$2 ORDER BY namespace,workload_uid,alias`, owner, scope)
	if err != nil {
		return nil, discoveryStorageError(err)
	}
	defer rows.Close()
	out := []ManualBinding{}
	for rows.Next() {
		var b ManualBinding
		if err = rows.Scan(&b.ScopeID, &b.Namespace, &b.WorkloadUID, &b.Alias, &b.RegistrationID, &b.ApprovedBy, &b.ApprovedAt); err != nil {
			return nil, discoveryStorageError(err)
		}
		out = append(out, b)
	}
	return out, discoveryStorageError(rows.Err())
}
func (s *PostgresStore) PutBinding(ctx context.Context, owner string, b ManualBinding) error {
	if !validBinding(owner, b) {
		return ErrConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return discoveryStorageError(err)
	}
	defer tx.Rollback()
	// SHARE conflicts with Save's upsert UPDATE, preserving the validated
	// workload/alias observation until this binding and its audit commit.
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT payload FROM mm2_discovery_snapshots WHERE owner_id=$1 AND scope_id=$2 FOR SHARE`, owner, b.ScopeID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return discoveryStorageError(err)
	}
	var snapshot Snapshot
	if json.Unmarshal(raw, &snapshot) != nil || snapshot.ScopeID != b.ScopeID {
		return ErrStorage
	}
	if !bindingObserved(snapshot, b) {
		return ErrNotFound
	}
	var id string
	err = tx.QueryRowContext(ctx, `SELECT id FROM kafka_cluster_registrations WHERE id=$1 AND created_by=$2 AND deleted_at IS NULL AND payload->>'Enabled'='true' FOR SHARE`, b.RegistrationID, owner).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return discoveryStorageError(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO mm2_cluster_alias_bindings(owner_id,scope_id,namespace,workload_uid,alias,registration_id,approved_by,approved_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(owner_id,scope_id,namespace,workload_uid,alias) DO UPDATE SET registration_id=EXCLUDED.registration_id,approved_by=EXCLUDED.approved_by,approved_at=EXCLUDED.approved_at`, owner, b.ScopeID, b.Namespace, b.WorkloadUID, b.Alias, b.RegistrationID, b.ApprovedBy, b.ApprovedAt)
	if err != nil {
		return discoveryStorageError(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO audit_events(actor_id,action,target_id,outcome) VALUES($1,'discovery.binding.put',$2,'success')`, owner, b.ScopeID); err != nil {
		return discoveryStorageError(err)
	}
	return discoveryStorageError(tx.Commit())
}
