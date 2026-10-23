package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

func (s *PostgresStore) GetByID(ctx context.Context, id string) (Registration, error) {
	var raw []byte
	e := s.db.QueryRowContext(ctx, `SELECT payload FROM kafka_cluster_registrations WHERE id=$1 AND deleted_at IS NULL`, id).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return Registration{}, ErrNotFound
	}
	if e != nil {
		return Registration{}, storageError(e)
	}
	var r Registration
	if json.Unmarshal(raw, &r) != nil {
		return r, ErrStorage
	}
	return r, nil
}
func (s *PostgresStore) Grants(ctx context.Context, id string) ([]Grant, error) {
	return s.readGrants(ctx, `SELECT payload FROM kafka_cluster_grants WHERE cluster_id=$1 ORDER BY id`, id)
}
func (s *PostgresStore) AllGrants(ctx context.Context) ([]Grant, error) {
	return s.readGrants(ctx, `SELECT payload FROM kafka_cluster_grants WHERE revoked_at IS NULL ORDER BY id`)
}
func (s *PostgresStore) readGrants(ctx context.Context, q string, args ...any) ([]Grant, error) {
	rows, e := s.db.QueryContext(ctx, q, args...)
	if e != nil {
		return nil, storageError(e)
	}
	defer rows.Close()
	out := []Grant{}
	for rows.Next() {
		var raw []byte
		var g Grant
		if rows.Scan(&raw) != nil || json.Unmarshal(raw, &g) != nil {
			return nil, ErrStorage
		}
		out = append(out, g)
	}
	return out, storageError(rows.Err())
}
func (s *PostgresStore) SaveGrant(ctx context.Context, g Grant, expected int64, actor, action string) error {
	if expected < 0 || g.Revision != expected+1 {
		return ErrConflict
	}
	raw, e := json.Marshal(g)
	if e != nil {
		return ErrStorage
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return storageError(e)
	}
	defer tx.Rollback()
	var result sql.Result
	if expected == 0 {
		result, e = tx.ExecContext(ctx, `INSERT INTO kafka_cluster_grants(id,cluster_id,revision,revoked_at,payload) SELECT $1,$2,$3,$4,$5::jsonb WHERE EXISTS(SELECT 1 FROM kafka_cluster_registrations WHERE id=$2 AND deleted_at IS NULL) ON CONFLICT DO NOTHING`, g.ID, g.ClusterID, g.Revision, g.RevokedAt, string(raw))
	} else {
		result, e = tx.ExecContext(ctx, `UPDATE kafka_cluster_grants SET revision=$1,revoked_at=$2,payload=$3::jsonb WHERE id=$4 AND cluster_id=$5 AND revision=$6 AND revoked_at IS NULL`, g.Revision, g.RevokedAt, string(raw), g.ID, g.ClusterID, expected)
	}
	if e != nil {
		return storageError(e)
	}
	n, e := result.RowsAffected()
	if e != nil {
		return storageError(e)
	}
	if n != 1 {
		return ErrConflict
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO audit_events(actor_id,action,target_id,outcome) VALUES($1,$2,$3,'success')`, actor, action, g.ClusterID); e != nil {
		return storageError(e)
	}
	return storageError(tx.Commit())
}
