package changes

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type PostgresStore struct{ db *sql.DB }

func NewPostgresStore(db *sql.DB) *PostgresStore { return &PostgresStore{db} }
func (s *PostgresStore) Get(ctx context.Context, id string) (Request, error) {
	var r Request
	var b []byte
	e := s.db.QueryRowContext(ctx, "SELECT owner_id,approval_hash,config_uid,payload FROM mm2_changes WHERE id=$1", id).Scan(&r.OwnerID, &r.ApprovalHash, &r.ConfigMapUID, &b)
	if errors.Is(e, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	if e != nil || json.Unmarshal(b, &r) != nil {
		return Request{}, ErrStorage
	}
	return r, nil
}
func (s *PostgresStore) List(ctx context.Context) ([]Request, error) {
	rows, e := s.db.QueryContext(ctx, "SELECT owner_id,approval_hash,config_uid,payload FROM mm2_changes ORDER BY id LIMIT 1000")
	if e != nil {
		return nil, ErrStorage
	}
	defer rows.Close()
	out := []Request{}
	for rows.Next() {
		var r Request
		var b []byte
		if rows.Scan(&r.OwnerID, &r.ApprovalHash, &r.ConfigMapUID, &b) != nil || json.Unmarshal(b, &r) != nil {
			return nil, ErrStorage
		}
		out = append(out, r)
	}
	if rows.Err() != nil {
		return nil, ErrStorage
	}
	return out, nil
}
func (s *PostgresStore) Backup(ctx context.Context, id string) ([]byte, error) {
	var b []byte
	e := s.db.QueryRowContext(ctx, "SELECT ciphertext FROM mm2_change_snapshots WHERE request_id=$1", id).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if e != nil {
		return nil, ErrStorage
	}
	return b, nil
}
func (s *PostgresStore) Save(ctx context.Context, r Request, expected int64, actor, action string, backup []byte) error {
	if r.Revision != expected+1 {
		return ErrConflict
	}
	r.CanApprove = false
	r.CanExecute = false
	b, e := json.Marshal(r)
	if e != nil {
		return ErrStorage
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return ErrStorage
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(71007)"); e != nil {
		return ErrStorage
	}
	if expected == 0 {
		var n int
		if tx.QueryRowContext(ctx, "SELECT count(*) FROM mm2_changes").Scan(&n) != nil || n >= maxObjects {
			return ErrStorage
		}
	}
	var result sql.Result
	if expected == 0 {
		result, e = tx.ExecContext(ctx, "INSERT INTO mm2_changes(id,owner_id,target_id,revision,approval_hash,config_uid,payload) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING", r.ID, r.OwnerID, r.TargetID, r.Revision, r.ApprovalHash, r.ConfigMapUID, b)
	} else {
		result, e = tx.ExecContext(ctx, "UPDATE mm2_changes SET revision=$1,approval_hash=$2,payload=$3 WHERE id=$4 AND owner_id=$5 AND revision=$6 AND target_id=$7 AND config_uid=$8", r.Revision, r.ApprovalHash, b, r.ID, r.OwnerID, expected, r.TargetID, r.ConfigMapUID)
	}
	if e != nil {
		return ErrStorage
	}
	n, e := result.RowsAffected()
	if e != nil {
		return ErrStorage
	}
	if n != 1 {
		return ErrConflict
	}
	if backup != nil {
		var total int64
		if tx.QueryRowContext(ctx, "SELECT COALESCE(sum(octet_length(ciphertext)),0) FROM mm2_change_snapshots").Scan(&total) != nil || len(backup) > maxSnapshot || total+int64(len(backup)) > maxSnapshotTotal {
			return ErrStorage
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO mm2_change_snapshots(request_id,ciphertext) VALUES($1,$2)", r.ID, backup); e != nil {
			return ErrStorage
		}
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO mm2_change_audit(request_id,actor_id,action,revision) VALUES($1,$2,$3,$4)", r.ID, actor, action, r.Revision); e != nil {
		return ErrStorage
	}
	if tx.Commit() != nil {
		return ErrStorage
	}
	return nil
}
