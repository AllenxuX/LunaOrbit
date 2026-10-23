package access

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
	e := s.db.QueryRowContext(ctx, "SELECT owner_id,payload FROM acl_requests WHERE id=$1", id).Scan(&r.OwnerID, &b)
	if errors.Is(e, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	if e != nil {
		return r, ErrStorage
	}
	if json.Unmarshal(b, &r) != nil {
		return r, ErrStorage
	}
	return r, nil
}
func (s *PostgresStore) List(ctx context.Context, owner, id string) ([]Request, error) {
	rows, e := s.db.QueryContext(ctx, "SELECT payload FROM acl_requests WHERE owner_id=$1 AND registration_id=$2 ORDER BY id", owner, id)
	if e != nil {
		return nil, ErrStorage
	}
	defer rows.Close()
	out := []Request{}
	for rows.Next() {
		var b []byte
		var r Request
		if rows.Scan(&b) != nil || json.Unmarshal(b, &r) != nil {
			return nil, ErrStorage
		}
		r.OwnerID = owner
		out = append(out, r)
	}
	if rows.Err() != nil {
		return nil, ErrStorage
	}
	return out, nil
}
func (s *PostgresStore) Save(ctx context.Context, r Request, expected int64, actor, action string) error {
	if r.Revision != expected+1 {
		return ErrConflict
	}
	b, e := json.Marshal(r)
	if e != nil {
		return ErrStorage
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return ErrStorage
	}
	defer tx.Rollback()
	if expected == 0 {
		if _, e = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(71006)"); e != nil {
			return ErrStorage
		}
		var count int
		if e = tx.QueryRowContext(ctx, "SELECT count(*) FROM acl_requests").Scan(&count); e != nil || count >= 10000 {
			return ErrStorage
		}
	}
	if expected != 0 {
		var previous Request
		var raw []byte
		e = tx.QueryRowContext(ctx, "SELECT owner_id,payload FROM acl_requests WHERE id=$1 FOR UPDATE", r.ID).Scan(&previous.OwnerID, &raw)
		if errors.Is(e, sql.ErrNoRows) {
			return ErrConflict
		}
		if e != nil || json.Unmarshal(raw, &previous) != nil {
			return ErrStorage
		}
		if previous.Revision != expected || !sameRequestIdentity(previous, r) {
			return ErrConflict
		}
	}
	var result sql.Result
	if expected == 0 {
		result, e = tx.ExecContext(ctx, "INSERT INTO acl_requests(id,owner_id,registration_id,revision,payload) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING", r.ID, r.OwnerID, r.RegistrationID, r.Revision, b)
	} else {
		result, e = tx.ExecContext(ctx, "UPDATE acl_requests SET revision=$1,payload=$2 WHERE id=$3 AND owner_id=$4 AND registration_id=$5 AND revision=$6", r.Revision, b, r.ID, r.OwnerID, r.RegistrationID, expected)
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
	if _, e = tx.ExecContext(ctx, "INSERT INTO acl_request_audit(request_id,actor_id,action,revision) VALUES($1,$2,$3,$4)", r.ID, actor, action, r.Revision); e != nil {
		return ErrStorage
	}
	if tx.Commit() != nil {
		return ErrStorage
	}
	return nil
}
func (s *PostgresStore) Pending(ctx context.Context) ([]Request, error) {
	rows, e := s.db.QueryContext(ctx, "SELECT owner_id,payload FROM acl_requests WHERE payload->>'status'='pending_approval' ORDER BY id LIMIT 10000")
	if e != nil {
		return nil, ErrStorage
	}
	defer rows.Close()
	out := []Request{}
	for rows.Next() {
		var r Request
		var b []byte
		if rows.Scan(&r.OwnerID, &b) != nil || json.Unmarshal(b, &r) != nil {
			return nil, ErrStorage
		}
		out = append(out, r)
	}
	if rows.Err() != nil {
		return nil, ErrStorage
	}
	return out, nil
}
