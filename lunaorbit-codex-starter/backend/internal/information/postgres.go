package information

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type PostgresStore struct{ db *sql.DB }

func NewPostgresStore(db *sql.DB) *PostgresStore { return &PostgresStore{db} }
func (s *PostgresStore) List(ctx context.Context, w string, page, size int, q string) (Page, error) {
	p := Page{Items: []Application{}, Page: page, PageSize: size}
	if !validPage(page, size, q) {
		return p, ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return p, ErrStorage
	}
	defer tx.Rollback()
	const filter = `workspace_id=$1 AND strpos(lower(concat_ws(' ',payload->>'name',payload->>'owner',payload->>'environment',payload->>'description')),lower($2))>0`
	if tx.QueryRowContext(ctx, `SELECT count(*) FROM information_applications WHERE `+filter, w, q).Scan(&p.Total) != nil {
		return p, ErrStorage
	}
	rows, err := tx.QueryContext(ctx, `SELECT payload, updated_at FROM information_applications WHERE `+filter+` ORDER BY payload->>'name',id LIMIT $3 OFFSET $4`, w, q, size, (page-1)*size)
	if err != nil {
		return p, ErrStorage
	}
	for rows.Next() {
		var raw []byte
		var a Application
		var updatedAt time.Time
		if rows.Scan(&raw, &updatedAt) != nil || json.Unmarshal(raw, &a) != nil || !valid(a) {
			rows.Close()
			return p, ErrStorage
		}
		a.UpdatedAt = updatedAt.UTC()
		p.Items = append(p.Items, copyApp(a))
	}
	err = rows.Err()
	rows.Close()
	if err != nil || tx.Commit() != nil {
		return p, ErrStorage
	}
	return p, nil
}
func (s *PostgresStore) Create(ctx context.Context, w string, a Application) (Application, error) {
	if !valid(a) || a.ID == "" || a.Revision != 0 || w == "" {
		return Application{}, ErrInvalid
	}
	a.Revision = 1
	a = copyApp(a)
	raw, _ := json.Marshal(a)
	err := s.db.QueryRowContext(ctx, `INSERT INTO information_applications(workspace_id,id,revision,payload) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING updated_at`, w, a.ID, a.Revision, raw).Scan(&a.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Application{}, ErrConflict
	}
	if err != nil {
		return Application{}, ErrStorage
	}
	a.UpdatedAt = a.UpdatedAt.UTC()
	return a, nil
}
func (s *PostgresStore) Update(ctx context.Context, w, id string, a Application) (Application, error) {
	if !valid(a) || a.Revision < 1 {
		return Application{}, ErrInvalid
	}
	rev := a.Revision
	a.ID = id
	a.Revision++
	a = copyApp(a)
	raw, _ := json.Marshal(a)
	err := s.db.QueryRowContext(ctx, `UPDATE information_applications SET revision=$4,payload=$5,updated_at=GREATEST(clock_timestamp(),updated_at + interval '1 microsecond') WHERE workspace_id=$1 AND id=$2 AND revision=$3 RETURNING updated_at`, w, id, rev, a.Revision, raw).Scan(&a.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Application{}, s.missing(ctx, w, id)
	}
	if err != nil {
		return Application{}, ErrStorage
	}
	a.UpdatedAt = a.UpdatedAt.UTC()
	return a, nil
}
func (s *PostgresStore) Delete(ctx context.Context, w, id string, rev int64) error {
	if rev < 1 {
		return ErrInvalid
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM information_applications WHERE workspace_id=$1 AND id=$2 AND revision=$3`, w, id, rev)
	if err != nil {
		return ErrStorage
	}
	n, err := result.RowsAffected()
	if err != nil {
		return ErrStorage
	}
	if n == 0 {
		return s.missing(ctx, w, id)
	}
	return nil
}
func (s *PostgresStore) missing(ctx context.Context, w, id string) error {
	var rev int64
	err := s.db.QueryRowContext(ctx, `SELECT revision FROM information_applications WHERE workspace_id=$1 AND id=$2`, w, id).Scan(&rev)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return ErrStorage
	}
	return ErrConflict
}
