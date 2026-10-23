package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
)

func empty() State {
	return State{Accounts: map[string]Account{}, AccountSecrets: map[string]AccountSecret{}, Applications: map[string]Application{}, Requests: map[string]Request{}, Audit: []Audit{}}
}
func clone(s State) State {
	b, _ := json.Marshal(s)
	var v State
	_ = json.Unmarshal(b, &v)
	normalizeAccounts(&v)
	return v
}

type MemoryStore struct {
	mu   sync.Mutex
	data map[string]State
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{data: map[string]State{}} }
func (s *MemoryStore) View(ctx context.Context, id string) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := ctx.Err(); e != nil {
		return State{}, e
	}
	v, ok := s.data[id]
	if !ok {
		v = empty()
	}
	return clone(v), nil
}
func (s *MemoryStore) Update(ctx context.Context, id string, f func(*State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := ctx.Err(); e != nil {
		return e
	}
	v, ok := s.data[id]
	if !ok {
		v = empty()
	}
	v = clone(v)
	if e := f(&v); e != nil {
		return e
	}
	s.data[id] = v
	return nil
}

type PostgresStore struct{ db *sql.DB }

func NewPostgresStore(db *sql.DB) *PostgresStore { return &PostgresStore{db} }
func (s *PostgresStore) View(ctx context.Context, id string) (State, error) {
	var b []byte
	e := s.db.QueryRowContext(ctx, "SELECT body FROM kafka_workspaces WHERE registration_id=$1", id).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return empty(), nil
	}
	if e != nil {
		return State{}, errors.New("STORAGE_UNAVAILABLE")
	}
	v := empty()
	e = json.Unmarshal(b, &v)
	normalizeAccounts(&v)
	return v, e
}
func (s *PostgresStore) Update(ctx context.Context, id string, f func(*State) error) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return errors.New("STORAGE_UNAVAILABLE")
	}
	defer tx.Rollback()
	_, e = tx.ExecContext(ctx, "INSERT INTO kafka_workspaces(registration_id,body) VALUES($1,$2) ON CONFLICT DO NOTHING", id, `{"applications":{},"requests":{},"audit":[]}`)
	if e != nil {
		return errors.New("STORAGE_UNAVAILABLE")
	}
	var b []byte
	if e = tx.QueryRowContext(ctx, "SELECT body FROM kafka_workspaces WHERE registration_id=$1 FOR UPDATE", id).Scan(&b); e != nil {
		return errors.New("STORAGE_UNAVAILABLE")
	}
	v := empty()
	if e = json.Unmarshal(b, &v); e != nil {
		return e
	}
	normalizeAccounts(&v)
	if e = f(&v); e != nil {
		return e
	}
	b, e = json.Marshal(v)
	if e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "UPDATE kafka_workspaces SET body=$2,revision=revision+1,updated_at=now() WHERE registration_id=$1", id, b); e != nil {
		return errors.New("STORAGE_UNAVAILABLE")
	}
	return tx.Commit()
}
