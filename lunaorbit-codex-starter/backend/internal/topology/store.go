package topology

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"sync"
)

var ErrConflict = errors.New("view conflict")
var ErrInvalid = errors.New("invalid view")
var ErrStorage = errors.New("view storage unavailable")

type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}
type Filters struct {
	Room        string `json:"room"`
	Environment string `json:"environment"`
}
type Layers struct {
	Kafka     bool `json:"kafka"`
	Workloads bool `json:"workloads"`
}
type View struct {
	Revision  int64               `json:"revision"`
	Positions map[string]Position `json:"positions"`
	Filters   Filters             `json:"filters"`
	Layers    Layers              `json:"layers"`
}
type Store interface {
	Load(context.Context, string, string) (View, error)
	Save(context.Context, string, string, View) (View, error)
}

func empty() View {
	return View{Positions: map[string]Position{}, Layers: Layers{Kafka: true, Workloads: true}}
}

var nodeID = regexp.MustCompile(`^[a-zA-Z0-9:_.-]{1,256}$`)

func valid(v View) bool {
	if v.Revision < 0 || v.Revision >= math.MaxInt64 || len(v.Positions) > 4096 || len(v.Filters.Room) > 128 || len(v.Filters.Environment) > 128 {
		return false
	}
	for id, p := range v.Positions {
		if !nodeID.MatchString(id) || math.IsNaN(p.X) || math.IsNaN(p.Y) || math.IsInf(p.X, 0) || math.IsInf(p.Y, 0) || math.Abs(p.X) > 100000 || math.Abs(p.Y) > 100000 {
			return false
		}
	}
	return true
}
func clone(v View) View {
	p := map[string]Position{}
	for id, pos := range v.Positions {
		p[id] = pos
	}
	v.Positions = p
	return v
}

type MemoryStore struct {
	mu    sync.Mutex
	views map[string]View
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{views: map[string]View{}} }
func (s *MemoryStore) Load(ctx context.Context, owner, scope string) (View, error) {
	if ctx.Err() != nil {
		return View{}, ErrStorage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.views[owner+"\x00"+scope]
	if !ok {
		return empty(), nil
	}
	return clone(v), nil
}
func (s *MemoryStore) Save(ctx context.Context, owner, scope string, v View) (View, error) {
	if !valid(v) || owner == "" || scope == "" {
		return View{}, ErrInvalid
	}
	if ctx.Err() != nil {
		return View{}, ErrStorage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := owner + "\x00" + scope
	if s.views[key].Revision != v.Revision {
		return View{}, ErrConflict
	}
	v.Revision++
	s.views[key] = clone(v)
	return clone(v), nil
}

type PostgresStore struct{ db *sql.DB }

func NewPostgresStore(db *sql.DB) *PostgresStore { return &PostgresStore{db: db} }
func (s *PostgresStore) Load(ctx context.Context, owner, scope string) (View, error) {
	var raw []byte
	var rev int64
	err := s.db.QueryRowContext(ctx, `SELECT revision,payload FROM topology_views WHERE owner_id=$1 AND scope_id=$2`, owner, scope).Scan(&rev, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return empty(), nil
	}
	if err != nil {
		return View{}, ErrStorage
	}
	var v View
	if json.Unmarshal(raw, &v) != nil || v.Revision != rev || !valid(v) {
		return View{}, ErrStorage
	}
	return v, nil
}
func (s *PostgresStore) Save(ctx context.Context, owner, scope string, v View) (View, error) {
	if !valid(v) || owner == "" || scope == "" {
		return View{}, ErrInvalid
	}
	previous := v.Revision
	v.Revision++
	raw, err := json.Marshal(v)
	if err != nil {
		return View{}, ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return View{}, ErrStorage
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO topology_views(owner_id,scope_id,revision,payload) SELECT $1,$2,$3,$4::jsonb WHERE $5::bigint=0 ON CONFLICT(owner_id,scope_id) DO NOTHING`, owner, scope, v.Revision, string(raw), previous)
	if previous != 0 {
		result, err = tx.ExecContext(ctx, `UPDATE topology_views SET revision=$3,payload=$4::jsonb WHERE owner_id=$1 AND scope_id=$2 AND revision=$5`, owner, scope, v.Revision, string(raw), previous)
	}
	if err != nil {
		return View{}, ErrStorage
	}
	n, err := result.RowsAffected()
	if err != nil {
		return View{}, ErrStorage
	}
	if n != 1 {
		return View{}, ErrConflict
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO audit_events(actor_id,action,target_id,outcome) VALUES($1,'topology.view.save',$2,'success')`, owner, scope); err != nil {
		return View{}, ErrStorage
	}
	if tx.Commit() != nil {
		return View{}, ErrStorage
	}
	return v, nil
}
