package information

import (
	"context"
	"errors"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var ErrInvalid = errors.New("invalid information record")
var ErrConflict = errors.New("information revision conflict")
var ErrNotFound = errors.New("information record not found")
var ErrStorage = errors.New("information storage unavailable")

type Relation struct {
	Account       string `json:"account,omitempty"`
	ClientID      string `json:"client_id,omitempty"`
	ClusterID     string `json:"cluster_id"`
	Topic         string `json:"topic"`
	Mode          string `json:"mode"`
	ConsumerGroup string `json:"consumer_group,omitempty"`
}
type Application struct {
	Location    string     `json:"location,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Owner       string     `json:"owner"`
	Environment string     `json:"environment"`
	Description string     `json:"description"`
	Revision    int64      `json:"revision"`
	Relations   []Relation `json:"relations"`
}
type Page struct {
	Items    []Application `json:"items"`
	Total    int           `json:"total"`
	Page     int           `json:"page"`
	PageSize int           `json:"page_size"`
}
type Store interface {
	List(context.Context, string, int, int, string) (Page, error)
	Create(context.Context, string, Application) (Application, error)
	Update(context.Context, string, string, Application) (Application, error)
	Delete(context.Context, string, string, int64) error
}

var topicPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

func validText(s string, max int) bool {
	return utf8.ValidString(s) && len(s) <= max && !strings.ContainsRune(s, 0)
}
func valid(a Application) bool {
	if strings.TrimSpace(a.Name) == "" || !validText(a.Name, 128) || !validText(a.Owner, 128) || !validText(a.Environment, 128) || !validText(a.Description, 4096) || !validText(a.Location, 512) || a.Revision < 0 || a.Revision >= math.MaxInt64 || len(a.Relations) > 200 {
		return false
	}
	seen := map[Relation]bool{}
	for _, r := range a.Relations {
		if strings.TrimSpace(r.ClusterID) == "" || !validText(r.ClusterID, 128) || !topicPattern.MatchString(r.Topic) || r.Topic == "." || r.Topic == ".." || (r.Mode != "produce" && r.Mode != "consume") || !validText(r.ConsumerGroup, 249) || !validText(r.Account, 256) || !validText(r.ClientID, 256) || (r.Mode == "produce" && r.ConsumerGroup != "") || seen[r] {
			return false
		}
		seen[r] = true
	}
	return true
}
func validPage(page, size int, q string) bool {
	return page >= 1 && page <= 1000000 && size >= 1 && size <= 100 && validText(q, 256)
}
func copyApp(a Application) Application { a.Relations = append([]Relation{}, a.Relations...); return a }

type MemoryStore struct {
	mu   sync.Mutex
	rows map[string]map[string]Application
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{rows: map[string]map[string]Application{}} }
func (s *MemoryStore) List(ctx context.Context, w string, page, size int, q string) (Page, error) {
	if !validPage(page, size, q) {
		return Page{}, ErrInvalid
	}
	if ctx.Err() != nil {
		return Page{}, ErrStorage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := []Application{}
	q = strings.ToLower(q)
	for _, a := range s.rows[w] {
		if strings.Contains(strings.ToLower(a.Name+" "+a.Owner+" "+a.Environment+" "+a.Description), q) {
			items = append(items, copyApp(a))
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Name == items[j].Name {
			return items[i].ID < items[j].ID
		}
		return items[i].Name < items[j].Name
	})
	total := len(items)
	start := (page - 1) * size
	if start > total {
		start = total
	}
	end := start + size
	if end > total {
		end = total
	}
	return Page{items[start:end], total, page, size}, nil
}
func (s *MemoryStore) Create(ctx context.Context, w string, a Application) (Application, error) {
	if !valid(a) || a.ID == "" || a.Revision != 0 || w == "" {
		return Application{}, ErrInvalid
	}
	if ctx.Err() != nil {
		return Application{}, ErrStorage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rows[w] == nil {
		s.rows[w] = map[string]Application{}
	}
	if _, ok := s.rows[w][a.ID]; ok {
		return Application{}, ErrConflict
	}
	a.Revision = 1
	a.UpdatedAt = time.Now().UTC()
	s.rows[w][a.ID] = copyApp(a)
	return copyApp(a), nil
}
func (s *MemoryStore) Update(ctx context.Context, w, id string, a Application) (Application, error) {
	if !valid(a) || a.Revision < 1 {
		return Application{}, ErrInvalid
	}
	if ctx.Err() != nil {
		return Application{}, ErrStorage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.rows[w][id]
	if !ok {
		return Application{}, ErrNotFound
	}
	if old.Revision != a.Revision {
		return Application{}, ErrConflict
	}
	a.ID = id
	a.Revision++
	a.UpdatedAt = time.Now().UTC()
	if !a.UpdatedAt.After(old.UpdatedAt) {
		a.UpdatedAt = old.UpdatedAt.Add(time.Nanosecond)
	}
	s.rows[w][id] = copyApp(a)
	return copyApp(a), nil
}
func (s *MemoryStore) Delete(ctx context.Context, w, id string, rev int64) error {
	if rev < 1 {
		return ErrInvalid
	}
	if ctx.Err() != nil {
		return ErrStorage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.rows[w][id]
	if !ok {
		return ErrNotFound
	}
	if old.Revision != rev {
		return ErrConflict
	}
	delete(s.rows[w], id)
	return nil
}
