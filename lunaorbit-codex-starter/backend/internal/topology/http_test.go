package topology

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/registry"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestHTTPPrivateLayoutDoesNotChangeDiscovery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	reg := registry.NewService(registry.NewMemoryStore(), registry.NewMemorySecrets())
	defer reg.Close()
	source, err := discovery.NewMockService(discovery.NewMemoryStore(), func(context.Context, string) ([]discovery.Registration, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	err = registry.MountModules(r, reg, false, nil, func(g *gin.RouterGroup, resolve func(*gin.Context) registry.Actor) {
		a := func(c *gin.Context) discovery.Actor { u := resolve(c); return discovery.Actor{ID: u.ID, Role: u.Role} }
		discovery.Mount(g, source, a)
		Mount(g, NewMemoryStore(), source, a)
	})
	if err != nil {
		t.Fatal(err)
	}
	req := func(method, path, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
		q := httptest.NewRequest(method, "http://127.0.0.1:8080/api/v1"+path, strings.NewReader(body))
		q.Header.Set("Content-Type", "application/json")
		if cookie != nil {
			q.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, q)
		return w
	}
	base := "/topology/scopes/synthetic-mm2/view"
	if w := req("GET", base, "", nil); w.Code != 401 {
		t.Fatal(w.Code)
	}
	one := req("POST", "/dev/session", "{}", nil).Result().Cookies()[0]
	two := req("POST", "/dev/session", "{}", nil).Result().Cookies()[0]
	if w := req("GET", base, "", one); w.Code != 404 {
		t.Fatal("scope without snapshot", w.Code)
	}
	if w := req("POST", "/discovery/scopes/synthetic-mm2/runs", "{}", one); w.Code != 202 {
		t.Fatal(w.Code)
	}
	for deadline := time.Now().Add(3 * time.Second); ; {
		w := req("GET", "/discovery/scopes/synthetic-mm2/runs/latest", "", one)
		var job discovery.Job
		json.Unmarshal(w.Body.Bytes(), &job)
		if job.Status == "done" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout")
		}
		time.Sleep(time.Millisecond)
	}
	before := req("GET", "/discovery/scopes/synthetic-mm2/snapshot", "", one).Body.String()
	payload := `{"revision":0,"positions":{"node":{"x":200,"y":100}},"filters":{"room":"lab","environment":"uat"},"layers":{"kafka":true,"workloads":true}}`
	if w := req("PUT", base, payload, one); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := req("PUT", base, payload, one); w.Code != 409 {
		t.Fatal("CAS", w.Code)
	}
	if w := req("GET", base, "", two); w.Code != 404 {
		t.Fatal("cross-user scope", w.Code)
	}
	if w := req("PUT", base, `{"revision":1,"edges":[{"source":"fake"}]}`, one); w.Code != 400 {
		t.Fatal("graph accepted", w.Code)
	}
	if w := req("PUT", base, `{"revision":1,"positions":{"node":{"x":100001,"y":0}}}`, one); w.Code != 400 {
		t.Fatal("coordinate boundary", w.Code)
	}
	after := req("GET", "/discovery/scopes/synthetic-mm2/snapshot", "", one).Body.String()
	if before != after {
		t.Fatal("layout changed discovery")
	}
}
func TestPostgresViewIntegration(t *testing.T) {
	dsn := os.Getenv("LUNAORBIT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("explicit lab database not configured")
	}
	db, err := registry.OpenDevelopmentDatabase(dsn)
	if err != nil {
		t.Fatal("test database rejected/unavailable")
	}
	defer db.Close()
	owner := fmt.Sprintf("synthetic-layout-%d", time.Now().UnixNano())
	ctx := context.Background()
	defer db.ExecContext(ctx, `DELETE FROM topology_views WHERE owner_id=$1`, owner)
	defer db.ExecContext(ctx, `DELETE FROM audit_events WHERE actor_id=$1`, owner)
	store := NewPostgresStore(db)
	v := empty()
	v.Positions["node"] = Position{X: 120, Y: 350}
	got, err := store.Save(ctx, owner, "scope", v)
	if err != nil || got.Revision != 1 {
		t.Fatal(got, err)
	}
	resumed, err := NewPostgresStore(db).Load(ctx, owner, "scope")
	if err != nil || resumed.Positions["node"].Y != 350 {
		t.Fatal(resumed, err)
	}
	if _, err = store.Save(ctx, owner, "scope", v); err != ErrConflict {
		t.Fatal("CAS", err)
	}
	other, err := store.Load(ctx, owner+"-other", "scope")
	if err != nil || other.Revision != 0 {
		t.Fatal("ownership", err)
	}
	resumed.Filters.Room = "lab"
	updated, err := store.Save(ctx, owner, "scope", resumed)
	if err != nil || updated.Revision != 2 {
		t.Fatal(updated, err)
	}
	bad := strings.Repeat("x", 129)
	if _, err = store.Save(ctx, bad, "scope", empty()); err != ErrStorage {
		t.Fatal("audit expected rollback", err)
	}
	missing, err := store.Load(ctx, bad, "scope")
	if err != nil || missing.Revision != 0 {
		t.Fatal("audit left view", err)
	}
}
