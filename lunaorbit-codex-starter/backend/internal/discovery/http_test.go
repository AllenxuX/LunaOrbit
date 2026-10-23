package discovery_test

import (
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/registry"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDiscoveryHTTPUsesRegistryIdentityAndPagination(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	reg := registry.NewService(registry.NewMemoryStore(), registry.NewMemorySecrets())
	defer reg.Close()
	s, e := discovery.NewMockService(discovery.NewMemoryStore(), func(context.Context, string) ([]discovery.Registration, error) { return nil, nil })
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	e = registry.MountModules(r, reg, false, nil, func(group *gin.RouterGroup, resolve func(*gin.Context) registry.Actor) {
		discovery.Mount(group, s, func(c *gin.Context) discovery.Actor { a := resolve(c); return discovery.Actor{ID: a.ID, Role: a.Role} })
	})
	if e != nil {
		t.Fatal(e)
	}
	req := func(method, path, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
		request := httptest.NewRequest(method, "http://127.0.0.1:8080/api/v1"+path, strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		if cookie != nil {
			request.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, request)
		return w
	}
	if w := req("GET", "/discovery/scopes", "", nil); w.Code != 401 {
		t.Fatal("discovery bypassed session")
	}
	first := req("POST", "/dev/session", "{}", nil).Result().Cookies()[0]
	second := req("POST", "/dev/session", "{}", nil).Result().Cookies()[0]
	var scopes struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	w := req("GET", "/discovery/scopes", "", first)
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &scopes) != nil || len(scopes.Items) != 1 {
		t.Fatal("missing configured scope")
	}
	base := "/discovery/scopes/" + scopes.Items[0].ID
	if w = req("POST", base+"/runs", `{"scenario":"success"}`, first); w.Code != 202 {
		t.Fatal("discovery not asynchronous")
	}
	for deadline := time.Now().Add(time.Second); ; {
		w = req("GET", base+"/runs/latest", "", first)
		var job discovery.Job
		_ = json.Unmarshal(w.Body.Bytes(), &job)
		if job.Status == "done" {
			if job.ErrorCode != "" {
				t.Fatal(job.ErrorCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("discovery timed out")
		}
		time.Sleep(time.Millisecond)
	}
	w = req("GET", base+"/snapshot?page_size=1", "", first)
	var page struct {
		Snapshot discovery.Snapshot `json:"snapshot"`
		Total    int                `json:"total"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil || page.Total != 3 || len(page.Snapshot.Links) != 1 || len(page.Snapshot.Edges) != 2 {
		t.Fatal("graph pagination invalid")
	}
	if strings.Contains(w.Body.String(), "DO_NOT_EXPOSE") || strings.Contains(w.Body.String(), "jaas") {
		t.Fatal("discovery exposed sensitive input")
	}
	if w = req("GET", base+"/snapshot", "", second); w.Code != 404 {
		t.Fatal("cross-user snapshot access")
	}
	if w = req("PUT", base+"/bindings", `{"namespace":"synthetic-mm2","workload_uid":"synthetic-mars-uid","alias":"mars","registration_id":"foreign","approved_by":"spoofed"}`, first); w.Code != 400 {
		t.Fatal("spoofed approval accepted")
	}
}
