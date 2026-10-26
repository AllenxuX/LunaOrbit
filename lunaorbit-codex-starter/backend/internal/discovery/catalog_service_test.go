package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"lunaorbit/backend/internal/discovery/mm2auth"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type fakeAccess struct {
	mu     sync.Mutex
	probes int
	fail   bool
}

func (f *fakeAccess) Probe(_ context.Context, ep AutoEndpoint, c mm2auth.Credential) ProbeResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probes++
	if f.fail || !c.UsesSASL() {
		return ProbeResult{Access: AccessFailed, Reason: "timeout", CheckedAt: time.Now()}
	}
	return ProbeResult{Access: AccessReadable, ClusterID: "cid-" + ep.Alias, CheckedAt: time.Now()}
}
func (f *fakeAccess) Topics(context.Context, AutoEndpoint, mm2auth.Credential) ([]TopicSummary, error) {
	return []TopicSummary{{Name: "a-topic", Partitions: 1}, {Name: "orders", Partitions: 3}, {Name: "payments", Partitions: 3}}, nil
}
func (f *fakeAccess) Topic(_ context.Context, _ AutoEndpoint, _ mm2auth.Credential, name string) (any, error) {
	if name != "orders" {
		return nil, errors.New("topic_not_found")
	}
	return map[string]any{"name": name, "partitions": []any{}}, nil
}
func (f *fakeAccess) Groups(context.Context, AutoEndpoint, mm2auth.Credential) ([]string, error) {
	return []string{"g1"}, nil
}

func catalogRouter(s *Service) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	Mount(r.Group("/api/v1"), s, func(*gin.Context) Actor { return autoActor })
	return r
}

func get(t *testing.T, r *gin.Engine, path string) (int, map[string]any, string) {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	var body map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	return w.Code, body, w.Body.String()
}

func TestSwitchOffNeverConnects(t *testing.T) {
	access := &fakeAccess{}
	s, _ := credService(t, nil)
	s.SetEndpointAccess(access)
	runScope(t, s, scopeByCluster(t, s, "edge-a").ID)
	if access.probes != 0 {
		t.Fatalf("switch off must not connect, probes=%d", access.probes)
	}
	r := catalogRouter(s)
	_, body, _ := get(t, r, "/api/v1/discovery/catalog")
	items := body["items"].([]any)
	if len(items) != 2 || items[0].(map[string]any)["access"] != AccessCredentialsOff {
		t.Fatalf("%v", items)
	}
	id := items[0].(map[string]any)["id"].(string)
	if code, body, _ := get(t, r, "/api/v1/discovery/catalog/"+id+"/topics"); code != http.StatusConflict || body["error"].(map[string]any)["code"] != "CREDENTIALS_OFF" {
		t.Fatalf("reads must be refused when off: %d %v", code, body)
	}
}

func TestSwitchOnProbesAndServesReadsThroughCatalog(t *testing.T) {
	access := &fakeAccess{}
	s, _ := credService(t, mm2auth.NewStore())
	s.SetEndpointAccess(access)
	runScope(t, s, scopeByCluster(t, s, "edge-a").ID)
	if access.probes != 2 {
		t.Fatalf("probes=%d", access.probes)
	}
	r := catalogRouter(s)
	_, body, raw := get(t, r, "/api/v1/discovery/catalog")
	items := body["items"].([]any)
	row := items[0].(map[string]any)
	if row["access"] != AccessReadable || row["credential_source"] != CredentialSourceMM2 || !strings.HasPrefix(row["cluster_id"].(string), "cid-") {
		t.Fatalf("%v", row)
	}
	id := row["id"].(string)
	code, page, _ := get(t, r, "/api/v1/discovery/catalog/"+id+"/topics?page=1&page_size=2&query=s")
	if code != 200 || page["total"].(float64) != 2 || len(page["items"].([]any)) != 2 {
		t.Fatalf("paged search %d %v", code, page)
	}
	if code, _, _ := get(t, r, "/api/v1/discovery/catalog/"+id+"/topics/orders"); code != 200 {
		t.Fatal("topic detail")
	}
	if code, g, _ := get(t, r, "/api/v1/discovery/catalog/"+id+"/groups"); code != 200 || len(g["items"].([]any)) != 1 {
		t.Fatal("groups")
	}
	if code, _, _ := get(t, r, "/api/v1/discovery/catalog/auto:unknown/topics"); code != 404 {
		t.Fatal("unknown endpoint")
	}
	if strings.Contains(raw, canaryPassword) || strings.Contains(raw, "mm2-canary-user") {
		t.Fatal("catalog leaked credentials")
	}
}

func TestFailedAutoAccessFallsBackToRegistrationForReads(t *testing.T) {
	access := &fakeAccess{fail: true}
	s := NewService(NewMemoryStore(), nil, credFactory, func(context.Context, string) ([]Registration, error) {
		return []Registration{{ID: "reg-k1", DisplayName: "luna-k1", BootstrapServers: []string{"kafka-k1.lab:9092"}, Enabled: true}}, nil
	})
	t.Cleanup(s.Close)
	s.EnableMM2Credentials(mm2auth.NewStore())
	s.SetEndpointAccess(access)
	s.EnableAutoScopes()
	s.SyncClusters([]ClusterEntry{{Name: "edge-a"}}, time.Now())
	runScope(t, s, scopeByCluster(t, s, "edge-a").ID)
	r := catalogRouter(s)
	_, body, _ := get(t, r, "/api/v1/discovery/catalog")
	for _, it := range body["items"].([]any) {
		row := it.(map[string]any)
		if row["registration_id"] != "reg-k1" {
			continue
		}
		if row["read_via"] != "registration" || row["access_reason"] != "timeout" {
			t.Fatalf("%v", row)
		}
		code, b, _ := get(t, r, "/api/v1/discovery/catalog/"+row["id"].(string)+"/topics")
		if code != http.StatusConflict || b["error"].(map[string]any)["code"] != "USE_REGISTRATION" || b["registration_id"] != "reg-k1" {
			t.Fatalf("fallback %d %v", code, b)
		}
		return
	}
	t.Fatal("merged k1 not found")
}
