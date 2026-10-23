package identity

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/registry"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func realmTest(t *testing.T, cfg RealmConfig) (*Realm, *gin.Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	p, _ := NewMockProvider([]MockUser{{Username: "synthetic", Password: "synthetic-password", Subject: Subject{ID: "stable-subject", Role: "security-admin"}}})
	realm, e := NewRealm(p, cfg)
	if e != nil {
		t.Fatal(e)
	}
	router := gin.New()
	realm.Mount(router)
	service := registry.NewService(registry.NewMemoryStore(), registry.NewMemorySecrets())
	service.IdentityProvider = realm
	t.Cleanup(service.Close)
	if e = registry.MountModules(router, service, false, nil); e != nil {
		t.Fatal(e)
	}
	return realm, router
}
func realmRequest(router *gin.Engine, method, path, body, origin string, cookie *http.Cookie) *httptest.ResponseRecorder {
	q := httptest.NewRequest(method, "http://127.0.0.1:8080"+path, strings.NewReader(body))
	q.RemoteAddr = "127.0.0.1:12345"
	q.Header.Set("Content-Type", "application/json")
	if origin != "" {
		q.Header.Set("Origin", origin)
	}
	if cookie != nil {
		q.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, q)
	return w
}
func TestRealmLoginSessionLogoutAndExpiry(t *testing.T) {
	realm, router := realmTest(t, RealmConfig{Mode: "mock", DevelopmentLoopback: true, TTL: time.Hour})
	now := time.Now()
	realm.now = func() time.Time { return now }
	login := func() *httptest.ResponseRecorder {
		return realmRequest(router, "POST", "/api/v1/auth/login", `{"username":"synthetic","password":"synthetic-password"}`, "http://127.0.0.1:8080", nil)
	}
	w := login()
	if w.Code != 200 || strings.Contains(w.Body.String(), "password") {
		t.Fatal(w.Code, w.Body.String())
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode || cookies[0].Value == "stable-subject" {
		t.Fatal(cookies)
	}
	cookie := cookies[0]
	for _, path := range []string{"/api/v1/auth/session", "/api/v1/session"} {
		w = realmRequest(router, "GET", path, "", "", cookie)
		if w.Code != 200 || !strings.Contains(w.Body.String(), "stable-subject") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	w = realmRequest(router, "POST", "/api/v1/auth/logout", "", "http://127.0.0.1:8080", cookie)
	if w.Code != 204 {
		t.Fatal(w.Code)
	}
	w = realmRequest(router, "GET", "/api/v1/auth/session", "", "", cookie)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	w = login()
	cookie = w.Result().Cookies()[0]
	now = now.Add(2 * time.Hour)
	w = realmRequest(router, "GET", "/api/v1/session", "", "", cookie)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
}
func TestRealmCSRFRateLimitAndDevFallback(t *testing.T) {
	_, router := realmTest(t, RealmConfig{Mode: "mock", DevelopmentLoopback: true, LoginPerMinute: 1})
	for _, origin := range []string{"", "https://evil.example"} {
		w := realmRequest(router, "POST", "/api/v1/auth/login", `{"username":"synthetic","password":"private-value"}`, origin, nil)
		if w.Code != 403 || strings.Contains(w.Body.String(), "private-value") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	w := realmRequest(router, "POST", "/api/v1/auth/login", `{"username":"synthetic","password":"wrong"}`, "http://127.0.0.1:8080", nil)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	w = realmRequest(router, "POST", "/api/v1/auth/login", `{"username":"synthetic","password":"synthetic-password"}`, "http://127.0.0.1:8080", nil)
	if w.Code != 429 {
		t.Fatal(w.Code)
	}
	w = realmRequest(router, "POST", "/api/v1/dev/session", `{}`, "http://127.0.0.1:8080", nil)
	if w.Code != 404 {
		t.Fatal(w.Code)
	}
	w = realmRequest(router, "GET", "/api/v1/session", "", "", &http.Cookie{Name: "lunaorbit_dev_session", Value: "fake"})
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	q := httptest.NewRequest("GET", "http://127.0.0.1:8080/api/v1/auth/session", nil)
	q.RemoteAddr = "192.0.2.1:42"
	w = httptest.NewRecorder()
	router.ServeHTTP(w, q)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
}
func TestRealmCapacityAndImmutableIdentity(t *testing.T) {
	_, router := realmTest(t, RealmConfig{Mode: "mock", DevelopmentLoopback: true, Capacity: 1})
	w := realmRequest(router, "POST", "/api/v1/auth/login", `{"username":"synthetic","password":"synthetic-password"}`, "http://127.0.0.1:8080", nil)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	var response map[string]any
	json.Unmarshal(w.Body.Bytes(), &response)
	if response["role"] != "security-admin" {
		t.Fatal(response)
	}
	w = realmRequest(router, "POST", "/api/v1/auth/login", `{"username":"synthetic","password":"synthetic-password"}`, "http://127.0.0.1:8080", nil)
	if w.Code != 429 {
		t.Fatal(w.Code)
	}
}
func TestRealmRejectsForgedGroupsAndRole(t *testing.T) {
	realm, router := realmTest(t, RealmConfig{Mode: "mock", DevelopmentLoopback: true, TTL: time.Hour})
	w := realmRequest(router, "POST", "/api/v1/auth/login", `{"username":"synthetic","password":"synthetic-password","groups":["engineering"],"role":"admin"}`, "http://127.0.0.1:8080", nil)
	if w.Code != 400 {
		t.Fatalf("forged membership accepted: %d", w.Code)
	}
	w = realmRequest(router, "POST", "/api/v1/auth/login", `{"username":"synthetic","password":"synthetic-password"}`, "http://127.0.0.1:8080", nil)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	q := httptest.NewRequest("GET", "http://127.0.0.1:8080/api/v1/session", nil)
	q.AddCookie(w.Result().Cookies()[0])
	q.RemoteAddr = "127.0.0.1:12345"
	q.Header.Set("X-Groups", "engineering")
	q.Header.Set("X-Role", "admin")
	a, e := realm.Resolve(q)
	if e != nil || len(a.Groups) != 0 || a.Role != "security-admin" {
		t.Fatalf("identity forged %+v %v", a, e)
	}
}
