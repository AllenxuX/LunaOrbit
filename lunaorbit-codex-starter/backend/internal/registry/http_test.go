package registry

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestHTTPAuthenticationCSRFAndIsolation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	s.BrowserOrigins = []string{"http://127.0.0.1:5173"}
	defer s.Close()
	Mount(r, s, false)
	request := func(method, path, body string, cookie *http.Cookie, origin string) *httptest.ResponseRecorder {
		q := httptest.NewRequest(method, "http://127.0.0.1:8080"+path, strings.NewReader(body))
		q.Header.Set("Content-Type", "application/json")
		if cookie != nil {
			q.AddCookie(cookie)
		}
		if origin != "" {
			q.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, q)
		return w
	}
	if w := request("GET", "/api/v1/kafka/clusters", "", nil, ""); w.Code != 401 {
		t.Fatal("unauthenticated access")
	}
	if w := request("POST", "/api/v1/dev/session", "{}", nil, "https://evil.example"); w.Code != 403 {
		t.Fatal("cross-origin session created")
	}
	if w := request("POST", "/api/v1/dev/session", "{}", nil, "http://127.0.0.1:5173"); w.Code != 200 {
		t.Fatal("explicit development browser origin rejected")
	}
	w := request("POST", "/api/v1/dev/session", "{}", nil, "")
	if w.Code != 200 {
		t.Fatalf("session: %d %s", w.Code, w.Body.String())
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatal("unsafe cookie")
	}
	first := cookies[0]
	w = request("POST", "/api/v1/dev/session", "{}", nil, "")
	second := w.Result().Cookies()[0]
	body, _ := json.Marshal(syntheticInput())
	w = request("POST", "/api/v1/kafka/clusters", string(body), first, "")
	if w.Code != 201 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	if bytes.Contains(w.Body.Bytes(), []byte("synthetic-password")) {
		t.Fatal("secret leaked")
	}
	var created Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if w := request("GET", "/api/v1/kafka/clusters/"+created.ID, "", second, ""); w.Code != 404 {
		t.Fatal("cross-session get")
	}
	w = request("GET", "/api/v1/kafka/clusters", "", second, "")
	if !strings.Contains(w.Body.String(), `"total":0`) {
		t.Fatal("cross-session list")
	}
	if w := request("PATCH", "/api/v1/kafka/clusters/"+created.ID, `{"revision":1,"enabled":false}`, first, "https://evil.example"); w.Code != 403 {
		t.Fatal("cross-origin write")
	}
	if w := request("POST", "/api/v1/kafka/clusters", `{"created_by":"spoofed"}`, first, ""); w.Code != 400 {
		t.Fatal("unknown field accepted")
	}
	if w := request("GET", "/api/v1/kafka/clusters?page_size=10000", "", first, ""); w.Code != 400 {
		t.Fatal("pagination bound missing")
	}
}

type testIdentity struct {
	actor Actor
	err   error
}

func (p testIdentity) Resolve(*http.Request) (Actor, error) { return p.actor, p.err }
func TestExternalIdentityDisablesDevSessionAndFallback(t *testing.T) {
	r := gin.New()
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	defer s.Close()
	s.IdentityProvider = testIdentity{err: errors.New("denied")}
	if e := Mount(r, s, true); e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{"/api/v1/dev/session", "/api/v1/session"} {
		method := "GET"
		if strings.Contains(path, "dev") {
			method = "POST"
		}
		q := httptest.NewRequest(method, "http://127.0.0.1:8080"+path, strings.NewReader("{}"))
		q.AddCookie(&http.Cookie{Name: "lunaorbit_dev_session", Value: signedSession("spoofed", time.Now().Add(time.Hour), make([]byte, 32))})
		w := httptest.NewRecorder()
		r.ServeHTTP(w, q)
		want := 401
		if method == "POST" {
			want = 404
		}
		if w.Code != want {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
}
