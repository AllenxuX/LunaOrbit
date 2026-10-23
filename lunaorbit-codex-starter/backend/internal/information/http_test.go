package information

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http/httptest"
	"testing"
)

func TestCRUDAndIsolation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := NewMemoryStore()
	r := gin.New()
	Mount(r.Group("/api/v1"), s, "shared", func(context.Context, string) bool { return true })
	call := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		q := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		q.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, q)
		return w
	}
	w := call("POST", "/api/v1/information/applications", `{"name":"orders","owner":"team","environment":"uat","description":"manual","relations":[{"cluster_id":"test","topic":"orders","mode":"produce"}]}`)
	if w.Code != 201 {
		t.Fatalf("create %d %s", w.Code, w.Body)
	}
	var app Application
	json.Unmarshal(w.Body.Bytes(), &app)
	if app.ID == "" || app.Revision != 1 {
		t.Fatal(app)
	}
	body := `{"name":"updated","owner":"team","environment":"uat","description":"manual","relations":[],"revision":1}`
	if w := call("PATCH", "/api/v1/information/applications/"+app.ID, body); w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	if w := call("PATCH", "/api/v1/information/applications/"+app.ID, body); w.Code != 409 {
		t.Fatal(w.Code, w.Body)
	}
	if w := call("DELETE", "/api/v1/information/applications/"+app.ID+"?revision=1", ""); w.Code != 409 {
		t.Fatal(w.Code)
	}
	page, err := s.List(context.Background(), "other", 1, 20, "")
	if err != nil || page.Total != 0 {
		t.Fatal(page, err)
	}
	if w := call("GET", "/api/v1/information/applications?query=updated", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := call("DELETE", "/api/v1/information/applications/"+app.ID+"?revision=2", ""); w.Code != 204 {
		t.Fatal(w.Code, w.Body)
	}
}
func TestRejectInvalid(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	Mount(r.Group("/api/v1"), NewMemoryStore(), "shared", func(context.Context, string) bool { return false })
	for _, body := range []string{`{"name":"x","relations":[{"cluster_id":"missing","topic":"x","mode":"produce"}]}`, `{"name":"x","updated_at":"2026-01-01T00:00:00Z"}`, `{"name":"x","password":"secret"}`, `{"name":"x","relations":[{"cluster_id":"x","topic":"events","mode":"produce","account":"synthetic-user","password":"synthetic-secret"}]}`, `{"name":"x","relations":[{"cluster_id":"x","topic":"..","mode":"consume"}]}`} {
		w := httptest.NewRecorder()
		q := httptest.NewRequest("POST", "/api/v1/information/applications", bytes.NewBufferString(body))
		q.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, q)
		if w.Code != 400 {
			t.Fatal(w.Code, w.Body)
		}
	}
}

func TestIdentityHTTPFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	Mount(r.Group("/api/v1"), NewMemoryStore(), "w", func(context.Context, string) bool { return true })
	w := httptest.NewRecorder()
	q := httptest.NewRequest("POST", "/api/v1/information/applications", bytes.NewBufferString(`{"name":"synthetic","location":"lab-service","relations":[{"cluster_id":"a","topic":"events","mode":"produce","account":"user-test","client_id":"producer-test"}]}`))
	q.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, q)
	var app Application
	json.Unmarshal(w.Body.Bytes(), &app)
	if w.Code != 201 || app.Location != "lab-service" || len(app.Relations) != 1 || app.Relations[0].Account != "user-test" || app.Relations[0].ClientID != "producer-test" {
		t.Fatal(w.Code, w.Body)
	}
}
