package workspace

import (
	"errors"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/registry"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPBodyLimitsPaginationAndSafeErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	fail(c, errors.New("postgres://secret@private invalid query"))
	if strings.Contains(rec.Body.String(), "secret") || !strings.Contains(rec.Body.String(), "STORAGE_UNAVAILABLE") {
		t.Fatal(rec.Body.String())
	}
	s, a, id, _ := setup(t)
	r := gin.New()
	Mount(r.Group("/api/v1"), s, func(*gin.Context) registry.Actor { return a })
	for _, path := range []string{"/apps?page_size=101", "/apps?query=" + strings.Repeat("x", 129), "/acls?principal=" + strings.Repeat("x", 257)} {
		rec = httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/kafka/clusters/"+id+"/workspace"+path, nil))
		if rec.Code != 400 {
			t.Fatal(path, rec.Code, rec.Body.String())
		}
	}
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("POST", "/api/v1/kafka/clusters/"+id+"/workspace/apps", strings.NewReader(`{"name":"app","principal":"User:test","password":"must-not-store"}`)))
	if rec.Code != 400 {
		t.Fatal(rec.Code, rec.Body.String())
	}
}
