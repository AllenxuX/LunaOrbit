package httpapi

import (
	"bytes"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthAndErrors(t *testing.T) {
	var logs bytes.Buffer
	r := NewRouter(slog.New(slog.NewJSONHandler(&logs, nil)))
	r.GET("/panic", func(c *gin.Context) { panic("private-password") })
	for _, tc := range []struct {
		method, path string
		status       int
		code         string
	}{{"GET", "/healthz", 200, ""}, {"GET", "/missing?token=private-password", 404, "NOT_FOUND"}, {"POST", "/healthz", 405, "METHOD_NOT_ALLOWED"}, {"GET", "/panic", 500, "INTERNAL_ERROR"}} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Header().Get("X-Request-ID") == "" {
			t.Fatal("missing request ID")
		}
		if tc.code != "" && body["error"].(map[string]any)["code"] != tc.code {
			t.Fatalf("unexpected error: %v", body)
		}
		if strings.Contains(w.Body.String(), "private-password") {
			t.Fatal("response leaked secret")
		}
	}
	if strings.Contains(logs.String(), "private-password") {
		t.Fatal("log leaked query or panic")
	}
}
