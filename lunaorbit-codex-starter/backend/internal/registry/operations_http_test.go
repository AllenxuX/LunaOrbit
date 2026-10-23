package registry

import (
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOperationsHTTPPagesAndFailureClasses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	defer s.Close()
	r := gin.New()
	key := []byte("01234567890123456789012345678901")
	if e := Mount(r, s, false, key); e != nil {
		t.Fatal(e)
	}
	request := func(path string, cookie *http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "http://127.0.0.1"+path, nil)
		if cookie != nil {
			req.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	q := httptest.NewRequest("POST", "http://127.0.0.1/api/v1/dev/session", strings.NewReader("{}"))
	q.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, q)
	cookie := w.Result().Cookies()[0]
	a, ok := sessionActor(cookie.Value, key)
	if !ok {
		t.Fatal("session")
	}
	row, e := s.Create(context.Background(), a, syntheticInput())
	if e != nil {
		t.Fatal(e)
	}
	base := "/api/v1/kafka/clusters/" + row.ID
	if w := request(base+"/topics", nil); w.Code != 401 {
		t.Fatal(w.Code)
	}
	for _, test := range []struct {
		path   string
		status int
	}{{"/topics?page=1&page_size=1&query=orders", 200}, {"/topics?page=0", 400}, {"/topics/events", 200}, {"/topics/missing", 404}, {"/groups/payments/offsets", 200}, {"/brokers?scenario=authorization_denied", 403}, {"/brokers?scenario=broker_unreachable", 503}, {"/brokers?scenario=unsupported_api", 501}, {"/brokers?scenario=timeout", 504}} {
		w := request(base+test.path, cookie)
		if w.Code != test.status {
			t.Fatalf("%s: %d %s", test.path, w.Code, w.Body)
		}
		if strings.Contains(w.Body.String(), "synthetic-password") || strings.Contains(w.Body.String(), "synthetic-user") {
			t.Fatal("secret exposed")
		}
	}
	w = request(base+"/topics?page_size=1&query=orders", cookie)
	var body struct {
		Items []struct {
			Name  string `json:"name"`
			RFMin int    `json:"rf_min"`
		}
		Total int
	}
	if e = json.Unmarshal(w.Body.Bytes(), &body); e != nil || body.Total != 1 || len(body.Items) != 1 || body.Items[0].RFMin != 3 {
		t.Fatal(w.Body.String())
	}
}
