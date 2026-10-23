package changes

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/registry"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPAuthenticationTypedBodyConflictAndSecrets(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s, owner, approver := setup(t)
	a := registry.Actor{}
	r := gin.New()
	Mount(r.Group("/api/v1"), s, func(*gin.Context) registry.Actor { return a })
	call := func(method, path, body string) *httptest.ResponseRecorder {
		q := httptest.NewRequest(method, "/api/v1"+path, strings.NewReader(body))
		if method == "POST" {
			q.Header.Set("Content-Type", "application/json")
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, q)
		if strings.Contains(w.Body.String(), "DO_NOT_EXPOSE") || strings.Contains(w.Body.String(), "ciphertext") {
			t.Fatal("secret in HTTP", w.Body)
		}
		return w
	}
	if w := call("GET", "/changes", ""); w.Code != 401 {
		t.Fatal(w.Code)
	}
	a = owner
	for _, b := range []string{`{"target_id":"synthetic-mars-topic-rule","topic_rule":"events.*","expected_resource_version":"1","owner_id":"forged"}`, `{"target_id":"synthetic-mars-topic-rule","topic_rule":"events.*","expected_resource_version":"stale"}`} {
		w := call("POST", "/changes", b)
		if w.Code != 400 && w.Code != 409 {
			t.Fatal(w.Code, w.Body)
		}
	}
	req := submit(t, s, owner)
	a = approver
	if w := call("POST", "/changes/"+req.ID+"/approve", `{"revision":1}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	a = owner
	if w := call("POST", "/changes/"+req.ID+"/execute", `{"revision":1}`); w.Code != 409 {
		t.Fatal(w.Code, w.Body)
	}
	w := call("POST", "/changes/"+req.ID+"/execute", `{"revision":2,"scenario":"success"}`)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"succeeded"`) {
		t.Fatal(w.Code, w.Body)
	}
	if w := call("GET", "/changes/targets", ""); !strings.Contains(w.Body.String(), `"resource_version":"2"`) {
		t.Fatal(w.Body)
	}
}

type failBackupStore struct{ Store }

func (s failBackupStore) Save(ctx context.Context, r Request, expected int64, a, action string, b []byte) error {
	if action == "change.backup" {
		return ErrStorage
	}
	return s.Store.Save(ctx, r, expected, a, action, b)
}
func TestBackupAuditFailurePreventsWrites(t *testing.T) {
	s, o, a := setup(t)
	r := approve(t, s, o, a)
	s.Store = failBackupStore{s.Store}
	if _, e := s.Execute(context.Background(), o, r.ID, r.Revision, "success"); e == nil {
		t.Fatal("backup audit failure ignored")
	}
	m := s.Adapter.(*MockAdapter)
	if m.Updates != 0 || m.Restarts != 0 {
		t.Fatal(fmt.Sprintf("write without durable backup: %d", m.Updates))
	}
}
