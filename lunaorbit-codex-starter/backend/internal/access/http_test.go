package access

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPRoleWhitelistAndPagination(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s, actor, id := setup(t)
	r := gin.New()
	Mount(r.Group("/api/v1"), s, func(*gin.Context) registry.Actor { return actor })
	get := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/kafka/clusters/"+id+path, nil))
		return w
	}
	if w := get("/acls?page_size=1"); w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"pending"`) {
		t.Fatal(w.Code, w.Body)
	}
	if w := get("/acls?page_size=101"); w.Code != 400 {
		t.Fatal(w.Code)
	}
	actor.Role = "viewer"
	if w := get("/acls"); w.Code != 403 {
		t.Fatal(w.Code)
	}
	actor.Role = "admin"
	for _, b := range []string{`{"owner_id":"forged"}`, `{"rule":{"principal":"ldap-user"}}`} {
		w := httptest.NewRecorder()
		q := httptest.NewRequest("POST", "/api/v1/kafka/clusters/"+id+"/acl-requests", strings.NewReader(b))
		q.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, q)
		if w.Code != 400 {
			t.Fatal(w.Code, w.Body)
		}
	}
}
func TestApprovalInboxHTTP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s, owner, id := setup(t)
	ctx := context.Background()
	r, e := s.Submit(ctx, owner, id, Input{Rule: rule()})
	if e != nil {
		t.Fatal(e)
	}
	actor := registry.Actor{ID: "independent-approver", Role: "admin"}
	engine := gin.New()
	Mount(engine.Group("/api/v1"), s, func(*gin.Context) registry.Actor { return actor })
	call := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		q := httptest.NewRequest(method, "/api/v1"+path, strings.NewReader(body))
		q.Header.Set("Content-Type", "application/json")
		engine.ServeHTTP(w, q)
		return w
	}
	if w := call("GET", "/acl-requests/inbox", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"total":0`) {
		t.Fatal(w.Code, w.Body)
	}
	s.ApprovalAuthorizer = StaticApprovals{actor.ID: []string{id}, owner.ID: []string{id}}
	if w := call("GET", "/acl-requests/inbox", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"can_approve":true`) {
		t.Fatal(w.Code, w.Body)
	}
	actor.ID = "outside"
	if w := call("GET", "/acl-requests/inbox", ""); !strings.Contains(w.Body.String(), `"total":0`) {
		t.Fatal(w.Body)
	}
	if w := call("POST", "/acl-requests/"+r.ID+"/approve", `{"revision":1}`); w.Code != 404 {
		t.Fatal(w.Code, w.Body)
	}
	actor = owner
	if w := call("POST", "/acl-requests/"+r.ID+"/approve", `{"revision":1}`); w.Code != 403 {
		t.Fatal(w.Code, w.Body)
	}
	actor = registry.Actor{ID: "independent-approver", Role: "admin"}
	if w := call("POST", "/acl-requests/"+r.ID+"/approve", `{"revision":1}`); w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"approved_not_executed"`) {
		t.Fatal(w.Code, w.Body)
	}
	if w := call("GET", "/kafka/clusters/"+id+"/acl-requests", ""); w.Code != 404 {
		t.Fatal("approver must not inherit owner list access", w.Code)
	}
	if w := call("GET", "/acl-requests/inbox", ""); !strings.Contains(w.Body.String(), `"total":0`) {
		t.Fatal(w.Body)
	}
}
func TestInboxPaginationDoesNotExposeOutsideScope(t *testing.T) {
	service, _ := inboxFlood(t)
	engine := gin.New()
	actor := registry.Actor{ID: "approver", Role: "admin"}
	Mount(engine.Group("/api/v1"), service, func(*gin.Context) registry.Actor { return actor })
	call := func(query string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/acl-requests/inbox"+query, nil))
		return w
	}
	for _, tc := range []struct{ page, items int }{{1, 2}, {2, 1}, {3, 0}} {
		w := call(fmt.Sprintf("?page=%d&page_size=2", tc.page))
		var response struct {
			Items       []Request `json:"items"`
			Total, Page int
			PageSize    int `json:"page_size"`
		}
		if e := json.Unmarshal(w.Body.Bytes(), &response); e != nil || w.Code != 200 || len(response.Items) != tc.items || response.Total != 3 || response.Page != tc.page || response.PageSize != 2 {
			t.Fatal(w.Code, w.Body.String())
		}
		for _, r := range response.Items {
			if r.RegistrationID != "authorized-registration" {
				t.Fatal("outside-scope item leaked")
			}
		}
	}
	actor.ID = "outside-approver"
	if w := call("?page=2&page_size=2"); w.Code != 200 || !strings.Contains(w.Body.String(), `"total":0`) || strings.Contains(w.Body.String(), "pending-") {
		t.Fatal(w.Code, w.Body.String())
	}
	actor.ID = "approver"
	if w := call("?page_size=101"); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if w := call("?query=unmatched"); w.Code != 200 || !strings.Contains(w.Body.String(), `"total":0`) {
		t.Fatal(w.Code, w.Body.String())
	}
}

type failingACLReader struct{ failure error }

func (r failingACLReader) ReadACLs(context.Context, string, string) (kafkaops.ACLSnapshot, error) {
	return kafkaops.ACLSnapshot{}, r.failure
}
func TestACLHTTPClassifiesTypedReaderErrorsSafely(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		code   string
		status int
	}{{"AUTHORIZER_UNVERIFIED", 501}, {"AUTHORIZATION_DENIED", 403}, {"NETWORK_ERROR", 503}, {"NETWORK_DENIED", 503}, {"BROKER_UNREACHABLE", 503}, {"ADVERTISED_UNREACHABLE", 503}, {"TLS_HANDSHAKE_FAILED", 503}, {"AUTHENTICATION_FAILED", 503}, {"UNSUPPORTED_API", 501}, {"TIMEOUT", 504}} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/wrapped=%t", tc.code, wrapped), func(t *testing.T) {
				service, actor, id := setup(t)
				var failure error = &kafkaops.Error{Code: tc.code}
				if wrapped {
					failure = fmt.Errorf("synthetic-private-upstream-details: %w", failure)
				}
				service.Reader = failingACLReader{failure}
				engine := gin.New()
				Mount(engine.Group("/api/v1"), service, func(*gin.Context) registry.Actor { return actor })
				w := httptest.NewRecorder()
				engine.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/kafka/clusters/"+id+"/acls", nil))
				var response struct {
					Error struct{ Code, Message string } `json:"error"`
				}
				if e := json.Unmarshal(w.Body.Bytes(), &response); e != nil || w.Code != tc.status || response.Error.Code != tc.code || response.Error.Message != "Request could not be completed" || strings.Contains(w.Body.String(), "synthetic-private") {
					t.Fatal(w.Code, w.Body.String())
				}
			})
		}
	}
	service, actor, id := setup(t)
	service.Reader = failingACLReader{&kafkaops.Error{Code: "synthetic-private-secret"}}
	engine := gin.New()
	Mount(engine.Group("/api/v1"), service, func(*gin.Context) registry.Actor { return actor })
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/kafka/clusters/"+id+"/acls", nil))
	if w.Code != 503 || !strings.Contains(w.Body.String(), `"code":"STORAGE_UNAVAILABLE"`) || strings.Contains(w.Body.String(), "synthetic-private") {
		t.Fatal(w.Code, w.Body.String())
	}
}
