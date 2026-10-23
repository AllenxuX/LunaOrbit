package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"lunaorbit/backend/internal/config"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/httpapi"
	"lunaorbit/backend/internal/information"
	"lunaorbit/backend/internal/registry"
	"lunaorbit/backend/internal/workspace"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInformationPlatformAnonymousSharedAndSensitiveRoutesAbsent(t *testing.T) {
	store := registry.NewMemoryStore()
	s := registry.NewService(store, registry.NewMemorySecrets())
	defer s.Close()
	create := func(owner, name string) registry.Cluster {
		t.Helper()
		r, e := s.Create(context.Background(), registry.Actor{ID: owner, Role: "admin"}, registry.CreateInput{DisplayName: name, BootstrapServers: []string{"localhost:9092"}, Enabled: true, CredentialInput: registry.CredentialInput{SecurityProtocol: "PLAINTEXT"}})
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	shared := create("shared-lab", "shared")
	private := create("private-owner", "private")
	router := httpapi.NewRouter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	cfg := config.Config{PlatformMode: "information", InformationWorkspaceID: "shared-lab", RegistryMode: "mock", DiscoveryMode: "mock", InformationAllowedHosts: []string{"luna.internal:8080"}}
	close, e := mountInformationPlatform(router, s, store, discovery.NewMemoryStore(), workspace.NewMemoryStore(), information.NewMemoryStore(), cfg, func(string) string { return "" })
	if e != nil {
		t.Fatal(e)
	}
	defer close()
	mountRuntime(router, cfg)
	request := func(method, path, body, origin string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, "http://luna.internal:8080"+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Cookie", "lunaorbit_dev_session=old-admin-cookie; lunaorbit_session=old-admin")
		req.Header.Set("Authorization", "Bearer old-admin-token")
		req.Header.Set("X-User-ID", "private-owner")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}
	list := request("GET", "/api/v1/kafka/clusters", "", "")
	if list.Code != 200 || !strings.Contains(list.Body.String(), shared.ID) || strings.Contains(list.Body.String(), private.ID) {
		t.Fatalf("shared list %d %s", list.Code, list.Body.String())
	}
	if w := request("GET", "/api/v1/kafka/clusters/"+private.ID, "", ""); w.Code != 404 {
		t.Fatalf("private leaked: %d", w.Code)
	}
	for _, suffix := range []string{"workspace/accounts", "workspace/acls", "workspace/apps", "workspace/audit", "workspace/requests", "workspace/resource-operations", "grants"} {
		if w := request("GET", "/api/v1/kafka/clusters/"+shared.ID+"/"+suffix, "", ""); w.Code != 404 && w.Code != 405 {
			t.Errorf("sensitive GET %s: %d %s", suffix, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/api/v1/session", "/api/v1/dev/session", "/api/v1/identity/session", "/api/v1/access/applications", "/api/v1/changes"} {
		if w := request("GET", path, "", ""); w.Code != 404 && w.Code != 405 {
			t.Errorf("sensitive %s %d", path, w.Code)
		}
	}
	for _, suffix := range []string{"credentials", "grants", "workspace/resource-plans", "workspace/resource-operations", "workspace/plans", "workspace/requests", "workspace/accounts"} {
		for _, method := range []string{"POST", "PUT", "PATCH", "DELETE"} {
			if w := request(method, "/api/v1/kafka/clusters/"+shared.ID+"/"+suffix, "{}", ""); w.Code != 404 && w.Code != 405 {
				t.Errorf("sensitive %s %s %d", method, suffix, w.Code)
			}
		}
	}
	for _, path := range []string{"workspace/meta", "workspace/resource-configs?resource_type=BROKER&resource_name=1", "brokers", "topics", "groups"} {
		if w := request("GET", "/api/v1/kafka/clusters/"+shared.ID+"/"+path, "", ""); w.Code != 200 {
			t.Errorf("read %s: %d %s", path, w.Code, w.Body.String())
		}
	}
	meta := request("GET", "/api/v1/kafka/clusters/"+shared.ID+"/workspace/meta", "", "")
	var m struct {
		WriteEnabled bool            `json:"write_enabled"`
		Permissions  map[string]bool `json:"permissions"`
	}
	json.Unmarshal(meta.Body.Bytes(), &m)
	if m.WriteEnabled {
		t.Fatal("write enabled")
	}
	for k, v := range m.Permissions {
		if k != "read" && v {
			t.Errorf("enabled permission %s", k)
		}
	}
	body := `{"display_name":"anonymous","bootstrap_servers":["localhost:9093"],"enabled":true,"security_protocol":"PLAINTEXT"}`
	if w := request("POST", "/api/v1/kafka/clusters", body, "https://evil.example"); w.Code != 403 {
		t.Fatalf("foreign origin write %d", w.Code)
	}
	w := request("POST", "/api/v1/kafka/clusters", body, "http://luna.internal:8080")
	if w.Code != 201 {
		t.Fatalf("anonymous registry write %d %s", w.Code, w.Body.String())
	}
	if w = request("GET", "/api/v1/kafka/clusters", "", ""); !strings.Contains(w.Body.String(), "anonymous") {
		t.Fatal("registered info not shared")
	}
	if w = request("GET", "/api/v1/discovery/scopes", "", ""); w.Code != 200 {
		t.Fatalf("discovery %d", w.Code)
	}
	if w = request("GET", "/api/runtime", "", ""); !strings.Contains(w.Body.String(), `"information_mode":true`) {
		t.Fatal(w.Body.String())
	}
	saslBody := `{"display_name":"collector","bootstrap_servers":["localhost:9094"],"enabled":true,"security_protocol":"SASL_PLAINTEXT","sasl_mechanism":"SCRAM-SHA-256","username":"collector","password":"connection-only-secret"}`
	sw := request("POST", "/api/v1/kafka/clusters", saslBody, "http://luna.internal:8080")
	if sw.Code != 201 || strings.Contains(sw.Body.String(), "connection-only-secret") {
		t.Fatalf("connection secret setup: %d %s", sw.Code, sw.Body.String())
	}
	var registered registry.Cluster
	if e := json.Unmarshal(sw.Body.Bytes(), &registered); e != nil {
		t.Fatal(e)
	}
	replacement := `{"revision":1,"security_protocol":"SASL_PLAINTEXT","sasl_mechanism":"SCRAM-SHA-256","username":"collector","password":"replacement-only-secret"}`
	sw = request("PUT", "/api/v1/kafka/clusters/"+registered.ID+"/connection-credentials", replacement, "http://luna.internal:8080")
	if sw.Code != 200 || strings.Contains(sw.Body.String(), "replacement-only-secret") {
		t.Fatalf("connection secret update %d %s", sw.Code, sw.Body.String())
	}
	if sw = request("GET", "/api/v1/kafka/clusters/"+registered.ID+"/connection-credentials", "", ""); sw.Code != 404 && sw.Code != 405 {
		t.Fatalf("connection secret reveal %d", sw.Code)
	}

	for _, route := range router.Routes() {
		if strings.Contains(route.Path, "resource-operation") || strings.Contains(route.Path, "/accounts") || strings.Contains(route.Path, "/session") || strings.Contains(route.Path, "/credentials") || strings.Contains(route.Path, "/acls") {
			t.Errorf("sensitive route registered: %s", route.Path)
		}
	}
}
