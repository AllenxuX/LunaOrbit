package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/kafkaadmin"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func catalogFixture(t *testing.T) (*Service, *kafkaadmin.NetworkPolicy, string) {
	t.Helper()
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	t.Cleanup(s.Close)
	p, e := kafkaadmin.NewNetworkPolicy(kafkaadmin.NetworkPolicyConfig{AllowedHosts: []string{"127.0.0.1", "127.0.0.2"}, AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{9092}})
	if e != nil {
		t.Fatal(e)
	}
	entries := []CreateInput{}
	for _, seed := range []string{"127.0.0.1:9092", "127.0.0.2:9092"} {
		entries = append(entries, CreateInput{DisplayName: "Synthetic lab", BootstrapServers: []string{seed}, Enabled: true, CredentialInput: CredentialInput{SecurityProtocol: "PLAINTEXT"}})
	}
	raw, _ := json.Marshal(entries)
	return s, p, string(raw)
}

func TestLabCatalogSupportsFourValidatedClustersIdempotently(t *testing.T) {
	s, _, _ := catalogFixture(t)
	p, err := kafkaadmin.NewNetworkPolicy(kafkaadmin.NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{9092}})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.UseReadOnlyKafka(p); err != nil {
		t.Fatal(err)
	}
	raw := multiLabCatalog(4)
	if err = s.ConfigureLabCatalog(context.Background(), raw, p); err != nil {
		t.Fatal(err)
	}
	if !s.HasLabCatalog() {
		t.Fatal("four-cluster catalog missing")
	}
	a := Actor{ID: "four-owner", Role: "admin"}
	first, err := s.SeedLabCatalog(context.Background(), a)
	if err != nil || len(first) != 4 {
		t.Fatalf("first seed: count=%d err=%v", len(first), err)
	}
	second, err := s.SeedLabCatalog(context.Background(), a)
	if err != nil || len(second) != 4 {
		t.Fatalf("second seed: count=%d err=%v", len(second), err)
	}
	for _, row := range second {
		if row.ID != first[0].ID && row.ID != first[1].ID && row.ID != first[2].ID && row.ID != first[3].ID {
			t.Fatal("seed duplicated resource")
		}
		if err = s.AuthorizeConfiguredLabTarget(context.Background(), a, row.ID); err != nil {
			t.Fatal("configured fourth target denied", err)
		}
	}
	for _, raw = range []string{multiLabCatalog(1), multiLabCatalog(17), strings.Replace(multiLabCatalog(4), "127.0.0.4:9092", "198.51.100.4:9092", 1), strings.Replace(multiLabCatalog(4), "127.0.0.4:9092", "127.0.0.3:9092", 1)} {
		if s.ConfigureLabCatalog(context.Background(), raw, p) == nil {
			t.Fatal("invalid expanded catalog accepted")
		}
	}
	if !s.HasLabCatalog() {
		t.Fatal("failed validation replaced valid catalog")
	}
}

func multiLabCatalog(count int) string {
	entries := []CreateInput{}
	for i := 1; i <= count; i++ {
		entries = append(entries, CreateInput{DisplayName: fmt.Sprintf("Synthetic %d", i), BootstrapServers: []string{fmt.Sprintf("127.0.0.%d:9092", i)}, Enabled: true, CredentialInput: CredentialInput{SecurityProtocol: "PLAINTEXT"}})
	}
	raw, _ := json.Marshal(entries)
	return string(raw)
}

func TestLabCatalogHTTPExplicitAndAuthenticated(t *testing.T) {
	s, p, raw := catalogFixture(t)
	_ = s.UseReadOnlyKafka(p)
	if e := s.ConfigureLabCatalog(context.Background(), raw, p); e != nil {
		t.Fatal(e)
	}
	r := gin.New()
	if e := Mount(r, s, false); e != nil {
		t.Fatal(e)
	}
	request := func(method, path, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
		q := httptest.NewRequest(method, "http://127.0.0.1:8080/api/v1"+path, strings.NewReader(body))
		q.Header.Set("Content-Type", "application/json")
		if cookie != nil {
			q.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, q)
		return w
	}
	if w := request("POST", "/lab/registrations", "{}", nil); w.Code != 401 {
		t.Fatal("unauthenticated seed allowed")
	}
	w := request("POST", "/dev/session", "{}", nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"lab_catalog":true`) {
		t.Fatal("session catalog marker missing")
	}
	cookie := w.Result().Cookies()[0]
	if w = request("GET", "/session", "", cookie); w.Code != 200 || !strings.Contains(w.Body.String(), `"lab_catalog":true`) {
		t.Fatal("authenticated marker missing")
	}
	if w = request("GET", "/kafka/clusters", "", cookie); !strings.Contains(w.Body.String(), `"total":0`) {
		t.Fatal("GET mutated registry")
	}
	if w = request("POST", "/lab/registrations", `{"bootstrap_servers":["other:9092"]}`, cookie); w.Code != 400 {
		t.Fatal("request controlled catalog")
	}
	if w = request("POST", "/lab/registrations", "{}", cookie); w.Code != 200 {
		t.Fatalf("seed failed %d", w.Code)
	}
	var result struct{ Items []Cluster }
	if json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result.Items) != 2 {
		t.Fatal("missing seeded items")
	}
	s.workers.Wait()
	for _, row := range result.Items {
		current, e := s.store.Get(context.Background(), extractUser(t, request("GET", "/session", "", cookie).Body.Bytes()), row.ID)
		if e != nil {
			t.Fatal(e)
		}
		if current.Status == "mock_connected" {
			t.Fatal("native failure marked mock success")
		}
	}
}

func extractUser(t *testing.T, raw []byte) string {
	t.Helper()
	var v struct {
		ID string `json:"user_id"`
	}
	if json.Unmarshal(raw, &v) != nil {
		t.Fatal("bad session")
	}
	return v.ID
}

func TestLabCatalogIsConcurrentOwnerScopedAndPreservesRemoval(t *testing.T) {
	s, p, raw := catalogFixture(t)
	if e := s.UseReadOnlyKafka(p); e != nil {
		t.Fatal(e)
	}
	if e := s.ConfigureLabCatalog(context.Background(), raw, p); e != nil {
		t.Fatal(e)
	}
	a := Actor{ID: "synthetic-a", Role: "admin"}
	b := Actor{ID: "synthetic-b", Role: "admin"}
	custom := syntheticInput()
	custom.DisplayName = "Synthetic custom registration"
	custom.BootstrapServers = []string{"custom.synthetic.test:9092"}
	customRow, e := s.Create(context.Background(), a, custom)
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := s.SeedLabCatalog(context.Background(), a); e != nil {
				t.Errorf("seed: %v", e)
			}
		}()
	}
	wg.Wait()
	rows, e := s.SeedLabCatalog(context.Background(), a)
	if e != nil || len(rows) != 2 {
		t.Fatal("concurrent seeds duplicated or failed")
	}
	other, e := s.SeedLabCatalog(context.Background(), b)
	if e != nil || len(other) != 2 || other[0].ID == rows[0].ID {
		t.Fatal("owner isolation failed")
	}
	// Wait for probes before deleting to preserve the current revision.
	s.workers.Wait()
	rows, _ = s.SeedLabCatalog(context.Background(), a)
	changedName := "Synthetic user edit"
	if _, e = s.Patch(context.Background(), a, rows[1].ID, PatchInput{Revision: rows[1].Revision, DisplayName: &changedName}); e != nil {
		t.Fatal(e)
	}
	if e = s.Delete(context.Background(), a, rows[0].ID, rows[0].Revision); e != nil {
		t.Fatal(e)
	}
	rows, e = s.SeedLabCatalog(context.Background(), a)
	if e != nil || len(rows) != 1 {
		t.Fatal("removed registration resurrected")
	}
	if rows[0].DisplayName != changedName {
		t.Fatal("seed overwrote user edit")
	}
	if _, e = s.Get(context.Background(), a, customRow.ID); e != nil {
		t.Fatal("custom registration changed or deleted")
	}
	if _, e = s.SeedLabCatalog(context.Background(), Actor{ID: "viewer", Role: "viewer"}); ErrorCode(e) != "FORBIDDEN" {
		t.Fatal("viewer seeded")
	}
}

func TestLabCatalogViewerSessionDoesNotAdvertiseAdminAction(t *testing.T) {
	s, p, raw := catalogFixture(t)
	_ = s.UseReadOnlyKafka(p)
	if e := s.ConfigureLabCatalog(context.Background(), raw, p); e != nil {
		t.Fatal(e)
	}
	s.IdentityProvider = testIdentity{actor: Actor{ID: "synthetic-viewer", Role: "viewer"}}
	r := gin.New()
	if e := Mount(r, s, false); e != nil {
		t.Fatal(e)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "http://127.0.0.1:8080/api/v1/session", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"lab_catalog":false`) {
		t.Fatal("viewer advertised admin seed action")
	}
	if s.CanSeedLabCatalog(Actor{ID: "synthetic-viewer", Role: "viewer"}) {
		t.Fatal("viewer catalog permission")
	}
}

func TestLabCatalogRequiresNativeAndValidatedReadOnlyEntries(t *testing.T) {
	s, p, raw := catalogFixture(t)
	if s.ConfigureLabCatalog(context.Background(), raw, p) == nil {
		t.Fatal("mock catalog enabled")
	}
	_ = s.UseReadOnlyKafka(p)
	for _, value := range []string{`[]`, `null`, `[{"password":"synthetic-private-value"}]`, raw[:len(raw)-1] + `,{}]`, strings.Replace(raw, "PLAINTEXT", "SASL_SSL", 1), strings.Replace(raw, "127.0.0.1:9092", "127.0.0.3:9092", 1), strings.Replace(raw, `"enabled":true`, `"enabled":false`, 1)} {
		if s.ConfigureLabCatalog(context.Background(), value, p) == nil {
			t.Fatal("invalid catalog accepted")
		}
	}
	if _, e := s.SeedLabCatalog(context.Background(), Actor{ID: "a", Role: "admin"}); ErrorCode(e) != "NOT_FOUND" {
		t.Fatal("missing catalog seeded")
	}
}
