package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"lunaorbit/backend/internal/discovery/collector"
	"lunaorbit/backend/internal/discovery/mm2auth"
	"lunaorbit/backend/internal/discovery/resource"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// canaryPassword is a unique fake password placed in the MM2 config; it must never
// appear anywhere outside the in-memory credential store.
const canaryPassword = "LUNA60-canary-9b2e7c41-pw"

func saslCollection(cluster string) resource.Collection {
	col := autoCollection(cluster)
	cm := &col.Namespaces[0].ConfigMaps[0]
	cm.Data = map[string]string{"mm2.properties": "clusters=k1,k2\nk1.bootstrap.servers=kafka-k1.lab:9092\nk2.bootstrap.servers=kafka-k2.lab:9093\nk1->k2.enabled=true\nk1->k2.topics=.*\n" +
		"security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\n" +
		"sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"mm2-canary-user\" password=\"" + canaryPassword + "\";\n"}
	dep := &col.Namespaces[0].Deployments[0]
	dep.Spec.Template.Spec.HostAliases = []resource.HostAlias{{IP: "10.9.0.1", Hostnames: []string{"kafka-k1.lab"}}, {IP: "10.9.0.2", Hostnames: []string{"kafka-k2.lab"}}}
	return col
}

type saslSource struct{}

func (saslSource) Collect(_ context.Context, scope resource.Scope) (resource.Collection, error) {
	col := saslCollection(scope.Cluster)
	col.Scope = scope
	col.ObservedAt = time.Now().UTC()
	return col, nil
}

var credFactory SourceFactory = func(resource.Scope, string) (collector.Source, func(), error) {
	return saslSource{}, func() {}, nil
}

func credService(t *testing.T, creds *mm2auth.Store) (*Service, *MemoryStore) {
	t.Helper()
	store := NewMemoryStore()
	s := NewService(store, nil, credFactory, func(context.Context, string) ([]Registration, error) { return nil, nil })
	if creds != nil {
		s.EnableMM2Credentials(creds)
	}
	t.Cleanup(s.Close)
	s.EnableAutoScopes()
	s.SyncClusters([]ClusterEntry{{Name: "edge-a"}}, time.Now())
	return s, store
}

func TestCanaryPasswordNeverLeavesMemory(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)

	creds := mm2auth.NewStore()
	s, store := credService(t, creds)
	id := scopeByCluster(t, s, "edge-a").ID
	if job := runScope(t, s, id); job.Status != "done" {
		t.Fatalf("job %+v", job)
	}
	if creds.Len() != 2 {
		t.Fatalf("credentials for both aliases expected in memory, got %d", creds.Len())
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	Mount(r.Group("/api/v1"), s, func(*gin.Context) Actor { return autoActor })
	var bodies strings.Builder
	for _, path := range []string{"/api/v1/discovery/scopes", "/api/v1/discovery/scopes/" + id + "/snapshot?page_size=100", "/api/v1/discovery/scopes/" + id + "/runs/latest", "/api/v1/discovery/endpoints"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("%s -> %d", path, w.Code)
		}
		b, _ := io.ReadAll(w.Body)
		bodies.Write(b)
	}
	persisted, _ := json.Marshal(store)
	latest, _ := store.Load(context.Background(), autoActor.ID, id)
	snap, _ := json.Marshal(latest)
	everything := bodies.String() + string(persisted) + string(snap) + logs.String()
	for _, secret := range []string{canaryPassword, "mm2-canary-user", "ScramLoginModule", "sasl.jaas.config"} {
		if strings.Contains(everything, secret) {
			t.Fatalf("%q leaked outside memory", secret)
		}
	}
	if !strings.Contains(bodies.String(), "MM2 配置（只读）") {
		t.Fatal("endpoints must state the credential source when enabled")
	}
}

func TestCredentialsSwitchOffReadsNothing(t *testing.T) {
	s, _ := credService(t, nil)
	id := scopeByCluster(t, s, "edge-a").ID
	runScope(t, s, id)
	eps := s.Endpoints()
	if len(eps) != 2 {
		t.Fatalf("auto endpoints are still listed without credentials: %+v", eps)
	}
	for _, e := range eps {
		if e.CredentialSource != "" || e.Access != AccessCredentialsOff {
			t.Fatalf("switch off must not read credentials: %+v", e)
		}
	}
}

func TestAutoEndpointsResolveHostAliases(t *testing.T) {
	s, _ := credService(t, mm2auth.NewStore())
	id := scopeByCluster(t, s, "edge-a").ID
	runScope(t, s, id)
	got := map[string]AutoEndpoint{}
	for _, e := range s.Endpoints() {
		got[e.Alias] = e
	}
	if k1 := got["k1"]; strings.Join(k1.Addresses, ",") != "kafka-k1.lab:9092" || strings.Join(k1.Resolved, ",") != "10.9.0.1:9092" || k1.KubeCluster != "edge-a" || k1.WorkloadName != "mm2" {
		t.Fatalf("k1 %+v", k1)
	}
	if k2 := got["k2"]; strings.Join(k2.Resolved, ",") != "10.9.0.2:9093" || k2.CredentialSource != "MM2 配置（只读）" {
		t.Fatalf("k2 %+v", k2)
	}
}
