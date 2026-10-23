package main

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/config"
	"lunaorbit/backend/internal/registry"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRuntimeCapabilitiesFollowConfiguredModes(t *testing.T) {
	for _, tc := range []struct {
		name                                        string
		env                                         map[string]string
		discovery, kafka, acl, changes, observation bool
	}{
		{name: "disabled", env: map[string]string{}},
		{name: "registry mock", env: map[string]string{"REGISTRY_MODE": "mock"}, kafka: true, acl: true},
		{name: "discovery mock", env: map[string]string{"REGISTRY_MODE": "mock", "DISCOVERY_MODE": "mock"}, discovery: true, kafka: true, acl: true},
		{name: "complete mock", env: map[string]string{"REGISTRY_MODE": "mock", "DISCOVERY_MODE": "mock", "CHANGES_MODE": "mock", "OBSERVABILITY_MODE": "mock"}, discovery: true, kafka: true, acl: true, changes: true, observation: true},
		{name: "readonly lab", env: map[string]string{"LUNAORBIT_RUNTIME_MODE": "readonly-lab", "REGISTRY_MODE": "mock", "DISCOVERY_MODE": "readonly-lab"}, discovery: true, kafka: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, e := config.Load(func(k string) string { return tc.env[k] })
			if e != nil {
				t.Fatal(e)
			}
			r := gin.New()
			mountRuntime(r, cfg)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest("GET", "/api/runtime", nil))
			var result struct {
				Capabilities map[string]bool `json:"capabilities"`
			}
			if json.Unmarshal(w.Body.Bytes(), &result) != nil {
				t.Fatal("invalid runtime response")
			}
			for k, want := range map[string]bool{"discovery": tc.discovery, "kafka_reads": tc.kafka, "acl_reads": tc.acl, "changes": tc.changes, "observability": tc.observation} {
				if result.Capabilities[k] != want {
					t.Errorf("%s capability mismatch", k)
				}
			}
		})
	}
}

func TestLabRuntimeIsSafeMetadata(t *testing.T) {
	r := gin.New()
	mountRuntime(r, config.Config{RuntimeMode: "readonly-lab", RegistryMode: "mock", DiscoveryMode: "readonly-lab"})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/api/runtime", nil))
	for _, want := range []string{`"mode":"readonly-lab"`, `"write_enabled":false`, `"acl_reads":false`, `"synthetic":false`} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatal("runtime marker mismatch")
		}
	}
}

func TestLabPolicyRequiresAllBounds(t *testing.T) {
	s := registry.NewService(registry.NewMemoryStore(), registry.NewMemorySecrets())
	defer s.Close()
	if configureLabKafka(s, func(string) string { return "" }) == nil {
		t.Fatal("unbounded policy accepted")
	}
}
