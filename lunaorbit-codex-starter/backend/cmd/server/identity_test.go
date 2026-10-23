package main

import (
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/registry"
	"testing"
)

func TestIdentityConfigurationFailsClosed(t *testing.T) {
	service := registry.NewService(registry.NewMemoryStore(), registry.NewMemorySecrets())
	defer service.Close()
	for _, env := range []map[string]string{{"IDENTITY_MODE": "unexpected"}, {"IDENTITY_MODE": "mock", "IDENTITY_MOCK_USERS": "private-invalid-value"}, {"IDENTITY_MODE": "ldap", "LDAP_URL": "ldap://invalid.test"}} {
		if err := mountIdentity(gin.New(), service, "http://127.0.0.1:5173", func(k string) string { return env[k] }); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	if service.IdentityProvider != nil {
		t.Fatal("failed config mutated identity")
	}
}
