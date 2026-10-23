package config

import "testing"

func TestDefaults(t *testing.T) {
	c, err := Load(func(string) string { return "" })
	if err != nil || c.Address != "127.0.0.1:8080" || c.LogLevel != "info" {
		t.Fatalf("unexpected configuration: %+v %v", c, err)
	}
}

func TestRejectInvalidConfig(t *testing.T) {
	for _, pair := range [][2]string{{"HTTP_ADDR", "0.0.0.0:99999"}, {"HTTP_ADDR", "host"}, {"LOG_LEVEL", "verbose"}, {"ENABLE_KITE_WRITES", "true"}, {"ENABLE_KAFKA_WRITES", "true"}, {"ENABLE_KITE_WRITES", "invalid"}} {
		t.Run(pair[0]+pair[1], func(t *testing.T) {
			_, err := Load(func(k string) string {
				if k == pair[0] {
					return pair[1]
				}
				return ""
			})
			if err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestRegistryDevelopmentConfigGuard(t *testing.T) {
	for _, values := range []map[string]string{
		{"REGISTRY_MODE": "live"},
		{"REGISTRY_MODE": "mock", "HTTP_ADDR": "0.0.0.0:8080"},
		{"REGISTRY_MODE": "postgres-mock"},
		{"REGISTRY_MODE": "postgres-mock", "DATABASE_URL": "synthetic-invalid", "DEV_SESSION_KEY": "too-short"},
	} {
		if _, e := Load(func(key string) string { return values[key] }); e == nil {
			t.Fatal("unsafe development mode accepted")
		}
	}
	if c, e := Load(func(key string) string {
		if key == "REGISTRY_MODE" {
			return "mock"
		}
		return ""
	}); e != nil || c.RegistryMode != "mock" {
		t.Fatal("safe mock configuration rejected")
	}
}

func TestIdentityModeRequiresRegistryAndKnownMode(t *testing.T) {
	for _, env := range []map[string]string{{"IDENTITY_MODE": "ldap"}, {"IDENTITY_MODE": "mock"}, {"IDENTITY_MODE": "invalid", "REGISTRY_MODE": "mock"}} {
		if _, err := Load(func(k string) string { return env[k] }); err == nil {
			t.Fatal("identity configuration bypass")
		}
	}
}

func TestChangesModeRequiresSyntheticDiscovery(t *testing.T) {
	for _, env := range []map[string]string{{"CHANGES_MODE": "live"}, {"CHANGES_MODE": "mock", "REGISTRY_MODE": "mock"}, {"CHANGES_MODE": "mock"}} {
		if _, err := Load(func(k string) string { return env[k] }); err == nil {
			t.Fatal("unsafe changes configuration")
		}
	}
}

func TestObservabilityModeRequiresSyntheticDiscovery(t *testing.T) {
	for _, env := range []map[string]string{{"OBSERVABILITY_MODE": "live"}, {"OBSERVABILITY_MODE": "mock", "REGISTRY_MODE": "mock"}, {"OBSERVABILITY_MODE": "mock"}} {
		if _, err := Load(func(k string) string { return env[k] }); err == nil {
			t.Fatal("unsafe observability configuration")
		}
	}
}

func TestAdminLabIsExplicitAndProductionDefaultsRemainDenied(t *testing.T) {
	env := map[string]string{"LUNAORBIT_RUNTIME_MODE": "admin-lab", "REGISTRY_MODE": "mock", "DISCOVERY_MODE": "readonly-lab", "CHANGES_MODE": "lab", "ENABLE_KITE_WRITES": "true", "ENABLE_KAFKA_WRITES": "true"}
	if _, err := Load(func(k string) string { return env[k] }); err != nil {
		t.Fatal(err)
	}
	env["LUNAORBIT_RUNTIME_MODE"] = "readonly-lab"
	if _, err := Load(func(k string) string { return env[k] }); err == nil {
		t.Fatal("read-only mode accepted writes")
	}
	delete(env, "LUNAORBIT_RUNTIME_MODE")
	if _, err := Load(func(k string) string { return env[k] }); err == nil {
		t.Fatal("implicit write mode accepted")
	}
}
