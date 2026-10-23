package config

import "testing"

func TestInformationRequiresExplicitNamespace(t *testing.T) {
	env := map[string]string{"PLATFORM_MODE": "information", "REGISTRY_MODE": "mock"}
	if _, e := Load(func(k string) string { return env[k] }); e == nil {
		t.Fatal("information startup accepted missing workspace")
	}
	env["INFORMATION_WORKSPACE_ID"] = "shared-lab"
	env["HTTP_ADDR"] = "0.0.0.0:8080"
	env["INFORMATION_ALLOWED_HOSTS"] = "luna.internal:8080"
	if _, e := Load(func(k string) string { return env[k] }); e != nil {
		t.Fatal(e)
	}
}
func TestInformationRejectsUnsafeConfiguration(t *testing.T) {
	for key, value := range map[string]string{"INFORMATION_ALLOWED_HOSTS": "*", "INFORMATION_BROWSER_ORIGINS": "http://*.internal", "ENABLE_KAFKA_WRITES": "true", "ENABLE_KITE_WRITES": "true", "CHANGES_MODE": "mock"} {
		t.Run(key, func(t *testing.T) {
			env := map[string]string{"PLATFORM_MODE": "information", "INFORMATION_WORKSPACE_ID": "shared", "REGISTRY_MODE": "mock"}
			env[key] = value
			if _, e := Load(func(k string) string { return env[k] }); e == nil {
				t.Fatalf("accepted %s", key)
			}
		})
	}
}
func TestInformationDoesNotRequireIdentityOrSessionKey(t *testing.T) {
	env := map[string]string{"PLATFORM_MODE": "information", "INFORMATION_WORKSPACE_ID": "shared", "REGISTRY_MODE": "postgres-mock", "DATABASE_URL": "postgres://example", "IDENTITY_MODE": "retired-identity", "DEV_SESSION_KEY": "retired"}
	if _, e := Load(func(k string) string { return env[k] }); e != nil {
		t.Fatal(e)
	}
}
