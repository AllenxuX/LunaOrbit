package config

import "testing"

func releaseEnv() map[string]string {
	return map[string]string{"PLATFORM_MODE": "information", "INFORMATION_WORKSPACE_ID": "lunaorbit", "REGISTRY_MODE": "postgres",
		"DATABASE_URL": "postgresql://u:p@lunaorbit-postgres:5432/lunaorbit?sslmode=disable", "HTTP_ADDR": "127.0.0.1:8081"}
}

func TestPostgresRegistryModeLoadsForInformationPlatform(t *testing.T) {
	env := releaseEnv()
	c, e := Load(func(k string) string { return env[k] })
	if e != nil || c.RegistryMode != "postgres" || c.DatabaseURL == "" || !c.PersistentRegistry() {
		t.Fatalf("%v %+v", e, c.RegistryMode)
	}
}

func TestPostgresRegistryModeRequiresDatabaseURLAndInformationPlatform(t *testing.T) {
	env := releaseEnv()
	delete(env, "DATABASE_URL")
	if _, e := Load(func(k string) string { return env[k] }); e == nil {
		t.Fatal("missing DATABASE_URL accepted")
	}
	env = releaseEnv()
	env["PLATFORM_MODE"] = ""
	if _, e := Load(func(k string) string { return env[k] }); e == nil {
		t.Fatal("postgres mode outside information platform accepted")
	}
}
