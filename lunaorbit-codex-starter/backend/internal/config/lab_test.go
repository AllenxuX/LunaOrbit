package config

import "testing"

func TestPersistentLabRequiresDedicatedConnectionAndFixedSessionKey(t *testing.T) {
	values := map[string]string{"LUNAORBIT_RUNTIME_MODE": "admin-lab", "REGISTRY_MODE": "postgres-mock", "DISCOVERY_MODE": "readonly-lab", "DATABASE_URL": "postgresql://synthetic@127.0.0.1/lunaorbit_phase02_test?sslmode=disable", "DEV_SESSION_KEY": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	if _, err := Load(func(k string) string { return values[k] }); err != nil {
		t.Fatal(err)
	}
	delete(values, "DEV_SESSION_KEY")
	if _, err := Load(func(k string) string { return values[k] }); err == nil {
		t.Fatal("persistent lab accepted missing stable session key")
	}
}

func TestLabMustBeExplicitAndReadOnly(t *testing.T) {
	base := map[string]string{"LUNAORBIT_RUNTIME_MODE": "readonly-lab", "REGISTRY_MODE": "mock", "DISCOVERY_MODE": "readonly-lab"}
	load := func(m map[string]string) error { _, e := Load(func(k string) string { return m[k] }); return e }
	if e := load(base); e != nil {
		t.Fatal(e)
	}
	for _, change := range []map[string]string{{"LUNAORBIT_RUNTIME_MODE": ""}, {"REGISTRY_MODE": "postgres-mock"}, {"DISCOVERY_MODE": "mock"}, {"ENABLE_KITE_WRITES": "true"}, {"ENABLE_KAFKA_WRITES": "true"}, {"CHANGES_MODE": "mock"}, {"OBSERVABILITY_MODE": "mock"}, {"HTTP_ADDR": "0.0.0.0:8080"}} {
		values := map[string]string{}
		for k, v := range base {
			values[k] = v
		}
		for k, v := range change {
			values[k] = v
		}
		if load(values) == nil {
			t.Fatal("unsafe lab configuration accepted")
		}
	}
}
