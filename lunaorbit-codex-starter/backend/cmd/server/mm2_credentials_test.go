package main

import (
	"testing"

	"lunaorbit/backend/internal/discovery"
)

func TestMM2CredentialsSwitchDefaultsOff(t *testing.T) {
	for _, v := range []string{"", "false", "0", "yes"} {
		s := discovery.NewService(discovery.NewMemoryStore(), nil, nil, nil)
		env := map[string]string{"KAFKA_MM2_CREDENTIALS": v, "KAFKA_LAB_ALLOWED_CIDRS": "10.0.0.0/8"}
		on, err := configureMM2Credentials(s, func(k string) string { return env[k] })
		s.Close()
		if err != nil || on {
			t.Fatalf("%q must stay off: %v %v", v, on, err)
		}
	}
}

func TestMM2CredentialsSwitchOnNeedsBoundedNetworkPolicy(t *testing.T) {
	s := discovery.NewService(discovery.NewMemoryStore(), nil, nil, nil)
	defer s.Close()
	if _, err := configureMM2Credentials(s, func(k string) string { return map[string]string{"KAFKA_MM2_CREDENTIALS": "true"}[k] }); err == nil {
		t.Fatal("switch on without KAFKA_LAB_ALLOWED_CIDRS must fail startup")
	}
	env := map[string]string{"KAFKA_MM2_CREDENTIALS": "true", "KAFKA_LAB_ALLOWED_CIDRS": "10.0.0.0/8,127.0.0.1/32"}
	on, err := configureMM2Credentials(s, func(k string) string { return env[k] })
	if err != nil || !on {
		t.Fatalf("switch on: %v %v", on, err)
	}
}
