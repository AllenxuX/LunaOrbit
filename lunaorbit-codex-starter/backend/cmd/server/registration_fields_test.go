package main

import (
	"encoding/json"
	"strings"
	"testing"

	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/registry"
)

func TestDiscoveryRegistrationCarriesBusinessFieldsOnly(t *testing.T) {
	row := registry.Registration{ID: "r1", DisplayName: "luna-k1", Room: "bj-1", Environment: "prod", Description: "交易主集群", BootstrapServers: []string{"k1:9092"}, Enabled: true,
		SecurityProtocol: "SASL_SSL", SASLMechanism: "SCRAM-SHA-512", UsernameMasked: "ad***", UsernameRef: "ref-user", PasswordRef: "ref-canary-pass"}
	got := discoveryRegistration(row)
	if got.Room != "bj-1" || got.Environment != "prod" || got.Description != "交易主集群" {
		t.Fatalf("%+v", got)
	}
	cat := discovery.BuildCatalog([]discovery.AutoEndpoint{{Key: "k", Alias: "k1", Addresses: []string{"k1:9092"}, Resolved: []string{}, Access: discovery.AccessCredentialsOff}}, []discovery.Registration{got}, nil)
	raw, _ := json.Marshal(cat)
	for _, want := range []string{`"room":"bj-1"`, `"environment":"prod"`, `"description":"交易主集群"`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("missing %s in %s", want, raw)
		}
	}
	for _, leak := range []string{"SCRAM", "SASL_SSL", "ad***", "ref-user", "ref-canary-pass"} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("leaked %s", leak)
		}
	}
	plain := discovery.BuildCatalog([]discovery.AutoEndpoint{{Key: "k", Alias: "k9", Addresses: []string{"k9:9092"}, Resolved: []string{}}}, nil, nil)
	if plain[0].Room != "" || plain[0].Environment != "" || plain[0].Description != "" {
		t.Fatalf("no registration must leave fields empty: %+v", plain[0])
	}
}
