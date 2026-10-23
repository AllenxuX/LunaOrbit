package workspace

import (
	"context"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/registry"
	"testing"
)

func TestInformationNativeBrokerConfigReadOnly(t *testing.T) {
	s := registry.NewService(registry.NewMemoryStore(), registry.NewMemorySecrets())
	defer s.Close()
	a := registry.Actor{ID: "shared", Role: "information"}
	row, e := s.Create(context.Background(), a, registry.CreateInput{DisplayName: "shared", BootstrapServers: []string{"localhost:9092"}, Enabled: true, CredentialInput: registry.CredentialInput{SecurityProtocol: "PLAINTEXT"}})
	if e != nil {
		t.Fatal(e)
	}
	n := &NativeResources{Native: &Native{Registry: s}}
	r := kafkaadmin.ConfigResource{Type: "BROKER", Name: "1"}
	if _, e := n.profile(context.Background(), a, row.ID, r, false); e != nil {
		t.Fatalf("broker read denied: %v", e)
	}
	if _, e := n.profile(context.Background(), a, row.ID, r, true); e == nil {
		t.Fatal("broker writes permitted")
	}
}
