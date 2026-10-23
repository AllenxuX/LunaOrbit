package main

import (
	"context"
	"lunaorbit/backend/internal/config"
	"lunaorbit/backend/internal/registry"
	"lunaorbit/backend/internal/workspace"
	"testing"
)

func TestWorkspaceDefaultMockDoesNotInstallNativeOrImplicitSelfApproval(t *testing.T) {
	r := registry.NewService(registry.NewMemoryStore(), registry.NewMemorySecrets())
	defer r.Close()
	s, err := configureWorkspace(r, workspace.NewMemoryStore(), config.Config{RegistryMode: "mock"})
	if err != nil || !s.Gateway.Synthetic() || s.LabAutoApprove {
		t.Fatal("unsafe default workspace configuration")
	}
	if s.WriteEnabled(context.Background(), registry.Actor{ID: "outside", Role: "admin"}, "missing") == nil {
		t.Fatal("unscoped mock write allowed")
	}
}

func TestWorkspaceNativeCannotStartWithoutBoundedFactory(t *testing.T) {
	r := registry.NewService(registry.NewMemoryStore(), registry.NewMemorySecrets())
	defer r.Close()
	for _, mode := range []string{"readonly-lab", "admin-lab"} {
		if _, err := configureWorkspace(r, workspace.NewMemoryStore(), config.Config{RuntimeMode: mode, KafkaWrites: true}); err == nil {
			t.Fatal("native workspace accepted unbounded factory")
		}
	}
}
