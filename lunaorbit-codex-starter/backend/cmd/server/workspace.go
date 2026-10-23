package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"lunaorbit/backend/internal/config"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"lunaorbit/backend/internal/workspace"
	"net"
	"os"
	"time"
)

func openRegistryDatabase(cfg config.Config) (*sql.DB, error) {
	deadline := time.Now().Add(20 * time.Second)
	for {
		db, err := registry.OpenDevelopmentDatabase(cfg.DatabaseURL)
		if err == nil {
			return db, nil
		}
		if (cfg.RuntimeMode != "admin-lab" && cfg.RuntimeMode != "readonly-lab") || time.Now().After(deadline) {
			return nil, errors.New("development database unavailable")
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func configureWorkspace(r *registry.Service, store workspace.Store, cfg config.Config) (*workspace.Service, error) {
	mock := workspace.NewMock()
	var gateway workspace.Gateway = mock
	native := cfg.RuntimeMode == "admin-lab" || cfg.RuntimeMode == "readonly-lab"
	if !native {
		r.OperationsFactory = func(row registry.Registration, scenario string) kafkaops.Reader {
			return mock.BindReader(row.ID, scenario)
		}
	}
	if native {
		factory := r.NativeKafkaFactory()
		if factory == nil {
			return nil, errors.New("bounded native Kafka factory required")
		}
		gateway = &workspace.Native{Registry: r, Factory: factory}
	}
	service := workspace.NewService(r, store, gateway)
	if n, ok := gateway.(*workspace.Native); ok {
		service.Resources = &workspace.NativeResources{Native: n}
	} else {
		service.Resources = workspace.NewMockResources(mock)
	}
	if cfg.PlatformMode == "information" {
		return service, nil
	}
	if cfg.AccountMasterKey != "" {
		service.AccountKey, _ = hex.DecodeString(cfg.AccountMasterKey)
	}
	if !native && cfg.RegistryMode != "postgres-mock" && len(service.AccountKey) == 0 {
		service.AccountKey = make([]byte, 32)
		if _, err := rand.Read(service.AccountKey); err != nil {
			return nil, err
		}
	}
	if native {
		endpoints := map[string]string{}
		if raw := os.Getenv("ACCOUNT_SCRAM_ENDPOINTS"); raw != "" {
			if len(raw) > 8192 || json.Unmarshal([]byte(raw), &endpoints) != nil || len(endpoints) > 8 {
				return nil, errors.New("invalid ACCOUNT_SCRAM_ENDPOINTS")
			}
			for source, target := range endpoints {
				sh, _, se := net.SplitHostPort(source)
				th, _, te := net.SplitHostPort(target)
				if se != nil || te != nil || sh != th {
					return nil, errors.New("invalid ACCOUNT_SCRAM_ENDPOINTS")
				}
			}
		}
		service.AccountGateway = &nativeAccounts{registry: r, saslEndpoints: endpoints}
	} else {
		service.AccountGateway = &mockAccounts{users: map[string]map[string]string{}}
	}
	service.LabAutoApprove = cfg.RuntimeMode == "admin-lab" && cfg.KafkaWrites
	service.WriteEnabled = func(ctx context.Context, a registry.Actor, id string) error {
		if !native {
			return r.AuthorizeResource(ctx, a, id, "read", "")
		}
		if cfg.RuntimeMode != "admin-lab" || !cfg.KafkaWrites {
			return errors.New("DISABLED")
		}
		// This checks the server-configured lab target only. Workspace authorization
		// and audit continue to use the real actor and each concrete resource scope.
		return r.AuthorizeConfiguredLabTarget(ctx, a, id)
	}
	return service, nil
}
