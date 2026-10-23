package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"log/slog"
	"lunaorbit/backend/internal/access"
	"lunaorbit/backend/internal/changes"
	"lunaorbit/backend/internal/config"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/httpapi"
	"lunaorbit/backend/internal/information"
	"lunaorbit/backend/internal/observability"
	"lunaorbit/backend/internal/registry"
	"lunaorbit/backend/internal/topology"
	"lunaorbit/backend/internal/workspace"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("server_failed", "reason", err.Error())
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.LogLevel))
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	router := httpapi.NewRouter(logger)
	mountRuntime(router, cfg)
	if cfg.RegistryMode != "disabled" {
		var store registry.Store = registry.NewMemoryStore()
		var discoveryStore discovery.Store = discovery.NewMemoryStore()
		var topologyStore topology.Store = topology.NewMemoryStore()
		var accessStore access.Store = access.NewMemoryStore()
		var changeStore changes.Store = changes.NewMemoryStore()
		var observationStore observability.Store = observability.NewMemoryStore()
		var workspaceStore workspace.Store = workspace.NewMemoryStore()
		var informationStore information.Store = information.NewMemoryStore()
		if cfg.RegistryMode == "postgres-mock" {
			db, err := openRegistryDatabase(cfg)
			if err != nil {
				return errors.New("development database unavailable")
			}
			defer db.Close()
			store = registry.NewPostgresStore(db)
			discoveryStore = discovery.NewPostgresStore(db)
			topologyStore = topology.NewPostgresStore(db)
			accessStore = access.NewPostgresStore(db)
			changeStore = changes.NewPostgresStore(db)
			observationStore = observability.NewPostgresStore(db)
			workspaceStore = workspace.NewPostgresStore(db)
			informationStore = information.NewPostgresStore(db)
		}
		service := registry.NewService(store, registry.NewMemorySecrets())
		service.BrowserOrigins = []string{cfg.DevBrowserOrigin}
		defer service.Close()
		if cfg.RuntimeMode == "readonly-lab" || cfg.RuntimeMode == "admin-lab" {
			if err := configureLabKafka(service, os.Getenv); err != nil {
				return err
			}
		}
		if cfg.PlatformMode == "information" {
			closeInformation, err := mountInformationPlatform(router, service, store, discoveryStore, workspaceStore, informationStore, cfg, os.Getenv)
			if err != nil {
				return err
			}
			defer closeInformation()
		} else {
			if err := mountIdentity(router, service, cfg.DevBrowserOrigin, os.Getenv); err != nil {
				return err
			}
			var approvalScopes map[string][]string
			if raw := os.Getenv("ACL_APPROVAL_SCOPES"); raw != "" {
				if len(raw) > 65536 || json.Unmarshal([]byte(raw), &approvalScopes) != nil {
					return errors.New("invalid approval configuration")
				}
			}
			aclService := access.NewService(service, accessStore)
			aclService.ApprovalAuthorizer = access.StaticApprovals(approvalScopes)
			if cfg.RuntimeMode == "admin-lab" {
				if err := configureLabAccess(aclService, service, os.Getenv); err != nil {
					return err
				}
			}
			modules := []registry.Extension{func(group *gin.RouterGroup, resolve func(*gin.Context) registry.Actor) {
				if cfg.RuntimeMode != "readonly-lab" {
					access.Mount(group, aclService, resolve)
				}
			}}
			workspaceService, err := configureWorkspace(service, workspaceStore, cfg)
			if err != nil {
				return err
			}
			modules = append(modules, func(group *gin.RouterGroup, resolve func(*gin.Context) registry.Actor) {
				workspace.Mount(group, workspaceService, resolve)
			})
			if cfg.DiscoveryMode == "mock" || cfg.DiscoveryMode == "readonly-lab" {
				lookup := func(ctx context.Context, owner string) ([]discovery.Registration, error) {
					rows, e := store.List(ctx, owner)
					if e != nil {
						return nil, e
					}
					result := make([]discovery.Registration, 0, len(rows))
					for _, row := range rows {
						result = append(result, discovery.Registration{ID: row.ID, DisplayName: row.DisplayName, BootstrapServers: row.BootstrapServers, Enabled: row.Enabled, DiscoveredClusterID: row.DiscoveredClusterID, IdentityVerified: row.Status == "connected" && row.LastProbeErrorCode == "" && row.LastProbeAt != nil})
					}
					return result, nil
				}
				discoveryService, e := discovery.NewMockService(discoveryStore, lookup)
				if cfg.DiscoveryMode == "readonly-lab" && e == nil {
					discoveryService.Close()
					discoveryService, e = newLabDiscovery(discoveryStore, lookup, os.Getenv)
				}
				if e != nil {
					return e
				}
				defer discoveryService.Close()
				observationService := observability.NewService(discoveryService, observability.NewMockProvider(time.Minute), observationStore, observability.ServiceConfig{TTL: time.Minute})
				changeService, e := configureChanges(changeStore, discoveryService, cfg.RegistryMode == "postgres-mock", os.Getenv)
				if e != nil {
					return e
				}
				modules = append(modules, func(group *gin.RouterGroup, resolve func(*gin.Context) registry.Actor) {
					resolveDiscovery := func(c *gin.Context) discovery.Actor {
						actor := resolve(c)
						return discovery.Actor{ID: actor.ID, Role: actor.Role}
					}
					discovery.Mount(group, discoveryService, resolveDiscovery)
					if changeService != nil {
						changes.Mount(group, changeService, resolve)
					}
					topology.Mount(group, topologyStore, discoveryService, resolveDiscovery)
					if os.Getenv("OBSERVABILITY_MODE") == "mock" {
						observability.Mount(group, observationService, func(c *gin.Context) observability.Actor {
							a := resolve(c)
							return observability.Actor{ID: a.ID, Role: a.Role}
						})
					}
				})
			}
			var key []byte
			if cfg.RegistryMode == "postgres-mock" {
				key, _ = hex.DecodeString(cfg.DevSessionKey)
			}
			if err := registry.MountModules(router, service, cfg.RegistryMode == "postgres-mock", key, modules...); err != nil {
				return err
			}
		}
	}
	server := &http.Server{Addr: cfg.Address, Handler: router, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	logger.Info("server_starting", "address", cfg.Address, "mode", "foundation")
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("HTTP listener failed")
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}
