package main

import (
	"context"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/config"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/information"
	"lunaorbit/backend/internal/lineage"
	"lunaorbit/backend/internal/registry"
	"lunaorbit/backend/internal/workspace"
)

// mountInformationPlatform mounts an explicit, small surface without identity,
// grants, account delivery, change requests or infrastructure mutation modules.
func mountInformationPlatform(router *gin.Engine, r *registry.Service, registrations registry.Store, ds discovery.Store, ws workspace.Store, records information.Store, cfg config.Config, env func(string) string) (func(), error) {
	w, err := configureWorkspace(r, ws, cfg)
	if err != nil {
		return func() {}, err
	}
	lookup := func(ctx context.Context, owner string) ([]discovery.Registration, error) {
		rows, e := registrations.List(ctx, owner)
		if e != nil {
			return nil, e
		}
		result := []discovery.Registration{}
		for _, row := range rows {
			result = append(result, discovery.Registration{ID: row.ID, DisplayName: row.DisplayName, BootstrapServers: row.BootstrapServers, Enabled: row.Enabled, DiscoveredClusterID: row.DiscoveredClusterID, IdentityVerified: row.Status == "connected" && row.LastProbeErrorCode == "" && row.LastProbeAt != nil})
		}
		return result, nil
	}
	var d *discovery.Service
	if cfg.DiscoveryMode == "mock" {
		d, err = discovery.NewMockService(ds, lookup)
	} else if cfg.DiscoveryMode == "readonly-lab" {
		d, err = newLabDiscovery(ds, lookup, env)
	}
	if err != nil {
		return func() {}, err
	}
	if d != nil && cfg.DiscoveryMode == "readonly-lab" {
		// Credentials and the endpoint catalog live in memory only: collect once at startup
		// so a restart does not need a manual refresh. Failures are logged, never fatal.
		go d.DiscoverAll(discovery.Actor{ID: cfg.InformationWorkspaceID, Role: "information"})
	}
	close := func() {
		if d != nil {
			d.Close()
		}
	}
	err = registry.MountInformation(router, r, cfg.InformationWorkspaceID, cfg.InformationAllowedHosts, cfg.InformationBrowserOrigins, func(g *gin.RouterGroup, a func(*gin.Context) registry.Actor) {
		workspace.MountInformation(g, w, a)
		lineage.Mount(g, lineage.New(&lineage.Adapter{Registry: r, Discovery: d, Records: records, Workspace: cfg.InformationWorkspaceID}))
		information.Mount(g, records, cfg.InformationWorkspaceID, func(ctx context.Context, id string) bool {
			_, e := r.Get(ctx, registry.Actor{ID: cfg.InformationWorkspaceID, Role: "information"}, id)
			return e == nil
		})
		if d != nil {
			discovery.Mount(g, d, func(c *gin.Context) discovery.Actor { u := a(c); return discovery.Actor{ID: u.ID, Role: u.Role} })
		}
	})
	if err != nil {
		close()
	}
	return close, err
}
