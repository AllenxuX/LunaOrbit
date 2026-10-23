// mm2discover demonstrates Phase 03 with synthetic data and local GETs only.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/registry"
	"os"
	"time"
)

func main() {
	stale := flag.Bool("stale-demo", false, "show retained evidence after a failed synthetic collection")
	flag.Parse()
	if e := run(context.Background(), os.Stdout, *stale); e != nil {
		_ = json.NewEncoder(os.Stderr).Encode(map[string]string{"error": discovery.CodeOf(e)})
		os.Exit(1)
	}
}
func run(ctx context.Context, out io.Writer, stale bool) error {
	registryStore := registry.NewMemoryStore()
	registryService := registry.NewService(registryStore, registry.NewMemorySecrets())
	defer registryService.Close()
	actor := registry.Actor{ID: "synthetic-cli-owner", Role: "admin"}
	for alias, host := range map[string]string{"mars": "mars.synthetic.invalid", "charon": "charon.synthetic.invalid", "moon-prod": "moon.synthetic.invalid", "styx": "styx.synthetic.invalid", "saturn-uat": "saturn.synthetic.invalid"} {
		if _, e := registryService.Create(ctx, actor, registry.CreateInput{DisplayName: alias, BootstrapServers: []string{host + ":9092"}, Enabled: true, CredentialInput: registry.CredentialInput{SecurityProtocol: "PLAINTEXT"}}); e != nil {
			return e
		}
	}
	lookup := func(ctx context.Context, owner string) ([]discovery.Registration, error) {
		rows, e := registryStore.List(ctx, owner)
		if e != nil {
			return nil, e
		}
		result := []discovery.Registration{}
		for _, r := range rows {
			result = append(result, discovery.Registration{ID: r.ID, DisplayName: r.DisplayName, BootstrapServers: r.BootstrapServers, Enabled: r.Enabled})
		}
		return result, nil
	}
	store := discovery.NewMemoryStore()
	service, e := discovery.NewMockService(store, lookup)
	if e != nil {
		return e
	}
	scopes, e := service.Scopes(discovery.Actor{ID: actor.ID, Role: actor.Role})
	if e != nil {
		service.Close()
		return e
	}
	scope := scopes[0].ID
	if _, e = service.Start(ctx, discovery.Actor{ID: actor.ID, Role: actor.Role}, scope, "success"); e != nil {
		service.Close()
		return e
	}
	if e = wait(ctx, service, actor.ID, scope); e != nil {
		service.Close()
		return e
	}
	service.Close()
	if stale {
		service, e = discovery.NewMockService(store, lookup)
		if e != nil {
			return e
		}
		defer service.Close()
		if _, e = service.Start(ctx, discovery.Actor{ID: actor.ID, Role: actor.Role}, scope, "partial_failure"); e != nil {
			return e
		}
		if e = wait(ctx, service, actor.ID, scope); e != nil {
			return e
		}
	}
	snapshot, e := service.Snapshot(ctx, discovery.Actor{ID: actor.ID, Role: actor.Role}, scope)
	if e != nil {
		return e
	}
	if e = json.NewEncoder(out).Encode(snapshot); e != nil {
		return &discovery.Error{Code: "OUTPUT_ERROR"}
	}
	return nil
}
func wait(ctx context.Context, service *discovery.Service, owner, scope string) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()
	for {
		job, e := service.Latest(ctx, discovery.Actor{ID: owner, Role: "admin"}, scope)
		if e != nil {
			return e
		}
		if job.Status == "done" {
			if job.ErrorCode != "" && job.ErrorCode != "PARTIAL_COLLECTION" {
				return &discovery.Error{Code: job.ErrorCode}
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return &discovery.Error{Code: "TIMEOUT"}
		case <-ticker.C:
		}
	}
}
