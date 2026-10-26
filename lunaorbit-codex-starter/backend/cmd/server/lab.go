package main

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/config"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/discovery/collector"
	"lunaorbit/backend/internal/discovery/resource"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/kite"
	"lunaorbit/backend/internal/registry"
	"strconv"
	"strings"
	"time"
)

func mountRuntime(router *gin.Engine, cfg config.Config) {
	router.GET("/api/runtime", func(c *gin.Context) {
		mode := "mock"
		if cfg.RegistryMode == "disabled" {
			mode = "disabled"
		}
		lab := cfg.RuntimeMode == "readonly-lab" || cfg.RuntimeMode == "admin-lab"
		admin := cfg.RuntimeMode == "admin-lab"
		if lab {
			mode = cfg.RuntimeMode
		}
		out := gin.H{"information_mode": cfg.PlatformMode == "information", "workspace_id": cfg.InformationWorkspaceID, "mode": mode, "synthetic": !lab, "write_enabled": false, "capabilities": gin.H{"discovery": cfg.DiscoveryMode != "disabled", "kafka_reads": cfg.RegistryMode != "disabled", "acl_reads": !lab && cfg.RegistryMode != "disabled", "changes": !lab && cfg.ChangesMode == "mock" && cfg.DiscoveryMode == "mock", "observability": !lab && cfg.ObservabilityMode == "mock" && cfg.DiscoveryMode == "mock"}}
		if lab {
			out["kite_version"] = "v0.16.0"
		}
		if admin {
			out["write_enabled"] = cfg.KiteWrites || cfg.KafkaWrites
			out["capabilities"].(gin.H)["changes"] = cfg.ChangesMode == "lab"
			out["capabilities"].(gin.H)["acl_reads"] = true
		}
		if cfg.PlatformMode == "information" {
			out["mode"] = "information"
			out["write_enabled"] = false
			caps := out["capabilities"].(gin.H)
			caps["acl_reads"] = false
			caps["changes"] = false
			caps["observability"] = false
		}
		c.JSON(200, out)
	})
}

type labAutoConfig struct {
	clusters, excludeClusters map[string]bool
	excludeNS, onlyNS         map[string]bool
	poll                      time.Duration
}

func csvSet(raw string) map[string]bool {
	out := map[string]bool{}
	for _, v := range strings.Split(raw, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out[v] = true
		}
	}
	return out
}

// autoLabConfig reads the optional limits for auto discovery. Manual settings
// only narrow or exclude; none are required.
func autoLabConfig(env func(string) string) labAutoConfig {
	cfg := labAutoConfig{clusters: csvSet(env("KITE_CLUSTERS")), excludeClusters: csvSet(env("KITE_EXCLUDE_CLUSTERS")), onlyNS: csvSet(env("KITE_NAMESPACES")), poll: 45 * time.Second}
	if name := env("KITE_CLUSTER_NAME"); name != "" {
		cfg.clusters[name] = true
	}
	if ns := env("KITE_NAMESPACE"); ns != "" {
		cfg.onlyNS[ns] = true
	}
	if raw := env("KITE_EXCLUDE_NAMESPACES"); raw != "" {
		cfg.excludeNS = csvSet(raw)
	} else {
		cfg.excludeNS = collector.DefaultExcludedNamespaces()
	}
	if n, e := strconv.Atoi(env("KITE_CLUSTER_POLL_SECONDS")); e == nil && n >= 10 && n <= 600 {
		cfg.poll = time.Duration(n) * time.Second
	}
	return cfg
}
func (c labAutoConfig) includeCluster(name string) bool {
	return !c.excludeClusters[name] && (len(c.clusters) == 0 || c.clusters[name])
}

func newLabDiscovery(store discovery.Store, lookup discovery.RegistrationLookup, env func(string) string) (*discovery.Service, error) {
	if env("KITE_LAB_VERSION") != "v0.16.0" {
		return nil, errors.New("invalid lab discovery configuration")
	}
	// Legacy: one fixed cluster and namespace keeps the original single scope.
	legacy := env("KITE_CLUSTER_NAME") != "" && env("KITE_NAMESPACE") != ""
	base := env("KITE_BASE_URL")
	newClient := func() (*kite.HTTPClient, error) {
		return kite.New(kite.Config{BaseURL: base, AllowedOrigins: []string{base}, APIKey: env("KITE_API_KEY"), Timeout: 4 * time.Second})
	}
	client, err := newClient()
	if err != nil {
		return nil, errors.New("invalid lab Kite transport configuration")
	}
	client.Close()
	auto := autoLabConfig(env)
	var scopes []resource.Scope
	if legacy {
		scopes = []resource.Scope{{ID: "readonly-lab", Cluster: env("KITE_CLUSTER_NAME"), Namespaces: []string{env("KITE_NAMESPACE")}}}
	}
	service := discovery.NewService(store, scopes, func(scope resource.Scope, _ string) (collector.Source, func(), error) {
		c, e := newClient()
		if e != nil {
			return nil, func() {}, errors.New("lab transport unavailable")
		}
		if scope.Auto {
			return collector.AutoKiteSource{Client: c, Decoder: kite.LabV016Decoder{}, ExcludeNamespaces: auto.excludeNS, OnlyNamespaces: auto.onlyNS}, c.Close, nil
		}
		return collector.KiteSource{Client: c, Decoder: kite.LabV016Decoder{}}, c.Close, nil
	}, lookup)
	if !legacy {
		service.EnableAutoScopes()
		service.WatchClusters(auto.poll, func(ctx context.Context) ([]discovery.ClusterEntry, error) {
			c, e := newClient()
			if e != nil {
				return nil, e
			}
			defer c.Close()
			rows, e := c.ListClusters(ctx)
			if e != nil {
				return nil, e
			}
			out := []discovery.ClusterEntry{}
			for _, r := range rows {
				if auto.includeCluster(r.Name) {
					out = append(out, discovery.ClusterEntry{Name: r.Name, Error: r.Error})
				}
			}
			return out, nil
		})
	}
	if raw := env("KAFKA_LAB_ALIAS_BINDINGS"); raw != "" {
		if err := service.ConfigureLabBindings(raw); err != nil {
			service.Close()
			return nil, err
		}
	}
	if _, err := configureMM2Credentials(service, env); err != nil {
		service.Close()
		return nil, err
	}
	return service, nil
}

func configureLabKafka(service *registry.Service, env func(string) string) error {
	split := func(key string) []string {
		v := env(key)
		if v == "" {
			return nil
		}
		return strings.Split(v, ",")
	}
	cidrs, hosts := split("KAFKA_LAB_ALLOWED_CIDRS"), split("KAFKA_LAB_ALLOWED_HOSTS")
	if len(cidrs) == 0 || len(hosts) == 0 || len(cidrs) > 16 || len(hosts) > 16 {
		return errors.New("bounded lab Kafka policy required")
	}
	ports := []int{}
	for _, v := range split("KAFKA_LAB_ALLOWED_PORTS") {
		n, e := strconv.Atoi(v)
		if e != nil {
			return errors.New("invalid lab Kafka ports")
		}
		ports = append(ports, n)
	}
	if len(ports) == 0 || len(ports) > 16 {
		return errors.New("bounded lab Kafka ports required")
	}
	policy, e := kafkaadmin.NewNetworkPolicy(kafkaadmin.NetworkPolicyConfig{AllowedCIDRs: cidrs, AllowedHosts: hosts, AllowedPorts: ports, MaxConcurrent: 2, MaxBrokers: 8, Timeout: 4 * time.Second})
	if e != nil {
		return errors.New("invalid lab Kafka policy")
	}
	if e := service.UseReadOnlyKafka(policy); e != nil {
		return e
	}
	if raw := env("KAFKA_LAB_REGISTRATIONS"); raw != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if service.ConfigureLabCatalog(ctx, raw, policy) != nil {
			return errors.New("invalid lab registration catalog")
		}
	}
	return nil
}
