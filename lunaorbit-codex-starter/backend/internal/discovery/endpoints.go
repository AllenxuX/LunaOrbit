package discovery

import (
	"lunaorbit/backend/internal/discovery/mm2auth"
	"lunaorbit/backend/internal/discovery/parser"
	"lunaorbit/backend/internal/discovery/resource"
	"net"
	"sort"
	"strings"
)

// Access states of an auto-discovered endpoint, before any connection attempt.
const (
	// AccessCredentialsOff: KAFKA_MM2_CREDENTIALS is off, or the config's credentials are unusable.
	AccessCredentialsOff = "credentials_unavailable"
	// AccessUnresolvable: no address could be parsed for the alias.
	AccessUnresolvable = "address_unresolvable"
	// AccessPending: credentials (or none needed) are ready; not yet connected.
	AccessPending = "not_connected"
)

// CredentialSourceMM2 is the only credential wording ever returned to clients.
const CredentialSourceMM2 = "MM2 配置（只读）"

// AutoEndpoint is one Kafka cluster as seen by one MM2 alias. It carries no
// credential material: only the source label when the switch is on.
type AutoEndpoint struct {
	Key              string   `json:"key"`
	ScopeID          string   `json:"scope_id"`
	KubeCluster      string   `json:"k8s_cluster,omitempty"`
	Namespace        string   `json:"namespace"`
	WorkloadUID      string   `json:"workload_uid"`
	WorkloadName     string   `json:"workload_name"`
	Alias            string   `json:"alias"`
	Addresses        []string `json:"addresses"`
	Resolved         []string `json:"resolved"`
	Access           string   `json:"access"`
	CredentialSource string   `json:"credential_source,omitempty"`
	// HostIPs is the MM2 Pod's hostAliases map (host -> ip), used only by the dialer.
	HostIPs map[string]string `json:"-"`
}

// EndpointKey identifies an MM2 alias within one workload of one scope.
func EndpointKey(scope, namespace, workloadUID, alias string) string {
	return strings.Join([]string{scope, namespace, workloadUID, alias}, "|")
}

// extractEndpoints lists one endpoint per MM2 alias in the collection. When creds is
// non-nil (KAFKA_MM2_CREDENTIALS on) the alias credentials are parsed into memory;
// otherwise credential keys are never read.
func extractEndpoints(col resource.Collection, creds *mm2auth.Store) []AutoEndpoint {
	if creds != nil {
		creds.ForgetPrefix(col.Scope.ID + "|")
	}
	var out []AutoEndpoint
	for _, batch := range col.Namespaces {
		configs := map[string]resource.ConfigMap{}
		for _, cm := range batch.ConfigMaps {
			if metadataOK(cm.Metadata, batch.Namespace) {
				configs[cm.Metadata.Name] = cm
			}
		}
		for _, dep := range batch.Deployments {
			if !metadataOK(dep.Metadata, batch.Namespace) {
				continue
			}
			hosts := map[string]string{}
			for _, h := range dep.Spec.Template.Spec.HostAliases {
				if net.ParseIP(h.IP) == nil {
					continue
				}
				for _, name := range h.Hostnames {
					hosts[strings.ToLower(name)] = h.IP
				}
			}
			seen := map[string]bool{}
			for _, ref := range references(dep) {
				cm, ok := configs[ref.name]
				if !ok {
					continue
				}
				keys := make([]string, 0, len(cm.Data))
				for key := range cm.Data {
					if supportedFile(key) && (ref.key == "" || ref.key == key) {
						keys = append(keys, key)
					}
				}
				sort.Strings(keys)
				for _, key := range keys {
					config, err := parser.Parse(key, cm.Data[key])
					if err != nil || len(config.Rules) == 0 {
						continue
					}
					var aliasCreds map[string]mm2auth.Credential
					if creds != nil {
						aliasCreds, _ = mm2auth.Parse(key, cm.Data[key])
					}
					for _, alias := range config.Aliases {
						epKey := EndpointKey(col.Scope.ID, batch.Namespace, dep.Metadata.UID, alias)
						if seen[epKey] {
							continue
						}
						seen[epKey] = true
						ep := AutoEndpoint{Key: epKey, ScopeID: col.Scope.ID, KubeCluster: col.Scope.Cluster, Namespace: batch.Namespace, WorkloadUID: dep.Metadata.UID, WorkloadName: dep.Metadata.Name, Alias: alias, Addresses: append([]string{}, config.Bootstraps[alias]...), Resolved: []string{}, HostIPs: hosts}
						for _, a := range ep.Addresses {
							host, port, e := net.SplitHostPort(a)
							if e != nil {
								continue
							}
							if ip, ok := hosts[host]; ok {
								ep.Resolved = append(ep.Resolved, net.JoinHostPort(ip, port))
							} else if net.ParseIP(host) != nil {
								ep.Resolved = append(ep.Resolved, a)
							}
						}
						switch {
						case len(ep.Addresses) == 0:
							ep.Access = AccessUnresolvable
						case creds == nil:
							ep.Access = AccessCredentialsOff
						default:
							c := aliasCreds[alias]
							if c.Usable() {
								creds.Put(epKey, c)
								ep.Access = AccessPending
								ep.CredentialSource = CredentialSourceMM2
							} else {
								ep.Access = AccessCredentialsOff
							}
						}
						out = append(out, ep)
					}
				}
			}
		}
	}
	return out
}
