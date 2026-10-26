package discovery

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"sort"
	"strings"
	"time"
)

// Catalog sources (Endpoint Catalog, see ARCHITECTURE.md).
const (
	SourceAuto   = "auto"   // only seen in MM2 configuration
	SourceMerged = "merged" // auto endpoint with one matching registration's business fields
	SourceManual = "manual" // registration not covered by any MM2 configuration ("手工补充")
)

// Access states after a connection attempt.
const (
	AccessReadable = "readable"
	AccessFailed   = "connect_failed"
)

// ProbeResult is the outcome of one read-only connection attempt for an auto endpoint.
// Reason is a fixed category (timeout, refused, tls, auth, authorization, unresolvable), never server text.
type ProbeResult struct {
	Access    string    `json:"access"`
	Reason    string    `json:"reason,omitempty"`
	ClusterID string    `json:"cluster_id,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
}

// CatalogEndpoint is one logical Kafka cluster: every MM2 alias that points at it,
// merged with at most one registration. It carries no credential material.
type CatalogEndpoint struct {
	ID               string     `json:"id"`
	Source           string     `json:"source"`
	Aliases          []string   `json:"aliases"`
	Members          []string   `json:"members"`
	Addresses        []string   `json:"addresses"`
	Resolved         []string   `json:"resolved"`
	ClusterID        string     `json:"cluster_id,omitempty"`
	RegistrationID   string     `json:"registration_id,omitempty"`
	DisplayName      string     `json:"display_name,omitempty"`
	Room             string     `json:"room,omitempty"`
	Environment      string     `json:"environment,omitempty"`
	Ambiguous        []string   `json:"ambiguous,omitempty"`
	Access           string     `json:"access"`
	AccessReason     string     `json:"access_reason,omitempty"`
	CheckedAt        *time.Time `json:"checked_at,omitempty"`
	CredentialSource string     `json:"credential_source,omitempty"`
	// LineageID is the cluster id to use for Topic lineage queries (registration or endpoint id).
	LineageID string `json:"lineage_id"`
	// ReadVia tells readers which path to use: the auto endpoint, or the registration fallback.
	ReadVia string `json:"read_via"`
}

func normAddr(a string) string {
	host, port, e := net.SplitHostPort(strings.TrimSpace(a))
	if e != nil {
		return ""
	}
	return net.JoinHostPort(strings.ToLower(host), port)
}

func addrSet(lists ...[]string) map[string]bool {
	out := map[string]bool{}
	for _, l := range lists {
		for _, a := range l {
			if n := normAddr(a); n != "" {
				out[n] = true
			}
		}
	}
	return out
}

func overlaps(a, b map[string]bool) bool {
	for k := range a {
		if b[k] {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// BuildCatalog merges auto endpoints into logical clusters (Cluster ID first, then any
// shared bootstrap address; aliases alone never merge), attaches the single matching
// registration's business fields, and lists unmatched registrations as manual supplements.
func BuildCatalog(eps []AutoEndpoint, regs []Registration, probes map[string]ProbeResult) []CatalogEndpoint {
	parent := make([]int, len(eps))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(i int) int {
		for parent[i] != i {
			parent[i] = parent[parent[i]]
			i = parent[i]
		}
		return i
	}
	union := func(a, b int) { parent[find(a)] = find(b) }
	sets := make([]map[string]bool, len(eps))
	for i, e := range eps {
		sets[i] = addrSet(e.Addresses, e.Resolved)
	}
	for i := range eps {
		for j := i + 1; j < len(eps); j++ {
			ci, cj := probes[eps[i].Key].ClusterID, probes[eps[j].Key].ClusterID
			if (ci != "" && ci == cj) || ((ci == "" || cj == "" || ci == cj) && overlaps(sets[i], sets[j])) {
				union(i, j)
			}
		}
	}
	groups := map[int][]int{}
	for i := range eps {
		groups[find(i)] = append(groups[find(i)], i)
	}
	used := map[string]bool{}
	var out []CatalogEndpoint
	for _, idx := range groups {
		row := CatalogEndpoint{Source: SourceAuto, Access: AccessCredentialsOff}
		aliases, addrs, resolved := map[string]bool{}, map[string]bool{}, map[string]bool{}
		var checked *time.Time
		best := ""
		for _, i := range idx {
			e := eps[i]
			row.Members = append(row.Members, e.Key)
			aliases[e.Alias] = true
			for _, a := range e.Addresses {
				addrs[normAddr(a)] = true
			}
			for _, a := range e.Resolved {
				resolved[normAddr(a)] = true
			}
			if e.CredentialSource != "" {
				row.CredentialSource = e.CredentialSource
			}
			state, reason := e.Access, ""
			if p, ok := probes[e.Key]; ok {
				state, reason = p.Access, p.Reason
				if p.ClusterID != "" {
					row.ClusterID = p.ClusterID
				}
				if !p.CheckedAt.IsZero() && (checked == nil || p.CheckedAt.After(*checked)) {
					t := p.CheckedAt
					checked = &t
				}
			}
			if rank(state) > rank(best) {
				best, row.AccessReason = state, reason
			}
		}
		delete(addrs, "")
		delete(resolved, "")
		row.Access, row.CheckedAt = best, checked
		row.Aliases, row.Addresses, row.Resolved = sortedKeys(aliases), sortedKeys(addrs), sortedKeys(resolved)
		sort.Strings(row.Members)
		var matches []Registration
		all := addrSet(row.Addresses, row.Resolved)
		for _, r := range regs {
			if !r.Enabled {
				continue
			}
			if (row.ClusterID != "" && r.DiscoveredClusterID == row.ClusterID) || overlaps(all, addrSet(r.BootstrapServers)) {
				matches = append(matches, r)
			}
		}
		for _, r := range matches {
			used[r.ID] = true
		}
		if len(matches) == 1 {
			r := matches[0]
			row.Source, row.RegistrationID, row.DisplayName, row.Room, row.Environment = SourceMerged, r.ID, r.DisplayName, r.Room, r.Environment
		} else if len(matches) > 1 {
			for _, r := range matches {
				row.Ambiguous = append(row.Ambiguous, r.ID)
			}
			sort.Strings(row.Ambiguous)
		}
		row.LineageID = LineageEndpointID(row.Addresses)
		if row.RegistrationID != "" {
			row.LineageID = row.RegistrationID
		}
		row.ReadVia = "auto"
		if row.Access != AccessReadable && row.RegistrationID != "" {
			row.ReadVia = "registration"
		}
		sum := sha256.Sum256([]byte(strings.Join(row.Members, "\n")))
		row.ID = "auto:" + hex.EncodeToString(sum[:8])
		out = append(out, row)
	}
	for _, r := range regs {
		if !r.Enabled || used[r.ID] {
			continue
		}
		out = append(out, CatalogEndpoint{ID: "reg:" + r.ID, Source: SourceManual, Aliases: []string{}, Members: []string{}, Addresses: sortedKeys(addrSet(r.BootstrapServers)), Resolved: []string{}, ClusterID: r.DiscoveredClusterID, RegistrationID: r.ID, DisplayName: r.DisplayName, Room: r.Room, Environment: r.Environment, Access: "registered", ReadVia: "registration", LineageID: r.ID})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// rank orders states so a cluster reports its best-known access across member aliases.
func rank(state string) int {
	switch state {
	case AccessReadable:
		return 5
	case AccessPending:
		return 4
	case AccessFailed:
		return 3
	case AccessCredentialsOff:
		return 2
	case AccessUnresolvable:
		return 1
	}
	return 0
}

// LineageEndpointID mirrors lineage.EndpointID: the smallest bootstrap address names the cluster.
func LineageEndpointID(addresses []string) string {
	best := ""
	for _, a := range addresses {
		a = strings.ToLower(strings.TrimSpace(a))
		if a != "" && (best == "" || a < best) {
			best = a
		}
	}
	if best == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(best))
	return "endpoint-" + hex.EncodeToString(sum[:8])
}
