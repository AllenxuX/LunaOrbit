package discovery

import (
	"net"
	"sort"
	"strconv"
	"strings"
)

func addresses(values []string) map[string]bool {
	result := map[string]bool{}
	for _, value := range values {
		host, port, e := net.SplitHostPort(strings.TrimSpace(value))
		if e == nil && host != "" {
			result[net.JoinHostPort(strings.ToLower(host), port)] = true
		}
	}
	return result
}

// Match never confirms an alias merely because a bootstrap address matches.
// Registrations are already owner-filtered by the service, not by display name.
func Match(scope, namespace, uid, alias string, seeds []string, registrations []Registration, bindings []ManualBinding, identities []AliasIdentity) Mapping {
	result := Mapping{Alias: alias, Status: "unknown", Method: "unmatched", Confidence: "low", CandidateIDs: []string{}, BootstrapAddresses: bootstrapAddresses(seeds)}
	byID := map[string]Registration{}
	for _, r := range registrations {
		if r.Enabled {
			byID[r.ID] = r
		}
	}
	manual := ""
	for _, b := range bindings {
		if b.ScopeID == scope && b.Namespace == namespace && b.WorkloadUID == uid && b.Alias == alias && b.ApprovedBy != "" && !b.ApprovedAt.IsZero() {
			if _, ok := byID[b.RegistrationID]; ok {
				manual = b.RegistrationID
			}
		}
	}
	proofID := ""
	proofConflict := false
	for _, proof := range identities {
		if proof.ScopeID == scope && proof.Namespace == namespace && proof.WorkloadUID == uid && proof.Alias == alias && proof.Verified && !proof.Synthetic && proof.ClusterID != "" {
			if proofID != "" && proofID != proof.ClusterID {
				proofConflict = true
			}
			proofID = proof.ClusterID
		}
	}
	if proofConflict {
		result.Method = "identity_conflict"
		return result
	}
	if proofID != "" {
		for _, r := range byID {
			if r.IdentityVerified && r.DiscoveredClusterID == proofID {
				result.CandidateIDs = append(result.CandidateIDs, r.ID)
			}
		}
		sort.Strings(result.CandidateIDs)
		if len(result.CandidateIDs) == 1 {
			result.Status = "confirmed"
			result.Method = "cluster_id"
			result.Confidence = "high"
			result.RegistrationID = result.CandidateIDs[0]
			result.ClusterID = proofID
			return result
		}
		if len(result.CandidateIDs) > 1 {
			for _, id := range result.CandidateIDs {
				if id == manual {
					result.Status = "confirmed"
					result.Method = "manual_cluster_id"
					result.Confidence = "high"
					result.RegistrationID = id
					result.ClusterID = proofID
					return result
				}
			}
			result.Status = "ambiguous"
			result.Method = "cluster_id_ambiguous"
			return result
		}
		result.Method = "cluster_id_unmatched"
		return result
	}
	if manual != "" {
		result.Status = "confirmed"
		result.Method = "manual"
		result.Confidence = "manual"
		result.RegistrationID = manual
		result.CandidateIDs = []string{manual}
		if byID[manual].IdentityVerified {
			result.ClusterID = byID[manual].DiscoveredClusterID
		}
		return result
	}
	requested := addresses(seeds)
	for _, r := range byID {
		for address := range addresses(r.BootstrapServers) {
			if requested[address] {
				result.CandidateIDs = append(result.CandidateIDs, r.ID)
				break
			}
		}
	}
	sort.Strings(result.CandidateIDs)
	if len(result.CandidateIDs) == 1 {
		result.Status = "candidate"
		result.Method = "bootstrap_candidate"
	} else if len(result.CandidateIDs) > 1 {
		result.Status = "ambiguous"
		result.Method = "bootstrap_ambiguous"
	}
	return result
}

func bootstrapFingerprint(values []string) string {
	set := addresses(values)
	sorted := make([]string, 0, len(set))
	for address := range set {
		sorted = append(sorted, address)
	}
	sort.Strings(sorted)
	return identity(sorted...)
}
func conflictingMapping(a, b Mapping) Mapping {
	set := map[string]bool{}
	for _, id := range append(append([]string{}, a.CandidateIDs...), b.CandidateIDs...) {
		set[id] = true
	}
	if a.RegistrationID != "" {
		set[a.RegistrationID] = true
	}
	if b.RegistrationID != "" {
		set[b.RegistrationID] = true
	}
	result := Mapping{Alias: a.Alias, Status: "unknown", Method: "configuration_conflict", Confidence: "low", CandidateIDs: []string{}, BootstrapAddresses: mergeAddresses(a.BootstrapAddresses, b.BootstrapAddresses)}
	for id := range set {
		result.CandidateIDs = append(result.CandidateIDs, id)
	}
	sort.Strings(result.CandidateIDs)
	if len(result.CandidateIDs) > 1 {
		result.Status = "ambiguous"
	}
	return result
}

// bootstrapAddresses returns the configured bootstrap servers as host:port only, in order and de-duplicated
// (case-insensitively). Scheme prefixes, user info/credentials and paths are dropped; anything that does not
// parse as host:port is skipped. It never returns nil, so the JSON field is always a list.
func bootstrapAddresses(values []string) []string {
	result, seen := []string{}, map[string]bool{}
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			item := strings.TrimSpace(part)
			if i := strings.Index(item, "://"); i >= 0 {
				item = item[i+3:]
			}
			if i := strings.LastIndex(item, "@"); i >= 0 {
				item = item[i+1:]
			}
			if i := strings.Index(item, "/"); i >= 0 {
				item = item[:i]
			}
			host, port, e := net.SplitHostPort(item)
			if e != nil || host == "" || strings.ContainsAny(host, " @") {
				continue
			}
			if n, e := strconv.Atoi(port); e != nil || n <= 0 || n > 65535 {
				continue
			}
			address := net.JoinHostPort(host, port)
			if key := strings.ToLower(address); !seen[key] {
				seen[key] = true
				result = append(result, address)
			}
		}
	}
	return result
}

func mergeAddresses(a, b []string) []string {
	return bootstrapAddresses(append(append([]string{}, a...), b...))
}
