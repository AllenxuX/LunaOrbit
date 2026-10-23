package discovery

import (
	"encoding/json"
	"errors"
	"io"
	"lunaorbit/backend/internal/discovery/resource"
	"strings"
	"time"
)

// LabBinding is a trusted deployment configuration, never a browser-supplied
// claim. Matching addresses alone continue to be candidates everywhere else.
type LabBinding struct {
	Namespace    string `json:"namespace"`
	WorkloadName string `json:"workload_name"`
	Alias        string `json:"alias"`
	Bootstrap    string `json:"bootstrap"`
}

func (s *Service) ConfigureLabBindings(raw string) error {
	if len(raw) > 16384 {
		return errors.New("invalid lab bindings")
	}
	var rows []LabBinding
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&rows) != nil || d.Decode(new(any)) != io.EOF || len(rows) < 2 || len(rows) > 16 {
		return errors.New("invalid lab bindings")
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if !safeName.MatchString(row.Namespace) || !safeName.MatchString(row.WorkloadName) || !safeName.MatchString(row.Alias) || len(addresses([]string{row.Bootstrap})) != 1 {
			return errors.New("invalid lab bindings")
		}
		allowed := false
		for _, scope := range s.scopes {
			for _, ns := range scope.Namespaces {
				if !scope.Synthetic && ns == row.Namespace {
					allowed = true
				}
			}
		}
		key := row.Namespace + "/" + row.WorkloadName + "/" + row.Alias
		if !allowed || seen[key] {
			return errors.New("invalid lab bindings")
		}
		seen[key] = true
	}
	s.labBindings = rows
	return nil
}

func (s *Service) resolveLabBindings(col resource.Collection, registrations []Registration, bindings []ManualBinding) []ManualBinding {
	if len(s.labBindings) == 0 || col.Scope.Synthetic || col.ErrorCode != "" {
		return bindings
	}
	// Build the observed references first; never invent a workload or alias.
	preview, err := Build(col, registrations, bindings, nil, Snapshot{})
	if err != nil {
		return bindings
	}
	for _, link := range preview.Links {
		if link.Stale || !link.Observed {
			continue
		}
		for _, entry := range s.labBindings {
			if entry.Namespace != link.Namespace || entry.WorkloadName != link.WorkloadName || (entry.Alias != link.SourceAlias && entry.Alias != link.TargetAlias) {
				continue
			}
			exists := false
			for _, b := range bindings {
				if b.Namespace == entry.Namespace && b.WorkloadUID == link.WorkloadUID && b.Alias == entry.Alias {
					exists = true
				}
			}
			if exists {
				continue
			}
			matches := []Registration{}
			mapping := link.Source
			if entry.Alias == link.TargetAlias {
				mapping = link.Target
			}
			for _, r := range registrations {
				if !r.Enabled || !r.IdentityVerified || r.DiscoveredClusterID == "" {
					continue
				}
				candidate := false
				for _, id := range mapping.CandidateIDs {
					if id == r.ID {
						candidate = true
					}
				}
				if candidate && len(r.BootstrapServers) == 1 && addresses(r.BootstrapServers)[entry.Bootstrap] {
					matches = append(matches, r)
				}
			}
			if len(matches) == 1 {
				bindings = append(bindings, ManualBinding{ScopeID: col.Scope.ID, Namespace: entry.Namespace, WorkloadUID: link.WorkloadUID, Alias: entry.Alias, RegistrationID: matches[0].ID, ApprovedBy: "server-lab-config", ApprovedAt: time.Now().UTC()})
			}
		}
	}
	return bindings
}
