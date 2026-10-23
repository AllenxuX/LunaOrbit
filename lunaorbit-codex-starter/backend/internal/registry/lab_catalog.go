package registry

import (
	"context"
	"encoding/json"
	"io"
	"slices"
	"strings"
)

func validLabCatalogSize(count int) bool { return count >= 2 && count <= 16 }

// ConfigureLabCatalog is a startup-only server-owned lab fixture. It accepts no
// credentials and never changes external Kafka resources.
func (s *Service) ConfigureLabCatalog(ctx context.Context, raw string, policy interface {
	ValidateSeed(context.Context, string) error
}) error {
	bad := func() error { return failure("INVALID_ARGUMENT") }
	if !s.readOnlyNative || policy == nil || len(raw) > 16384 {
		return bad()
	}
	var entries []CreateInput
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&entries) != nil || !validLabCatalogSize(len(entries)) {
		return bad()
	}
	if dec.Decode(new(any)) != io.EOF {
		return bad()
	}
	seen := map[string]bool{}
	for _, entry := range entries {
		c := entry.CredentialInput
		if c.SecurityProtocol != "PLAINTEXT" || c.SASLMechanism != "" || c.Username != nil || c.Password != nil || c.TLSCAPEM != nil || c.TLSCertPEM != nil || c.TLSKeyPEM != nil || c.TLSServerName != "" || !entry.Enabled {
			return bad()
		}
		if validateRegistration(Registration{DisplayName: entry.DisplayName, Room: entry.Room, Environment: entry.Environment, Description: entry.Description, BootstrapServers: entry.BootstrapServers, VersionReported: entry.VersionReported}) != nil {
			return bad()
		}
		for _, seed := range entry.BootstrapServers {
			if seen[seed] || policy.ValidateSeed(ctx, seed) != nil {
				return bad()
			}
			seen[seed] = true
		}
	}
	s.labMu.Lock()
	defer s.labMu.Unlock()
	s.labCatalog = entries
	s.labOwners = map[string][]string{}
	s.labCatalogIDs = map[string][]string{}
	return nil
}

func (s *Service) HasLabCatalog() bool {
	s.labMu.Lock()
	defer s.labMu.Unlock()
	return s.readOnlyNative && validLabCatalogSize(len(s.labCatalog))
}

func (s *Service) CanSeedLabCatalog(actor Actor) bool {
	return actor.ID != "" && actor.Role == "admin" && s.HasLabCatalog()
}

func sameLabSeeds(a, b []string) bool {
	a = append([]string(nil), a...)
	b = append([]string(nil), b...)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

func (s *Service) SeedLabCatalog(ctx context.Context, actor Actor) ([]Cluster, error) {
	if actor.ID == "" || actor.Role != "admin" {
		return nil, failure("FORBIDDEN")
	}
	s.labMu.Lock()
	defer s.labMu.Unlock()
	if !s.readOnlyNative || !validLabCatalogSize(len(s.labCatalog)) {
		return nil, failure("NOT_FOUND")
	}
	pending, initialized := s.labOwners[actor.ID]
	if !initialized {
		if len(s.labOwners) >= 128 {
			return nil, failure("CAPACITY_EXCEEDED")
		}
		rows, e := s.List(ctx, actor)
		if e != nil {
			return nil, e
		}
		ids := []string{}
		for _, entry := range s.labCatalog {
			var found *Cluster
			for i := range rows {
				if rows[i].SecurityProtocol == entry.SecurityProtocol && sameLabSeeds(rows[i].BootstrapServers, entry.BootstrapServers) {
					found = &rows[i]
					break
				}
			}
			if found == nil {
				created, e := s.Create(ctx, actor, entry)
				if e != nil {
					return nil, e
				}
				rows = append(rows, created)
				found = &rows[len(rows)-1]
			}
			if found.Enabled && found.Status == "unverified" {
				pending = append(pending, found.ID)
			}
			ids = append(ids, found.ID)
		}
		s.labOwners[actor.ID] = pending
		s.labCatalogIDs[actor.ID] = ids
	}
	remaining := []string{}
	for _, id := range pending {
		row, e := s.Get(ctx, actor, id)
		if ErrorCode(e) == "NOT_FOUND" {
			continue
		}
		if e != nil {
			return nil, e
		}
		if !row.Enabled || row.Status != "unverified" {
			continue
		}
		if _, e = s.StartProbe(ctx, actor, id, ""); e != nil {
			remaining = append(remaining, id)
		}
	}
	s.labOwners[actor.ID] = remaining
	rows, err := s.List(ctx, actor)
	if err != nil {
		return nil, err
	}
	result := make([]Cluster, 0, len(s.labCatalog))
	for _, row := range rows {
		if slices.Contains(s.labCatalogIDs[actor.ID], row.ID) {
			result = append(result, row)
		}
	}
	return result, nil
}
