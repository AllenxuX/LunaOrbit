package workspace

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"sort"
	"strings"
)

func authorizeACL(ctx context.Context, reg *registry.Service, a registry.Actor, id, action string, r kafkaops.ACLRule) error {
	if r.ResourceType != "TOPIC" && r.ResourceType != "GROUP" || r.ResourceName == "*" {
		return reg.AuthorizeUnscopedResource(ctx, a, id, action)
	}
	return reg.AuthorizeResource(ctx, a, id, action, strings.ToLower(r.ResourceType)+":"+r.ResourceName)
}

func (s *Service) ListACLRules(ctx context.Context, a registry.Actor, id, principal string) (kafkaops.ACLSnapshot, error) {
	if len(principal) > 256 {
		return kafkaops.ACLSnapshot{}, errInvalid
	}
	if e := s.auth(ctx, a, id, "acl.manage", ""); e != nil {
		return kafkaops.ACLSnapshot{}, e
	}
	if s.Gateway == nil {
		return kafkaops.ACLSnapshot{}, errors.New("DISABLED")
	}
	snap, e := s.Gateway.ACLs(ctx, a, id)
	if e != nil {
		return snap, e
	}
	visible := []kafkaops.ACLRule{}
	for _, r := range snap.Items {
		if (principal == "" || r.Principal == principal) && authorizeACL(ctx, s.Registry, a, id, "acl.manage", r) == nil {
			visible = append(visible, r)
		}
	}
	sort.Slice(visible, func(i, j int) bool { return key(visible[i]) < key(visible[j]) })
	snap.Items = visible
	return snap, nil
}

func (s *Service) previewCustomACL(ctx context.Context, a registry.Actor, id string, p Plan) (Plan, error) {
	in := p.Input
	if in.Principal == "" || len(in.DesiredRules)+len(in.SelectedRules) < 1 || len(in.DesiredRules)+len(in.SelectedRules) > 100 {
		return p, errInvalid
	}
	snap, e := s.Gateway.ACLs(ctx, a, id)
	if e != nil {
		return p, e
	}
	if snap.State != "available" && snap.State != "observed" && snap.State != "pending" && snap.State != "" {
		return p, errors.New("AUTHORIZER_UNVERIFIED")
	}
	current := map[string]bool{}
	for _, r := range snap.Items {
		current[key(r)] = true
	}
	seen := map[string]bool{}
	for _, r := range append(append([]kafkaops.ACLRule{}, in.DesiredRules...), in.SelectedRules...) {
		if !kafkaops.ValidACLRule(r) || r.Principal != in.Principal {
			return p, errInvalid
		}
		if e = authorizeACL(ctx, s.Registry, a, id, "acl.manage", r); e != nil {
			return p, e
		}
		if seen[key(r)] {
			return p, errInvalid
		}
		seen[key(r)] = true
	}
	for _, r := range in.DesiredRules {
		if current[key(r)] {
			p.Existing = append(p.Existing, r)
		} else {
			p.Additions = append(p.Additions, r)
		}
	}
	for _, r := range in.SelectedRules {
		if !current[key(r)] {
			return p, errConflict
		}
		p.Removals = append(p.Removals, r)
	}
	p.Retained = []kafkaops.ACLRule{}
	for _, r := range snap.Items {
		if r.Principal == in.Principal && !seen[key(r)] && authorizeACL(ctx, s.Registry, a, id, "acl.manage", r) == nil {
			p.Retained = append(p.Retained, r)
		}
	}
	keys := []string{}
	for k := range current {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	p.Fingerprint = fingerprint(struct {
		Input Input
		Rules []string
	}{p.Input, keys})
	return p, nil
}
