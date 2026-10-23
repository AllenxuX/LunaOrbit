// Package collector reads only the documented Kite list endpoints.
package collector

import (
	"context"
	"lunaorbit/backend/internal/discovery/resource"
	"lunaorbit/backend/internal/kite"
	"regexp"
	"strings"
	"time"
)

type Source interface {
	Collect(context.Context, resource.Scope) (resource.Collection, error)
}
type KiteSource struct {
	Client  kite.KiteClient
	Decoder kite.ResourceDecoder
}

const MaxResources = 1000

var label = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$`)

func validLabel(s string) bool { return len(s) > 0 && len(s) <= 63 && label.MatchString(s) }
func validScope(s resource.Scope) bool {
	if !validLabel(s.ID) || len(s.Cluster) == 0 || len(s.Cluster) > 4096 || strings.TrimSpace(s.Cluster) != s.Cluster || len(s.Namespaces) > MaxResources {
		return false
	}
	for _, c := range s.Cluster {
		if c < 32 || c > 126 {
			return false
		}
	}
	for _, ns := range s.Namespaces {
		if !validLabel(ns) {
			return false
		}
	}
	return true
}
func (s KiteSource) Collect(ctx context.Context, scope resource.Scope) (resource.Collection, error) {
	out := resource.Collection{Scope: scope, ObservedAt: time.Now().UTC()}
	fail := func(code string) (resource.Collection, error) {
		err := &kite.Error{Code: code}
		out.ErrorCode = kite.CodeOf(err)
		return out, err
	}
	if !validScope(scope) {
		return fail("INVALID_ARGUMENT")
	}
	if s.Client == nil {
		return fail("INVALID_CONFIG")
	}
	namespaces := append([]string(nil), scope.Namespaces...)
	if len(namespaces) == 0 {
		doc, err := s.Client.ListNamespaces(ctx, scope.Cluster)
		if err != nil {
			return fail(kite.CodeOf(err))
		}
		var items []resource.Namespace
		if err = doc.DecodeResource(s.Decoder, &items); err != nil {
			return fail(kite.CodeOf(err))
		}
		if len(items) > MaxResources {
			return fail("RESPONSE_TOO_LARGE")
		}
		for _, item := range items {
			if !validLabel(item.Metadata.Name) {
				return fail("INVALID_RESPONSE")
			}
			namespaces = append(namespaces, item.Metadata.Name)
		}
	}
	seen := map[string]bool{}
	for _, ns := range namespaces {
		if seen[ns] {
			continue
		}
		seen[ns] = true
		b := resource.NamespaceBatch{Namespace: ns, Complete: true}
		query := func(doc kite.Document, err error, target any) {
			if err == nil {
				err = doc.DecodeResource(s.Decoder, target)
			}
			if err != nil {
				b.Complete = false
				if b.ErrorCode == "" {
					b.ErrorCode = kite.CodeOf(err)
				}
			}
		}
		doc, err := s.Client.ListDeployments(ctx, scope.Cluster, ns)
		query(doc, err, &b.Deployments)
		doc, err = s.Client.ListConfigMaps(ctx, scope.Cluster, ns)
		query(doc, err, &b.ConfigMaps)
		doc, err = s.Client.ListPods(ctx, scope.Cluster, ns)
		query(doc, err, &b.Pods)
		if len(b.Deployments) > MaxResources || len(b.ConfigMaps) > MaxResources || len(b.Pods) > MaxResources {
			b.Complete = false
			b.ErrorCode = kite.CodeOf(&kite.Error{Code: "RESPONSE_TOO_LARGE"})
			b.Deployments = nil
			b.ConfigMaps = nil
			b.Pods = nil
		}
		for _, d := range b.Deployments {
			if d.Metadata.Namespace != ns {
				b.Complete = false
				b.ErrorCode = "INVALID_RESPONSE"
			}
		}
		for _, c := range b.ConfigMaps {
			if c.Metadata.Namespace != ns {
				b.Complete = false
				b.ErrorCode = "INVALID_RESPONSE"
			}
		}
		for _, p := range b.Pods {
			if p.Metadata.Namespace != ns {
				b.Complete = false
				b.ErrorCode = "INVALID_RESPONSE"
			}
		}
		out.Namespaces = append(out.Namespaces, b)
	}
	return out, nil
}
