package collector

import (
	"context"
	"lunaorbit/backend/internal/discovery/resource"
	"lunaorbit/backend/internal/kite"
	"sort"
	"strings"
	"time"
)

// AllDeploymentsClient is a Kite client that can list one cluster's
// deployments across every namespace in a single read.
type AllDeploymentsClient interface {
	kite.KiteClient
	ListAllDeployments(context.Context, string) (kite.Document, error)
}

// AutoKiteSource collects one auto-enumerated Kubernetes cluster: a single
// all-namespace deployment read, MM2 identification by workload features
// (never by labels alone), then ConfigMaps only for namespaces that hold a
// likely MM2. Pods are never scanned.
type AutoKiteSource struct {
	Client            AllDeploymentsClient
	Decoder           kite.ResourceDecoder
	ExcludeNamespaces map[string]bool
	OnlyNamespaces    map[string]bool
}

func DefaultExcludedNamespaces() map[string]bool {
	return map[string]bool{"kube-system": true, "kube-public": true, "kube-node-lease": true}
}

// MM2Signals returns identification evidence, strongest first. Labels and
// names alone are never evidence.
func MM2Signals(d resource.Deployment) []string {
	var out []string
	add := func(s string) {
		for _, v := range out {
			if v == s {
				return
			}
		}
		out = append(out, s)
	}
	containers := append(append([]resource.Container{}, d.Spec.Template.Spec.Containers...), d.Spec.Template.Spec.InitContainers...)
	for _, c := range containers {
		for _, v := range append(append([]string{}, c.Command...), c.Args...) {
			t := strings.ToLower(v)
			if strings.Contains(t, "connect-mirror-maker.sh") {
				add("command:connect-mirror-maker.sh")
			}
			if strings.Contains(t, "org.apache.kafka.connect.mirror.mirrormaker") {
				add("command:MirrorMaker")
			}
		}
	}
	for _, c := range containers {
		img := strings.ToLower(c.Image)
		if strings.Contains(img, "mirrormaker") || strings.Contains(img, "mirror-maker") || strings.Contains(img, "mm2") {
			add("image:" + c.Image)
		}
	}
	return out
}

func (s AutoKiteSource) Collect(ctx context.Context, scope resource.Scope) (resource.Collection, error) {
	out := resource.Collection{Scope: scope, ObservedAt: time.Now().UTC()}
	fail := func(err error) (resource.Collection, error) {
		out.ErrorCode = kite.CodeOf(err)
		return out, err
	}
	if !scope.Auto || !validScope(scope) {
		return fail(&kite.Error{Code: "INVALID_ARGUMENT"})
	}
	if s.Client == nil {
		return fail(&kite.Error{Code: "INVALID_CONFIG"})
	}
	doc, err := s.Client.ListAllDeployments(ctx, scope.Cluster)
	if err != nil {
		return fail(err)
	}
	var deployments []resource.Deployment
	if err = doc.DecodeResource(s.Decoder, &deployments); err != nil {
		return fail(err)
	}
	if len(deployments) > MaxResources {
		return fail(&kite.Error{Code: "RESPONSE_TOO_LARGE"})
	}
	scanned := map[string]bool{}
	candidates := map[string]*resource.NamespaceBatch{}
	for _, d := range deployments {
		ns := d.Metadata.Namespace
		if !validLabel(ns) || s.ExcludeNamespaces[ns] || (len(s.OnlyNamespaces) > 0 && !s.OnlyNamespaces[ns]) {
			continue
		}
		scanned[ns] = true
		out.DeploymentsScanned++
		signals := MM2Signals(d)
		if len(signals) == 0 {
			continue
		}
		b := candidates[ns]
		if b == nil {
			b = &resource.NamespaceBatch{Namespace: ns, Complete: true, Signals: map[string][]string{}}
			candidates[ns] = b
		}
		b.Deployments = append(b.Deployments, d)
		b.Signals[d.Metadata.UID] = signals
	}
	out.NamespacesScanned = len(scanned)
	names := make([]string, 0, len(candidates))
	for ns := range candidates {
		names = append(names, ns)
	}
	sort.Strings(names)
	for _, ns := range names {
		b := candidates[ns]
		doc, err := s.Client.ListConfigMaps(ctx, scope.Cluster, ns)
		if err == nil {
			err = doc.DecodeResource(s.Decoder, &b.ConfigMaps)
		}
		if err != nil {
			b.Complete = false
			b.ErrorCode = kite.CodeOf(err)
			b.ConfigMaps = nil
		}
		for _, c := range b.ConfigMaps {
			if c.Metadata.Namespace != ns {
				b.Complete = false
				b.ErrorCode = "INVALID_RESPONSE"
			}
		}
		out.Namespaces = append(out.Namespaces, *b)
	}
	return out, nil
}
