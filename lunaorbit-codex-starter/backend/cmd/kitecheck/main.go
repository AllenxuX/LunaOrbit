// kitecheck is a metadata-free transport diagnostic, not an inventory collector.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io"
	"lunaorbit/backend/internal/kite"
	"lunaorbit/backend/internal/kite/mock"
	"net/http/httptest"
	"os"
	"os/signal"
	"strings"
	"time"
)

func main() {
	mode := flag.String("mode", "mock", "mock (offline) or real (explicit environment configuration)")
	resource := flag.String("resource", "all", "all (mock only), namespaces, configmaps, deployments, pods")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Stdout, os.Getenv, *mode, *resource); err != nil {
		_ = json.NewEncoder(os.Stderr).Encode(map[string]string{"error": kite.CodeOf(err)})
		os.Exit(1)
	}
}

type query struct {
	resource string
	single   bool
	call     func(context.Context) (kite.Document, error)
}

func run(ctx context.Context, out io.Writer, getenv func(string) string, mode, resource string) error {
	if mode != "mock" && mode != "real" {
		return &kite.Error{Code: "INVALID_ARGUMENT"}
	}
	if resource != "all" && resource != "namespaces" && resource != "configmaps" && resource != "deployments" && resource != "pods" {
		return &kite.Error{Code: "INVALID_ARGUMENT"}
	}
	var cfg kite.Config
	var cluster, namespace, name string
	var decoder kite.Decoder
	if mode == "mock" {
		secret := make([]byte, 24)
		if _, err := rand.Read(secret); err != nil {
			return &kite.Error{Code: "INTERNAL_ERROR"}
		}
		key := hex.EncodeToString(secret)
		cluster, namespace, name = "synthetic-cluster", "synthetic-ns", "synthetic-name"
		server := httptest.NewServer(mock.Handler(key, cluster))
		defer server.Close()
		cfg = kite.Config{BaseURL: server.URL, AllowedOrigins: []string{server.URL}, APIKey: key, Timeout: 5 * time.Second}
		decoder = mock.Decoder{}
	} else {
		if resource == "all" {
			return &kite.Error{Code: "INVALID_ARGUMENT"}
		}
		cfg = kite.Config{BaseURL: getenv("KITE_BASE_URL"), AllowedOrigins: strings.Split(getenv("KITE_ALLOWED_ORIGINS"), ","), APIKey: getenv("KITE_API_KEY"), Timeout: 5 * time.Second, MaxRetries: 1}
		cluster, namespace, name = getenv("KITE_CLUSTER_NAME"), getenv("KITE_NAMESPACE"), getenv("KITE_RESOURCE_NAME")
	}
	client, err := kite.New(cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	queries := []query{
		{"namespaces", false, func(ctx context.Context) (kite.Document, error) { return client.ListNamespaces(ctx, cluster) }},
		{"configmaps", false, func(ctx context.Context) (kite.Document, error) {
			return client.ListConfigMaps(ctx, cluster, namespace)
		}},
		{"configmaps", true, func(ctx context.Context) (kite.Document, error) {
			return client.GetConfigMap(ctx, cluster, namespace, name)
		}},
		{"deployments", false, func(ctx context.Context) (kite.Document, error) {
			return client.ListDeployments(ctx, cluster, namespace)
		}},
		{"deployments", true, func(ctx context.Context) (kite.Document, error) {
			return client.GetDeployment(ctx, cluster, namespace, name)
		}},
		{"pods", false, func(ctx context.Context) (kite.Document, error) { return client.ListPods(ctx, cluster, namespace) }},
		{"pods", true, func(ctx context.Context) (kite.Document, error) { return client.GetPod(ctx, cluster, namespace, name) }},
	}
	for _, q := range queries {
		if resource != "all" && (q.resource != resource || (q.single != (name != "") && resource != "namespaces")) {
			continue
		}
		document, err := q.call(ctx)
		if err != nil {
			return err
		}
		snapshot, err := document.Decode(decoder)
		if err != nil {
			if writeErr := json.NewEncoder(out).Encode(map[string]any{"mode": mode, "resource": q.resource, "single": q.single, "transport": "ok", "response_format": "unverified"}); writeErr != nil {
				return &kite.Error{Code: "OUTPUT_ERROR"}
			}
			return err
		}
		if err := json.NewEncoder(out).Encode(map[string]any{"mode": mode, "resource": q.resource, "single": q.single, "transport": "ok", "response_format": "synthetic", "items": len(snapshot.Items)}); err != nil {
			return &kite.Error{Code: "OUTPUT_ERROR"}
		}
	}
	return nil
}
