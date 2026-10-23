package main

import (
	"bytes"
	"context"
	"encoding/json"
	"lunaorbit/backend/internal/kite"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOfflineMockAllRoutes(t *testing.T) {
	var output bytes.Buffer
	// Mock must not consult real connection configuration at all.
	if err := run(context.Background(), &output, func(string) string { t.Fatal("mock read environment"); return "" }, "mock", "all"); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 7 {
		t.Fatalf("expected seven routes, got %d", len(lines))
	}
	for _, line := range lines {
		var result map[string]any
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			t.Fatal(err)
		}
		if result["mode"] != "mock" || result["response_format"] != "synthetic" || result["items"] != float64(1) {
			t.Fatal("unexpected mock result")
		}
	}
	if strings.Contains(output.String(), "Authorization") || strings.Contains(output.String(), "synthetic-name") {
		t.Fatal("CLI exposed configuration")
	}
}

func TestExplicitRealTransportDoesNotGuessEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/api/v1/namespaces/_all" {
			t.Error("unexpected route")
		}
		if r.Header.Get("Authorization") != "synthetic-configured-key" || r.Header.Get("x-cluster-name") != "synthetic-configured-cluster" {
			t.Error("configured headers missing")
		}
		_, _ = w.Write([]byte(`{"arbitrary_secret":"never-print-this"}`))
	}))
	defer server.Close()
	values := map[string]string{"KITE_BASE_URL": server.URL, "KITE_ALLOWED_ORIGINS": server.URL, "KITE_API_KEY": "synthetic-configured-key", "KITE_CLUSTER_NAME": "synthetic-configured-cluster"}
	var output bytes.Buffer
	err := run(context.Background(), &output, func(key string) string { return values[key] }, "real", "namespaces")
	if kite.CodeOf(err) != "RESPONSE_FORMAT_UNVERIFIED" || !strings.Contains(output.String(), `"response_format":"unverified"`) || strings.Contains(output.String(), "never-print-this") {
		t.Fatal("real response was guessed or exposed")
	}
}

func TestRealModeRequiresExplicitConfiguration(t *testing.T) {
	var output bytes.Buffer
	if err := run(context.Background(), &output, func(string) string { return "" }, "real", "namespaces"); err == nil {
		t.Fatal("real mode accepted missing config")
	}
	if output.Len() != 0 {
		t.Fatal("invalid real configuration produced output")
	}
}
