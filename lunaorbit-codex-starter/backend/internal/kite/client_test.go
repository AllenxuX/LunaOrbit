package kite

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const syntheticKey = "synthetic-kite-test-key"

func testClient(t *testing.T, handler http.HandlerFunc, retries int, timeout time.Duration) (*HTTPClient, *httptest.Server) {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	c, err := New(Config{BaseURL: s.URL, AllowedOrigins: []string{s.URL}, APIKey: syntheticKey, Timeout: timeout, MaxRetries: retries, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, s
}

func TestDeadlineIncludesRetryBackoff(t *testing.T) {
	var calls atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(429) }, 2, time.Second)
	c.delay = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.ListNamespaces(ctx, "synthetic-cluster"); CodeOf(err) != "TIMEOUT" || calls.Load() != 1 {
		t.Fatalf("retry exceeded caller deadline: %v", err)
	}
}

func TestTimeoutDuringBodyAndNetworkFailure(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}, 0, 20*time.Millisecond)
	if _, err := c.ListNamespaces(context.Background(), "synthetic-cluster"); CodeOf(err) != "TIMEOUT" {
		t.Fatalf("body timeout: %v", err)
	}
	c, s := testClient(t, func(w http.ResponseWriter, r *http.Request) {}, 0, time.Second)
	s.Close()
	_, err := c.ListNamespaces(context.Background(), "synthetic-cluster")
	if CodeOf(err) != "NETWORK_ERROR" || strings.Contains(err.Error(), s.URL) || strings.Contains(err.Error(), syntheticKey) {
		t.Fatal("network error classification or redaction failed")
	}
}

type failingDecoder struct{}

func (failingDecoder) Decode(string, bool, []byte) (Snapshot, error) {
	return Snapshot{}, fmt.Errorf("%s", syntheticKey)
}
func TestDecoderErrorIsSafe(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }, 0, time.Second)
	document, err := c.ListNamespaces(context.Background(), "synthetic-cluster")
	if err != nil {
		t.Fatal(err)
	}
	_, err = document.Decode(failingDecoder{})
	if CodeOf(err) != "INVALID_RESPONSE" || strings.Contains(err.Error(), syntheticKey) {
		t.Fatal("decoder error was exposed")
	}
}

func TestReadOnlyRoutesAndHeaders(t *testing.T) {
	var paths []string
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.Header.Get("Authorization") != syntheticKey || r.Header.Get("x-cluster-name") != "synthetic-cluster" || r.URL.RawQuery != "" {
			t.Error("request violates contract")
		}
		paths = append(paths, r.URL.Path)
		fmt.Fprint(w, `{"synthetic_secret":"do-not-expose"}`)
	}, 0, time.Second)
	ctx := context.Background()
	queries := []func() (Document, error){
		func() (Document, error) { return c.ListNamespaces(ctx, "synthetic-cluster") },
		func() (Document, error) { return c.ListConfigMaps(ctx, "synthetic-cluster", "synthetic-ns") },
		func() (Document, error) {
			return c.GetConfigMap(ctx, "synthetic-cluster", "synthetic-ns", "synthetic-name")
		},
		func() (Document, error) { return c.ListDeployments(ctx, "synthetic-cluster", "synthetic-ns") },
		func() (Document, error) {
			return c.GetDeployment(ctx, "synthetic-cluster", "synthetic-ns", "synthetic-name")
		},
		func() (Document, error) { return c.ListPods(ctx, "synthetic-cluster", "synthetic-ns") },
		func() (Document, error) { return c.GetPod(ctx, "synthetic-cluster", "synthetic-ns", "synthetic-name") },
	}
	want := []string{"/api/v1/namespaces/_all", "/api/v1/configmaps/synthetic-ns", "/api/v1/configmaps/synthetic-ns/synthetic-name", "/api/v1/deployments/synthetic-ns", "/api/v1/deployments/synthetic-ns/synthetic-name", "/api/v1/pods/synthetic-ns", "/api/v1/pods/synthetic-ns/synthetic-name"}
	for i, q := range queries {
		doc, err := q()
		if err != nil {
			t.Fatal(err)
		}
		if paths[i] != want[i] {
			t.Fatalf("route %d differs", i)
		}
		data, _ := json.Marshal(doc)
		for _, printed := range []string{string(data), fmt.Sprintf("%v", doc), fmt.Sprintf("%+v", doc), fmt.Sprintf("%#v", doc)} {
			if strings.Contains(printed, "do-not-expose") {
				t.Fatal("document leaked content")
			}
		}
		if _, err := doc.Decode(nil); CodeOf(err) != "RESPONSE_FORMAT_UNVERIFIED" {
			t.Fatal("unknown envelope treated as verified")
		}
	}
}

func TestValidationBeforeRequest(t *testing.T) {
	var calls atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }, 0, time.Second)
	for _, value := range []string{"", "../secret", "%2e%2e", "a/b", "a?x=y", "_all", "a\r\nX: y", ".", "a..b", strings.Repeat("a", 64)} {
		if _, err := c.ListPods(context.Background(), "synthetic-cluster", value); CodeOf(err) != "INVALID_ARGUMENT" {
			t.Fatalf("invalid namespace accepted: %q", value)
		}
	}
	for _, value := range []string{"", "../secret", "a/b", "%2f", "a..b"} {
		if _, err := c.GetPod(context.Background(), "synthetic-cluster", "synthetic-ns", value); CodeOf(err) != "INVALID_ARGUMENT" {
			t.Fatal("invalid name accepted")
		}
	}
	for _, value := range []string{"", " bad", "bad\r\nX: y"} {
		if _, err := c.ListNamespaces(context.Background(), value); CodeOf(err) != "INVALID_ARGUMENT" {
			t.Fatal("invalid cluster accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid arguments reached network")
	}
}

func TestURLAndConfigValidation(t *testing.T) {
	for _, value := range []string{"https://allowed.example/path", "https://user:password@allowed.example", "https://allowed.example?token=secret", "https://allowed.example#secret", "https://other.example", "http://allowed.example", "file:///etc/passwd"} {
		_, err := New(Config{BaseURL: value, AllowedOrigins: []string{"https://allowed.example"}, APIKey: syntheticKey, Timeout: time.Second})
		if CodeOf(err) != "INVALID_CONFIG" || strings.Contains(err.Error(), "secret") {
			t.Fatal("unsafe URL accepted or leaked")
		}
	}
	for _, key := range []string{"", "Bearer synthetic", "synthetic\nkey"} {
		_, err := New(Config{BaseURL: "https://allowed.example", AllowedOrigins: []string{"https://allowed.example"}, APIKey: key, Timeout: time.Second})
		if CodeOf(err) != "INVALID_CONFIG" {
			t.Fatal("invalid key accepted")
		}
	}
}

func TestStatusClassificationAndRetries(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
		calls  int
	}{{401, "UNAUTHENTICATED", 1}, {403, "PERMISSION_DENIED", 1}, {404, "NOT_FOUND", 1}, {429, "RATE_LIMITED", 3}, {500, "UPSTREAM_UNAVAILABLE", 3}, {503, "UPSTREAM_UNAVAILABLE", 3}, {600, "UNEXPECTED_STATUS", 1}} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			var calls atomic.Int32
			c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				fmt.Fprint(w, syntheticKey)
			}, 2, time.Second)
			_, err := c.ListNamespaces(context.Background(), "synthetic-cluster")
			if CodeOf(err) != tc.code || calls.Load() != int32(tc.calls) || strings.Contains(fmt.Sprintf("%+v", err), syntheticKey) {
				t.Fatalf("incorrect classification/retry: %v %d", err, calls.Load())
			}
		})
	}
}

func TestRetrySuccessAndRedirectBlocked(t *testing.T) {
	var calls atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		fmt.Fprint(w, `{}`)
	}, 1, time.Second)
	if _, err := c.ListNamespaces(context.Background(), "synthetic-cluster"); err != nil || calls.Load() != 2 {
		t.Fatal("retry did not recover")
	}
	var leaked atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true) }))
	defer target.Close()
	c, _ = testClient(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }, 0, time.Second)
	if _, err := c.ListNamespaces(context.Background(), "synthetic-cluster"); CodeOf(err) != "REDIRECT_BLOCKED" || leaked.Load() {
		t.Fatal("redirect forwarded")
	}
}

func TestTimeoutCancellationAndBodyLimit(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }, 0, 20*time.Millisecond)
	if _, err := c.ListNamespaces(context.Background(), "synthetic-cluster"); CodeOf(err) != "TIMEOUT" {
		t.Fatalf("timeout: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.ListNamespaces(ctx, "synthetic-cluster"); CodeOf(err) != "CANCELED" {
		t.Fatalf("cancel: %v", err)
	}
	c, _ = testClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, strings.Repeat("x", MaxBodyBytes+1)) }, 0, time.Second)
	if _, err := c.ListNamespaces(context.Background(), "synthetic-cluster"); CodeOf(err) != "RESPONSE_TOO_LARGE" {
		t.Fatal("oversized body accepted")
	}
}

func TestInClusterHTTPBaseURLIsAccepted(t *testing.T) {
	for _, base := range []string{"http://kite.kite-system.svc.cluster.local:8080", "http://10.96.0.10:8080", "https://kite.lluna.cn", "http://127.0.0.1:28080"} {
		c, err := New(Config{BaseURL: base, AllowedOrigins: []string{base}, APIKey: "synthetic-key", Timeout: time.Second})
		if err != nil {
			t.Fatalf("%s rejected: %v", base, err)
		}
		c.Close()
	}
	for _, base := range []string{"http://u:p@kite.kite-system.svc:8080", "ftp://kite.kite-system.svc", "http://kite.kite-system.svc:8080/api?x=1", "http://"} {
		if _, err := New(Config{BaseURL: base, AllowedOrigins: []string{base}, APIKey: "synthetic-key", Timeout: time.Second}); CodeOf(err) != "INVALID_CONFIG" {
			t.Fatalf("%s must be rejected", base)
		}
	}
}
