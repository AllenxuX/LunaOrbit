package kite

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWritesDisabledNeverCallUpstream(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer server.Close()
	client, e := New(Config{BaseURL: server.URL, AllowedOrigins: []string{server.URL}, APIKey: "synthetic-key", Timeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	scope := WriteScope{Cluster: "test-cluster", Namespace: "synthetic", ConfigMap: "mm2-config", Deployment: "mm2-worker"}
	writer, e := NewWriter(client, WriteConfig{Scopes: []WriteScope{scope}})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = writer.RestartDeployment(context.Background(), scope, "uid", "rv"); CodeOf(e) != "WRITES_DISABLED" {
		t.Fatal(e)
	}
	object, _ := NewWriteObject(map[string]any{})
	if _, e = writer.PutConfigMap(context.Background(), scope, object); CodeOf(e) != "WRITES_DISABLED" {
		t.Fatal(e)
	}
	if calls != 0 {
		t.Fatal(calls)
	}
}
func writeTestClient(t *testing.T, handler http.HandlerFunc, timeout time.Duration) (*HTTPClient, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, e := New(Config{BaseURL: server.URL, AllowedOrigins: []string{server.URL}, APIKey: "synthetic-key", Timeout: timeout, MaxRetries: 2, RetryDelay: time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(client.Close)
	return client, server
}
func testWriteScope() WriteScope {
	return WriteScope{Cluster: "test-cluster", Namespace: "synthetic", ConfigMap: "mm2-config", Deployment: "mm2-worker"}
}
func testConfigMap() map[string]any {
	return map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "mm2-config", "namespace": "synthetic", "uid": "synthetic-cm-uid", "resourceVersion": "42", "labels": map[string]any{"app": "mm2"}}, "data": map[string]any{"mm2.properties": "synthetic-private-value", "unmodified": "preserved"}}
}
func TestConfigMapPutExactPayloadAndSafeFormatting(t *testing.T) {
	calls := 0
	client, _ := writeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "PUT" || r.URL.Path != "/api/v1/configmaps/synthetic/mm2-config" || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "synthetic-key" || r.Header.Get("x-cluster-name") != "test-cluster" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("incorrect PUT contract")
		}
		var got map[string]any
		if json.NewDecoder(r.Body).Decode(&got) != nil || !reflect.DeepEqual(got, testConfigMap()) {
			t.Errorf("payload not preserved")
		}
		w.Write([]byte(`{"unknownEnvelope":"synthetic-private-value"}`))
	}, time.Second)
	scope := testWriteScope()
	writer, e := NewWriter(client, WriteConfig{Enabled: true, Scopes: []WriteScope{scope}})
	if e != nil {
		t.Fatal(e)
	}
	source := testConfigMap()
	object, e := NewWriteObject(source)
	if e != nil {
		t.Fatal(e)
	}
	source["data"].(map[string]any)["mm2.properties"] = "mutated-after-copy"
	for _, formatted := range []string{fmt.Sprintf("%v", object), fmt.Sprintf("%+v", object), fmt.Sprintf("%#v", object), fmt.Sprintf("%s", object), string(mustJSON(t, object))} {
		if strings.Contains(formatted, "synthetic-private") || strings.Contains(formatted, "mm2.properties") {
			t.Fatal("object leaked")
		}
	}
	document, e := writer.PutConfigMap(context.Background(), scope, object)
	if e != nil || calls != 1 {
		t.Fatal(e, calls)
	}
	if e = document.DecodeResource(nil, &map[string]any{}); CodeOf(e) != "RESPONSE_FORMAT_UNVERIFIED" {
		t.Fatal(e)
	}
	if strings.Contains(fmt.Sprintf("%#v", document), "synthetic-private") {
		t.Fatal("response leaked")
	}
}
func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, e := json.Marshal(value)
	if e != nil {
		t.Fatal(e)
	}
	return body
}
func TestDeploymentRestartExactMergePatchUniqueUTC(t *testing.T) {
	var stamps []string
	client, _ := writeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PATCH" || r.URL.Path != "/api/v1/deployments/synthetic/mm2-worker" || r.URL.RawQuery != "patchType=merge" {
			t.Error("incorrect patch contract")
		}
		var got map[string]any
		json.NewDecoder(r.Body).Decode(&got)
		metadata := got["metadata"].(map[string]any)
		if metadata["uid"] != "synthetic-deployment-uid" || metadata["resourceVersion"] != "19" || len(metadata) != 2 || len(got) != 2 {
			t.Error("missing optimistic guard")
		}
		template := got["spec"].(map[string]any)["template"].(map[string]any)
		annotations := template["metadata"].(map[string]any)["annotations"].(map[string]any)
		if len(annotations) != 1 {
			t.Error("arbitrary annotation")
		}
		stamp := annotations["kite.kubernetes.io/restartedAt"].(string)
		parsed, e := time.Parse(time.RFC3339Nano, stamp)
		if e != nil || parsed.Location() != time.UTC || !strings.HasSuffix(stamp, "Z") {
			t.Error("non UTC timestamp")
		}
		stamps = append(stamps, stamp)
		w.Write([]byte(`{}`))
	}, time.Second)
	scope := testWriteScope()
	writer, _ := NewWriter(client, WriteConfig{Enabled: true, Scopes: []WriteScope{scope}})
	writer.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("synthetic", 3600)) }
	for i := 0; i < 2; i++ {
		if _, e := writer.RestartDeployment(context.Background(), scope, "synthetic-deployment-uid", "19"); e != nil {
			t.Fatal(e)
		}
	}
	if len(stamps) != 2 || stamps[0] == stamps[1] {
		t.Fatal("restart stamp reused")
	}
}
func TestScopeAndPayloadRejectBeforeRequests(t *testing.T) {
	calls := 0
	client, _ := writeTestClient(t, func(http.ResponseWriter, *http.Request) { calls++ }, time.Second)
	scope := testWriteScope()
	writer, _ := NewWriter(client, WriteConfig{Enabled: true, Scopes: []WriteScope{scope}})
	for _, other := range []WriteScope{{Cluster: "other", Namespace: scope.Namespace, ConfigMap: scope.ConfigMap, Deployment: scope.Deployment}, {Cluster: scope.Cluster, Namespace: "other", ConfigMap: scope.ConfigMap, Deployment: scope.Deployment}, {Cluster: scope.Cluster, Namespace: scope.Namespace, ConfigMap: "other", Deployment: scope.Deployment}, {Cluster: scope.Cluster, Namespace: scope.Namespace, ConfigMap: scope.ConfigMap, Deployment: "other"}, {Cluster: scope.Cluster, Namespace: "../evil", ConfigMap: scope.ConfigMap, Deployment: scope.Deployment}, {Cluster: "bad\r\nx: secret", Namespace: scope.Namespace, ConfigMap: scope.ConfigMap, Deployment: scope.Deployment}} {
		if _, e := writer.ReadConfigMap(context.Background(), other); e == nil {
			t.Fatal("read scope bypass")
		}
		if _, e := writer.ReadDeployment(context.Background(), other); e == nil {
			t.Fatal("deployment read scope bypass")
		}
		if _, e := writer.RestartDeployment(context.Background(), other, "uid", "rv"); e == nil {
			t.Fatal("write scope bypass")
		}
	}
	for _, field := range []string{"uid", "resourceVersion", "name", "namespace"} {
		raw := testConfigMap()
		delete(raw["metadata"].(map[string]any), field)
		object, _ := NewWriteObject(raw)
		if _, e := writer.PutConfigMap(context.Background(), scope, object); CodeOf(e) != "INVALID_ARGUMENT" {
			t.Fatal(field, e)
		}
	}
	raw := testConfigMap()
	raw["kind"] = "Secret"
	object, _ := NewWriteObject(raw)
	if _, e := writer.PutConfigMap(context.Background(), scope, object); CodeOf(e) != "INVALID_ARGUMENT" {
		t.Fatal(e)
	}
	if calls != 0 {
		t.Fatal(calls)
	}
}
func TestWriteFailuresNeverRetryOrExposeBodies(t *testing.T) {
	for _, tc := range []struct {
		status int
		delay  time.Duration
		want   string
	}{{409, 0, "CONFLICT"}, {500, 0, "UPSTREAM_UNAVAILABLE"}, {302, 0, "REDIRECT_BLOCKED"}, {403, 0, "PERMISSION_DENIED"}, {200, 80 * time.Millisecond, "TIMEOUT"}} {
		t.Run(tc.want, func(t *testing.T) {
			var calls atomic.Int32
			client, server := writeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if tc.delay > 0 {
					time.Sleep(tc.delay)
				}
				w.Header().Set("Location", "http://127.0.0.1:1/never")
				w.WriteHeader(tc.status)
				w.Write([]byte("synthetic-error-secret"))
			}, 20*time.Millisecond)
			_ = server
			scope := testWriteScope()
			writer, _ := NewWriter(client, WriteConfig{Enabled: true, Scopes: []WriteScope{scope}})
			_, e := writer.RestartDeployment(context.Background(), scope, "uid", "rv")
			if CodeOf(e) != tc.want || strings.Contains(fmt.Sprint(e), "synthetic-error-secret") {
				t.Fatal(e)
			}
			if calls.Load() != 1 {
				t.Fatal(calls.Load())
			}
		})
	}
}
