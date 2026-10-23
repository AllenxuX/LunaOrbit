package kite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// WriteScope is server-owned configuration, never a request DTO. Every field is
// part of the grant, including the ConfigMap/Deployment pair.
type WriteScope struct{ Cluster, Namespace, ConfigMap, Deployment string }
type WriteConfig struct {
	Enabled bool
	Scopes  []WriteScope
}
type Writer struct {
	client      *HTTPClient
	enabled     bool
	scopes      map[WriteScope]struct{}
	mu          sync.Mutex
	lastRestart time.Time
	now         func() time.Time
}

func validScope(s WriteScope) bool {
	return validName(s.Cluster) && validLabel(s.Namespace) && validName(s.ConfigMap) && validName(s.Deployment)
}
func NewWriter(client *HTTPClient, cfg WriteConfig) (*Writer, error) {
	if client == nil {
		return nil, &Error{Code: "INVALID_CONFIG"}
	}
	w := &Writer{client: client, enabled: cfg.Enabled, scopes: map[WriteScope]struct{}{}, now: time.Now}
	for _, s := range cfg.Scopes {
		if !validScope(s) {
			return nil, &Error{Code: "INVALID_CONFIG"}
		}
		w.scopes[s] = struct{}{}
	}
	return w, nil
}
func (w *Writer) authorize(scope WriteScope, write bool) error {
	if write && !w.enabled {
		return &Error{Code: "WRITES_DISABLED"}
	}
	if !validScope(scope) {
		return &Error{Code: "INVALID_ARGUMENT"}
	}
	if _, ok := w.scopes[scope]; !ok {
		return &Error{Code: "SCOPE_DENIED"}
	}
	return nil
}
func (w *Writer) ReadConfigMap(ctx context.Context, scope WriteScope) (Document, error) {
	if e := w.authorize(scope, false); e != nil {
		return Document{}, e
	}
	return w.client.GetConfigMap(ctx, scope.Cluster, scope.Namespace, scope.ConfigMap)
}
func (w *Writer) ReadDeployment(ctx context.Context, scope WriteScope) (Document, error) {
	if e := w.authorize(scope, false); e != nil {
		return Document{}, e
	}
	return w.client.GetDeployment(ctx, scope.Cluster, scope.Namespace, scope.Deployment)
}

// WriteObject is an immutable server-side copy of the complete ConfigMap. Its
// formatting and JSON representation never expose content. It has no raw getter.
type WriteObject struct{ body []byte }

func NewWriteObject(object map[string]any) (WriteObject, error) {
	body, e := json.Marshal(object)
	if e != nil || len(body) > MaxBodyBytes {
		return WriteObject{}, &Error{Code: "INVALID_ARGUMENT"}
	}
	return WriteObject{body: body}, nil
}
func (o WriteObject) String() string                    { return "kite.WriteObject[redacted]" }
func (o WriteObject) GoString() string                  { return o.String() }
func (o WriteObject) Format(state fmt.State, verb rune) { _, _ = state.Write([]byte(o.String())) }
func (o WriteObject) MarshalJSON() ([]byte, error)      { return []byte(`{"content":"redacted"}`), nil }
func validVersion(v string) bool {
	if v == "" || len(v) > 256 || strings.TrimSpace(v) != v {
		return false
	}
	for _, r := range v {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}
func validateConfigMap(scope WriteScope, object WriteObject) bool {
	var resource map[string]any
	if json.Unmarshal(object.body, &resource) != nil {
		return false
	}
	if resource["apiVersion"] != "v1" || resource["kind"] != "ConfigMap" {
		return false
	}
	for key := range resource {
		switch key {
		case "apiVersion", "kind", "metadata", "data", "binaryData", "immutable":
		default:
			return false
		}
	}
	metadata, ok := resource["metadata"].(map[string]any)
	if !ok || metadata["name"] != scope.ConfigMap || metadata["namespace"] != scope.Namespace {
		return false
	}
	uid, uok := metadata["uid"].(string)
	rv, rok := metadata["resourceVersion"].(string)
	if !uok || !rok || !validVersion(uid) || !validVersion(rv) {
		return false
	}
	for _, key := range []string{"data", "binaryData"} {
		if raw, exists := resource[key]; exists && raw != nil {
			m, ok := raw.(map[string]any)
			if !ok {
				return false
			}
			for _, value := range m {
				if _, ok := value.(string); !ok {
					return false
				}
			}
		}
	}
	if value, ok := resource["immutable"]; ok && value != nil {
		if _, ok := value.(bool); !ok {
			return false
		}
	}
	return true
}
func (w *Writer) PutConfigMap(ctx context.Context, scope WriteScope, object WriteObject) (Document, error) {
	if e := w.authorize(scope, true); e != nil {
		return Document{}, e
	}
	if !validateConfigMap(scope, object) {
		return Document{}, &Error{Code: "INVALID_ARGUMENT"}
	}
	return w.write(ctx, scope, "configmaps", http.MethodPut, object.body)
}
func (w *Writer) RestartDeployment(ctx context.Context, scope WriteScope, uid, resourceVersion string) (Document, error) {
	if e := w.authorize(scope, true); e != nil {
		return Document{}, e
	}
	if !validVersion(uid) || !validVersion(resourceVersion) {
		return Document{}, &Error{Code: "INVALID_ARGUMENT"}
	}
	w.mu.Lock()
	now := w.now().UTC()
	if !now.After(w.lastRestart) {
		now = w.lastRestart.Add(time.Nanosecond)
	}
	w.lastRestart = now
	w.mu.Unlock()
	patch := map[string]any{"metadata": map[string]any{"uid": uid, "resourceVersion": resourceVersion}, "spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"annotations": map[string]string{"kite.kubernetes.io/restartedAt": now.Format(time.RFC3339Nano)}}}}}
	body, e := json.Marshal(patch)
	if e != nil {
		return Document{}, &Error{Code: "INVALID_ARGUMENT"}
	}
	return w.write(ctx, scope, "deployments", http.MethodPatch, body)
}
func (w *Writer) write(ctx context.Context, scope WriteScope, kind, method string, body []byte) (Document, error) {
	// Fixed call sites supply kind/method; no public arbitrary request interface.
	name := scope.ConfigMap
	query := ""
	if kind == "deployments" {
		name = scope.Deployment
		query = "?patchType=merge"
	}
	ctx, cancel := context.WithTimeout(ctx, w.client.timeout)
	defer cancel()
	if ctx.Err() != nil {
		return Document{}, contextError(ctx)
	}
	req, e := http.NewRequestWithContext(ctx, method, w.client.baseURL+"/api/v1/"+kind+"/"+scope.Namespace+"/"+name+query, bytes.NewReader(body))
	if e != nil {
		return Document{}, &Error{Code: "INVALID_ARGUMENT"}
	}
	req.Header.Set("Authorization", w.client.key)
	req.Header.Set("x-cluster-name", scope.Cluster)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	response, e := w.client.http.Do(req)
	if e != nil {
		if ctx.Err() != nil {
			return Document{}, contextError(ctx)
		}
		var timeout net.Error
		if errors.As(e, &timeout) && timeout.Timeout() {
			return Document{}, &Error{Code: "TIMEOUT"}
		}
		return Document{}, &Error{Code: "NETWORK_ERROR"}
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusConflict {
		return Document{}, &Error{Code: "CONFLICT", HTTPStatus: http.StatusConflict}
	}
	if response.StatusCode != http.StatusOK {
		return Document{}, statusError(response.StatusCode)
	}
	result, e := io.ReadAll(io.LimitReader(response.Body, MaxBodyBytes+1))
	if e != nil {
		return Document{}, contextError(ctx)
	}
	if len(result) > MaxBodyBytes {
		return Document{}, &Error{Code: "RESPONSE_TOO_LARGE"}
	}
	// A 200 response is only an opaque Document. A verified deployed-version
	// ResourceDecoder must inspect it before any caller reports observed success.
	return Document{kind: kind, single: true, body: result}, nil
}
