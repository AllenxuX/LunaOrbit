package changes

// This adapter is deliberately restricted to the isolated loopback laboratory.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type LabAdapter struct {
	base, key, cluster string
	writes             bool
	client             *http.Client
}

func NewLabAdapter(base, key, cluster string, writes bool) (*LabAdapter, error) {
	u, e := url.Parse(base)
	if e != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || (u.Hostname() != "localhost" && !net.ParseIP(u.Hostname()).IsLoopback()) || key == "" || cluster == "" || strings.ContainsAny(key+cluster, "\r\n") || strings.HasPrefix(strings.ToLower(key), "bearer ") {
		return nil, errors.New("INVALID_ARGUMENT")
	}
	return &LabAdapter{base: base, key: key, cluster: cluster, writes: writes, client: &http.Client{Timeout: 4 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (*LabAdapter) Synthetic() bool { return false }

var labName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,252}$`)

func (a *LabAdapter) call(ctx context.Context, t Target, kind, method string, body []byte) ([]byte, error) {
	name := t.ConfigMapName
	if kind == "deployments" {
		name = t.DeploymentName
	}
	if !labName.MatchString(t.Namespace) || !labName.MatchString(name) {
		return nil, errors.New("INVALID_ARGUMENT")
	}
	path := a.base + "/api/v1/" + kind + "/" + t.Namespace + "/" + name
	if method == http.MethodPatch {
		path += "?patchType=merge"
	}
	req, e := http.NewRequestWithContext(ctx, method, path, bytes.NewReader(body))
	if e != nil {
		return nil, errors.New("ADAPTER_UNAVAILABLE")
	}
	req.Header.Set("Authorization", a.key)
	req.Header.Set("x-cluster-name", a.cluster)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, e := a.client.Do(req)
	if e != nil {
		return nil, errors.New("ADAPTER_UNAVAILABLE")
	}
	defer res.Body.Close()
	if res.StatusCode == 409 {
		return nil, ErrConflict
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, errors.New("ADAPTER_UNAVAILABLE")
	}
	b, e := io.ReadAll(io.LimitReader(res.Body, maxSnapshot+1))
	if e != nil || len(b) > maxSnapshot {
		return nil, errors.New("ADAPTER_UNAVAILABLE")
	}
	return b, nil
}

type labObject struct {
	Metadata struct {
		UID             string `json:"uid"`
		ResourceVersion string `json:"resourceVersion"`
		Generation      int64  `json:"generation"`
	} `json:"metadata"`
	Status struct {
		ObservedGeneration int64 `json:"observedGeneration"`
		ReadyReplicas      int64 `json:"readyReplicas"`
		UpdatedReplicas    int64 `json:"updatedReplicas"`
		AvailableReplicas  int64 `json:"availableReplicas"`
	} `json:"status"`
	Spec struct {
		Replicas *int64 `json:"replicas"`
		Template struct {
			Metadata struct {
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
		} `json:"template"`
	} `json:"spec"`
}

func configDoc(b []byte) (ConfigDocument, error) {
	var o labObject
	if json.Unmarshal(b, &o) != nil || o.Metadata.UID == "" || o.Metadata.ResourceVersion == "" {
		return ConfigDocument{}, errors.New("ADAPTER_UNAVAILABLE")
	}
	return ConfigDocument{UID: o.Metadata.UID, ResourceVersion: o.Metadata.ResourceVersion, Raw: b}, nil
}
func (a *LabAdapter) ReadConfig(ctx context.Context, t Target) (ConfigDocument, error) {
	b, e := a.call(ctx, t, "configmaps", http.MethodGet, nil)
	if e != nil {
		return ConfigDocument{}, e
	}
	return configDoc(b)
}
func (a *LabAdapter) UpdateConfig(ctx context.Context, t Target, d ConfigDocument, rule string) (ConfigDocument, error) {
	if !a.writes {
		return ConfigDocument{}, errors.New("WRITES_DISABLED")
	}
	original, e := configDoc(d.Raw)
	if e != nil || original.UID != t.ConfigMapUID || original.UID != d.UID || original.ResourceVersion != d.ResourceVersion {
		return ConfigDocument{}, ErrConflict
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(d.Raw, &obj) != nil {
		return ConfigDocument{}, ErrConflict
	}
	var data map[string]string
	if json.Unmarshal(obj["data"], &data) != nil {
		return ConfigDocument{}, errors.New("INVALID_ARGUMENT")
	}
	text, ok := data[t.ConfigDataKey]
	if !ok || t.TopicRuleKey == "" {
		return ConfigDocument{}, errors.New("INVALID_ARGUMENT")
	}
	lines := strings.Split(text, "\n")
	found := 0
	for i, line := range lines {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "#") || strings.HasPrefix(trim, "!") {
			continue
		}
		p := strings.IndexAny(trim, "=:")
		if p >= 0 && strings.TrimSpace(trim[:p]) == t.TopicRuleKey {
			found++
			lines[i] = t.TopicRuleKey + " = " + rule
		}
	}
	if found != 1 {
		return ConfigDocument{}, errors.New("INVALID_ARGUMENT")
	}
	data[t.ConfigDataKey] = strings.Join(lines, "\n")
	obj["data"], _ = json.Marshal(data)
	body, _ := json.Marshal(obj)
	b, e := a.call(ctx, t, "configmaps", http.MethodPut, body)
	if e != nil {
		return ConfigDocument{}, e
	}
	return configDoc(b)
}
func depDoc(b []byte) (Deployment, error) {
	var o labObject
	if json.Unmarshal(b, &o) != nil || o.Metadata.UID == "" || o.Metadata.ResourceVersion == "" {
		return Deployment{}, errors.New("ADAPTER_UNAVAILABLE")
	}
	stamp, _ := time.Parse(time.RFC3339Nano, o.Spec.Template.Metadata.Annotations["kite.kubernetes.io/restartedAt"])
	return Deployment{UID: o.Metadata.UID, ResourceVersion: o.Metadata.ResourceVersion, Generation: o.Metadata.Generation, RestartedAt: stamp}, nil
}
func (a *LabAdapter) ReadDeployment(ctx context.Context, t Target) (Deployment, error) {
	b, e := a.call(ctx, t, "deployments", http.MethodGet, nil)
	if e != nil {
		return Deployment{}, e
	}
	return depDoc(b)
}
func (a *LabAdapter) Restart(ctx context.Context, t Target, d Deployment, at time.Time) (Deployment, error) {
	if !a.writes {
		return Deployment{}, errors.New("WRITES_DISABLED")
	}
	if d.UID != t.DeploymentUID || d.ResourceVersion == "" || at.IsZero() {
		return Deployment{}, ErrConflict
	}
	body, _ := json.Marshal(map[string]any{"metadata": map[string]string{"uid": d.UID, "resourceVersion": d.ResourceVersion}, "spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"annotations": map[string]string{"kite.kubernetes.io/restartedAt": at.UTC().Format(time.RFC3339Nano)}}}}})
	b, e := a.call(ctx, t, "deployments", http.MethodPatch, body)
	if e != nil {
		return Deployment{}, e
	}
	return depDoc(b)
}
func (a *LabAdapter) Verify(ctx context.Context, t Target, d Deployment) (Evidence, error) {
	b, e := a.call(ctx, t, "deployments", http.MethodGet, nil)
	if e != nil {
		return Evidence{}, e
	}
	var o labObject
	if json.Unmarshal(b, &o) != nil || o.Metadata.UID != d.UID {
		return Evidence{}, ErrConflict
	}
	replicas := int64(1)
	if o.Spec.Replicas != nil {
		replicas = *o.Spec.Replicas
	}
	return Evidence{Generation: o.Metadata.Generation, ObservedGeneration: o.Status.ObservedGeneration, Ready: replicas > 0 && o.Status.ReadyReplicas == replicas && o.Status.AvailableReplicas == replicas && o.Status.UpdatedReplicas == replicas}, nil
}
