package changes

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type scenarioKey struct{}

func DefaultCatalog() []Target {
	return []Target{{ID: "synthetic-mars-topic-rule", Name: "Synthetic mars → moon-prod topic rule", ScopeID: "synthetic-mm2", Namespace: "synthetic-mm2", WorkloadUID: "synthetic-mars-uid", ConfigMapUID: "synthetic-mars-cm", DeploymentUID: "synthetic-mars-uid", ResourceVersion: "1", TopicRuleKey: "mars->moon-prod.topics", Synthetic: true}}
}
func (d ConfigDocument) MarshalJSON() ([]byte, error) {
	return []byte(`"[redacted ConfigDocument]"`), nil
}
func (d ConfigDocument) String() string             { return "[redacted ConfigDocument]" }
func (d ConfigDocument) Format(f fmt.State, v rune) { fmt.Fprint(f, "[redacted ConfigDocument]") }

type MockAdapter struct {
	mu                sync.Mutex
	configs           map[string]ConfigDocument
	deployments       map[string]Deployment
	Updates, Restarts int
}

func NewMockAdapter(catalog ...[]Target) *MockAdapter {
	rows := DefaultCatalog()
	if len(catalog) > 0 {
		rows = catalog[0]
	}
	m := &MockAdapter{configs: map[string]ConfigDocument{}, deployments: map[string]Deployment{}}
	for _, t := range rows {
		raw, _ := json.Marshal(map[string]any{"metadata": map[string]string{"uid": t.ConfigMapUID, "resourceVersion": "1"}, "data": map[string]string{"mm2.properties": t.TopicRuleKey + "=orders.*\npassword=DO_NOT_EXPOSE_SYNTHETIC_SECRET\n"}})
		m.configs[t.ID] = ConfigDocument{UID: t.ConfigMapUID, ResourceVersion: "1", Raw: raw}
		m.deployments[t.ID] = Deployment{UID: t.DeploymentUID, ResourceVersion: "1", Generation: 1}
	}
	return m
}
func (m *MockAdapter) Synthetic() bool { return true }
func (m *MockAdapter) ReadConfig(ctx context.Context, t Target) (ConfigDocument, error) {
	if ctx.Value(scenarioKey{}) == "timeout" {
		<-ctx.Done()
		return ConfigDocument{}, ctx.Err()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.configs[t.ID]
	if !ok {
		return d, ErrNotFound
	}
	d.Raw = append([]byte(nil), d.Raw...)
	return d, nil
}
func (m *MockAdapter) ReadDeployment(ctx context.Context, t Target) (Deployment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.deployments[t.ID]
	if !ok {
		return d, ErrNotFound
	}
	return d, nil
}
func (m *MockAdapter) UpdateConfig(ctx context.Context, t Target, old ConfigDocument, topic string) (ConfigDocument, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.configs[t.ID]
	if !ok || ctx.Value(scenarioKey{}) == "conflict" || d.UID != old.UID || d.ResourceVersion != old.ResourceVersion {
		return ConfigDocument{}, ErrConflict
	}
	if ctx.Err() != nil {
		return ConfigDocument{}, ctx.Err()
	}
	var body struct {
		Metadata map[string]string `json:"metadata"`
		Data     map[string]string `json:"data"`
	}
	if json.Unmarshal(d.Raw, &body) != nil {
		return ConfigDocument{}, ErrStorage
	}
	text := body.Data["mm2.properties"]
	lines := strings.Split(text, "\n")
	matches := 0
	for i, line := range lines {
		if strings.HasPrefix(line, t.TopicRuleKey+"=") {
			lines[i] = t.TopicRuleKey + "=" + topic
			matches++
		}
	}
	if matches != 1 {
		return ConfigDocument{}, ErrConflict
	}
	body.Data["mm2.properties"] = strings.Join(lines, "\n")
	n, _ := strconv.Atoi(d.ResourceVersion)
	d.ResourceVersion = strconv.Itoa(n + 1)
	body.Metadata["resourceVersion"] = d.ResourceVersion
	d.Raw, _ = json.Marshal(body)
	m.configs[t.ID] = d
	m.Updates++
	return d, nil
}
func (m *MockAdapter) Restart(ctx context.Context, t Target, old Deployment, stamp time.Time) (Deployment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.deployments[t.ID]
	if !ok || d.UID != old.UID || d.ResourceVersion != old.ResourceVersion || !stamp.After(d.RestartedAt) || stamp.Location() != time.UTC {
		return Deployment{}, ErrConflict
	}
	if ctx.Err() != nil {
		return Deployment{}, ctx.Err()
	}
	n, _ := strconv.Atoi(d.ResourceVersion)
	d.ResourceVersion = strconv.Itoa(n + 1)
	d.Generation++
	d.RestartedAt = stamp
	m.deployments[t.ID] = d
	m.Restarts++
	return d, nil
}
func (m *MockAdapter) Verify(ctx context.Context, t Target, d Deployment) (Evidence, error) {
	v := true
	e := Evidence{Generation: d.Generation, ObservedGeneration: d.Generation, Ready: true, ReplicationHealthy: &v}
	switch ctx.Value(scenarioKey{}) {
	case "rollout_failed":
		e.ObservedGeneration--
		e.Ready = false
	case "metrics_missing":
		e.ReplicationHealthy = nil
	}
	return e, nil
}
