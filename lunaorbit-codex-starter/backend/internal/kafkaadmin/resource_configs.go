package kafkaadmin

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"lunaorbit/backend/internal/kafkaops"
	"strconv"
	"strings"
)

type ConfigResource struct {
	Type string `json:"type"`
	Name string `json:"name"`
}
type ConfigSynonym struct {
	Name   string  `json:"name"`
	Value  *string `json:"value"`
	Source string  `json:"source"`
}
type ConfigEntry struct {
	Name          string          `json:"name"`
	Value         *string         `json:"value"`
	Source        string          `json:"source"`
	Sensitive     bool            `json:"sensitive"`
	ReadOnly      bool            `json:"read_only"`
	Type          string          `json:"type"`
	Documentation string          `json:"documentation"`
	Synonyms      []ConfigSynonym `json:"synonyms"`
	DefaultValue  *string         `json:"default_value"`
}
type ConfigSnapshot struct {
	Resource ConfigResource `json:"resource"`
	Entries  []ConfigEntry  `json:"entries"`
}
type ConfigChange struct {
	Name      string  `json:"name"`
	Operation string  `json:"operation"`
	Value     *string `json:"value"`
}

func secretConfig(name string) bool {
	n := strings.ToLower(name)
	for _, s := range []string{"password", "secret", "credential", "jaas", "token", "private.key", "private_key", "keystore.key", "ssl.key", "access.key", "access_key"} {
		if strings.Contains(n, s) {
			return true
		}
	}
	return false
}
func resourceType(r ConfigResource) (kmsg.ConfigResourceType, error) {
	switch r.Type {
	case "TOPIC":
		if validTopic(r.Name) {
			return 2, nil
		}
	case "BROKER":
		if r.Name == "" {
			return 4, nil
		}
		if id, e := strconv.ParseInt(r.Name, 10, 32); e == nil && id >= 0 && strconv.FormatInt(id, 10) == r.Name {
			return 4, nil
		}
	}
	return 0, invalidManagement()
}
func configTarget(ctx context.Context, cl *kgo.Client, r ConfigResource) (kmsg.Requestor, error) {
	if r.Type != "BROKER" {
		return cl, nil
	}
	m, e := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if e != nil {
		return nil, &kafkaops.Error{Code: operationCode(e)}
	}
	if r.Name == "" {
		for _, b := range m.Brokers {
			if b.NodeID == m.ControllerID {
				return cl.Broker(int(b.NodeID)), nil
			}
		}
		if len(m.Brokers) > 0 {
			return cl.Broker(int(m.Brokers[0].NodeID)), nil
		}
		return nil, malformedManagement()
	}
	id, _ := strconv.Atoi(r.Name)
	for _, b := range m.Brokers {
		if int(b.NodeID) == id {
			return cl.Broker(id), nil
		}
	}
	return nil, invalidManagement()
}
func snapshotConfigs(r ConfigResource, configs []kmsg.DescribeConfigsResponseResourceConfig) ConfigSnapshot {
	out := ConfigSnapshot{Resource: r, Entries: []ConfigEntry{}}
	for _, c := range configs {
		sensitive := c.IsSensitive || secretConfig(c.Name) || c.ConfigType.String() == "PASSWORD"
		x := ConfigEntry{Name: c.Name, Value: c.Value, Source: c.Source.String(), Sensitive: sensitive, ReadOnly: c.ReadOnly, Type: c.ConfigType.String(), Synonyms: []ConfigSynonym{}}
		if c.Documentation != nil {
			x.Documentation = *c.Documentation
		}
		for _, s := range c.ConfigSynonyms {
			v := s.Value
			if sensitive || secretConfig(s.Name) {
				v = nil
			}
			x.Synonyms = append(x.Synonyms, ConfigSynonym{Name: s.Name, Value: v, Source: s.Source.String()})
			if s.Source == 5 && !sensitive && !secretConfig(s.Name) {
				x.DefaultValue = v
			}
		}
		if c.Source == 5 && !sensitive {
			x.DefaultValue = c.Value
		}
		if sensitive {
			x.Value = nil
			x.DefaultValue = nil
		}
		out.Entries = append(out.Entries, x)
	}
	return out
}
func describeResource(ctx context.Context, target kmsg.Requestor, r ConfigResource) (ConfigSnapshot, error) {
	typ, e := resourceType(r)
	if e != nil {
		return ConfigSnapshot{}, e
	}
	req := kmsg.NewPtrDescribeConfigsRequest()
	req.IncludeSynonyms = true
	req.IncludeDocumentation = true
	req.Resources = []kmsg.DescribeConfigsRequestResource{{ResourceType: typ, ResourceName: r.Name}}
	resp, e := req.RequestWith(ctx, target)
	if e != nil {
		return ConfigSnapshot{}, &kafkaops.Error{Code: operationCode(e)}
	}
	if len(resp.Resources) != 1 || resp.Resources[0].ResourceName != r.Name || resp.Resources[0].ResourceType != typ {
		return ConfigSnapshot{}, malformedManagement()
	}
	if e = managementError(resp.Resources[0].ErrorCode); e != nil {
		return ConfigSnapshot{}, e
	}
	return snapshotConfigs(r, resp.Resources[0].Configs), nil
}
func (f *Factory) DescribeResourceConfigs(ctx context.Context, p Profile, r ConfigResource) (out ConfigSnapshot, err error) {
	if _, err = resourceType(r); err != nil {
		return
	}
	err = f.managementClient(ctx, p, func(ctx context.Context, cl *kgo.Client) error {
		target, e := configTarget(ctx, cl, r)
		if e != nil {
			return e
		}
		out, e = describeResolvedResource(ctx, cl, target, r)
		return e
	})
	return
}

// The empty broker resource returns only explicit cluster-wide overrides. Use a
// real broker as a schema catalogue, never as the cluster's effective value:
// static and per-node overrides can differ between brokers.
func defaultBrokerCatalogue(overrides, catalogue ConfigSnapshot) ConfigSnapshot {
	seen := map[string]bool{}
	for _, entry := range overrides.Entries {
		seen[entry.Name] = true
	}
	for _, entry := range catalogue.Entries {
		if seen[entry.Name] {
			continue
		}
		entry.Value = nil
		entry.Source = "NO_CLUSTER_DEFAULT_OVERRIDE"
		entry.Synonyms = []ConfigSynonym{}
		overrides.Entries = append(overrides.Entries, entry)
	}
	return overrides
}
func describeResolvedResource(ctx context.Context, cl *kgo.Client, target kmsg.Requestor, r ConfigResource) (ConfigSnapshot, error) {
	out, e := describeResource(ctx, target, r)
	if e != nil || r.Type != "BROKER" || r.Name != "" {
		return out, e
	}
	m, e := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if e != nil {
		return ConfigSnapshot{}, &kafkaops.Error{Code: operationCode(e)}
	}
	if len(m.Brokers) == 0 {
		return ConfigSnapshot{}, malformedManagement()
	}
	// Stable ordering keeps previews independent of Metadata response ordering.
	id := m.Brokers[0].NodeID
	for _, b := range m.Brokers {
		if b.NodeID < id {
			id = b.NodeID
		}
	}
	catalogue, e := describeResource(ctx, cl.Broker(int(id)), ConfigResource{Type: "BROKER", Name: strconv.Itoa(int(id))})
	if e != nil {
		return ConfigSnapshot{}, e
	}
	return defaultBrokerCatalogue(out, catalogue), nil
}
func validateChanges(r ConfigResource, changes []ConfigChange) error {
	if _, e := resourceType(r); e != nil {
		return e
	}
	if r.Type == "TOPIC" && strings.HasPrefix(r.Name, "__") {
		return invalidManagement()
	}
	if len(changes) == 0 || len(changes) > 1000 {
		return invalidManagement()
	}
	seen := map[string]bool{}
	for _, c := range changes {
		if c.Name == "" || len(c.Name) > 512 || seen[c.Name] || secretConfig(c.Name) || (c.Operation != "SET" && c.Operation != "DELETE") || (c.Operation == "SET" && c.Value == nil) || (c.Operation == "DELETE" && c.Value != nil) {
			return invalidManagement()
		}
		seen[c.Name] = true
	}
	return nil
}
func alterResource(ctx context.Context, target kmsg.Requestor, r ConfigResource, changes []ConfigChange, validate bool) error {
	typ, _ := resourceType(r)
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	req.ValidateOnly = validate
	res := kmsg.IncrementalAlterConfigsRequestResource{ResourceType: typ, ResourceName: r.Name}
	for _, c := range changes {
		op := kmsg.IncrementalAlterConfigOp(0)
		if c.Operation == "DELETE" {
			op = 1
		}
		res.Configs = append(res.Configs, kmsg.IncrementalAlterConfigsRequestResourceConfig{Name: c.Name, Value: c.Value, Op: op})
	}
	req.Resources = []kmsg.IncrementalAlterConfigsRequestResource{res}
	resp, e := req.RequestWith(ctx, target)
	if e != nil {
		return &kafkaops.Error{Code: operationCode(e)}
	}
	if len(resp.Resources) != 1 || resp.Resources[0].ResourceName != r.Name || resp.Resources[0].ResourceType != typ {
		return malformedManagement()
	}
	return managementError(resp.Resources[0].ErrorCode)
}
func (f *Factory) changeResourceConfigs(ctx context.Context, p Profile, r ConfigResource, changes []ConfigChange, validate bool) error {
	if e := validateChanges(r, changes); e != nil {
		return e
	}
	return f.managementClient(ctx, p, func(ctx context.Context, cl *kgo.Client) error {
		target, e := configTarget(ctx, cl, r)
		if e != nil {
			return e
		}
		if r.Type == "TOPIC" {
			m, err := topicMetadata(ctx, cl, r.Name)
			if err != nil {
				return err
			}
			if m.Topics[0].IsInternal {
				return invalidManagement()
			}
		}
		snapshot, e := describeResolvedResource(ctx, cl, target, r)
		if e != nil {
			return e
		}
		entries := map[string]ConfigEntry{}
		for _, c := range snapshot.Entries {
			entries[c.Name] = c
		}
		for _, change := range changes {
			c, ok := entries[change.Name]
			if !ok || c.Sensitive || c.ReadOnly {
				return invalidManagement()
			}
		}
		if e = alterResource(ctx, target, r, changes, true); e != nil {
			return e
		}
		if validate {
			return nil
		}
		return alterResource(ctx, target, r, changes, false)
	})
}
func (f *Factory) ValidateResourceConfigs(ctx context.Context, p Profile, r ConfigResource, c []ConfigChange) error {
	return f.changeResourceConfigs(ctx, p, r, c, true)
}
func (f *Factory) AlterResourceConfigs(ctx context.Context, p Profile, r ConfigResource, c []ConfigChange) error {
	return f.changeResourceConfigs(ctx, p, r, c, false)
}
