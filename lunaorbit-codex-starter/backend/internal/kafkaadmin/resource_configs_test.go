package kafkaadmin

import (
	"context"
	"github.com/twmb/franz-go/pkg/kmsg"
	"testing"
)

func TestDefaultBrokerCatalogueDoesNotInheritNodeOverrides(t *testing.T) {
	v, def := "node-private-value", "604800000"
	resource := ConfigResource{Type: "BROKER"}
	got := defaultBrokerCatalogue(ConfigSnapshot{Resource: resource, Entries: []ConfigEntry{}}, ConfigSnapshot{Entries: []ConfigEntry{{Name: "log.retention.ms", Value: &v, Source: "DYNAMIC_BROKER_CONFIG", DefaultValue: &def, Synonyms: []ConfigSynonym{{Value: &v, Source: "STATIC_BROKER_CONFIG"}}}}})
	if len(got.Entries) != 1 || got.Entries[0].Value != nil || got.Entries[0].Source != "NO_CLUSTER_DEFAULT_OVERRIDE" || len(got.Entries[0].Synonyms) != 0 || *got.Entries[0].DefaultValue != def {
		t.Fatal("node values must not become cluster-wide defaults", got)
	}
}

type configRequestor struct {
	t     *testing.T
	code  int16
	calls int
	last  *kmsg.IncrementalAlterConfigsRequest
}

func TestReassignmentQuerySelectsActualPartitions(t *testing.T) {
	request := reassignmentQuery("synthetic", []kmsg.MetadataResponseTopicPartition{{Partition: 0}, {Partition: 3}})
	decoded := kmsg.NewPtrListPartitionReassignmentsRequest()
	decoded.SetVersion(request.GetVersion())
	if e := decoded.ReadFrom(request.AppendTo(nil)); e != nil {
		t.Fatal(e)
	}
	if len(decoded.Topics) != 1 || decoded.Topics[0].Topic != "synthetic" || len(decoded.Topics[0].Partitions) != 2 || decoded.Topics[0].Partitions[0] != 0 || decoded.Topics[0].Partitions[1] != 3 {
		t.Fatal("empty partition filter hides all ongoing migrations", decoded.Topics)
	}
}

func (r *configRequestor) Request(_ context.Context, req kmsg.Request) (kmsg.Response, error) {
	switch q := req.(type) {
	case *kmsg.DescribeConfigsRequest:
		if !q.IncludeSynonyms || !q.IncludeDocumentation || q.Resources[0].ConfigNames != nil {
			r.t.Fatal("configs must fetch complete metadata", q)
		}
		v := "123"
		return &kmsg.DescribeConfigsResponse{Resources: []kmsg.DescribeConfigsResponseResource{{ResourceType: q.Resources[0].ResourceType, ResourceName: q.Resources[0].ResourceName, Configs: []kmsg.DescribeConfigsResponseResourceConfig{{Name: "retention.ms", Value: &v, Source: 1}}}}}, nil
	case *kmsg.IncrementalAlterConfigsRequest:
		r.calls++
		r.last = q
		return &kmsg.IncrementalAlterConfigsResponse{Resources: []kmsg.IncrementalAlterConfigsResponseResource{{ResourceType: q.Resources[0].ResourceType, ResourceName: q.Resources[0].ResourceName, ErrorCode: r.code}}}, nil
	}
	r.t.Fatal("unexpected request")
	return nil, nil
}
func TestConfigProtocolMapping(t *testing.T) {
	r := &configRequestor{t: t}
	resource := ConfigResource{Type: "BROKER", Name: "2"}
	if _, e := describeResource(context.Background(), r, resource); e != nil {
		t.Fatal(e)
	}
	v := "123"
	for _, validate := range []bool{true, false} {
		if e := alterResource(context.Background(), r, resource, []ConfigChange{{Name: "retention.ms", Operation: "SET", Value: &v}, {Name: "cleanup.policy", Operation: "DELETE"}}, validate); e != nil {
			t.Fatal(e)
		}
		q := r.last
		if q.ValidateOnly != validate || q.Resources[0].ResourceType != 4 || q.Resources[0].ResourceName != "2" || q.Resources[0].Configs[0].Op != 0 || q.Resources[0].Configs[1].Op != 1 || q.Resources[0].Configs[1].Value != nil {
			t.Fatal(q)
		}
	}
	r.code = 31
	if alterResource(context.Background(), r, resource, []ConfigChange{{Name: "retention.ms", Operation: "DELETE"}}, true) == nil {
		t.Fatal("ignored per resource error")
	}
}
func TestConfigSecretsAndExplicitDefaults(t *testing.T) {
	v := "synthetic-sensitive"
	configs := []kmsg.DescribeConfigsResponseResourceConfig{{Name: "ssl.keystore.password", Value: &v, ConfigSynonyms: []kmsg.DescribeConfigsResponseResourceConfigConfigSynonym{{Name: "ordinary", Value: &v, Source: 5}}}, {Name: "ordinary", Value: &v, IsSensitive: true}, {Name: "no.default", Value: &v, IsDefault: true}, {Name: "explicit", Value: &v, Source: 5}, {Name: "inherited", Source: 1, ConfigSynonyms: []kmsg.DescribeConfigsResponseResourceConfigConfigSynonym{{Name: "inherited", Value: &v, Source: 5}}}}
	s := snapshotConfigs(ConfigResource{}, configs)
	for _, i := range []int{0, 1} {
		if s.Entries[i].Value != nil || s.Entries[i].DefaultValue != nil || !s.Entries[i].Sensitive {
			t.Fatal(s)
		}
	}
	if s.Entries[0].Synonyms[0].Value != nil || s.Entries[2].DefaultValue != nil || s.Entries[3].DefaultValue == nil || s.Entries[4].DefaultValue == nil {
		t.Fatal(s)
	}
}
func TestResourceInputAndReplicaValidation(t *testing.T) {
	for _, r := range []ConfigResource{{"BROKER", "-1"}, {"BROKER", "01"}, {"BROKER", "bad"}, {"TOPIC", "bad/name"}} {
		if _, e := resourceType(r); e == nil {
			t.Fatal(r)
		}
	}
	if _, e := resourceType(ConfigResource{"BROKER", ""}); e != nil {
		t.Fatal(e)
	}
	for _, a := range [][]ReplicaAssignment{{{Partition: 0, Replicas: []int32{1, 1}}}, {{Partition: 0, Replicas: []int32{1}}, {Partition: 0, Replicas: []int32{2}}}, {{Partition: 0}}, {{Partition: -1, Replicas: []int32{1}}}} {
		if validateAssignments(a, false) == nil {
			t.Fatal(a)
		}
	}
}
func TestResourceSocketUnknownBrokerNeverWrites(t *testing.T) {
	f, p, count := managementBroker(t, 0)
	v := "100"
	if e := f.AlterResourceConfigs(context.Background(), p, ConfigResource{"BROKER", "999"}, []ConfigChange{{Name: "retention.ms", Operation: "SET", Value: &v}}); e == nil {
		t.Fatal("unknown broker accepted")
	}
	if count.Load() != 0 {
		t.Fatal("unknown broker mutated")
	}
}
