package collector_test

import (
	"context"
	"encoding/json"
	"errors"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/discovery/collector"
	"lunaorbit/backend/internal/discovery/resource"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/kite"
	"os"
	"testing"
	"time"
)

// Test-only decoder for the v0.16.0 envelope observed on the dedicated lab.
// It is deliberately unavailable to production runtime configuration.
type labV016Decoder struct{}

func (labV016Decoder) DecodeResource(kind string, single bool, body []byte, target any) error {
	if kind != "namespaces" && kind != "configmaps" && kind != "deployments" && kind != "pods" {
		return errors.New("unsupported lab resource")
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil || object["metadata"] == nil {
		return errors.New("unexpected lab envelope")
	}
	if single {
		var meta resource.Metadata
		if json.Unmarshal(object["metadata"], &meta) != nil || meta.Name == "" || meta.UID == "" {
			return errors.New("invalid lab object metadata")
		}
		return json.Unmarshal(body, target)
	}
	var meta struct {
		Continue string `json:"continue"`
	}
	var items []json.RawMessage
	if json.Unmarshal(object["metadata"], &meta) != nil || meta.Continue != "" ||
		json.Unmarshal(object["items"], &items) != nil || items == nil || len(items) > collector.MaxResources {
		return errors.New("unsupported or incomplete lab list")
	}
	return json.Unmarshal(object["items"], target)
}

func TestMinikubeKiteReadAndDiscovery(t *testing.T) {
	base := os.Getenv("LUNAORBIT_KITE_LAB_URL")
	if base == "" {
		t.Skip("dedicated Minikube Kite lab not configured")
	}
	if base != "http://127.0.0.1:28080" || os.Getenv("LUNAORBIT_KITE_LAB_VERSION") != "v0.16.0" {
		t.Fatal("dedicated loopback lab and verified version required")
	}
	client, err := kite.New(kite.Config{BaseURL: base, AllowedOrigins: []string{base},
		APIKey: os.Getenv("LUNAORBIT_KITE_LAB_KEY"), Timeout: 15 * time.Second})
	if err != nil {
		t.Fatal("lab transport configuration failed")
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const cluster = "lunaorbit-lab"
	const ns = "lunaorbit-integration"
	source := collector.KiteSource{Client: client, Decoder: labV016Decoder{}}
	col, err := source.Collect(ctx, resource.Scope{ID: "lab-integration", Cluster: cluster, Synthetic: true})
	if err != nil || len(col.Namespaces) != 1 || col.Namespaces[0].Namespace != ns || !col.Namespaces[0].Complete {
		t.Fatal("real Kite collection failed", kite.CodeOf(err))
	}
	batch := col.Namespaces[0]
	var podName string
	for _, pod := range batch.Pods {
		if pod.Metadata.Labels["app"] == "mm2" && pod.Status.Phase == "Running" {
			podName = pod.Metadata.Name
		}
	}
	if podName == "" {
		t.Fatal("running MM2 Pod not found")
	}
	cmDoc, err := client.GetConfigMap(ctx, cluster, ns, "mm2-config")
	var cm resource.ConfigMap
	if err != nil || cmDoc.DecodeResource(labV016Decoder{}, &cm) != nil || cm.Data["mm2.properties"] == "" {
		t.Fatal("real ConfigMap GET failed")
	}
	depDoc, err := client.GetDeployment(ctx, cluster, ns, "mm2")
	var dep resource.Deployment
	if err != nil || depDoc.DecodeResource(labV016Decoder{}, &dep) != nil || dep.Status.ReadyReplicas != 1 {
		t.Fatal("real Deployment GET failed")
	}
	podDoc, err := client.GetPod(ctx, cluster, ns, podName)
	var pod resource.Pod
	if err != nil || podDoc.DecodeResource(labV016Decoder{}, &pod) != nil || pod.Status.Phase != "Running" {
		t.Fatal("real Pod GET failed")
	}
	snapshot, err := discovery.Build(col, nil, nil, nil, discovery.Snapshot{})
	if err != nil {
		t.Fatal("real discovery failed")
	}
	found := false
	for _, link := range snapshot.Links {
		if link.SourceAlias == "source" && link.TargetAlias == "target" {
			found = true
			if !link.Enabled || !link.Declared || !link.Observed || link.Verified || link.Stale ||
				link.WorkloadUID != dep.Metadata.UID || len(link.Evidence) == 0 ||
				link.TopicsRaw != "lunaorbit-lab-events" || link.Evidence[0].ConfigMapUID != cm.Metadata.UID {
				t.Fatal("real discovery evidence/state mismatch")
			}
		}
	}
	if !found || len(snapshot.Nodes) < 3 || len(snapshot.Edges) < 2 {
		t.Fatal("real MM2 direction or graph missing")
	}
	t.Log("seven real Kite GETs, namespace-scoped collection, MM2 UID/config evidence and declared/observed graph PASS; verified remains false")
}

func TestMinikubeKafkaNativeReads(t *testing.T) {
	if os.Getenv("LUNAORBIT_MINIKUBE_KAFKA") != "1" {
		t.Skip("dedicated in-Pod Kafka lab not configured")
	}
	namespace, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil || string(namespace) != "lunaorbit-integration" {
		t.Fatal("test requires dedicated lab Pod")
	}
	policy, err := kafkaadmin.NewNetworkPolicy(kafkaadmin.NetworkPolicyConfig{
		AllowedCIDRs: []string{"10.96.0.0/12", "10.244.0.0/16"}, AllowedPorts: []int{9092}})
	if err != nil {
		t.Fatal("lab network policy invalid")
	}
	factory, err := kafkaadmin.NewFactory(policy, nil)
	if err != nil {
		t.Fatal("lab Kafka factory invalid")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var previousID string
	for _, alias := range []string{"source", "target"} {
		profile := kafkaadmin.Profile{BootstrapServers: []string{"kafka-" + alias + ".lunaorbit-integration.svc.cluster.local:9092"}, SecurityProtocol: "PLAINTEXT"}
		probe, err := factory.Probe(ctx, profile)
		if err != nil || probe.ClusterID == "" || probe.ClusterID == previousID || len(probe.Brokers) != 1 {
			t.Fatalf("%s probe failed: code=%s, has_id=%t, repeated_id=%t, brokers=%d", alias,
				kafkaadmin.CodeOf(err), probe.ClusterID != "", probe.ClusterID == previousID, len(probe.Brokers))
		}
		previousID = probe.ClusterID
		topic := "lunaorbit-lab-events"
		if alias == "target" {
			topic = "source." + topic
		}
		data, err := factory.Read(ctx, profile, "topics", topic)
		if err != nil || len(data.Topics) != 1 {
			t.Fatal("native topic read failed")
		}
		detail := data.Topics[0]
		if detail.RFMin == nil || *detail.RFMin != 1 || len(detail.Partitions) != 1 || len(detail.Partitions[0].ISR) != 1 {
			t.Fatal("actual topic RF/ISR mismatch")
		}
		groups, err := factory.Read(ctx, profile, "groups", "")
		if err != nil || groups.Synthetic {
			t.Fatal("native group read failed")
		}
	}
	t.Log("actual franz-go seed/advertised reachability, distinct Cluster IDs, Topic RF/ISR and Groups PASS on both lab clusters")
}
