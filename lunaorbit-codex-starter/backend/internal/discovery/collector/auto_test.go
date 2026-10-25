package collector

import (
	"context"
	"fmt"
	"lunaorbit/backend/internal/discovery/resource"
	"lunaorbit/backend/internal/kite"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func dep(ns, name, uid, image, command, cm string) string {
	cmd := "[]"
	if command != "" {
		cmd = fmt.Sprintf("[%q,\"/etc/mm2/mm2.properties\"]", command)
	}
	vol := "[]"
	if cm != "" {
		vol = fmt.Sprintf(`[{"name":"cfg","configMap":{"name":%q}}]`, cm)
	}
	return fmt.Sprintf(`{"metadata":{"name":%q,"namespace":%q,"uid":%q,"resourceVersion":"1"},"spec":{"template":{"spec":{"volumes":%s,"containers":[{"name":"c","image":%q,"command":%s}]}}},"status":{}}`, name, ns, uid, vol, image, cmd)
}

type autoServer struct {
	mu    sync.Mutex
	paths []string
}

func newAutoSource(t *testing.T, clusters map[string][]string) (AutoKiteSource, *autoServer) {
	t.Helper()
	rec := &autoServer{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cluster := r.Header.Get("x-cluster-name")
		rec.mu.Lock()
		rec.paths = append(rec.paths, cluster+" "+r.URL.Path)
		rec.mu.Unlock()
		deps, ok := clusters[cluster]
		if !ok {
			w.WriteHeader(404)
			return
		}
		envelope := func(items string) {
			_, _ = fmt.Fprintf(w, `{"metadata":{"continue":"continue-not-supported"},"items":[%s]}`, items)
		}
		switch {
		case r.URL.Path == "/api/v1/deployments":
			envelope(strings.Join(deps, ","))
		case strings.HasPrefix(r.URL.Path, "/api/v1/configmaps/"):
			ns := strings.TrimPrefix(r.URL.Path, "/api/v1/configmaps/")
			envelope(fmt.Sprintf(`{"metadata":{"name":"mm2-config","namespace":%q,"uid":"cm-%s","resourceVersion":"1"},"data":{"mm2.properties":"clusters=a,b\na.bootstrap.servers=a:9092\nb.bootstrap.servers=b:9092\na->b.enabled=true\na->b.topics=.*"}}`, ns, ns))
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(s.Close)
	c, err := kite.New(kite.Config{BaseURL: s.URL, AllowedOrigins: []string{s.URL}, APIKey: "synthetic-auto-key", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return AutoKiteSource{Client: c, Decoder: kite.LabV016Decoder{}, ExcludeNamespaces: DefaultExcludedNamespaces()}, rec
}

func TestAutoCollectScansAllNamespacesOnceAndFetchesConfigOnlyWhereMM2IsLikely(t *testing.T) {
	src, rec := newAutoSource(t, map[string][]string{"edge-a": {
		dep("luna-kafka", "luna-mm2-k1-to-k2", "u1", "apache/kafka:3.9.2", "/opt/kafka/bin/connect-mirror-maker.sh", "mm2-config"),
		dep("legacy", "mm2", "u2", "apache/kafka:3.9.2", "connect-mirror-maker.sh", "mm2-config"),
		dep("odd", "replicator", "u3", "example/mirrormaker:1", "", ""),
		dep("apps", "web", "u4", "nginx:1", "", ""),
		dep("kube-system", "mm2", "u5", "apache/kafka:3.9.2", "connect-mirror-maker.sh", "mm2-config"),
	}})
	scope := resource.Scope{ID: "k8s-edge-a", Cluster: "edge-a", Auto: true}
	col, err := src.Collect(context.Background(), scope)
	if err != nil || col.ErrorCode != "" || !col.Scope.Auto {
		t.Fatalf("collect %v %q", err, col.ErrorCode)
	}
	if col.NamespacesScanned != 4 || col.DeploymentsScanned != 4 {
		t.Fatalf("scanned ns=%d deps=%d", col.NamespacesScanned, col.DeploymentsScanned)
	}
	var names []string
	for _, b := range col.Namespaces {
		names = append(names, b.Namespace)
		if len(b.Pods) != 0 || !b.Complete {
			t.Fatalf("batch %s pods=%d complete=%v", b.Namespace, len(b.Pods), b.Complete)
		}
		for _, d := range b.Deployments {
			if len(b.Signals[d.Metadata.UID]) == 0 {
				t.Fatalf("candidate %s without signals", d.Metadata.Name)
			}
		}
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "legacy,luna-kafka,odd" {
		t.Fatalf("candidate namespaces %v", names)
	}
	for _, p := range rec.paths {
		if strings.Contains(p, "/pods") || strings.Contains(p, "kube-system") || strings.Contains(p, "/configmaps/apps") {
			t.Fatalf("unexpected Kite request %s", p)
		}
	}
	deployCalls := 0
	for _, p := range rec.paths {
		if strings.HasSuffix(p, "/api/v1/deployments") {
			deployCalls++
		}
	}
	if deployCalls != 1 {
		t.Fatalf("deployments listed %d times", deployCalls)
	}
}

func TestAutoCollectReportsRemovedClusterAsNotFound(t *testing.T) {
	src, _ := newAutoSource(t, map[string][]string{})
	col, err := src.Collect(context.Background(), resource.Scope{ID: "k8s-gone", Cluster: "gone", Auto: true})
	if err == nil || col.ErrorCode != "NOT_FOUND" {
		t.Fatalf("expected NOT_FOUND, got %v %q", err, col.ErrorCode)
	}
}

func TestMM2SignalsDoNotRelyOnLabels(t *testing.T) {
	var d resource.Deployment
	d.Metadata.Labels = map[string]string{"app": "mm2"}
	d.Spec.Template.Spec.Containers = []resource.Container{{Image: "nginx:1"}}
	if len(MM2Signals(d)) != 0 {
		t.Fatal("a label alone must not mark a deployment as MM2")
	}
	d.Spec.Template.Spec.Containers = []resource.Container{{Image: "apache/kafka:3.9.2", Command: []string{"/opt/kafka/bin/connect-mirror-maker.sh"}}}
	if got := MM2Signals(d); len(got) == 0 || got[0] != "command:connect-mirror-maker.sh" {
		t.Fatalf("signals %v", got)
	}
}
