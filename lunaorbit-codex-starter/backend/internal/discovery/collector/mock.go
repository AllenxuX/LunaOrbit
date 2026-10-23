package collector

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"lunaorbit/backend/internal/discovery/resource"
	"lunaorbit/backend/internal/kite"
	"net/http"
	"net/http/httptest"
	"time"
)

// syntheticDecoder understands only our explicitly synthetic test envelope.
// It is intentionally not exported or usable as a real Kite response adapter.
type syntheticDecoder struct{}

func (syntheticDecoder) DecodeResource(kind string, single bool, body []byte, target any) error {
	var envelope struct {
		Marker string          `json:"synthetic_resource_fixture"`
		Kind   string          `json:"kind"`
		Items  json.RawMessage `json:"items"`
	}
	if single || json.Unmarshal(body, &envelope) != nil || envelope.Marker != "v1" || envelope.Kind != kind {
		return fmt.Errorf("invalid synthetic fixture")
	}
	return json.Unmarshal(envelope.Items, target)
}
func NewMock(scenario string) (Source, resource.Scope, func(), error) {
	scope := resource.Scope{ID: "synthetic-mm2", Cluster: "synthetic-kite", Synthetic: true}
	if scenario != "success" && scenario != "partial_failure" && scenario != "unavailable" {
		return nil, scope, func() {}, &kite.Error{Code: "INVALID_ARGUMENT"}
	}
	keyBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, scope, func() {}, &kite.Error{Code: "INTERNAL_ERROR"}
	}
	key := hex.EncodeToString(keyBytes)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != key || r.Header.Get("x-cluster-name") != scope.Cluster {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if scenario == "unavailable" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var kind, items string
		switch r.URL.Path {
		case "/api/v1/namespaces/_all":
			kind = "namespaces"
			items = `[{"metadata":{"name":"synthetic-mm2"}}]`
		case "/api/v1/deployments/synthetic-mm2":
			kind = "deployments"
			items = mockDeployments
		case "/api/v1/configmaps/synthetic-mm2":
			if scenario == "partial_failure" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			kind = "configmaps"
			items = mockConfigMaps
		case "/api/v1/pods/synthetic-mm2":
			kind = "pods"
			items = mockPods
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"synthetic_resource_fixture":"v1","kind":%q,"items":%s}`, kind, items)
	}))
	client, err := kite.New(kite.Config{BaseURL: server.URL, AllowedOrigins: []string{server.URL}, APIKey: key, Timeout: time.Second})
	if err != nil {
		server.Close()
		return nil, scope, func() {}, err
	}
	return KiteSource{Client: client, Decoder: syntheticDecoder{}}, scope, func() { client.Close(); server.Close() }, nil
}

const mockDeployments = `[
{"metadata":{"name":"mars2moon","namespace":"synthetic-mm2","uid":"synthetic-mars-uid","generation":1},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"mars2moon"}},"template":{"spec":{"volumes":[{"name":"config","configMap":{"name":"mars-config"}}],"containers":[{"name":"mm2","image":"synthetic/mm2:test","command":["connect-mirror-maker.sh"],"args":["/config/mm2.properties"]},{"name":"sidecar","envFrom":[{"configMapRef":{"name":"mars-config"}}]}],"initContainers":[{"name":"init","env":[{"valueFrom":{"configMapKeyRef":{"name":"mars-config","key":"mm2.properties"}}}]}]}}},"status":{"observedGeneration":1,"replicas":1,"readyReplicas":1}},
{"metadata":{"name":"charon2moon","namespace":"synthetic-mm2","uid":"synthetic-charon-uid","generation":1},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"charon2moon"}},"template":{"spec":{"containers":[{"name":"mm2","image":"synthetic/mm2:test","envFrom":[{"configMapRef":{"name":"charon-config"}}]}]}}},"status":{"observedGeneration":1,"readyReplicas":1}},
{"metadata":{"name":"styx2saturn-res","namespace":"synthetic-mm2","uid":"synthetic-styx-uid","generation":1},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"styx2saturn-res"}},"template":{"spec":{"containers":[{"name":"mm2","image":"synthetic/mm2:test","env":[{"valueFrom":{"configMapKeyRef":{"name":"styx-config","key":"mm2.yaml"}}}]}]}}},"status":{"observedGeneration":1,"readyReplicas":1}}
]`
const mockConfigMaps = `[
{"metadata":{"name":"mars-config","namespace":"synthetic-mm2","uid":"synthetic-mars-cm","resourceVersion":"1"},"data":{"mm2.properties":"clusters = mars, moon-prod\nmars.bootstrap.servers = mars.synthetic.invalid:9092\nmoon-prod.bootstrap.servers = moon.synthetic.invalid:9092\nmars->moon-prod.enabled = true\nmars->moon-prod.topics = orders.*\nmars.sasl.jaas.config = synthetic password=DO_NOT_EXPOSE_SYNTHETIC_SECRET;\npassword=DO_NOT_EXPOSE_SYNTHETIC_SECRET"}},
{"metadata":{"name":"charon-config","namespace":"synthetic-mm2","uid":"synthetic-charon-cm","resourceVersion":"1"},"data":{"mm2.properties":"clusters=charon,moon-prod\ncharon.bootstrap.servers=charon.synthetic.invalid:9092\nmoon-prod.bootstrap.servers=moon.synthetic.invalid:9092\ncharon->moon-prod.enabled=true\ncharon->moon-prod.topics=orders.*"}},
{"metadata":{"name":"styx-config","namespace":"synthetic-mm2","uid":"synthetic-styx-cm","resourceVersion":"1"},"data":{"mm2.yaml":"clusters: styx, saturn-uat\nstyx.bootstrap.servers: styx.synthetic.invalid:9092\nsaturn-uat.bootstrap.servers: saturn.synthetic.invalid:9092\nstyx->saturn-uat.enabled: true\nstyx->saturn-uat.topics: inventory.*\npassword: DO_NOT_EXPOSE_SYNTHETIC_SECRET\n"}}
]`
const mockPods = `[
{"metadata":{"name":"mars2moon-pod","namespace":"synthetic-mm2","uid":"synthetic-mars-pod","labels":{"app":"mars2moon"}},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
{"metadata":{"name":"charon2moon-pod","namespace":"synthetic-mm2","uid":"synthetic-charon-pod","labels":{"app":"charon2moon"}},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
{"metadata":{"name":"styx2saturn-res-pod","namespace":"synthetic-mm2","uid":"synthetic-styx-pod","labels":{"app":"styx2saturn-res"}},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}
]`
