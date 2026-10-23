// Package mock provides explicitly synthetic offline fixtures, never a Kite server implementation.
package mock

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"lunaorbit/backend/internal/kite"
	"net/http"
)

//go:embed testdata/catalog.json
var catalogJSON []byte

type envelope struct {
	Synthetic bool            `json:"synthetic_fixture"`
	Kind      string          `json:"kind"`
	Single    bool            `json:"single"`
	Items     []kite.Metadata `json:"items"`
}

// Handler creates a seven-route test fixture handler. It cannot reach Kubernetes.
func Handler(key, cluster string) http.Handler {
	var catalog map[string][]kite.Metadata
	if err := json.Unmarshal(catalogJSON, &catalog); err != nil {
		panic("invalid embedded synthetic fixture")
	}
	routes := make(map[string]envelope)
	routes["/api/v1/namespaces/_all"] = envelope{Synthetic: true, Kind: "namespaces", Items: catalog["namespaces"]}
	for _, kind := range []string{"configmaps", "deployments", "pods"} {
		root := "/api/v1/" + kind + "/synthetic-ns"
		routes[root] = envelope{Synthetic: true, Kind: kind, Items: catalog[kind]}
		routes[root+"/synthetic-name"] = envelope{Synthetic: true, Kind: kind, Single: true, Items: catalog[kind]}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			w.WriteHeader(405)
			return
		}
		if r.Header.Get("Authorization") != key {
			w.WriteHeader(401)
			return
		}
		if r.Header.Get("x-cluster-name") != cluster {
			w.WriteHeader(403)
			return
		}
		data, ok := routes[r.URL.Path]
		if !ok || r.URL.RawQuery != "" {
			w.WriteHeader(404)
			return
		}
		_ = json.NewEncoder(w).Encode(data)
	})
}

type Decoder struct{}

func (Decoder) Decode(kind string, single bool, body []byte) (kite.Snapshot, error) {
	var data envelope
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&data); err != nil {
		return kite.Snapshot{}, errors.New("invalid synthetic fixture")
	}
	if !data.Synthetic || data.Kind != kind || data.Single != single || len(data.Items) != 1 {
		return kite.Snapshot{}, errors.New("not the expected synthetic fixture")
	}
	return kite.Snapshot{Items: data.Items}, nil
}
