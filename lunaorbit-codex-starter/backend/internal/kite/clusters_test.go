package kite

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestListClustersReadsNamesVersionsAndErrorsWithoutClusterHeader(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/clusters" || r.Header.Get("x-cluster-name") != "" || r.Header.Get("Authorization") != syntheticKey {
			w.WriteHeader(400)
			return
		}
		_, _ = w.Write([]byte(`[{"name":"in-cluster","version":"v1.32.13","isDefault":true},{"name":"edge-b","version":"v1.30.1","error":"dial timeout"}]`))
	}, 0, time.Second)
	got, err := c.ListClusters(context.Background())
	if err != nil || len(got) != 2 || got[0] != (ClusterInfo{Name: "in-cluster", Version: "v1.32.13"}) || got[1].Name != "edge-b" || got[1].Error != "dial timeout" {
		t.Fatalf("clusters %+v %v", got, err)
	}
}

func TestListClustersRejectsUnsafeOrDuplicateNames(t *testing.T) {
	for _, body := range []string{`{"items":[]}`, `[{"name":""}]`, `[{"name":"a"},{"name":"a"}]`, `[{"name":"bad\nname"}]`} {
		c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }, 0, time.Second)
		if _, err := c.ListClusters(context.Background()); CodeOf(err) != "INVALID_RESPONSE" {
			t.Fatalf("%s accepted: %v", body, err)
		}
	}
}

func TestListAllDeploymentsUsesTheAllNamespaceRoute(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/deployments" || r.Header.Get("x-cluster-name") != "edge-b" {
			w.WriteHeader(404)
			return
		}
		_, _ = w.Write([]byte(`{"metadata":{"continue":"continue-not-supported"},"items":[{"metadata":{"name":"mm2","namespace":"a","uid":"u1","resourceVersion":"1"},"spec":{}}]}`))
	}, 0, time.Second)
	doc, err := c.ListAllDeployments(context.Background(), "edge-b")
	if err != nil {
		t.Fatal(err)
	}
	var items []struct {
		Metadata struct{ Name, Namespace string }
	}
	if err = doc.DecodeResource(LabV016Decoder{}, &items); err != nil || len(items) != 1 || items[0].Metadata.Namespace != "a" {
		t.Fatalf("decode %v %+v", err, items)
	}
	if _, err = c.ListAllDeployments(context.Background(), "gone"); CodeOf(err) != "NOT_FOUND" {
		t.Fatalf("missing cluster must be NOT_FOUND: %v", err)
	}
}
