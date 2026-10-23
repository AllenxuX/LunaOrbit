package changes

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLabWritesDisabled(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; t.Error("disabled write reached upstream") }))
	defer srv.Close()
	a, e := NewLabAdapter(srv.URL, "test-key", "lab", false)
	if e != nil {
		t.Fatal(e)
	}
	_, e = a.UpdateConfig(context.Background(), Target{}, ConfigDocument{}, "a.*")
	if e == nil || e.Error() != "WRITES_DISABLED" || calls != 0 {
		t.Fatal(e, calls)
	}
}

func TestLabExactCASRequests(t *testing.T) {
	target := Target{Namespace: "lab", ConfigMapName: "mm2-config", DeploymentName: "mm2", ConfigDataKey: "mm2.properties", TopicRuleKey: "topics", ConfigMapUID: "cm-uid", DeploymentUID: "dep-uid"}
	raw := []byte(`{"apiVersion":"v1","metadata":{"uid":"cm-uid","resourceVersion":"7","labels":{"keep":"yes"}},"data":{"mm2.properties":"# comment\ntopics = old.*\nother = keep\n","secret":"preserve"},"binaryData":{"keep":"eA=="}}`)
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "lab-key" || r.Header.Get("x-cluster-name") != "cluster" {
			t.Error("incorrect headers")
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			t.Error("invalid body")
		}
		switch calls {
		case 1:
			if r.Method != "PUT" || r.URL.RequestURI() != "/api/v1/configmaps/lab/mm2-config" {
				t.Error(r.Method, r.URL)
			}
			data := body["data"].(map[string]any)
			if data["secret"] != "preserve" || data["mm2.properties"] != "# comment\ntopics = next.*\nother = keep\n" || body["binaryData"] == nil {
				t.Error("lost config fields", body)
			}
			if body["metadata"].(map[string]any)["resourceVersion"] != "7" {
				t.Error("missing CAS")
			}
			io.WriteString(w, `{"metadata":{"uid":"cm-uid","resourceVersion":"8"}}`)
		case 2:
			if r.Method != "PATCH" || r.URL.RequestURI() != "/api/v1/deployments/lab/mm2?patchType=merge" {
				t.Error(r.Method, r.URL)
			}
			meta := body["metadata"].(map[string]any)
			if meta["uid"] != "dep-uid" || meta["resourceVersion"] != "9" {
				t.Error("missing deployment CAS")
			}
			io.WriteString(w, `{"metadata":{"uid":"dep-uid","resourceVersion":"10","generation":2},"spec":{"template":{"metadata":{"annotations":{"kite.kubernetes.io/restartedAt":"2026-10-20T00:00:00Z"}}}}}`)
		default:
			t.Error("unexpected retry")
			w.WriteHeader(409)
		}
	}))
	defer srv.Close()
	a, e := NewLabAdapter(srv.URL, "lab-key", "cluster", true)
	if e != nil {
		t.Fatal(e)
	}
	_, e = a.UpdateConfig(context.Background(), target, ConfigDocument{UID: "cm-uid", ResourceVersion: "7", Raw: raw}, "next.*")
	if e != nil {
		t.Fatal(e)
	}
	_, e = a.Restart(context.Background(), target, Deployment{UID: "dep-uid", ResourceVersion: "9"}, time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC))
	if e != nil {
		t.Fatal(e)
	}
	_, e = a.UpdateConfig(context.Background(), target, ConfigDocument{UID: "cm-uid", ResourceVersion: "stale", Raw: raw}, "next.*")
	if e != ErrConflict || calls != 2 {
		t.Fatal("stale document sent", e, calls)
	}
}
func TestLabConflictNeverRetries(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(409) }))
	defer srv.Close()
	a, _ := NewLabAdapter(srv.URL, "key", "cluster", true)
	_, e := a.Restart(context.Background(), Target{Namespace: "lab", DeploymentName: "mm2", DeploymentUID: "dep"}, Deployment{UID: "dep", ResourceVersion: "1"}, time.Now())
	if e != ErrConflict || calls != 1 {
		t.Fatal(e, calls)
	}
}
