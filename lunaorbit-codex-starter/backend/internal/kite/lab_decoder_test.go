package kite

import (
	"strings"
	"testing"
)

func TestLabDecoderFailsClosed(t *testing.T) {
	valid := `{"metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"synthetic","uid":"synthetic-uid","resourceVersion":"1"},"spec":{}}]}`
	for _, body := range []string{`null`, `{}`, `{"metadata":null,"items":[]}`, `{"metadata":{},"items":[]}`, `{"metadata":{"resourceVersion":"1"},"items":null}`, strings.Replace(valid, `"resourceVersion":"1"}`, `"resourceVersion":"1","continue":"next"}`, 1), strings.Replace(valid, `"resourceVersion":"1"}`, `"resourceVersion":"1","remainingItemCount":1}`, 1), strings.Replace(valid, `"uid":"synthetic-uid",`, "", 1)} {
		var out []any
		e := (LabV016Decoder{}).DecodeResource("pods", false, []byte(body), &out)
		if e == nil {
			t.Fatalf("accepted incomplete envelope %s", body)
		}
	}
	var out []any
	if e := (LabV016Decoder{}).DecodeResource("pods", false, []byte(valid), &out); e != nil || len(out) != 1 {
		t.Fatal("valid observed shape rejected")
	}
}

func TestLabDecoderRejectsInvalidObjectProjections(t *testing.T) {
	meta := `"metadata":{"name":"synthetic","uid":"synthetic-uid","resourceVersion":"1"}`
	for _, kind := range []string{"deployments", "pods", "configmaps"} {
		bodies := []string{`{` + meta + `,"spec":null}`, `{` + meta + `,"data":[]}`, `{` + meta + `,"status":"invalid"}`}
		if kind != "configmaps" {
			bodies = append(bodies, `{`+meta+`}`)
		}
		for _, body := range bodies {
			var out any
			if (LabV016Decoder{}).DecodeResource(kind, true, []byte(body), &out) == nil {
				t.Fatalf("accepted invalid %s projection", kind)
			}
		}
	}
	// Empty ConfigMaps and absent runtime status are legitimate Kubernetes shapes.
	for _, tc := range []struct{ kind, body string }{{"configmaps", `{` + meta + `}`}, {"pods", `{` + meta + `,"spec":{}}`}} {
		var out any
		if e := (LabV016Decoder{}).DecodeResource(tc.kind, true, []byte(tc.body), &out); e != nil {
			t.Fatal("valid object rejected")
		}
	}
}

func TestLabDecoderErrorsDoNotExposeBody(t *testing.T) {
	d := Document{kind: "configmaps", single: true, body: []byte(`{"data":{"password":"synthetic-private-value"}}`)}
	var out any
	e := d.DecodeResource(LabV016Decoder{}, &out)
	if e == nil || strings.Contains(e.Error(), "synthetic-private-value") {
		t.Fatal("unsafe decoder failure")
	}
	if strings.Contains(d.String(), "synthetic-private-value") {
		t.Fatal("document exposed content")
	}
}
