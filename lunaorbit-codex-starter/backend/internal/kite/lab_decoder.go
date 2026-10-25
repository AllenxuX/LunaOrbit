package kite

import "encoding/json"

// LabV016Decoder accepts only the envelope observed on the dedicated v0.16.0
// lab. Unverified pagination fails closed; it never claims partial data complete.
type LabV016Decoder struct{}

func (LabV016Decoder) DecodeResource(kind string, single bool, body []byte, target any) error {
	bad := func() error { return &Error{Code: "INVALID_RESPONSE"} }
	if kind != "namespaces" && kind != "configmaps" && kind != "deployments" && kind != "pods" {
		return bad()
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(body, &obj) != nil || obj == nil {
		return bad()
	}
	var meta map[string]json.RawMessage
	if json.Unmarshal(obj["metadata"], &meta) != nil || meta == nil {
		return bad()
	}
	check := func(raw json.RawMessage) bool {
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) != nil || object == nil {
			return false
		}
		if (kind == "deployments" || kind == "pods") && object["spec"] == nil {
			return false
		}
		for _, key := range []string{"data", "spec", "status"} {
			if value, exists := object[key]; exists {
				var projection map[string]json.RawMessage
				if json.Unmarshal(value, &projection) != nil || projection == nil {
					return false
				}
			}
		}
		var item struct {
			Metadata struct{ Name, UID, ResourceVersion string }
		}
		return json.Unmarshal(raw, &item) == nil && item.Metadata.Name != "" && item.Metadata.UID != "" && item.Metadata.ResourceVersion != ""
	}
	if single {
		if obj["items"] != nil || !check(body) {
			return bad()
		}
		return json.Unmarshal(body, target)
	}
	var rv string
	// Some v0.16.0 deployments omit the list resourceVersion and mark the
	// single, non-paginated page with a fixed continue value; accept only that.
	var cont string
	if raw, ok := meta["continue"]; ok {
		if json.Unmarshal(raw, &cont) != nil || (cont != "" && cont != "continue-not-supported") {
			return bad()
		}
	}
	rawRV, hasRV := meta["resourceVersion"]
	if hasRV || cont != "continue-not-supported" {
		if json.Unmarshal(rawRV, &rv) != nil || rv == "" {
			return bad()
		}
	}
	if raw, ok := meta["remainingItemCount"]; ok {
		var v int64
		if string(raw) == "null" || json.Unmarshal(raw, &v) != nil || v != 0 {
			return bad()
		}
	}
	var items []json.RawMessage
	if json.Unmarshal(obj["items"], &items) != nil || items == nil || len(items) > 1000 {
		return bad()
	}
	for _, item := range items {
		if !check(item) {
			return bad()
		}
	}
	return json.Unmarshal(obj["items"], target)
}
