package collector

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestMockScenarios(t *testing.T) {
	for _, scenario := range []string{"success", "partial_failure", "unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			s, scope, close, err := NewMock(scenario)
			if err != nil {
				t.Fatal(err)
			}
			defer close()
			got, err := s.Collect(context.Background(), scope)
			if scenario == "unavailable" {
				if err == nil || got.ErrorCode != "UPSTREAM_UNAVAILABLE" {
					t.Fatalf("expected safe unavailable code")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Namespaces) != 1 {
				t.Fatalf("namespaces %d", len(got.Namespaces))
			}
			b := got.Namespaces[0]
			if scenario == "success" {
				if !b.Complete || len(b.Deployments) != 3 || len(b.ConfigMaps) != 3 {
					t.Fatalf("incomplete fixture")
				}
				if b.ConfigMaps[0].Data["mm2.properties"] == "" {
					t.Fatal("raw fixture was redacted")
				}
			} else if b.Complete || b.ErrorCode != "PERMISSION_DENIED" {
				t.Fatalf("expected namespace failure")
			}
		})
	}
}

func TestWhitelistAndDecoderFailClosed(t *testing.T) {
	source, scope, close, err := NewMock("success")
	if err != nil {
		t.Fatal(err)
	}
	defer close()
	scope.Namespaces = []string{"synthetic-mm2", "synthetic-mm2"}
	got, err := source.Collect(context.Background(), scope)
	if err != nil || len(got.Namespaces) != 1 || !got.Namespaces[0].Complete {
		t.Fatal("whitelist collection failed")
	}
	s := source.(KiteSource)
	s.Decoder = nil
	got, err = s.Collect(context.Background(), scope)
	if err != nil || got.Namespaces[0].Complete || got.Namespaces[0].ErrorCode != "RESPONSE_FORMAT_UNVERIFIED" {
		t.Fatal("unverified decoder must fail namespace")
	}
	scope.Namespaces = nil
	got, err = s.Collect(context.Background(), scope)
	if err == nil || got.ErrorCode != "RESPONSE_FORMAT_UNVERIFIED" {
		t.Fatal("unverified namespaces must fail collection")
	}
	scope.ID = "../secret"
	got, err = s.Collect(context.Background(), scope)
	if err == nil || got.ErrorCode != "INVALID_ARGUMENT" {
		t.Fatal("invalid scope accepted")
	}
}

func TestNamespaceFailuresContinue(t *testing.T) {
	source, scope, close, err := NewMock("success")
	if err != nil {
		t.Fatal(err)
	}
	defer close()
	scope.Namespaces = []string{"missing", "synthetic-mm2"}
	got, err := source.Collect(context.Background(), scope)
	if err != nil || len(got.Namespaces) != 2 {
		t.Fatal("failed namespace stopped collection")
	}
	if got.Namespaces[0].Complete || got.Namespaces[0].ErrorCode != "NOT_FOUND" || !got.Namespaces[1].Complete {
		t.Fatal("namespace failure lost")
	}
	if got.ObservedAt.Location().String() != "UTC" {
		t.Fatal("timestamp not UTC")
	}
}

func TestSyntheticSecretsStayServerOnly(t *testing.T) {
	source, scope, close, err := NewMock("success")
	if err != nil {
		t.Fatal(err)
	}
	defer close()
	got, err := source.Collect(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	raw := got.Namespaces[0].ConfigMaps[0].Data["mm2.properties"]
	if !strings.Contains(raw, "DO_NOT_EXPOSE_SYNTHETIC_SECRET") {
		t.Fatal("secret fixture missing")
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "DO_NOT_EXPOSE_SYNTHETIC_SECRET") || strings.Contains(string(encoded), "jaas.config") {
		t.Fatal("raw configuration escaped collection")
	}
}
