package discovery

import (
	"strings"
	"testing"
)

func ep(scope, wl, alias string, addrs ...string) AutoEndpoint {
	return AutoEndpoint{Key: EndpointKey(scope, "ns", wl, alias), ScopeID: scope, Namespace: "ns", WorkloadUID: wl, WorkloadName: wl, Alias: alias, Addresses: addrs, Resolved: []string{}, Access: AccessPending, CredentialSource: CredentialSourceMM2}
}

func byAlias(rows []CatalogEndpoint) map[string]CatalogEndpoint {
	out := map[string]CatalogEndpoint{}
	for _, r := range rows {
		out[strings.Join(r.Aliases, ",")] = r
	}
	return out
}

func TestCatalogMergesTheSameClusterSeenByManyMM2(t *testing.T) {
	eps := []AutoEndpoint{
		ep("s", "mm2-a", "k1", "k1:9092"), ep("s", "mm2-a", "k8", "k8:9092"),
		ep("s", "mm2-b", "k8", "k8:9092", "k8b:9092"), ep("s", "mm2-b", "k9", "k9:9092"),
	}
	rows := BuildCatalog(eps, nil, nil)
	if len(rows) != 3 {
		t.Fatalf("k8 must appear once: %+v", rows)
	}
	k8 := byAlias(rows)["k8"]
	if len(k8.Members) != 2 || strings.Join(k8.Addresses, ",") != "k8:9092,k8b:9092" || k8.Source != SourceAuto {
		t.Fatalf("k8 %+v", k8)
	}
}

func TestCatalogMergesByClusterIDBeforeAddress(t *testing.T) {
	eps := []AutoEndpoint{ep("s", "mm2-a", "east", "a-1:9092"), ep("s", "mm2-b", "east2", "a-2:9092")}
	probes := map[string]ProbeResult{eps[0].Key: {Access: AccessReadable, ClusterID: "CID-1"}, eps[1].Key: {Access: AccessReadable, ClusterID: "CID-1"}}
	rows := BuildCatalog(eps, nil, probes)
	if len(rows) != 1 || rows[0].ClusterID != "CID-1" || len(rows[0].Aliases) != 2 {
		t.Fatalf("same Cluster ID must be one endpoint: %+v", rows)
	}
}

func TestCatalogAttachesRegistrationFieldsAndKeepsManualSupplements(t *testing.T) {
	eps := []AutoEndpoint{ep("s", "mm2", "k1", "k1:9092")}
	regs := []Registration{
		{ID: "r1", DisplayName: "luna-k1", BootstrapServers: []string{"K1:9092"}, Room: "bj", Environment: "prod", Enabled: true},
		{ID: "r7", DisplayName: "legacy-only", BootstrapServers: []string{"legacy:9092"}, Room: "sh", Enabled: true},
	}
	rows := BuildCatalog(eps, regs, nil)
	if len(rows) != 2 {
		t.Fatalf("%+v", rows)
	}
	k1 := byAlias(rows)["k1"]
	if k1.Source != SourceMerged || k1.RegistrationID != "r1" || k1.DisplayName != "luna-k1" || k1.Room != "bj" || k1.Environment != "prod" {
		t.Fatalf("merged %+v", k1)
	}
	manual := byAlias(rows)[""]
	if manual.Source != SourceManual || manual.RegistrationID != "r7" || manual.DisplayName != "legacy-only" {
		t.Fatalf("manual supplement %+v", manual)
	}
}

func TestCatalogNeverPicksAmongSeveralRegistrations(t *testing.T) {
	eps := []AutoEndpoint{ep("s", "mm2", "k1", "k1:9092")}
	regs := []Registration{{ID: "r1", BootstrapServers: []string{"k1:9092"}, Enabled: true}, {ID: "r2", BootstrapServers: []string{"k1:9092"}, Enabled: true}}
	k1 := byAlias(BuildCatalog(eps, regs, nil))["k1"]
	if k1.RegistrationID != "" || strings.Join(k1.Ambiguous, ",") != "r1,r2" || k1.Source != SourceAuto {
		t.Fatalf("ambiguous %+v", k1)
	}
}

func TestCatalogFallsBackToRegistrationWhenAutoAccessFails(t *testing.T) {
	eps := []AutoEndpoint{ep("s", "mm2", "k1", "k1:9092")}
	regs := []Registration{{ID: "r1", BootstrapServers: []string{"k1:9092"}, Enabled: true}}
	probes := map[string]ProbeResult{eps[0].Key: {Access: AccessFailed, Reason: "timeout"}}
	k1 := BuildCatalog(eps, regs, probes)[0]
	if k1.ReadVia != "registration" || k1.Access != AccessFailed || k1.AccessReason != "timeout" {
		t.Fatalf("fallback %+v", k1)
	}
	probes[eps[0].Key] = ProbeResult{Access: AccessReadable}
	if k1 = BuildCatalog(eps, regs, probes)[0]; k1.ReadVia != "auto" {
		t.Fatalf("auto first %+v", k1)
	}
}
