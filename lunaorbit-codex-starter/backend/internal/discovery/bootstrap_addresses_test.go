package discovery

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBootstrapAddressesKeepsOnlyHostPortInOrderWithoutDuplicates(t *testing.T) {
	got := bootstrapAddresses([]string{
		"k9-kafka-0.k9.luna-kafka.svc:9092, k9-kafka-1.k9.luna-kafka.svc:9092",
		"SASL_SSL://svc-user:synthetic-secret@K9-Kafka-0.k9.luna-kafka.svc:9092/path",
		"PLAINTEXT://10.0.0.7:9093",
		"k9-kafka-1.k9.luna-kafka.svc:9092",
	})
	want := []string{"k9-kafka-0.k9.luna-kafka.svc:9092", "k9-kafka-1.k9.luna-kafka.svc:9092", "10.0.0.7:9093"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
	for _, a := range got {
		if strings.Contains(a, "secret") || strings.Contains(a, "@") || strings.Contains(a, "://") {
			t.Fatalf("leaked %q", a)
		}
	}
}

func TestBootstrapAddressesUnparseableIsEmptyList(t *testing.T) {
	for _, in := range [][]string{nil, {""}, {"not an address"}, {"host-without-port"}, {"host:notaport"}, {"://@:"}} {
		got := bootstrapAddresses(in)
		if got == nil || len(got) != 0 {
			t.Fatalf("%v -> %#v", in, got)
		}
	}
}

func TestMatchCarriesBootstrapAddressesWithoutChangingMatching(t *testing.T) {
	regs := []Registration{{ID: "r1", Enabled: true, BootstrapServers: []string{"k1:9092"}}}
	seeds := []string{"k1:9092", "k1:9092"}
	m := Match("s", "ns", "uid", "k1", seeds, regs, nil, nil)
	if m.Status != "candidate" || !reflect.DeepEqual(m.CandidateIDs, []string{"r1"}) || !reflect.DeepEqual(m.BootstrapAddresses, []string{"k1:9092"}) {
		t.Fatalf("%+v", m)
	}
	bound := Match("s", "ns", "uid", "k1", seeds, regs, []ManualBinding{{ScopeID: "s", Namespace: "ns", WorkloadUID: "uid", Alias: "k1", RegistrationID: "r1", ApprovedBy: "u", ApprovedAt: time.Now()}}, nil)
	if bound.Status != "confirmed" || bound.Method != "manual" || bound.RegistrationID != "r1" || !reflect.DeepEqual(bound.BootstrapAddresses, []string{"k1:9092"}) {
		t.Fatalf("%+v", bound)
	}
	none := Match("s", "ns", "uid", "k9", nil, regs, nil, nil)
	raw, _ := json.Marshal(none)
	if !strings.Contains(string(raw), `"bootstrap_addresses":[]`) {
		t.Fatalf("%s", raw)
	}
}
