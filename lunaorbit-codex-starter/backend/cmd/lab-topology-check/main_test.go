package main

import (
	"strings"
	"testing"
)

func TestLabInputRequiresExplicitFlagAndPrivateIPv4(t *testing.T) {
	for _, raw := range []string{`{"remote_host":"127.0.0.1"}`, `{"remote_host":"8.8.8.8"}`, `{"remote_host":"224.0.0.1"}`, `{"remote_host":"::1"}`, `{"remote_host":"192.168.111.129:19092"}`, `{"remote_host":"192.168.111.129","password":"synthetic"}`, `{"remote_host":"192.168.111.129"}{}`} {
		if _, err := readInput(strings.NewReader(raw), true); err == nil {
			t.Fatal("unsafe input accepted")
		}
	}
	if _, err := readInput(strings.NewReader(`{"remote_host":"192.168.111.129"}`), false); err == nil {
		t.Fatal("missing lab flag accepted")
	}
	in, err := readInput(strings.NewReader(`{"remote_host":"192.168.111.129"}`), true)
	if err != nil || in.RemoteHost != "192.168.111.129" {
		t.Fatal("valid lab input rejected")
	}
}

func TestLabEndpointAllowlistIsExact(t *testing.T) {
	in := input{RemoteHost: "192.168.111.129"}
	for _, address := range []string{sourceSeed, targetSeed, "kafka-source-extra.lunaorbit-integration.svc.cluster.local:9092", "192.168.111.129:19092", "192.168.111.129:29092"} {
		if !allowedEndpoint(in, address) {
			t.Fatal("required endpoint denied", address)
		}
	}
	for _, address := range []string{"192.168.111.128:19092", "192.168.111.129:9092", "kafka-source.other.svc.cluster.local:9092", "evil.invalid:9092", sourceSeed + ".evil", "127.0.0.1:9092"} {
		if allowedEndpoint(in, address) {
			t.Fatal("unexpected endpoint allowed", address)
		}
	}
}

func TestReadWindowIsBoundedAndRetainsEarliestOffset(t *testing.T) {
	for _, tc := range []struct{ earliest, latest, want int64 }{{0, 12, 0}, {45, 78, 45}, {45, 1000, 900}, {0, 0, 0}} {
		if got := readStart(tc.earliest, tc.latest); got != tc.want {
			t.Fatalf("got %d want %d", got, tc.want)
		}
	}
}

func TestSameTopicModeIsExplicit(t *testing.T) {
	in, e := readInput(strings.NewReader(`{"remote_host":"192.168.111.129","same_topic":true}`), true)
	if e != nil || !in.SameTopic {
		t.Fatal("same-topic lab mode rejected", e)
	}
}
