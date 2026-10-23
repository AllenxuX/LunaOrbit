package main

import (
	"bytes"
	"context"
	"encoding/json"
	"lunaorbit/backend/internal/discovery"
	"strings"
	"testing"
)

func TestSyntheticCLISnapshots(t *testing.T) {
	for _, stale := range []bool{false, true} {
		var out bytes.Buffer
		if e := run(context.Background(), &out, stale); e != nil {
			t.Fatal(e)
		}
		var snapshot discovery.Snapshot
		if json.Unmarshal(out.Bytes(), &snapshot) != nil || len(snapshot.Links) != 3 || snapshot.Stale != stale {
			t.Fatal("invalid CLI snapshot")
		}
		if strings.Contains(out.String(), "DO_NOT_EXPOSE") || strings.Contains(out.String(), "jaas") {
			t.Fatal("CLI leaked raw config")
		}
		for _, link := range snapshot.Links {
			if link.Verified || link.Source.RegistrationID != "" || link.Source.Status != "candidate" {
				t.Fatal("synthetic candidate promoted")
			}
		}
	}
}
