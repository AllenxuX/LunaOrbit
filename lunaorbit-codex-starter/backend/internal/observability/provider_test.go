package observability

import (
	"context"
	"testing"
	"time"
)

func TestMockEvidenceAndExpiry(t *testing.T) {
	p := NewMockProvider(time.Minute)
	samples, e := p.Read(context.Background(), "scope", "link", "stale")
	if e != nil || len(samples) != 2 {
		t.Fatal(samples, e)
	}
	for _, s := range samples {
		if !s.Synthetic || !s.Stale || s.State == "healthy" {
			t.Fatal(s)
		}
	}
	samples, e = p.Read(context.Background(), "scope", "link", "no_data")
	if e != nil || samples[0].Value != nil || samples[0].State != "unknown" {
		t.Fatal(samples, e)
	}
}
