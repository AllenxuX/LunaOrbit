package kafkaadmin

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/kafkaops"
	"testing"
)

func TestSCRAMRejectsInvalidInputBeforeNetwork(t *testing.T) {
	f, p, _ := managementBroker(t, 0)
	for _, u := range []string{"", "bad:name", "bad\nname"} {
		for _, err := range []error{f.CreateSCRAM(context.Background(), p, u, "synthetic-password"), f.VerifySCRAM(context.Background(), p, u, "synthetic-password")} {
			var safe *kafkaops.Error
			if !errors.As(err, &safe) || safe.Code != "INVALID_ARGUMENT" {
				t.Fatalf("unsafe validation: %v", err)
			}
		}
	}
}
func TestSCRAMVerifyRejectsUnauthenticatedListener(t *testing.T) {
	f, p, _ := managementBroker(t, 0)
	if err := f.VerifySCRAM(context.Background(), p, "synthetic", "synthetic-password"); err == nil {
		t.Fatal("plaintext verification accepted")
	}
}
