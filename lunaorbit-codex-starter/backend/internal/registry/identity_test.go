package registry

import (
	"context"
	"testing"
)

func TestSecurityAdminReadsOnlyOwnRegistrations(t *testing.T) {
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	defer s.Close()
	ctx := context.Background()
	owner := Actor{ID: "trusted-subject", Role: "admin"}
	row, e := s.Create(ctx, owner, syntheticInput())
	if e != nil {
		t.Fatal(e)
	}
	readOnly := Actor{ID: owner.ID, Role: "security-admin"}
	if _, e = s.Get(ctx, readOnly, row.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Get(ctx, Actor{ID: "other-subject", Role: "security-admin"}, row.ID); ErrorCode(e) != "NOT_FOUND" {
		t.Fatal(e)
	}
	if _, e = s.Create(ctx, readOnly, syntheticInput()); ErrorCode(e) != "FORBIDDEN" {
		t.Fatal(e)
	}
	if _, e = s.StartProbe(ctx, readOnly, row.ID, ""); ErrorCode(e) != "FORBIDDEN" {
		t.Fatal(e)
	}
}
