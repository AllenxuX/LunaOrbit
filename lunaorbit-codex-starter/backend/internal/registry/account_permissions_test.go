package registry

import (
	"context"
	"testing"
)

func TestBusinessCredentialPermissionsRequireUnscopedAdmin(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	s := NewService(st, NewMemorySecrets())
	defer s.Close()
	owner := Actor{ID: "owner", Role: "admin"}
	row := Registration{ID: "account-permissions", CreatedBy: owner.ID, Revision: 1, Enabled: true}
	if err := st.Save(ctx, row, 0, "create"); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"account.manage", "credentials.read"} {
		if err := s.AuthorizeUnscopedResource(ctx, owner, row.ID, action); err != nil {
			t.Fatalf("owner %s denied: %v", action, err)
		}
	}
	for _, tc := range []struct {
		name, role, prefix string
		allowed            bool
	}{{"security", "security-admin", "", false}, {"admin", "admin", "", true}, {"scoped-admin", "admin", "orders.", false}} {
		actor := Actor{ID: tc.name, Role: "viewer"}
		if _, err := s.CreateGrant(ctx, owner, row.ID, GrantInput{SubjectType: "user", SubjectID: actor.ID, Role: tc.role, TopicPrefix: tc.prefix}); err != nil {
			t.Fatal(err)
		}
		for _, action := range []string{"account.manage", "credentials.read"} {
			if got := s.AuthorizeUnscopedResource(ctx, actor, row.ID, action) == nil; got != tc.allowed {
				t.Fatalf("%s %s allowed=%v", tc.name, action, got)
			}
		}
	}
}
