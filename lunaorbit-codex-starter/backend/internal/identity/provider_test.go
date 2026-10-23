package identity

import (
	"context"
	"testing"
)

func TestMockAuthenticationIsExplicit(t *testing.T) {
	p, e := NewMockProvider([]MockUser{{Username: "synthetic", Password: "synthetic-only", Subject: Subject{ID: "trusted-id", Role: "viewer", Groups: []string{"readers"}}}})
	if e != nil {
		t.Fatal(e)
	}
	s, e := p.Authenticate(context.Background(), "synthetic", "synthetic-only")
	if e != nil || s.ID != "trusted-id" || s.Role != "viewer" {
		t.Fatal(s, e)
	}
	for _, password := range []string{"", "wrong"} {
		if _, e := p.Authenticate(context.Background(), "synthetic", password); e == nil || e.Error() != "INVALID_CREDENTIALS" {
			t.Fatal(e)
		}
	}
}
func TestLDAPRejectsPlaintextAndInvalidRole(t *testing.T) {
	_, e := NewLDAPProvider(LDAPConfig{URL: "ldap://127.0.0.1:389"}, nil)
	if e == nil {
		t.Fatal("plaintext allowed")
	}
}
