package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/identity"
	"lunaorbit/backend/internal/registry"
	"strings"
)

func mountIdentity(router *gin.Engine, service *registry.Service, origin string, getenv func(string) string) error {
	mode := getenv("IDENTITY_MODE")
	if mode == "" || mode == "dev" {
		for _, path := range []string{"/api/v1/auth/login", "/api/v1/auth/logout"} {
			router.POST(path, func(c *gin.Context) {
				c.AbortWithStatusJSON(501, gin.H{"error": gin.H{"code": "IDENTITY_NOT_CONFIGURED", "message": "LDAP identity is not configured"}})
			})
		}
		return nil
	}
	invalid := errors.New("identity configuration unavailable")
	var provider identity.Provider
	switch mode {
	case "mock":
		raw := getenv("IDENTITY_MOCK_USERS")
		if len(raw) > 65536 {
			return invalid
		}
		var users []identity.MockUser
		if json.Unmarshal([]byte(raw), &users) != nil || len(users) == 0 || len(users) > 128 {
			return invalid
		}
		p, err := identity.NewMockProvider(users)
		if err != nil {
			return invalid
		}
		provider = p
	case "ldap":
		secrets := registry.NewMemorySecrets()
		bind := getenv("LDAP_BIND_PASSWORD")
		if bind == "" {
			return invalid
		}
		ref, err := secrets.Put(context.Background(), []byte(bind))
		if err != nil {
			return invalid
		}
		var caRef string
		if ca := getenv("LDAP_CA_PEM"); ca != "" {
			caRef, err = secrets.Put(context.Background(), []byte(ca))
			if err != nil {
				return invalid
			}
		}
		var groups map[string]string
		raw := getenv("LDAP_ROLE_GROUPS")
		if len(raw) > 16384 || json.Unmarshal([]byte(raw), &groups) != nil {
			return invalid
		}
		p, err := identity.NewLDAPProvider(identity.LDAPConfig{URL: getenv("LDAP_URL"), BaseDN: getenv("LDAP_BASE_DN"), BindDN: getenv("LDAP_BIND_DN"), BindPasswordRef: ref, TLSCASecretRef: caRef, AllowedAddresses: splitConfig(getenv("LDAP_ALLOWED_ADDRESSES")), AllowedCIDRs: splitConfig(getenv("LDAP_ALLOWED_CIDRS")), RoleGroups: groups}, secrets)
		if err != nil {
			return invalid
		}
		provider = p
	default:
		return invalid
	}
	realm, err := identity.NewRealm(provider, identity.RealmConfig{Mode: mode, AllowedOrigins: []string{origin}, DevelopmentLoopback: true})
	if err != nil {
		return invalid
	}
	service.IdentityProvider = realm
	realm.Mount(router)
	return nil
}
func splitConfig(raw string) []string {
	var out []string
	for _, v := range strings.Split(raw, ",") {
		if x := strings.TrimSpace(v); x != "" {
			out = append(out, x)
		}
	}
	return out
}
