package identity

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	ber "github.com/go-asn1-ber/asn1-ber"
	"github.com/go-ldap/ldap/v3"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

type testSecrets map[string][]byte

func (s testSecrets) Get(_ context.Context, ref string) ([]byte, error) { return s[ref], nil }
func ldapResult(id int64, tag ber.Tag, code int64) *ber.Packet {
	root := ber.NewSequence("message")
	root.AppendChild(ber.NewInteger(ber.ClassUniversal, ber.TypePrimitive, ber.TagInteger, id, "id"))
	result := ber.Encode(ber.ClassApplication, ber.TypeConstructed, tag, nil, "result")
	result.AppendChild(ber.NewInteger(ber.ClassUniversal, ber.TypePrimitive, ber.TagEnumerated, code, "code"))
	result.AppendChild(ber.NewString(ber.ClassUniversal, ber.TypePrimitive, ber.TagOctetString, "", "dn"))
	result.AppendChild(ber.NewString(ber.ClassUniversal, ber.TypePrimitive, ber.TagOctetString, "", "message"))
	root.AppendChild(result)
	return root
}
func ldapFixture(t *testing.T) (LDAPConfig, testSecrets) {
	t.Helper()
	certServer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	cert := certServer.TLS.Certificates[0]
	certServer.Close()
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	secure := tls.NewListener(listener, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	t.Cleanup(func() { secure.Close() })
	go func() {
		for {
			c, e := secure.Accept()
			if e != nil {
				return
			}
			go func() {
				defer c.Close()
				for {
					packet, e := ber.ReadPacket(c)
					if e != nil {
						return
					}
					id := packet.Children[0].Value.(int64)
					op := packet.Children[1]
					switch op.Tag {
					case ldap.ApplicationBindRequest:
						code := int64(0)
						password := string(op.Children[2].Data.Bytes())
						if password != "synthetic-bind" && password != "synthetic-user-password" {
							code = 49
						}
						c.Write(ldapResult(id, ldap.ApplicationBindResponse, code).Bytes())
					case ldap.ApplicationSearchRequest:
						entry := ber.NewSequence("message")
						entry.AppendChild(ber.NewInteger(ber.ClassUniversal, ber.TypePrimitive, ber.TagInteger, id, "id"))
						result := ber.Encode(ber.ClassApplication, ber.TypeConstructed, ldap.ApplicationSearchResultEntry, nil, "entry")
						result.AppendChild(ber.NewString(ber.ClassUniversal, ber.TypePrimitive, ber.TagOctetString, "uid=synthetic,dc=example,dc=test", "dn"))
						attributes := ber.NewSequence("attributes")
						for _, a := range [][2]string{{"entryUUID", "synthetic-entry-id"}, {"memberOf", "cn=readers,dc=example,dc=test"}} {
							attribute := ber.NewSequence("attribute")
							attribute.AppendChild(ber.NewString(ber.ClassUniversal, ber.TypePrimitive, ber.TagOctetString, a[0], "name"))
							values := ber.Encode(ber.ClassUniversal, ber.TypeConstructed, ber.TagSet, nil, "values")
							values.AppendChild(ber.NewString(ber.ClassUniversal, ber.TypePrimitive, ber.TagOctetString, a[1], "value"))
							attribute.AppendChild(values)
							attributes.AppendChild(attribute)
						}
						result.AppendChild(attributes)
						entry.AppendChild(result)
						c.Write(entry.Bytes())
						c.Write(ldapResult(id, ldap.ApplicationSearchResultDone, 0).Bytes())
					case ldap.ApplicationUnbindRequest:
						return
					default:
						return
					}
				}
			}()
		}
	}()
	cfg := LDAPConfig{URL: "ldaps://" + listener.Addr().String(), BaseDN: "dc=example,dc=test", BindDN: "cn=service,dc=example,dc=test", BindPasswordRef: "bind", TLSCASecretRef: "ca", AllowedAddresses: []string{listener.Addr().String()}, AllowedCIDRs: []string{"127.0.0.0/8"}, RoleGroups: map[string]string{"cn=readers,dc=example,dc=test": "viewer"}}
	return cfg, testSecrets{"bind": []byte("synthetic-bind"), "ca": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})}
}
func TestRealLDAPTLSProtocolAuthentication(t *testing.T) {
	cfg, secrets := ldapFixture(t)
	p, e := NewLDAPProvider(cfg, secrets)
	if e != nil {
		t.Fatal(e)
	}
	s, e := p.Authenticate(context.Background(), "synthetic", "synthetic-user-password")
	if e != nil || s.Role != "viewer" || s.ID != stableID(cfg.URL+"/"+cfg.BaseDN, "synthetic-entry-id") {
		t.Fatal(s, e)
	}
	if _, e = p.Authenticate(context.Background(), "synthetic", "private-wrong-password"); e != ErrCredentials {
		t.Fatal(e)
	}
	delete(secrets, "ca")
	if _, e = p.Authenticate(context.Background(), "synthetic", "synthetic-user-password"); e != ErrUnavailable {
		t.Fatal(e)
	}
}
func TestLDAPEscapesFiltersAndMapsOnlyServerGroups(t *testing.T) {
	p := &LDAPProvider{cfg: LDAPConfig{UsernameAttribute: "uid", RoleGroups: map[string]string{"cn=readers,dc=example,dc=test": "viewer", "cn=admins,dc=example,dc=test": "admin"}}}
	if p.userFilter("*)(uid=*)") != "(uid=\\2a\\29\\28uid=\\2a\\29)" {
		t.Fatal(p.userFilter("*)(uid=*)"))
	}
	role, e := p.role([]string{"cn=readers,dc=example,dc=test", "cn=admins,dc=example,dc=test"})
	if e != nil || role != "admin" {
		t.Fatal(role, e)
	}
	if _, e = p.role([]string{"cn=unknown,dc=example,dc=test"}); e != ErrCredentials {
		t.Fatal(e)
	}
}
