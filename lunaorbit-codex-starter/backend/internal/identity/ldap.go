package identity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	ber "github.com/go-asn1-ber/asn1-ber"
	"github.com/go-ldap/ldap/v3"
	"io"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Bound BER allocations before any connection exists; this dependency is used only by LDAP.
func init() { ber.MaxPacketLengthBytes = 1 << 20 }

type LDAPConfig struct {
	URL, BaseDN, BindDN, BindPasswordRef, TLSCASecretRef string
	AllowedAddresses, AllowedCIDRs                       []string
	UsernameAttribute, SubjectAttribute, GroupAttribute  string
	RoleGroups                                           map[string]string
	Timeout                                              time.Duration
}
type LDAPProvider struct {
	cfg           LDAPConfig
	secrets       SecretReader
	host, address string
	nets          []*net.IPNet
	slots         chan struct{}
}

var attributeName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-]{0,63}$`)

func NewLDAPProvider(cfg LDAPConfig, secrets SecretReader) (*LDAPProvider, error) {
	u, e := url.Parse(cfg.URL)
	if e != nil || u.Scheme != "ldaps" || u.User != nil || u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || cfg.BaseDN == "" || cfg.BindDN == "" || cfg.BindPasswordRef == "" || secrets == nil {
		return nil, ErrConfig
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "636"
	}
	address := net.JoinHostPort(host, port)
	allowed := false
	for _, a := range cfg.AllowedAddresses {
		if strings.EqualFold(a, address) {
			allowed = true
		}
	}
	if !allowed {
		return nil, ErrConfig
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Timeout < time.Millisecond || cfg.Timeout > 30*time.Second {
		return nil, ErrConfig
	}
	if cfg.UsernameAttribute == "" {
		cfg.UsernameAttribute = "uid"
	}
	if cfg.SubjectAttribute == "" {
		cfg.SubjectAttribute = "entryUUID"
	}
	if cfg.GroupAttribute == "" {
		cfg.GroupAttribute = "memberOf"
	}
	for _, a := range []string{cfg.UsernameAttribute, cfg.SubjectAttribute, cfg.GroupAttribute} {
		if !attributeName.MatchString(a) {
			return nil, ErrConfig
		}
	}
	if _, e = ldap.ParseDN(cfg.BaseDN); e != nil {
		return nil, ErrConfig
	}
	if _, e = ldap.ParseDN(cfg.BindDN); e != nil {
		return nil, ErrConfig
	}
	roles := map[string]string{}
	for group, role := range cfg.RoleGroups {
		if !validRole(role) {
			return nil, ErrConfig
		}
		dn, e := ldap.ParseDN(group)
		if e != nil {
			return nil, ErrConfig
		}
		roles[strings.ToLower(dn.String())] = role
	}
	cfg.RoleGroups = roles
	p := &LDAPProvider{cfg: cfg, secrets: secrets, host: host, address: address, slots: make(chan struct{}, 4)}
	for _, raw := range cfg.AllowedCIDRs {
		_, n, e := net.ParseCIDR(raw)
		if e != nil {
			return nil, ErrConfig
		}
		p.nets = append(p.nets, n)
	}
	if len(p.nets) == 0 {
		return nil, ErrConfig
	}
	return p, nil
}
func (p *LDAPProvider) userFilter(user string) string {
	return "(" + p.cfg.UsernameAttribute + "=" + ldap.EscapeFilter(user) + ")"
}
func (p *LDAPProvider) role(groups []string) (string, error) {
	best := ""
	rank := map[string]int{"viewer": 1, "operator": 2, "security-admin": 3, "admin": 4}
	for _, g := range groups {
		dn, e := ldap.ParseDN(g)
		if e != nil {
			continue
		}
		role := p.cfg.RoleGroups[strings.ToLower(dn.String())]
		if rank[role] > rank[best] {
			best = role
		}
	}
	if best == "" {
		return "", ErrCredentials
	}
	return best, nil
}

type boundedConn struct {
	net.Conn
	reader io.Reader
}

func (c *boundedConn) Read(b []byte) (int, error) { return c.reader.Read(b) }
func (p *LDAPProvider) Authenticate(ctx context.Context, user, password string) (Subject, error) {
	if strings.TrimSpace(user) == "" || len(user) > 256 || password == "" || len(password) > 4096 || strings.ContainsRune(user, 0) {
		return Subject{}, ErrCredentials
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	select {
	case p.slots <- struct{}{}:
		defer func() { <-p.slots }()
	case <-ctx.Done():
		return Subject{}, ErrUnavailable
	}
	ips, e := net.DefaultResolver.LookupIPAddr(ctx, p.host)
	if e != nil || len(ips) == 0 {
		return Subject{}, ErrUnavailable
	}
	for _, ip := range ips {
		ok := false
		for _, n := range p.nets {
			if n.Contains(ip.IP) {
				ok = true
			}
		}
		if !ok {
			return Subject{}, ErrUnavailable
		}
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: p.host}
	if p.cfg.TLSCASecretRef != "" {
		pem, e := p.secrets.Get(ctx, p.cfg.TLSCASecretRef)
		if e != nil {
			return Subject{}, ErrUnavailable
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return Subject{}, ErrUnavailable
		}
		tc.RootCAs = pool
	}
	_, port, _ := net.SplitHostPort(p.address)
	raw, e := (&net.Dialer{Timeout: p.cfg.Timeout}).DialContext(ctx, "tcp", net.JoinHostPort(ips[0].IP.String(), port))
	if e != nil {
		return Subject{}, ErrUnavailable
	}
	defer raw.Close()
	deadline, _ := ctx.Deadline()
	raw.SetDeadline(deadline)
	secure := tls.Client(raw, tc)
	if e = secure.HandshakeContext(ctx); e != nil {
		return Subject{}, ErrUnavailable
	}
	conn := ldap.NewConn(&boundedConn{Conn: secure, reader: io.LimitReader(secure, 2<<20)}, true)
	conn.Start()
	defer conn.Close()
	conn.SetTimeout(p.cfg.Timeout)
	stop := context.AfterFunc(ctx, func() { raw.Close() })
	defer stop()
	bindPassword, e := p.secrets.Get(ctx, p.cfg.BindPasswordRef)
	if e != nil || len(bindPassword) == 0 {
		return Subject{}, ErrUnavailable
	}
	if e = conn.Bind(p.cfg.BindDN, string(bindPassword)); e != nil {
		return Subject{}, ErrUnavailable
	}
	result, e := conn.Search(ldap.NewSearchRequest(p.cfg.BaseDN, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 2, int(p.cfg.Timeout.Seconds())+1, false, p.userFilter(user), []string{p.cfg.SubjectAttribute, p.cfg.GroupAttribute}, nil))
	if e != nil {
		return Subject{}, ErrUnavailable
	}
	if len(result.Entries) != 1 {
		return Subject{}, ErrCredentials
	}
	entry := result.Entries[0]
	subject := entry.GetAttributeValue(p.cfg.SubjectAttribute)
	groups := entry.GetAttributeValues(p.cfg.GroupAttribute)
	if subject == "" || len(subject) > 4096 || len(groups) > 128 || len(entry.DN) > 4096 {
		return Subject{}, ErrCredentials
	}
	for _, g := range groups {
		if len(g) > 4096 {
			return Subject{}, ErrCredentials
		}
	}
	role, e := p.role(groups)
	if e != nil {
		return Subject{}, e
	}
	if e = conn.Bind(entry.DN, password); e != nil {
		return Subject{}, ErrCredentials
	}
	return Subject{ID: stableID(p.cfg.URL+"/"+p.cfg.BaseDN, subject), Role: role, Groups: append([]string(nil), groups...)}, nil
}
