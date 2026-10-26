// Package mm2auth reads Kafka client credentials from MM2 configuration for
// read-only metadata collection (user-approved "auth B" exception).
//
// Credentials live only in process memory. Every type here redacts itself for
// fmt, log/slog and encoding/json so a credential can never be printed, logged
// or serialised by accident. Nothing in this package returns the jaas text.
package mm2auth

import (
	"errors"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"go.yaml.in/yaml/v3"
)

const redacted = "[redacted]"

// Credential is one MM2 cluster alias's client security settings.
type Credential struct {
	protocol, mechanism, user, pass string
}

func (c Credential) String() string               { return redacted }
func (c Credential) GoString() string             { return redacted }
func (c Credential) LogValue() slog.Value         { return slog.StringValue(redacted) }
func (c Credential) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }
func (c Credential) MarshalText() ([]byte, error) { return []byte(redacted), nil }

// Protocol is the Kafka security.protocol (PLAINTEXT when unset).
func (c Credential) Protocol() string {
	if c.protocol == "" {
		return "PLAINTEXT"
	}
	return c.protocol
}

// UsesSASL reports whether the protocol requires SASL.
func (c Credential) UsesSASL() bool { return strings.HasPrefix(c.Protocol(), "SASL_") }

// UsesTLS reports whether the protocol requires TLS.
func (c Credential) UsesTLS() bool { return strings.HasSuffix(c.Protocol(), "SSL") }

// Usable reports whether a client can be built: no SASL, or SASL with a supported
// mechanism and both username and password.
func (c Credential) Usable() bool {
	if !c.UsesSASL() {
		return true
	}
	_, err := c.SASL()
	return err == nil
}

// ErrUnsupported is returned for mechanisms other than PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512
// or when username/password are missing. It never contains configuration text.
var ErrUnsupported = errors.New("mm2 credential unsupported")

// SASL builds the franz-go mechanism; the jaas text is never passed through.
func (c Credential) SASL() (sasl.Mechanism, error) {
	if c.user == "" || c.pass == "" {
		return nil, ErrUnsupported
	}
	switch strings.ToUpper(c.mechanism) {
	case "", "PLAIN":
		return plain.Auth{User: c.user, Pass: c.pass}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: c.user, Pass: c.pass}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: c.user, Pass: c.pass}.AsSha512Mechanism(), nil
	}
	return nil, ErrUnsupported
}

var jaasField = regexp.MustCompile(`(?s)\b(username|password)\s*=\s*"((?:[^"\\]|\\.)*)"`)

// parseJAAS extracts username/password from a PlainLoginModule or ScramLoginModule line.
func parseJAAS(s string) (user, pass string) {
	if !strings.Contains(s, "PlainLoginModule") && !strings.Contains(s, "ScramLoginModule") {
		return "", ""
	}
	for _, m := range jaasField.FindAllStringSubmatch(s, -1) {
		v := strings.NewReplacer(`\"`, `"`, `\\`, `\`).Replace(m[2])
		if m[1] == "username" {
			user = v
		} else {
			pass = v
		}
	}
	return user, pass
}

var aliasRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$`)

// Parse reads security.protocol, sasl.mechanism and sasl.jaas.config (global and
// "<alias>."-prefixed) from an MM2 properties or YAML file and returns one
// Credential per alias that has any setting. Errors never contain input text.
func Parse(key, text string) (map[string]Credential, error) {
	values, err := flatten(key, text)
	if err != nil {
		return nil, err
	}
	global := Credential{}
	perAlias := map[string]*Credential{}
	set := func(c *Credential, field, v string) {
		switch field {
		case "security.protocol":
			c.protocol = strings.ToUpper(strings.TrimSpace(v))
		case "sasl.mechanism":
			c.mechanism = strings.ToUpper(strings.TrimSpace(v))
		case "sasl.jaas.config":
			c.user, c.pass = parseJAAS(v)
		}
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, field := range []string{"security.protocol", "sasl.mechanism", "sasl.jaas.config"} {
			if k == field {
				set(&global, field, values[k])
			} else if alias, ok := strings.CutSuffix(k, "."+field); ok && aliasRE.MatchString(alias) {
				c := perAlias[alias]
				if c == nil {
					c = &Credential{}
					perAlias[alias] = c
				}
				set(c, field, values[k])
			}
		}
	}
	out := map[string]Credential{}
	for _, alias := range aliasesOf(values) {
		c := global
		if p := perAlias[alias]; p != nil {
			if p.protocol != "" {
				c.protocol = p.protocol
			}
			if p.mechanism != "" {
				c.mechanism = p.mechanism
			}
			if p.user != "" || p.pass != "" {
				c.user, c.pass = p.user, p.pass
			}
		}
		if c != (Credential{}) {
			out[alias] = c
		}
	}
	return out, nil
}

func aliasesOf(values map[string]string) []string {
	set := map[string]bool{}
	for _, a := range strings.Split(values["clusters"], ",") {
		if a = strings.TrimSpace(a); aliasRE.MatchString(a) {
			set[a] = true
		}
	}
	for k := range values {
		if a, ok := strings.CutSuffix(k, ".bootstrap.servers"); ok && aliasRE.MatchString(a) {
			set[a] = true
		}
	}
	out := make([]string, 0, len(set))
	for a := range set {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

var errInvalid = errors.New("mm2 configuration invalid")

func wanted(k string) bool {
	return k == "clusters" || strings.HasSuffix(k, ".bootstrap.servers") || strings.HasSuffix(k, "security.protocol") || strings.HasSuffix(k, "sasl.mechanism") || strings.HasSuffix(k, "sasl.jaas.config")
}

func flatten(key, text string) (map[string]string, error) {
	if len(text) > 1<<20 {
		return nil, errInvalid
	}
	values := map[string]string{}
	lower := strings.ToLower(key)
	if strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml") {
		var root map[string]any
		if yaml.Unmarshal([]byte(text), &root) != nil {
			return nil, errInvalid
		}
		for k, v := range root {
			switch t := v.(type) {
			case string:
				if wanted(k) {
					values[k] = t
				}
			case []any:
				if k == "clusters" {
					parts := make([]string, 0, len(t))
					for _, x := range t {
						if s, ok := x.(string); ok {
							parts = append(parts, s)
						}
					}
					values[k] = strings.Join(parts, ",")
				}
			}
		}
		return values, nil
	}
	logical := ""
	for _, line := range strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n") {
		if logical != "" {
			line = strings.TrimLeft(line, " \t\f")
		}
		if strings.HasSuffix(line, `\`) && !strings.HasSuffix(line, `\\`) {
			logical += strings.TrimSuffix(line, `\`)
			continue
		}
		s := strings.TrimSpace(logical + line)
		logical = ""
		if s == "" || s[0] == '#' || s[0] == '!' {
			continue
		}
		i := strings.IndexAny(s, "=:")
		if i <= 0 {
			continue
		}
		k, v := strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:])
		if wanted(k) {
			values[k] = v
		}
	}
	return values, nil
}

// Store keeps credentials in memory only, keyed by endpoint key. It is safe for
// concurrent use. Nothing is persisted; a restart forgets everything.
type Store struct {
	mu sync.RWMutex
	m  map[string]Credential
}

func NewStore() *Store { return &Store{m: map[string]Credential{}} }

func (s *Store) String() string               { return redacted }
func (s *Store) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }

// Put records the credential for an endpoint key.
func (s *Store) Put(key string, c Credential) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = c
}

// Get returns the credential for an endpoint key.
func (s *Store) Get(key string) (Credential, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.m[key]
	return c, ok
}

// ForgetPrefix drops every key that starts with prefix (e.g. one scope before re-ingest).
func (s *Store) ForgetPrefix(prefix string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.m {
		if strings.HasPrefix(k, prefix) {
			delete(s.m, k)
		}
	}
}

// Len is the number of stored credentials.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}
