// Package parser extracts bounded, allowlisted MM2 declaration semantics only.
package parser

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	Aliases []string
	// DeclaredAliases contains only valid members of an explicit clusters key.
	DeclaredAliases []string
	Bootstraps      map[string][]string
	Rules           []Rule
	Fingerprint     string
	Issues          []string
}
type Rule struct {
	Source, Target, TopicsRaw, Policy             string
	Enabled                                       bool
	EnabledExplicit                               bool
	TopicsExcludeRaw, Separator, TopicFilterClass string
	SemanticsKnown                                bool
	InternalSeparatorEnabled                      bool
}

const maxText = 1 << 20

var aliasRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$`)
var policyRE = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*(\.[A-Za-z_$][A-Za-z0-9_$]*)*$`)
var hostRE = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$`)
var errInvalid = errors.New("invalid_configuration")
var errLimit = errors.New("configuration_limit")

// Parse never returns source input in errors. Fingerprints exclude unknown and
// sensitive fields. This parser makes no health, identity, or topic-count claims.
func Parse(key, text string) (Config, error) {
	empty := Config{}
	if len(key) > 256 || len(text) > maxText {
		return empty, errLimit
	}
	p := &extractor{values: map[string]string{}}
	lower := strings.ToLower(key)
	if strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml") {
		dec := yaml.NewDecoder(strings.NewReader(text))
		var root yaml.Node
		if dec.Decode(&root) != nil {
			return empty, errInvalid
		}
		var extra yaml.Node
		if dec.Decode(&extra) != io.EOF {
			return empty, errInvalid
		}
		count := 0
		if e := validateNode(&root, 0, &count); e != nil {
			return empty, e
		}
		if e := p.walk(&root, 0); e != nil {
			return empty, e
		}
	} else {
		if e := p.properties(text); e != nil {
			return empty, e
		}
	}
	c := Config{Bootstraps: map[string][]string{}}
	aliases := map[string]bool{}
	declaredAliases := map[string]bool{}
	rules := map[string]*Rule{}
	issue := func(s string) {
		for _, v := range c.Issues {
			if v == s {
				return
			}
		}
		c.Issues = append(c.Issues, s)
	}
	keys := make([]string, 0, len(p.values))
	for k := range p.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := p.values[k]
		if isSemanticField(k) {
			continue
		}
		if k == "clusters" {
			for _, a := range strings.Split(v, ",") {
				a = strings.TrimSpace(a)
				if !aliasRE.MatchString(a) {
					issue("invalid_alias")
					continue
				}
				aliases[a] = true
				declaredAliases[a] = true
			}
			continue
		}
		if strings.HasSuffix(k, ".bootstrap.servers") {
			a := strings.TrimSuffix(k, ".bootstrap.servers")
			if !aliasRE.MatchString(a) {
				issue("invalid_alias")
				continue
			}
			var hosts []string
			valid := true
			for _, h := range strings.Split(v, ",") {
				h = strings.TrimSpace(h)
				host, port, e := net.SplitHostPort(h)
				n, pe := strconv.Atoi(port)
				if e != nil || pe != nil || n < 1 || n > 65535 || (!hostRE.MatchString(host) && net.ParseIP(host) == nil) || strings.ContainsAny(h, "/@?#\\\r\n") {
					valid = false
					break
				}
				hosts = append(hosts, net.JoinHostPort(strings.ToLower(host), strconv.Itoa(n)))
			}
			if !valid || len(hosts) > 64 {
				issue("invalid_bootstrap")
				continue
			}
			sort.Strings(hosts)
			c.Bootstraps[a] = hosts
			aliases[a] = true
			continue
		}
		field := ""
		for _, s := range []string{"enabled", "topics.exclude", "topics.blacklist", "topics", "replication.policy.class", "replication.policy.separator", "topic.filter.class", "replication.policy.internal.topic.separator.enabled"} {
			if strings.HasSuffix(k, "."+s) {
				field = s
				break
			}
		}
		route := strings.TrimSuffix(k, "."+field)
		pair := strings.Split(route, "->")
		if len(pair) != 2 || !aliasRE.MatchString(pair[0]) || !aliasRE.MatchString(pair[1]) {
			issue("invalid_rule")
			continue
		}
		r := rules[route]
		if r == nil {
			r = &Rule{Source: pair[0], Target: pair[1]}
			rules[route] = r
		}
		aliases[pair[0]] = true
		aliases[pair[1]] = true
		switch field {
		case "enabled":
			if v != "true" && v != "false" {
				return empty, errInvalid
			}
			r.EnabledExplicit = true
			r.Enabled = v == "true"
		case "topics":
			if len(v) > 8192 || strings.ContainsAny(v, "\x00\r\n") {
				issue("invalid_topics")
			} else {
				r.TopicsRaw = v
			}
		case "replication.policy.class":
			if len(v) > 256 || !policyRE.MatchString(v) {
				issue("invalid_policy")
			} else {
				r.Policy = v
			}
		}
	}
	if len(aliases) > 128 || len(rules) > 256 {
		return empty, errLimit
	}
	for a := range aliases {
		c.Aliases = append(c.Aliases, a)
	}
	sort.Strings(c.Aliases)
	for a := range declaredAliases {
		c.DeclaredAliases = append(c.DeclaredAliases, a)
	}
	sort.Strings(c.DeclaredAliases)
	for _, r := range rules {
		effective := func(key, fallback string) string {
			if v, ok := p.values[r.Source+"->"+r.Target+"."+key]; ok {
				return v
			}
			if v, ok := p.values[key]; ok {
				return v
			}
			return fallback
		}
		// Kafka 3.9 DefaultTopicFilter and DefaultReplicationPolicy defaults.
		// Keep Java regex text intact; matching support is decided by lineage.
		r.TopicsRaw = effective("topics", ".*")
		r.TopicsExcludeRaw = effective("topics.exclude", effective("topics.blacklist", `.*[\-\.]internal, .*\.replica, __.*`))
		r.Policy = effective("replication.policy.class", "org.apache.kafka.connect.mirror.DefaultReplicationPolicy")
		r.Separator = effective("replication.policy.separator", ".")
		r.TopicFilterClass = effective("topic.filter.class", "org.apache.kafka.connect.mirror.DefaultTopicFilter")
		r.SemanticsKnown = (r.Policy == "org.apache.kafka.connect.mirror.DefaultReplicationPolicy" || r.Policy == "org.apache.kafka.connect.mirror.IdentityReplicationPolicy") && r.TopicFilterClass == "org.apache.kafka.connect.mirror.DefaultTopicFilter"
		internalSeparator := effective("replication.policy.internal.topic.separator.enabled", "true")
		r.InternalSeparatorEnabled = internalSeparator == "true"
		if internalSeparator != "true" && internalSeparator != "false" {
			r.SemanticsKnown = false
			issue("invalid_internal_separator_enabled")
		}
		if len(r.TopicsRaw) > 8192 || len(r.TopicsExcludeRaw) > 8192 || len(r.Separator) > 32 || strings.ContainsAny(r.TopicsRaw+r.TopicsExcludeRaw+r.Separator, "\x00\r\n") {
			return empty, errInvalid
		}
		if len(r.TopicFilterClass) > 256 || !policyRE.MatchString(r.TopicFilterClass) || len(r.Policy) > 256 || !policyRE.MatchString(r.Policy) {
			return empty, errInvalid
		}
		c.Rules = append(c.Rules, *r)
	}
	sort.Slice(c.Rules, func(i, j int) bool {
		return c.Rules[i].Source+"->"+c.Rules[i].Target < c.Rules[j].Source+"->"+c.Rules[j].Target
	})
	sort.Strings(c.Issues)
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	c.Fingerprint = hex.EncodeToString(sum[:])
	return c, nil
}

type extractor struct {
	values         map[string]string
	nodes, entries int
	lines          int
}

func isSemanticField(k string) bool {
	switch k {
	case "topics", "topics.exclude", "topics.blacklist", "replication.policy.class", "replication.policy.separator", "topic.filter.class", "replication.policy.internal.topic.separator.enabled":
		return true
	}
	return false
}
func allowed(k string) bool {
	if k == "clusters" || isSemanticField(k) || strings.HasSuffix(k, ".bootstrap.servers") {
		return true
	}
	if strings.Contains(k, "->") {
		for _, field := range []string{"enabled", "topics", "topics.exclude", "topics.blacklist", "replication.policy.class", "replication.policy.separator", "topic.filter.class", "replication.policy.internal.topic.separator.enabled"} {
			if strings.HasSuffix(k, "."+field) {
				return true
			}
		}
	}
	return false
}

func sensitive(k string) bool {
	k = strings.ToLower(k)
	for _, s := range []string{"secret", "password", "passwd", "jaas", "token", "credential", "sasl", "private"} {
		if strings.Contains(k, s) {
			return true
		}
	}
	return false
}

// Validate structure without following YAML references or inspecting secret
// subtrees. Decode into Node never expands aliases into Go values.
func validateNode(n *yaml.Node, depth int, count *int) error {
	*count++
	if depth > 24 || *count > 8192 {
		return errLimit
	}
	if n.Kind == yaml.AliasNode || n.Anchor != "" {
		return errInvalid
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			if e := validateNode(k, depth+1, count); e != nil {
				return e
			}
			if k.Kind != yaml.ScalarNode {
				return errInvalid
			}
			if sensitive(k.Value) {
				continue
			}
			if e := validateNode(v, depth+1, count); e != nil {
				return e
			}
		}
	} else {
		for _, v := range n.Content {
			if e := validateNode(v, depth+1, count); e != nil {
				return e
			}
		}
	}
	return nil
}
func (p *extractor) put(k, v string) error {
	p.entries++
	if p.entries > 2048 || len(k) > 256 {
		return errLimit
	}
	if !allowed(k) {
		return nil
	}
	if _, ok := p.values[k]; ok {
		return errInvalid
	}
	p.values[k] = v
	return nil
}
func (p *extractor) walk(n *yaml.Node, depth int) error {
	p.nodes++
	if depth > 24 || p.nodes > 8192 {
		return errLimit
	}
	if n.Kind == yaml.AliasNode || n.Anchor != "" {
		return errInvalid
	}
	switch n.Kind {
	case yaml.DocumentNode:
		for _, v := range n.Content {
			if e := p.walk(v, depth+1); e != nil {
				return e
			}
		}
	case yaml.MappingNode:
		for i := 0; i < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			p.nodes += 2
			if p.nodes > 8192 {
				return errLimit
			}
			if k.Kind != yaml.ScalarNode {
				return errInvalid
			}
			name := k.Value
			if sensitive(name) {
				continue
			}
			if allowed(name) {
				var value string
				if name == "clusters" && v.Kind == yaml.SequenceNode {
					var a []string
					for _, x := range v.Content {
						p.nodes++
						if x.Kind != yaml.ScalarNode || x.Anchor != "" {
							return errInvalid
						}
						a = append(a, x.Value)
					}
					value = strings.Join(a, ",")
				} else {
					if v.Kind != yaml.ScalarNode || v.Anchor != "" {
						return errInvalid
					}
					value = v.Value
				}
				if e := p.put(name, value); e != nil {
					return e
				}
				continue
			}
			// Only explicit safe wrapper names and file suffixes are traversed.
			if name == "data" || name == "config" || name == "mm2" {
				var e error
				if v.Kind == yaml.ScalarNode {
					e = p.properties(v.Value)
				} else {
					e = p.walk(v, depth+1)
				}
				if e != nil {
					return e
				}
			} else if strings.HasSuffix(name, ".properties") && v.Kind == yaml.ScalarNode {
				if e := p.properties(v.Value); e != nil {
					return e
				}
			}
		}
	}
	return nil
}

func (p *extractor) properties(text string) error {
	var logical string
	for _, line := range strings.Split(strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n"), "\n") {
		p.lines++
		if p.lines > 4096 {
			return errLimit
		}
		if logical != "" {
			line = strings.TrimLeft(line, " \t\f")
		}
		logical += line
		slash := 0
		for i := len(logical) - 1; i >= 0 && logical[i] == '\\'; i-- {
			slash++
		}
		if slash%2 == 1 {
			logical = logical[:len(logical)-1]
			continue
		}
		s := strings.TrimLeft(logical, " \t\f")
		logical = ""
		if s == "" || s[0] == '#' || s[0] == '!' {
			continue
		}
		end := len(s)
		escaped := false
		for i := 0; i < len(s); i++ {
			if !escaped && (s[i] == '=' || s[i] == ':' || s[i] == ' ' || s[i] == '\t' || s[i] == '\f') {
				end = i
				break
			}
			if s[i] == '\\' {
				escaped = !escaped
			} else {
				escaped = false
			}
		}
		k, e := unescape(s[:end])
		if e != nil {
			return e
		}
		if !allowed(k) {
			continue
		}
		v := strings.TrimLeft(s[end:], " \t\f")
		if len(v) > 0 && (v[0] == '=' || v[0] == ':') {
			v = strings.TrimLeft(v[1:], " \t\f")
		}
		value, e := unescape(v)
		if e != nil {
			return e
		}
		if e = p.put(k, value); e != nil {
			return e
		}
	}
	if logical != "" {
		return errInvalid
	}
	return nil
}
func unescape(s string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			continue
		}
		i++
		if i >= len(s) {
			return "", errInvalid
		}
		switch s[i] {
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		case 'n':
			b.WriteByte('\n')
		case 'f':
			b.WriteByte('\f')
		case 'u':
			if i+4 >= len(s) {
				return "", errInvalid
			}
			v, e := strconv.ParseUint(s[i+1:i+5], 16, 16)
			if e != nil {
				return "", errInvalid
			}
			i += 4
			r := rune(v)
			if utf16.IsSurrogate(r) {
				if r > 0xDBFF || i+6 >= len(s) || s[i+1:i+3] != "\\u" {
					return "", errInvalid
				}
				v, e = strconv.ParseUint(s[i+3:i+7], 16, 16)
				if e != nil || v < 0xDC00 || v > 0xDFFF {
					return "", errInvalid
				}
				r = utf16.DecodeRune(r, rune(v))
				i += 6
			}
			b.WriteRune(r)
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String(), nil
}
