package mm2auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

const canary = "canary-pw-7f3c9e1d"

const props = `clusters = src, dst
src.bootstrap.servers = src-kafka:9092
dst.bootstrap.servers = dst-kafka:9093
src.security.protocol = SASL_PLAINTEXT
src.sasl.mechanism = SCRAM-SHA-512
src.sasl.jaas.config = org.apache.kafka.common.security.scram.ScramLoginModule required username="mm2-src" password="` + canary + `";
security.protocol = SASL_SSL
sasl.mechanism = PLAIN
sasl.jaas.config = org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="mm2-all" password="global-` + canary + `";
`

func TestParseGlobalAndPerAliasCredentials(t *testing.T) {
	got, err := Parse("mm2.properties", props)
	if err != nil {
		t.Fatal(err)
	}
	src, dst := got["src"], got["dst"]
	if src.Protocol() != "SASL_PLAINTEXT" || src.mechanism != "SCRAM-SHA-512" || src.user != "mm2-src" || src.pass != canary {
		t.Fatalf("src not parsed")
	}
	if dst.Protocol() != "SASL_SSL" || dst.mechanism != "PLAIN" || dst.user != "mm2-all" || dst.pass != "global-"+canary || !dst.UsesTLS() {
		t.Fatalf("dst should inherit global settings")
	}
	for _, c := range []Credential{src, dst} {
		if m, e := c.SASL(); e != nil || m == nil {
			t.Fatalf("sasl: %v", e)
		}
	}
}

func TestParseYAMLAndNoAuth(t *testing.T) {
	got, err := Parse("mm2.yaml", "clusters: [a, b]\na.bootstrap.servers: a:9092\nb.bootstrap.servers: b:9092\nb.security.protocol: SASL_PLAINTEXT\nb.sasl.mechanism: SCRAM-SHA-256\nb.sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule required username=\"u\" password=\"p\";\n")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["a"]; ok {
		t.Fatal("alias without settings must be absent (no auth)")
	}
	if b := got["b"]; b.mechanism != "SCRAM-SHA-256" || !b.Usable() {
		t.Fatal("yaml alias not parsed")
	}
}

func TestUnsupportedOrIncompleteIsNotUsable(t *testing.T) {
	for _, c := range []Credential{{protocol: "SASL_SSL", mechanism: "GSSAPI", user: "u", pass: "p"}, {protocol: "SASL_SSL", mechanism: "PLAIN", user: "u"}} {
		if c.Usable() {
			t.Fatalf("%v usable", c.mechanism)
		}
		if _, e := c.SASL(); e == nil || strings.Contains(e.Error(), "p") && e != ErrUnsupported {
			t.Fatal("error must be generic")
		}
	}
	if !(Credential{protocol: "PLAINTEXT"}).Usable() {
		t.Fatal("plaintext needs no credential")
	}
}

func TestCredentialAndStoreNeverPrintSecrets(t *testing.T) {
	got, _ := Parse("mm2.properties", props)
	s := NewStore()
	for a, c := range got {
		s.Put("scope|ns|uid|"+a, c)
	}
	var logs bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&logs, nil))
	l.Info("creds", "src", got["src"], "store", s, "all", got)
	j, _ := json.Marshal(map[string]any{"c": got, "s": s})
	out := fmt.Sprintf("%v %+v %#v %s %v", got["src"], got["dst"], got, got["src"], s) + logs.String() + string(j)
	for _, secret := range []string{canary, "mm2-src", "mm2-all", "ScramLoginModule", "PlainLoginModule"} {
		if strings.Contains(out, secret) {
			t.Fatalf("leaked %q in %s", secret, out)
		}
	}
	if s.Len() != 2 {
		t.Fatal(s.Len())
	}
	s.ForgetPrefix("scope|")
	if s.Len() != 0 {
		t.Fatal("forget")
	}
}

func TestParseErrorsContainNoInput(t *testing.T) {
	_, err := Parse("mm2.yaml", "a: [unclosed "+canary)
	if err == nil || strings.Contains(err.Error(), canary) {
		t.Fatal(err)
	}
}
