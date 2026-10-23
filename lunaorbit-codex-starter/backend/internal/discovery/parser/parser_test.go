package parser

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestFixturesAndUnicode(t *testing.T) {
	for _, name := range []string{"safe.properties", "safe.yaml"} {
		b, e := os.ReadFile("testdata/" + name)
		if e != nil {
			t.Fatal(e)
		}
		c, e := Parse(name, string(b))
		if e != nil || len(c.Rules) != 1 || c.Rules[0].Policy != "org.example.IdentityReplicationPolicy" {
			t.Fatalf("fixture mismatch %#v %v", c, e)
		}
	}
	c, e := Parse("x.properties", "clu\\u0073ters=a,b\na->b.topics\\:suffix=ignored\na->b.topics=foo\\\\.[0-9]+\na->b.enabled=true")
	if e != nil || len(c.Aliases) != 2 || c.Rules[0].TopicsRaw != "foo\\.[0-9]+" {
		t.Fatalf("unicode mismatch %#v %v", c, e)
	}
}

func TestLimitsAndFixedErrors(t *testing.T) {
	for _, s := range []string{strings.Repeat("x", maxText+1), strings.Repeat("x=y\n", 5000), "clusters=\\uSECRET"} {
		_, e := Parse("x.properties", s)
		if e == nil || (e.Error() != "invalid_configuration" && e.Error() != "configuration_limit") {
			t.Fatal("missing fixed error")
		}
	}
	// Unknown/sensitive subtrees are never treated as configuration wrappers.
	c, e := Parse("x.yaml", "password.properties: |\n  clusters=evil\nsecret:\n  clusters: [evil]\nclusters: [a,b]\na->b.topics: 'x.*'\n")
	if e != nil || len(c.Aliases) != 2 {
		t.Fatalf("sensitive subtree consumed %#v %v", c, e)
	}
}

func TestYAMLStructuralBounds(t *testing.T) {
	for _, s := range []string{strings.Repeat("[", 30) + "x" + strings.Repeat("]", 30), "unknown: &a [*a]", "unknown: [" + strings.Repeat("x,", 9000) + "x]"} {
		if _, e := Parse("x.yaml", s); e == nil {
			t.Fatal("unbounded YAML accepted")
		}
	}
	c, e := Parse("x.yaml", "config: |\n  clusters=a,b\n  a->b.enabled=true\n")
	if e != nil || len(c.Rules) != 1 {
		t.Fatalf("config block rejected %#v %v", c, e)
	}
}

func TestProperties(t *testing.T) {
	c, e := Parse("mm2.properties", "clusters = mars, moon\nmars.bootstrap.servers=broker.invalid:9092\nmars->moon.enabled=false\nmars->moon.topics=orders\\\\..*,\\\n  events.*\nmars.sasl.jaas.config=SECRET")
	if e != nil || len(c.Rules) != 1 || !c.Rules[0].EnabledExplicit || c.Rules[0].Enabled || c.Rules[0].TopicsRaw != "orders\\..*,events.*" {
		t.Fatalf("safe parse mismatch: %#v %v", c, e)
	}
	b, _ := json.Marshal(c)
	if strings.Contains(string(b), "SECRET") {
		t.Fatal("secret leaked")
	}
	d, _ := Parse("mm2.properties", "clusters = mars, moon\nmars.bootstrap.servers=broker.invalid:9092\nmars->moon.enabled=false\nmars->moon.topics=orders\\\\..*,\\\n  events.*\nmars.sasl.jaas.config=DIFFERENT")
	if c.Fingerprint != d.Fingerprint {
		t.Fatal("secret changed fingerprint")
	}
}

func TestDeclaredAliasesDistinguishTopicsOnly(t *testing.T) {
	inferred, e := Parse("x.properties", "a->b.topics=x.*")
	if e != nil || len(inferred.Aliases) != 2 || len(inferred.DeclaredAliases) != 0 {
		t.Fatalf("inferred aliases mismatch %#v %v", inferred, e)
	}
	declared, e := Parse("x.properties", "clusters=b,a,a\na->b.topics=x.*")
	if e != nil || strings.Join(declared.DeclaredAliases, ",") != "a,b" {
		t.Fatalf("declared aliases mismatch %#v %v", declared, e)
	}
	if inferred.Fingerprint == declared.Fingerprint {
		t.Fatal("explicit declaration missing from fingerprint")
	}
	yamlConfig, e := Parse("x.yaml", "clusters: [a,b]\na->b.topics: x.*")
	if e != nil || strings.Join(yamlConfig.DeclaredAliases, ",") != "a,b" {
		t.Fatalf("yaml declaration mismatch %#v %v", yamlConfig, e)
	}
}

func TestYAML(t *testing.T) {
	c, e := Parse("mm2.yaml", "clusters: [mars, moon]\nmars.bootstrap.servers: broker.invalid:9092\nmars->moon.topics: 'orders\\..*'\nsecret:\n  clusters: [evil]\n")
	if e != nil || len(c.Aliases) != 2 || len(c.Rules) != 1 || c.Rules[0].EnabledExplicit || c.Rules[0].TopicsRaw != "orders\\..*" {
		t.Fatalf("mismatch %#v %v", c, e)
	}
	c, e = Parse("config.yaml", "data:\n  mm2.properties: |\n    clusters=mars,moon\n    mars->moon.enabled=true\n")
	if e != nil || len(c.Rules) != 1 || !c.Rules[0].Enabled {
		t.Fatalf("embedded mismatch %#v %v", c, e)
	}
}

func TestRejectAmbiguityAndUnsafe(t *testing.T) {
	for _, s := range []string{"clusters: [a,b]\nclusters: [b,c]", "clusters: &a [*a]", "a: {b: {c: {}}}\nclusters: [a,b]\na->b.enabled: yes"} {
		if _, e := Parse("x.yaml", s); e == nil {
			t.Fatal("accepted unsafe/ambiguous input")
		}
	}
	c, e := Parse("x.properties", "clusters=a,b\na.bootstrap.servers=http://user:password@host:9092\na->b.enabled=true")
	if e != nil || len(c.Bootstraps) != 0 || len(c.Issues) == 0 {
		t.Fatal("unsafe bootstrap accepted")
	}
	if _, e = Parse("x.properties", "clusters=a,b\nclusters=b,c"); e == nil {
		t.Fatal("duplicate accepted")
	}
}
