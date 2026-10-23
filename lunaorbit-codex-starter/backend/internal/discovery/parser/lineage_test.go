package parser

import "testing"

func TestEffectiveLineageSemantics(t *testing.T) {
	c, e := Parse("mm2.properties", "clusters=a,b\na->b.enabled=true\ntopics=orders.*\ntopics.exclude=private.*\nreplication.policy.separator=_\na->b.topics.exclude=local.*\n")
	if e != nil {
		t.Fatal(e)
	}
	r := c.Rules[0]
	if !r.SemanticsKnown || r.TopicsRaw != "orders.*" || r.TopicsExcludeRaw != "local.*" || r.Separator != "_" || r.Policy != "org.apache.kafka.connect.mirror.DefaultReplicationPolicy" {
		t.Fatalf("unexpected semantics: %+v", r)
	}
	c, e = Parse("mm2.properties", "a->b.enabled=true\ntopic.filter.class=custom.Filter\n")
	if e != nil || c.Rules[0].SemanticsKnown {
		t.Fatalf("custom filter must remain unknown: %+v %v", c, e)
	}
}
func TestLineageDefaultsAndOverrideFingerprint(t *testing.T) {
	a, e := Parse("mm2.properties", "a->b.enabled=true\n")
	if e != nil {
		t.Fatal(e)
	}
	r := a.Rules[0]
	if !r.SemanticsKnown || r.TopicsRaw != ".*" || r.TopicsExcludeRaw != `.*[\-\.]internal, .*\.replica, __.*` || r.Separator != "." {
		t.Fatalf("%+v", r)
	}
	b, e := Parse("mm2.properties", "a->b.enabled=true\na->b.topics.exclude=\n")
	if e != nil {
		t.Fatal(e)
	}
	if b.Rules[0].TopicsExcludeRaw != "" || a.Fingerprint == b.Fingerprint {
		t.Fatal("explicit empty exclusion lost")
	}
}
func TestInternalSeparatorFlagSemantics(t *testing.T) {
	for _, tc := range []struct {
		config         string
		enabled, known bool
	}{
		{"", true, true},
		{"replication.policy.internal.topic.separator.enabled=false\n", false, true},
		{"replication.policy.internal.topic.separator.enabled=false\na->b.replication.policy.internal.topic.separator.enabled=true\n", true, true},
		{"replication.policy.internal.topic.separator.enabled=secret-invalid\n", false, false},
	} {
		c, e := Parse("mm2.properties", "a->b.enabled=true\n"+tc.config)
		if e != nil {
			t.Fatal(e)
		}
		r := c.Rules[0]
		if r.InternalSeparatorEnabled != tc.enabled || r.SemanticsKnown != tc.known {
			t.Fatalf("%+v", r)
		}
		if !tc.known && (len(c.Issues) != 1 || c.Issues[0] != "invalid_internal_separator_enabled") {
			t.Fatalf("unsafe or absent issue: %+v", c.Issues)
		}
	}
}
