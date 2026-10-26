package parser

import (
	"strings"
	"testing"
)

// Production shape (desensitised): hyphenated aliases, global emit/sync switches, route-prefixed emit.heartbeats.
const productionShape = `clusters = OPTION-SHFE-ZX-MARS, MARS-OPTION-SHFE-zx
OPTION-SHFE-ZX-MARS.bootstrap.servers = 10.0.0.1:9092
MARS-OPTION-SHFE-zx.bootstrap.servers = 10.0.0.2:9092
OPTION-SHFE-ZX-MARS->MARS-OPTION-SHFE-zx.enabled = true
MARS-OPTION-SHFE-zx->OPTION-SHFE-ZX-MARS.enabled = false
OPTION-SHFE-ZX-MARS->MARS-OPTION-SHFE-zx.topics = derivatives.*|yd.*
OPTION-SHFE-ZX-MARS->MARS-OPTION-SHFE-zx.emit.heartbeats = true
replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy
emit.offset-syncs.enabled = false
emit.checkpoints.enabled = false
sync.group.offsets.enabled = false
OPTION-SHFE-ZX-MARS.sasl.jaas.config = org.apache.kafka.common.security.scram.ScramLoginModule required username="<user>" password="<placeholder>";
tasks.max = 4
`

// SRE lab shape: route-prefixed multi-segment switches.
const routePrefixed = `clusters = k1, k3
k1.bootstrap.servers = k1:9092
k3.bootstrap.servers = k3:9092
k1->k3.enabled = true
k1->k3.topics = luna-.*
k1->k3.emit.heartbeats.enabled = true
k1->k3.emit.offset-syncs.enabled = false
k1->k3.emit.checkpoints.enabled = false
k1->k3.sync.group.offsets.enabled = false
k1->k3.sync.topic.acls.enabled = false
k1->k3.refresh.topics.interval.seconds = 60
k1->k3.offset-syncs.topic.location = target
`

func TestMM2StandardMultiSegmentPropertiesAreNotInvalidRules(t *testing.T) {
	for name, text := range map[string]string{"production": productionShape, "route-prefixed": routePrefixed} {
		c, err := Parse("mm2.properties", text)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(c.Issues) != 0 {
			t.Fatalf("%s: unexpected issues %v", name, c.Issues)
		}
		if len(c.Rules) != map[string]int{"production": 2, "route-prefixed": 1}[name] {
			t.Fatalf("%s rules %+v", name, c.Rules)
		}
	}
	c, _ := Parse("mm2.properties", productionShape)
	for _, r := range c.Rules {
		if r.Source == "OPTION-SHFE-ZX-MARS" && (!r.Enabled || r.TopicsRaw != "derivatives.*|yd.*") {
			t.Fatalf("forward rule %+v", r)
		}
		if r.Source == "MARS-OPTION-SHFE-zx" && r.Enabled {
			t.Fatalf("reverse must be disabled %+v", r)
		}
	}
	c, _ = Parse("mm2.properties", routePrefixed)
	if c.Rules[0].Source != "k1" || c.Rules[0].Target != "k3" || !c.Rules[0].Enabled || c.Rules[0].TopicsRaw != "luna-.*" {
		t.Fatalf("route-prefixed rule %+v", c.Rules[0])
	}
}

func TestGenuinelyInvalidRulesAreStillRejected(t *testing.T) {
	c, _ := Parse("mm2.properties", "clusters = a, b\na.bootstrap.servers = a:9092\nb.bootstrap.servers = b:9092\na->b->c.enabled = true\na->b.enabled = true\n")
	if !strings.Contains(strings.Join(c.Issues, ","), "invalid_rule") {
		t.Fatalf("three-hop route must stay invalid: %v", c.Issues)
	}
	c, _ = Parse("mm2.properties", "clusters = a, b\na->b.enabled = true\na.b->b.enabled = true\na->b!.enabled = true\n")
	if !strings.Contains(strings.Join(c.Issues, ","), "invalid_rule") {
		t.Fatalf("bad alias must stay invalid: %v", c.Issues)
	}
	if _, err := Parse("mm2.properties", "clusters = a, b\na->b.enabled = maybe\n"); err == nil {
		t.Fatal("non-boolean enabled must be rejected")
	}
}
