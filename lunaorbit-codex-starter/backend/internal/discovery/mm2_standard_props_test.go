package discovery

import "testing"

func TestStandardMM2SwitchesKeepTheSnapshotCompleteAndFresh(t *testing.T) {
	col := autoCollection("edge-a")
	col.Namespaces[0].ConfigMaps[0].Data = map[string]string{"mm2.properties": "clusters=k1,k2\nk1.bootstrap.servers=k1:9092\nk2.bootstrap.servers=k2:9092\nk1->k2.enabled=true\nk1->k2.topics=.*\n" +
		"k1->k2.emit.heartbeats.enabled=true\nk1->k2.emit.offset-syncs.enabled=false\nk1->k2.emit.checkpoints.enabled=false\nk1->k2.sync.group.offsets.enabled=false\nk1->k2.sync.topic.acls.enabled=false\n" +
		"emit.offset-syncs.enabled=false\nemit.checkpoints.enabled=false\nsync.group.offsets.enabled=false\n"}
	s, err := Build(col, nil, nil, nil, Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range s.Issues {
		if i.Code == "INVALID_RULE" || i.Code == "PARTIAL_COLLECTION" {
			t.Fatalf("standard MM2 switches must not raise %s: %+v", i.Code, s.Issues)
		}
	}
	if s.Stale || len(s.Links) != 1 || !s.Links[0].Enabled {
		t.Fatalf("stale=%v links=%+v", s.Stale, s.Links)
	}
}
