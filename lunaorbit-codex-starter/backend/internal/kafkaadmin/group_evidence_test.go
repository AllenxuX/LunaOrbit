package kafkaadmin

import (
	"encoding/json"
	"github.com/twmb/franz-go/pkg/kmsg"
	"strings"
	"testing"
)

func TestGroupEvidenceCurrentAndUnknown(t *testing.T) {
	meta := kmsg.ConsumerMemberMetadata{Version: 0, Topics: []string{"subscribed"}, UserData: []byte("secret")}
	assignment := kmsg.ConsumerMemberAssignment{Version: 0, Topics: []kmsg.ConsumerMemberAssignmentTopic{{Topic: "assigned", Partitions: []int32{0}}}}
	r := kmsg.DescribeGroupsResponseGroup{Group: "g", State: "Stable", ProtocolType: "consumer", Members: []kmsg.DescribeGroupsResponseGroupMember{{ProtocolMetadata: meta.AppendTo(nil), MemberAssignment: assignment.AppendTo(nil)}}}
	g := groupEvidence(r)
	if !g.AssignmentsKnown || !g.SubscriptionsKnown || len(g.AssignedTopics) != 1 || g.AssignedTopics[0] != "assigned" || g.SubscribedTopics[0] != "subscribed" {
		t.Fatalf("%+v", g)
	}
	raw, _ := json.Marshal(g)
	if strings.Contains(string(raw), "secret") {
		t.Fatal("userdata leaked")
	}
	r.Members[0].MemberAssignment = []byte("secret")
	g = groupEvidence(r)
	if g.AssignmentsKnown || g.EvidenceIssue == "" {
		t.Fatalf("malformed assignment: %+v", g)
	}
	r.ProtocolType = "custom"
	if groupEvidence(r).AssignmentsKnown {
		t.Fatal("unsupported protocol known")
	}
}
func TestGroupEvidenceBounds(t *testing.T) {
	r := kmsg.DescribeGroupsResponseGroup{Group: "g", ProtocolType: "consumer", Members: make([]kmsg.DescribeGroupsResponseGroupMember, 257)}
	if g := groupEvidence(r); g.AssignmentsKnown || g.SubscriptionsKnown || g.EvidenceIssue != "group_evidence_limit" {
		t.Fatalf("%+v", g)
	}
	r.Members = make([]kmsg.DescribeGroupsResponseGroupMember, 1)
	r.Members[0].ProtocolMetadata = make([]byte, 65537)
	if g := groupEvidence(r); g.EvidenceIssue != "group_evidence_limit" {
		t.Fatalf("%+v", g)
	}
}
