package kafkaadmin

import (
	"github.com/twmb/franz-go/pkg/kmsg"
	"lunaorbit/backend/internal/kafkaops"
	"sort"
	"strings"
)

// groupEvidence projects only current topic names. Member identifiers, host,
// client metadata and opaque user data never enter the public snapshot.
func groupEvidence(r kmsg.DescribeGroupsResponseGroup) kafkaops.Group {
	g := kafkaops.Group{ID: r.Group, State: r.State, ProtocolType: r.ProtocolType, AssignedTopics: []string{}, SubscribedTopics: []string{}}
	if r.ProtocolType != "consumer" {
		g.EvidenceIssue = "unsupported_group_protocol"
		return g
	}
	if len(r.Members) > 256 {
		g.EvidenceIssue = "group_evidence_limit"
		return g
	}
	g.AssignmentsKnown = true
	g.SubscriptionsKnown = true
	assigned, subscribed := map[string]bool{}, map[string]bool{}
	total := 0
	validTopic := func(t string) bool { return len(t) > 0 && len(t) <= 249 && !strings.ContainsAny(t, "\x00\r\n") }
	for _, m := range r.Members {
		total += len(m.ProtocolMetadata) + len(m.MemberAssignment)
		if total > 1<<20 || len(m.ProtocolMetadata) > 64<<10 || len(m.MemberAssignment) > 64<<10 {
			g.AssignmentsKnown = false
			g.SubscriptionsKnown = false
			g.EvidenceIssue = "group_evidence_limit"
			break
		}
		var metadata kmsg.ConsumerMemberMetadata
		if metadata.ReadFrom(m.ProtocolMetadata) != nil || metadata.Version < 0 || metadata.Version > 3 || len(metadata.Topics) > 1024 {
			g.SubscriptionsKnown = false
			g.EvidenceIssue = "invalid_group_metadata"
		} else {
			for _, t := range metadata.Topics {
				if !validTopic(t) {
					g.SubscriptionsKnown = false
					g.EvidenceIssue = "invalid_group_metadata"
					break
				}
				subscribed[t] = true
			}
		}
		var assignment kmsg.ConsumerMemberAssignment
		if assignment.ReadFrom(m.MemberAssignment) != nil || assignment.Version != 0 || len(assignment.Topics) > 1024 {
			g.AssignmentsKnown = false
			g.EvidenceIssue = "invalid_group_assignment"
		} else {
			for _, t := range assignment.Topics {
				if !validTopic(t.Topic) || len(t.Partitions) > 16384 {
					g.AssignmentsKnown = false
					g.EvidenceIssue = "invalid_group_assignment"
					break
				}
				if len(t.Partitions) > 0 {
					assigned[t.Topic] = true
				}
			}
		}
		if len(assigned) > 1024 || len(subscribed) > 1024 {
			g.AssignmentsKnown = false
			g.SubscriptionsKnown = false
			g.EvidenceIssue = "group_evidence_limit"
			break
		}
	}
	if g.AssignmentsKnown {
		for t := range assigned {
			g.AssignedTopics = append(g.AssignedTopics, t)
		}
	}
	if g.SubscriptionsKnown {
		for t := range subscribed {
			g.SubscribedTopics = append(g.SubscribedTopics, t)
		}
	}
	sort.Strings(g.AssignedTopics)
	sort.Strings(g.SubscribedTopics)
	return g
}
