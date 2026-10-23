package lineage

import (
	"context"
	"slices"
	"sort"
	"time"
)

// Consumer enrichment is deliberately last and uses at most 26 reads per cluster.
// Missing details are unknown, never evidence that no consumer exists.
func (s *Service) consumers(ctx context.Context, g *Graph, clusters map[string]string, invs map[string]inventory, q Query, addNode func(Node) bool, addEdge func(Edge), issue func(string, string, string, string)) {
	if q.Direction == "upstream" {
		return
	}
	topics := map[string][]string{}
	for _, n := range g.Nodes {
		if n.Kind == "topic" {
			topics[n.ClusterID] = append(topics[n.ClusterID], n.Topic)
		}
	}
	cs := []string{}
	for c := range topics {
		cs = append(cs, c)
	}
	sort.Strings(cs)
	total := 0
	for _, c := range cs {
		if ctx.Err() != nil || total >= 64 {
			g.Truncated = true
			issue("CONSUMERS_TRUNCATED", "已达到请求时限或消费组总读取预算（64 次）", c, "")
			break
		}
		snap, e := s.p.Read(ctx, c, "groups", "")
		total++
		if e != nil {
			issue("GROUPS_UNAVAILABLE", "无法读取消费组清单，消费关系未知", c, "")
			continue
		}
		if snap.Stale || snap.State == "partial" || snap.State == "unavailable" {
			issue("GROUPS_PARTIAL", "消费组清单不完整", c, "")
		}
		groups := slices.Clone(snap.Groups)
		sort.Slice(groups, func(i, j int) bool { return groups[i].ID < groups[j].ID })
		if len(groups) > 12 {
			groups = groups[:12]
			g.Truncated = true
			issue("GROUPS_TRUNCATED", "每个集群仅检查前 12 个消费组，其余已截断", c, "")
		}
		for _, group := range groups {
			if ctx.Err() != nil || total >= 64 {
				g.Truncated = true
				break
			}
			detail, err := s.p.Read(ctx, c, "groups", group.ID)
			total++
			// Kafka Connect worker coordination groups do not describe business Topic subscriptions.
			if err == nil && !detail.Stale && detail.State != "partial" && detail.State != "unavailable" {
				connect := false
				for _, row := range detail.Groups {
					if row.ID == group.ID && row.ProtocolType == "connect" && row.ErrorCode == "" {
						connect = true
					}
				}
				if connect {
					continue
				}
			}
			groupIssue := func(code, msg string) { issue(code, clusters[c]+" / "+group.ID+"："+msg, c, "") }
			current := map[string]bool{}
			at := detail.ObservedAt
			if err != nil {
				groupIssue("GROUP_DETAILS_UNAVAILABLE", "当前消费成员证据不可用")
			} else if detail.Stale || detail.State == "partial" || detail.State == "unavailable" {
				groupIssue("GROUP_EVIDENCE_UNKNOWN", "当前消费成员证据已过期或不完整")
			} else {
				for _, row := range detail.Groups {
					if row.ID != group.ID {
						continue
					}
					if row.ErrorCode != "" || row.EvidenceIssue != "" || (!row.AssignmentsKnown && !row.SubscriptionsKnown) {
						groupIssue("GROUP_EVIDENCE_UNKNOWN", "无法确认当前订阅或分区分配")
					}
					for _, t := range topics[c] {
						if (row.AssignmentsKnown && slices.Contains(row.AssignedTopics, t)) || (row.SubscriptionsKnown && slices.Contains(row.SubscribedTopics, t)) {
							current[t] = true
						}
					}
				}
			}
			historical := map[string]bool{}
			var historicalAt time.Time
			if total < 64 && ctx.Err() == nil {
				offsets, err := s.p.Read(ctx, c, "offsets", group.ID)
				total++
				if err != nil {
					groupIssue("OFFSETS_UNAVAILABLE", "历史已提交消费位点不可用")
				} else {
					historicalAt = offsets.ObservedAt
					for _, o := range offsets.Offsets {
						if o.Group == group.ID && o.Committed != nil && o.ErrorCode == "" {
							historical[o.Topic] = true
						}
					}
					if at.IsZero() {
						at = offsets.ObservedAt
					}
				}
			}
			for _, topic := range topics[c] {
				edgeAt := at
				evidence := ""
				if current[topic] {
					evidence = "observed"
				} else if historical[topic] {
					evidence = "historical"
					edgeAt = historicalAt
				}
				if evidence == "" {
					continue
				}
				gid := id("observed_group", c, group.ID)
				if !addNode(Node{ID: gid, Kind: "consumer_group", Label: group.ID, ClusterID: c, ClusterName: clusters[c], Group: group.ID, Evidence: evidence, ObservedAt: edgeAt}) {
					continue
				}
				addEdge(Edge{Source: topicID(c, topic), Target: gid, Kind: "consumes", Evidence: evidence, ObservedAt: edgeAt})
			}
		}
	}
}
