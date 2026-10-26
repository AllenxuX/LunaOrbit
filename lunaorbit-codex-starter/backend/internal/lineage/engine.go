package lineage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/information"
	"lunaorbit/backend/internal/kafkaops"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var ErrNotFound = errors.New("topic not found")
var ErrInvalid = errors.New("invalid lineage query")

type Query struct {
	ClusterID, Topic string
	Depth            int
	Direction        string
}
type Node struct {
	ID            string `json:"id"`
	Kind          string `json:"kind"`
	Label         string `json:"label"`
	ClusterID     string `json:"cluster_id,omitempty"`
	ClusterName   string `json:"cluster_name,omitempty"`
	Topic         string `json:"topic,omitempty"`
	Group         string `json:"group,omitempty"`
	Location      string `json:"location,omitempty"`
	Owner         string `json:"owner,omitempty"`
	ApplicationID string `json:"application_id,omitempty"`
	LinkID        string `json:"link_id,omitempty"`
	Exists        *bool  `json:"exists"`
	// ExistenceNote explains an unknown existence for endpoints that cannot be read.
	ExistenceNote string    `json:"existence_note,omitempty"`
	Evidence      string    `json:"evidence"`
	ObservedAt    time.Time `json:"observed_at"`
}
type Edge struct {
	Account    string    `json:"account,omitempty"`
	ClientID   string    `json:"client_id,omitempty"`
	ID         string    `json:"id"`
	Source     string    `json:"source"`
	Target     string    `json:"target"`
	Kind       string    `json:"kind"`
	Evidence   string    `json:"evidence"`
	ObservedAt time.Time `json:"observed_at"`
	ScopeID    string    `json:"scope_id,omitempty"`
	LinkID     string    `json:"link_id,omitempty"`
	Policy     string    `json:"policy,omitempty"`
	Rule       string    `json:"rule,omitempty"`
}
type Issue struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	ClusterID string `json:"cluster_id,omitempty"`
	LinkID    string `json:"link_id,omitempty"`
}
type Graph struct {
	RootID     string    `json:"root_id"`
	ObservedAt time.Time `json:"observed_at"`
	Nodes      []Node    `json:"nodes"`
	Edges      []Edge    `json:"edges"`
	Issues     []Issue   `json:"issues"`
	Truncated  bool      `json:"truncated"`
	Depth      int       `json:"depth"`
}
type Provider interface {
	Clusters(context.Context) (map[string]string, error)
	Snapshots(context.Context) ([]discovery.Snapshot, error)
	Applications(context.Context) ([]information.Application, bool, error)
	Read(context.Context, string, string, string) (kafkaops.Snapshot, error)
}

// EndpointReader is optionally implemented by providers that can read auto-discovered
// endpoints (through the read-only catalog). ok=false/err means existence stays unknown.
type EndpointReader interface {
	EndpointTopics(ctx context.Context, endpointID string) ([]string, time.Time, error)
}

type Service struct{ p Provider }

func New(p Provider) *Service { return &Service{p} }
func id(kind string, values ...string) string {
	b, _ := json.Marshal(values)
	return kind + ":" + base64.RawURLEncoding.EncodeToString(b)
}
func topicID(c, t string) string { return id("topic", c, t) }
func matches(raw, topic string) (bool, bool) {
	matched := false
	for _, part := range strings.Split(raw, ",") {
		r, e := regexp.Compile("^(?:" + strings.TrimSpace(part) + ")$")
		if e != nil {
			return false, false
		}
		if r.MatchString(topic) {
			matched = true
		}
	}
	return matched, true
}

// Kafka 3.9 ReplicationPolicy.isInternalTopic is an independent veto,
// including when DefaultTopicFilter excludes are explicitly empty.
func internalTopic(topic, separator string, customInternalSeparator bool) bool {
	internalSeparator := "."
	if customInternalSeparator {
		internalSeparator = separator
	}
	return strings.HasPrefix(topic, "__") || strings.HasPrefix(topic, ".") || strings.HasSuffix(topic, "-internal") || strings.HasSuffix(topic, ".internal") || strings.HasSuffix(topic, internalSeparator+"internal")
}

type inventory struct {
	topics map[string]bool
	known  bool
	at     time.Time
}
type scopedLink struct {
	link  discovery.Link
	scope string
	stale bool
}

func (s *Service) Trace(ctx context.Context, q Query) (Graph, error) {
	g := Graph{RootID: topicID(q.ClusterID, q.Topic), ObservedAt: time.Now().UTC(), Depth: q.Depth, Nodes: []Node{}, Edges: []Edge{}, Issues: []Issue{}}
	if q.ClusterID == "" || len(q.ClusterID) > 128 || !regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`).MatchString(q.Topic) || q.Topic == "." || q.Topic == ".." || q.Depth < 1 || q.Depth > 4 || (q.Direction != "both" && q.Direction != "upstream" && q.Direction != "downstream") {
		return g, ErrInvalid
	}
	clusters, e := s.p.Clusters(ctx)
	if e != nil {
		return g, e
	}
	snapshots, snapErr := s.p.Snapshots(ctx)
	// MM2 ends that are not confirmed to a registration but carry addresses become
	// endpoint pseudo-clusters, so the chain continues through them (existence unknown).
	endpointNames := map[string]string{}
	for _, snap := range snapshots {
		for _, l := range snap.Links {
			if l.Removed {
				continue
			}
			for _, m := range []discovery.Mapping{l.Source, l.Target} {
				if id, name, ok := endpointOf(m, clusters); ok && strings.HasPrefix(id, "endpoint-") {
					if _, seen := endpointNames[id]; !seen {
						endpointNames[id] = name
					}
				}
			}
		}
	}
	if _, ok := clusters[q.ClusterID]; !ok {
		if _, ok := endpointNames[q.ClusterID]; !ok {
			return g, ErrNotFound
		}
	}
	issue := func(code, msg, c, l string) {
		if len(g.Issues) < 200 {
			g.Issues = append(g.Issues, Issue{code, msg, c, l})
		}
	}
	inventories := map[string]inventory{}
	calls := 0
	load := func(c string) inventory {
		if v, ok := inventories[c]; ok {
			return v
		}
		v := inventory{topics: map[string]bool{}}
		if len(inventories) >= 16 || calls >= 96 || ctx.Err() != nil {
			g.Truncated = true
			issue("READ_BUDGET", "已达到读取预算或请求时限", c, "")
			inventories[c] = v
			return v
		}
		calls++
		snap, err := s.p.Read(ctx, c, "topics", "")
		if err != nil {
			issue("TOPICS_UNAVAILABLE", "无法读取 Topic 清单，存在状态未知", c, "")
		} else {
			v.known = !snap.Stale && snap.State != "unavailable" && snap.State != "partial"
			v.at = snap.ObservedAt
			for _, t := range snap.Topics {
				if t.ErrorCode == "" {
					v.topics[t.Name] = true
				} else {
					v.known = false
				}
			}
			if !v.known {
				issue("TOPICS_PARTIAL", "Topic 清单不完整，缺失条目的存在状态未知", c, "")
			}
		}
		inventories[c] = v
		return v
	}
	reader, canRead := s.p.(EndpointReader)
	loadEndpoint := func(c string) inventory {
		if v, ok := inventories[c]; ok {
			return v
		}
		v := inventory{topics: map[string]bool{}}
		if canRead && len(inventories) < 16 && calls < 96 && ctx.Err() == nil {
			calls++
			if names, at, err := reader.EndpointTopics(ctx, c); err == nil {
				v.known, v.at = true, at
				for _, name := range names {
					v.topics[name] = true
				}
			}
		}
		inventories[c] = v
		return v
	}
	nodes := map[string]bool{}
	edges := map[string]bool{}
	addNode := func(n Node) bool {
		if nodes[n.ID] {
			return true
		}
		if len(g.Nodes) >= 200 {
			g.Truncated = true
			return false
		}
		nodes[n.ID] = true
		g.Nodes = append(g.Nodes, n)
		return true
	}
	addTopic := func(c, t string) bool {
		if _, ok := inventories[c]; !ok && len(inventories) >= 16 {
			g.Truncated = true
			issue("CLUSTERS_TRUNCATED", "最多读取 16 个集群，其他集群已截断", c, "")
			return false
		}
		if name, ok := endpointNames[c]; ok {
			n := Node{ID: topicID(c, t), Kind: "topic", Label: t, ClusterID: c, ClusterName: name, Topic: t, Evidence: "configured", ExistenceNote: "Topic 是否存在无法确认"}
			if v := loadEndpoint(c); v.known {
				b := v.topics[t]
				n.Exists, n.ExistenceNote, n.ObservedAt = &b, "", v.at
				if b {
					n.Evidence = "observed"
				}
			}
			return addNode(n)
		}
		v := load(c)
		n := Node{ID: topicID(c, t), Kind: "topic", Label: t, ClusterID: c, ClusterName: clusters[c], Topic: t, Evidence: "unknown", ObservedAt: v.at}
		if v.topics[t] {
			b := true
			n.Exists = &b
			n.Evidence = "observed"
		} else if v.known {
			b := false
			n.Exists = &b
		}
		return addNode(n)
	}
	addEdge := func(e Edge) {
		e.ID = id("edge", e.Source, e.Target, e.Kind, e.Evidence, e.LinkID, e.Account, e.ClientID)
		if edges[e.ID] {
			return
		}
		if len(g.Edges) >= 400 {
			g.Truncated = true
			return
		}
		if !nodes[e.Source] || !nodes[e.Target] {
			g.Truncated = true
			return
		}
		edges[e.ID] = true
		g.Edges = append(g.Edges, e)
	}
	if _, endpoint := endpointNames[q.ClusterID]; !endpoint {
		root := load(q.ClusterID)
		if root.known && !root.topics[q.Topic] {
			return g, ErrNotFound
		}
	}
	addTopic(q.ClusterID, q.Topic)
	if snapErr != nil {
		issue("DISCOVERY_UNAVAILABLE", "部分发现快照不可用", "", "")
	}
	links := []scopedLink{}
	linksLimited := false
collectLinks:
	for _, snap := range snapshots {
		for _, l := range snap.Links {
			if l.Removed {
				continue
			}
			if ctx.Err() != nil || len(links) >= 4096 {
				linksLimited = true
				break collectLinks
			}
			links = append(links, scopedLink{l, snap.ScopeID, snap.Stale})
		}
	}
	if linksLimited {
		g.Truncated = true
		issue("LINKS_TRUNCATED", "同步声明最多检查 4096 条，或请求已达到时限", "", "")
	}
	type step struct {
		c, t string
		d    int
	}
	queue := []step{{q.ClusterID, q.Topic, 0}}
	visited := map[string]bool{}
	identityAdj := map[string]map[string]bool{}
	linkVisits := 0
traverse:
	for len(queue) > 0 {
		if ctx.Err() != nil {
			g.Truncated = true
			issue("TRAVERSAL_TRUNCATED", "请求已达到时限，已停止展开同步链路", "", "")
			break
		}
		cur := queue[0]
		queue = queue[1:]
		key := topicID(cur.c, cur.t)
		if visited[key] {
			continue
		}
		visited[key] = true
		if cur.d >= q.Depth {
			continue
		}
		for _, sl := range links {
			if ctx.Err() != nil || linkVisits >= 16384 {
				g.Truncated = true
				issue("TRAVERSAL_TRUNCATED", "已达到同步链路检查预算（16384 次）或请求时限", "", "")
				break traverse
			}
			linkVisits++
			l := sl.link
			srcC, _, srcOK := endpointOf(l.Source, clusters)
			dstC, _, dstOK := endpointOf(l.Target, clusters)
			forward := srcOK && dstOK && srcC == cur.c && q.Direction != "upstream"
			backward := srcOK && dstOK && dstC == cur.c && q.Direction != "downstream"
			if !forward && !backward {
				// The far end has no address at all: the only place the chain stops, with the reason.
				if srcOK && srcC == cur.c && !dstOK && q.Direction != "upstream" && l.Enabled {
					issue("ENDPOINT_UNRESOLVABLE", "MM2 配置中 "+l.Target.Alias+" 的地址无法解析，链路在此中断", cur.c, l.ID)
				} else if dstOK && dstC == cur.c && !srcOK && q.Direction != "downstream" && l.Enabled {
					issue("ENDPOINT_UNRESOLVABLE", "MM2 配置中 "+l.Source.Alias+" 的地址无法解析，链路在此中断", cur.c, l.ID)
				}
				continue
			}
			if !l.Enabled {
				continue
			}
			if !l.SemanticsKnown || l.Stale || sl.stale {
				issue("SEMANTICS_UNKNOWN", "同步规则语义缺失或快照已过期，请刷新发现", cur.c, l.ID)
				continue
			}
			conflict := false
			for _, decl := range l.Declarations {
				if !decl.SemanticsKnown || decl.TopicsRaw != l.TopicsRaw || decl.TopicsExcludeRaw != l.TopicsExcludeRaw || decl.Policy != l.Policy || decl.Separator != l.Separator || decl.Enabled != l.Enabled || decl.InternalSeparatorEnabled != l.InternalSeparatorEnabled {
					conflict = true
				}
			}
			if conflict {
				issue("CONFLICTING_DECLARATIONS", "同步配置声明冲突，无法确定 Topic 映射", cur.c, l.ID)
				continue
			}
			sourceTopic := cur.t
			dest := dstC
			targetTopic := ""
			policy := l.Policy
			identity := policy == "org.apache.kafka.connect.mirror.IdentityReplicationPolicy"
			standard := policy == "org.apache.kafka.connect.mirror.DefaultReplicationPolicy"
			if identity && strings.HasSuffix(sourceTopic, "heartbeats") {
				issue("HEARTBEAT_POLICY_UNKNOWN", "Identity 策略的心跳 Topic 命名尚无法确认", cur.c, l.ID)
				continue
			}
			if !identity && !standard {
				issue("POLICY_UNKNOWN", "自定义同步策略的 Topic 命名未知", cur.c, l.ID)
				continue
			}
			if backward {
				dest = srcC
				if standard {
					prefix := l.SourceAlias + l.Separator
					if !strings.HasPrefix(cur.t, prefix) {
						continue
					}
					sourceTopic = strings.TrimPrefix(cur.t, prefix)
				}
				targetTopic = cur.t
			} else if identity {
				targetTopic = sourceTopic
			} else {
				targetTopic = l.SourceAlias + l.Separator + sourceTopic
			}
			if internalTopic(sourceTopic, l.Separator, l.InternalSeparatorEnabled) {
				continue
			}
			yes, known := matches(l.TopicsRaw, sourceTopic)
			exclude, knownExclude := false, true
			if l.TopicsExcludeRaw != "" {
				exclude, knownExclude = matches(l.TopicsExcludeRaw, sourceTopic)
			}
			// DefaultReplicationPolicy heartbeat topics bypass DefaultTopicFilter.
			if standard && (sourceTopic == "heartbeats" || strings.HasSuffix(sourceTopic, l.Separator+"heartbeats")) {
				yes, known, exclude, knownExclude = true, true, false, true
			}
			if !known || !knownExclude {
				issue("FILTER_UNKNOWN", "Topic 筛选规则包含暂不支持的正则语义", cur.c, l.ID)
				continue
			}
			if !yes || exclude {
				continue
			}
			nextTopic := targetTopic
			if standard {
				// MM2 refuses a remote topic whose alias ancestry already
				// contains the target cluster; graph visitation alone is insufficient.
				ancestry := sourceTopic
				cycle := false
				if l.Separator == "" {
					issue("SEMANTICS_UNKNOWN", "同步命名分隔符为空，无法确认映射", cur.c, l.ID)
					continue
				}
				for strings.Contains(ancestry, l.Separator) {
					alias, rest, _ := strings.Cut(ancestry, l.Separator)
					if alias == l.TargetAlias {
						cycle = true
						break
					}
					ancestry = rest
				}
				if cycle {
					issue("REPLICATION_CYCLE", "MM2 环路过滤规则排除了此路径", cur.c, l.ID)
					continue
				}
			}
			if !regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`).MatchString(targetTopic) {
				issue("TARGET_NAME_UNKNOWN", "同步命名结果不是有效的 Kafka Topic", cur.c, l.ID)
				continue
			}
			if backward {
				nextTopic = sourceTopic
			}
			if !addTopic(dest, nextTopic) {
				continue
			}
			task := id("mm2", sl.scope, l.ID, sourceTopic, targetTopic)
			if !addNode(Node{ID: task, Kind: "mm2", Label: l.WorkloadName, LinkID: l.ID, Evidence: "configured", ObservedAt: l.ObservedAt}) {
				continue
			}
			srcID, tgtID := topicID(srcC, sourceTopic), topicID(dstC, targetTopic)
			for _, pair := range [][2]string{{srcID, task}, {task, tgtID}} {
				addEdge(Edge{Source: pair[0], Target: pair[1], Kind: "replicates_to", Evidence: "configured", ObservedAt: l.ObservedAt, ScopeID: sl.scope, LinkID: l.ID, Policy: policy, Rule: l.TopicsRaw})
			}
			queue = append(queue, step{dest, nextTopic, cur.d + 1})
			if identity {
				if identityAdj[srcID] == nil {
					identityAdj[srcID] = map[string]bool{}
				}
				if !identityAdj[srcID][tgtID] {
					if directedReachable(identityAdj, tgtID, srcID) {
						issue("REPLICATION_CYCLE", "同名 Topic 的配置同步方向形成环路，请检查 MM2 配置", dstC, l.ID)
					}
					identityAdj[srcID][tgtID] = true
				}
			}
		}
	}
	apps, truncated, err := s.p.Applications(ctx)
	if err != nil {
		issue("APPLICATIONS_UNAVAILABLE", "人工登记的应用信息暂不可用", "", "")
	}
	if truncated {
		g.Truncated = true
		issue("APPLICATIONS_TRUNCATED", "人工应用仅扫描前 1000 条，其余记录已截断", "", "")
	}
	for _, app := range apps {
		for _, rel := range app.Relations {
			tid := topicID(rel.ClusterID, rel.Topic)
			if !nodes[tid] || (rel.Mode == "produce" && q.Direction == "downstream") || (rel.Mode == "consume" && q.Direction == "upstream") {
				continue
			}
			aid := id("application", app.ID)
			addNode(Node{ID: aid, Kind: "application", Label: app.Name, Location: app.Location, Owner: app.Owner, ApplicationID: app.ID, Evidence: "manual", ObservedAt: app.UpdatedAt})
			src, dst := aid, tid
			if rel.Mode == "consume" {
				src, dst = tid, aid
				if rel.ConsumerGroup != "" {
					gid := id("manual_group", rel.ClusterID, rel.ConsumerGroup)
					addNode(Node{ID: gid, Kind: "consumer_group", Label: rel.ConsumerGroup, ClusterID: rel.ClusterID, Group: rel.ConsumerGroup, Evidence: "manual", ObservedAt: app.UpdatedAt})
					addEdge(Edge{Source: tid, Target: gid, Kind: "consumes", Evidence: "manual", ObservedAt: app.UpdatedAt})
					src = gid
				}
			}
			addEdge(Edge{Source: src, Target: dst, Kind: rel.Mode, Account: rel.Account, ClientID: rel.ClientID, Evidence: "manual", ObservedAt: app.UpdatedAt})
		}
	}
	s.consumers(ctx, &g, clusters, inventories, q, addNode, addEdge, issue)
	if ctx.Err() != nil {
		g.Truncated = true
		issue("DEADLINE_REACHED", "请求已达到时限，已保留读取到的部分结果", "", "")
	}
	sort.Slice(g.Nodes, func(i, j int) bool { return g.Nodes[i].ID < g.Nodes[j].ID })
	sort.Slice(g.Edges, func(i, j int) bool { return g.Edges[i].ID < g.Edges[j].ID })
	if g.Truncated {
		issue("TRUNCATED", "已达到图节点、连线或读取上限，结果已截断", "", "")
	}
	return g, nil
}

// Graph nodes/edges are already bounded; re-visiting an undirected query path
// or a fan-in destination is not a directed replication cycle.
func directedReachable(adj map[string]map[string]bool, from, to string) bool {
	queue := []string{from}
	seen := map[string]bool{}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		if n == to {
			return true
		}
		if seen[n] {
			continue
		}
		seen[n] = true
		for next := range adj[n] {
			if !seen[next] {
				queue = append(queue, next)
			}
		}
	}
	return false
}

// EndpointID names an MM2 end that is not confirmed to a registration by its smallest
// bootstrap address, so every MM2 that points at the same cluster yields the same id.
func EndpointID(addresses []string) string {
	best := ""
	for _, a := range addresses {
		a = strings.ToLower(strings.TrimSpace(a))
		if a != "" && (best == "" || a < best) {
			best = a
		}
	}
	if best == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(best))
	return "endpoint-" + hex.EncodeToString(sum[:8])
}

// endpointOf returns the traversal node for one MM2 end: the registration when confirmed,
// otherwise an endpoint pseudo-cluster named "alias · first-address" (ok=false only when
// the end has no address at all).
func endpointOf(m discovery.Mapping, clusters map[string]string) (string, string, bool) {
	if m.Status == "confirmed" && clusters[m.RegistrationID] != "" {
		return m.RegistrationID, clusters[m.RegistrationID], true
	}
	id := EndpointID(m.BootstrapAddresses)
	if id == "" {
		return "", "", false
	}
	sorted := append([]string{}, m.BootstrapAddresses...)
	sort.Strings(sorted)
	name := m.Alias + " · " + sorted[0]
	if len(sorted) > 2 {
		name += " 等 " + strconv.Itoa(len(sorted)) + " 个"
	} else if len(sorted) == 2 {
		name += ", " + sorted[1]
	}
	return id, name, true
}
