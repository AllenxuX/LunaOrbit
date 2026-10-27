// UX-v2 "本次查询摘要": describes only what this lineage response returned.
import type { Direction, Lineage } from '../api/topic-lineage'

export interface QuerySummary { direction: string; outside: string; depth: number; relations: number; clusters: number; boundary: string[]; truncated: boolean; issues: number; confirmed: string; pending: string }
const dirText: Record<Direction, string> = { upstream: '向上游追踪', downstream: '向下游追踪', both: '上下游追踪' }

export function querySummary(graph: Lineage, direction: Direction, depth: number): QuerySummary {
  const root = graph.nodes.find(n => n.id === graph.root_id)
  const topics = graph.nodes.filter(n => n.kind === 'topic' && n.id !== graph.root_id)
  const links = new Set(graph.edges.filter(e => e.link_id || graph.nodes.find(n => n.id === e.source || n.id === e.target)?.kind === 'mm2').map(e => e.link_id ?? e.id))
  const relations = graph.nodes.some(n => n.kind === 'mm2') ? graph.nodes.filter(n => n.kind === 'mm2').length : links.size
  const clusters = new Set(topics.map(n => n.cluster_id ?? n.cluster_name).filter(c => c && c !== root?.cluster_id)).size
  const hasIn = new Set(graph.edges.map(e => e.target)), hasOut = new Set(graph.edges.map(e => e.source))
  const boundary = topics.filter(n => direction === 'upstream' ? !hasIn.has(n.id) : direction === 'downstream' ? !hasOut.has(n.id) : !hasIn.has(n.id) || !hasOut.has(n.id)).map(n => n.cluster_name ?? n.cluster_id ?? n.label)
  const word = direction === 'upstream' ? '上游' : direction === 'downstream' ? '下游' : '相关'
  return {
    direction: dirText[direction], depth, relations, clusters, boundary: [...new Set(boundary)], truncated: graph.truncated, issues: graph.issues.length,
    outside: direction === 'upstream' ? '下游不在本次查询方向内' : direction === 'downstream' ? '上游不在本次查询方向内' : '超出最大深度的路径不在本次范围内',
    confirmed: relations ? `返回 ${relations} 条配置关系、${clusters} 个${word}集群` : `未返回可确认的${word}配置关系（0 条）`,
    pending: relations ? `${[...new Set(boundary)].join(' / ') || '无'} 是本次查询的边界节点，不等于真实源头或终点` : `不能据此认定没有${word}`,
  }
}
