// UX-v2 inspection summary (contract docs/UX_V2_API_CONTRACT.md §2).
// Every count states its unit and dedupe key; unknown stays null, never 0.
import type { ScopeEntry, ScopeInfo } from '../api/topology'
import { validTime } from './time'

export interface FailedScope { scope: string; namespace?: string; reason: string; observedAt?: string; attemptedAt?: string; hasHistory: boolean }
export interface Inspection {
  readScopes: number; unreadScopes: number; complete: boolean  // counts cover read scopes only when !complete
  relations: number | null          // MM2 links, deduped by (scope_id, link.id)
  mm2Instances: number | null       // deduped by (scope_id, namespace, workload_uid)
  unverifiedMm2: number | null      // instances with >=1 non-removed !verified link
  directedPairs: number | null      // source→target pairs by mapped cluster key
  failedScopes: FailedScope[]       // deduped by (scope_id, namespace); never-collected scopes excluded
  uncollectedScopes: string[]       // NO_SNAPSHOT: not yet collected, not a failure
  staleScopes: string[]
  oldestObservedAt: string
  synthetic: boolean
}

function clusterKey(m: { registration_id?: string; cluster_id?: string; alias: string }) { return m.cluster_id || m.registration_id || `alias:${m.alias}` }

export function scopeLabel(s: ScopeInfo) {
  const ns = s.namespace_mode === 'all_accessible' ? '全部可访问 Namespace' : s.namespaces?.length ? s.namespaces.join(', ') : 'Namespace 未读取'
  return `${s.k8s_cluster ? `${s.k8s_cluster} · ` : ''}${s.id} / ${ns}`
}

export function inspect(entries: ScopeEntry[]): Inspection {
  const live = entries.filter(e => !e.scope.removed)
  // A snapshot only counts as data when it carries a real success time; a first
  // failed collection returns a snapshot with a zero observed_at and no history.
  const read = live.filter(e => e.snapshot && validTime(e.snapshot.observed_at))
  const links = new Map<string, { scope: string; ns: string; uid: string; verified: boolean; pair: string }>()
  const failed = new Map<string, FailedScope>(), uncollected: string[] = []
  for (const e of live) {
    const s = e.snapshot
    if (!s) { if (e.error === 'NO_SNAPSHOT') uncollected.push(e.scope.id); else failed.set(`${e.scope.id}|*`, { scope: e.scope.id, reason: '快照读取失败，无可用数据', hasHistory: false }); continue }
    const history = validTime(s.observed_at)
    if (!history) {
      const issues = s.issues ?? []
      if (!issues.length) failed.set(`${e.scope.id}|*`, { scope: e.scope.id, reason: '未见成功采集', attemptedAt: validTime(s.attempted_at) ? s.attempted_at : undefined, hasHistory: false })
      for (const i of issues) { const k = `${e.scope.id}|${i.namespace ?? '*'}`; if (!failed.has(k)) failed.set(k, { scope: e.scope.id, namespace: i.namespace, reason: i.code, attemptedAt: validTime(s.attempted_at) ? s.attempted_at : undefined, hasHistory: false }) }
      continue
    }
    const ends = new Map<string, { from?: string; to?: string }>()
    for (const g of s.edges ?? []) { const x = ends.get(g.link_id) ?? {}; if (g.relation === 'declared_input') x.from = g.source; if (g.relation === 'declared_output') x.to = g.target; ends.set(g.link_id, x) }
    for (const l of s.links) if (!l.removed) { const x = ends.get(l.id); links.set(`${e.scope.id}|${l.id}`, { scope: e.scope.id, ns: l.namespace, uid: l.workload_uid, verified: l.verified, pair: x?.from && x.to ? `${x.from}→${x.to}` : `${clusterKey(l.source)}→${clusterKey(l.target)}` }) }
    for (const i of s.issues ?? []) { const k = `${e.scope.id}|${i.namespace ?? '*'}`; if (!failed.has(k)) failed.set(k, { scope: e.scope.id, namespace: i.namespace, reason: i.code, observedAt: s.observed_at, attemptedAt: validTime(s.attempted_at) ? s.attempted_at : undefined, hasHistory: true }) }
  }
  const complete = read.length === live.length && live.length > 0, any = read.length > 0
  const instances = new Map<string, boolean>()
  for (const l of links.values()) { const k = `${l.scope}|${l.ns}|${l.uid}`; instances.set(k, (instances.get(k) ?? false) || !l.verified) }
  const times = read.map(e => e.snapshot!.observed_at).sort()
  return {
    readScopes: read.length, unreadScopes: live.length - read.length, complete,
    relations: any ? links.size : null,
    mm2Instances: any ? instances.size : null,
    unverifiedMm2: any ? [...instances.values()].filter(Boolean).length : null,
    directedPairs: any ? new Set([...links.values()].map(l => l.pair)).size : null,
    failedScopes: [...failed.values()], uncollectedScopes: uncollected,
    staleScopes: read.filter(e => e.snapshot!.stale).map(e => e.scope.id),
    oldestObservedAt: times[0] ?? '',
    synthetic: read.length > 0 && read.every(e => e.snapshot!.synthetic),
  }
}

/** Overview re-collect result text; only claims retained data when a success history exists. */
export function collectMessage(failures: string[], s: Inspection | null): string {
  if (!failures.length) return '重新采集完成；同步健康仍未验证。'
  const kept = !!s && s.readScopes > 0, allKept = !!s && s.unreadScopes === 0 && s.failedScopes.every(f => f.hasHistory)
  return `重新采集结束，部分范围未完成：${failures.join('；')}。${allKept ? '已保留上次成功数据，数据可能过期。' : kept ? '有成功历史的范围保留旧数据；其余范围未见成功采集，数量未知。' : '这些范围未见成功采集，数量未知。'}`
}
