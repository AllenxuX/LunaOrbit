// UX-v2 inspection summary (contract docs/UX_V2_API_CONTRACT.md §2).
// Every count states its unit and dedupe key; unknown stays null, never 0.
import type { ScopeEntry, ScopeInfo } from '../api/topology'

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
  const read = live.filter(e => e.snapshot)
  const links = new Map<string, { scope: string; ns: string; uid: string; verified: boolean; pair: string }>()
  const failed = new Map<string, FailedScope>(), uncollected: string[] = []
  for (const e of live) {
    const s = e.snapshot
    if (!s) { if (e.error === 'NO_SNAPSHOT') uncollected.push(e.scope.id); else failed.set(`${e.scope.id}|*`, { scope: e.scope.id, reason: '快照读取失败，无可用数据', hasHistory: false }); continue }
    const ends = new Map<string, { from?: string; to?: string }>()
    for (const g of s.edges ?? []) { const x = ends.get(g.link_id) ?? {}; if (g.relation === 'declared_input') x.from = g.source; if (g.relation === 'declared_output') x.to = g.target; ends.set(g.link_id, x) }
    for (const l of s.links) if (!l.removed) { const x = ends.get(l.id); links.set(`${e.scope.id}|${l.id}`, { scope: e.scope.id, ns: l.namespace, uid: l.workload_uid, verified: l.verified, pair: x?.from && x.to ? `${x.from}→${x.to}` : `${clusterKey(l.source)}→${clusterKey(l.target)}` }) }
    for (const i of s.issues ?? []) { const k = `${e.scope.id}|${i.namespace ?? '*'}`; if (!failed.has(k)) failed.set(k, { scope: e.scope.id, namespace: i.namespace, reason: i.code, observedAt: s.observed_at, attemptedAt: s.attempted_at, hasHistory: !!s.observed_at && !s.observed_at.startsWith('0001') }) }
  }
  const complete = read.length + uncollected.length === live.length && read.length > 0 || read.length === live.length && live.length > 0, any = read.length > 0
  const instances = new Map<string, boolean>()
  for (const l of links.values()) { const k = `${l.scope}|${l.ns}|${l.uid}`; instances.set(k, (instances.get(k) ?? false) || !l.verified) }
  const times = read.map(e => e.snapshot!.observed_at).filter(t => t && !t.startsWith('0001')).sort()
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
