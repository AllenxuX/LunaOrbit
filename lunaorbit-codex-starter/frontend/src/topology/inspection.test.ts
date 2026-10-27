import { describe, expect, it } from 'vitest'
import { collectMessage, inspect, scopeLabel } from './inspection'
import type { Link, ScopeEntry } from '../api/topology'

const m = (alias: string) => ({ alias, status: 'mapped', method: '', confidence: '', candidate_ids: null, cluster_id: alias })
function link(i: number, over: Partial<Link> = {}): Link {
  return { id: `l${i}`, namespace: 'messaging', workload_uid: `uid-${i}`, workload_name: `mm2-commerce-order-replication-apac-production-${i}`, source_alias: 'mars', target_alias: 'moon', topics_raw: 'a.*', policy: '', enabled: true, confidence: '', declared: true, observed: true, verified: false, stale: false, state: '', observed_at: '2026-10-27T02:20:00Z', source: m(i < 12 ? 'mars' : 'charon'), target: m('moon'), evidence: [], notes: [], runtime: { generation: 1, observed_generation: 1, replicas: 1, ready_replicas: 1, pod_candidates: 1, pod_association: '' }, ...over }
}
const snap = (links: Link[], extra = {}) => ({ scope_id: 's', synthetic: false, observed_at: '2026-10-27T02:20:00Z', attempted_at: '2026-10-27T02:25:00Z', stale: false, nodes: [], edges: [], links, issues: [], ...extra })

describe('inspection summary', () => {
  it('dedupes 24 MM2 converging from two sources', () => {
    const links = Array.from({ length: 24 }, (_, i) => link(i))
    links.push(link(0, { id: 'l0b', verified: true })) // same instance, second rule
    const r = inspect([{ scope: { id: 's', synthetic: false }, snapshot: snap(links) }])
    expect(r).toMatchObject({ relations: 25, mm2Instances: 24, unverifiedMm2: 24, directedPairs: 2, complete: true, failedScopes: [] })
  })
  it('keeps unknown as null and dedupes failures by scope+namespace', () => {
    const entries: ScopeEntry[] = [{ scope: { id: 'a', synthetic: false }, error: 'READ_FAILED' }]
    expect(inspect(entries).relations).toBeNull()
    const r = inspect([...entries, { scope: { id: 'b', synthetic: false }, snapshot: snap([link(1)], { stale: true, issues: [{ code: 'TIMEOUT', namespace: 'x' }, { code: 'PARSE', namespace: 'x' }] }) }])
    expect(r.complete).toBe(false); expect(r.relations).toBe(1)
    expect(r.failedScopes.map(f => f.scope + '/' + (f.namespace ?? '*'))).toEqual(['a/*', 'b/x'])
    expect(r.staleScopes).toEqual(['b'])
  })
  it('labels scope namespaces without guessing', () => {
    expect(scopeLabel({ id: 's', synthetic: false })).toBe('当前发现范围 / Namespace 未读取')
    expect(scopeLabel({ id: 's', synthetic: false, k8s_cluster: 'k', namespace_mode: 'all_accessible' })).toBe('k / 全部可访问 Namespace')
  })
})

describe('never-collected scopes', () => {
  it('NO_SNAPSHOT is "not yet collected", not a failure', () => {
    const r = inspect([{ scope: { id: 'new', synthetic: false }, error: 'NO_SNAPSHOT' }, { scope: { id: 'b', synthetic: false }, snapshot: snap([link(1)]) }])
    expect(r.failedScopes).toEqual([]); expect(r.uncollectedScopes).toEqual(['new'])
  })
  it('real failure with history keeps last success, without history says so', () => {
    const r = inspect([{ scope: { id: 'a', synthetic: false }, error: 'READ_FAILED' }, { scope: { id: 'b', synthetic: false }, snapshot: snap([link(1)], { issues: [{ code: 'TIMEOUT', namespace: 'x' }] }) }])
    expect(r.failedScopes.map(f => [f.scope, f.hasHistory])).toEqual([['a', false], ['b', true]])
  })
})

describe('LUNA-UX-12 success history', () => {
  const zero = '0001-01-01T00:00:00Z'
  it('first failed collection: counts unknown, no success time', () => {
    const r = inspect([{ scope: { id: 'a', synthetic: false }, snapshot: snap([], { observed_at: zero, stale: true, issues: [{ code: 'TIMEOUT', namespace: 'ns' }] }) }])
    expect([r.relations, r.mm2Instances, r.unverifiedMm2, r.directedPairs]).toEqual([null, null, null, null])
    expect(r.failedScopes).toEqual([{ scope: 'a', namespace: 'ns', reason: 'TIMEOUT', attemptedAt: '2026-10-27T02:25:00Z', hasHistory: false }])
    expect(r.oldestObservedAt).toBe('')
  })
  it('first failure without issues still counts as a failed scope', () => {
    const r = inspect([{ scope: { id: 'a', synthetic: false }, snapshot: snap([], { observed_at: '', attempted_at: zero }) }])
    expect(r.failedScopes).toEqual([{ scope: 'a', reason: '未见成功采集', attemptedAt: undefined, hasHistory: false }]); expect(r.relations).toBeNull()
  })
  it('successful empty snapshot keeps a real 0', () => {
    const r = inspect([{ scope: { id: 'a', synthetic: false }, snapshot: snap([]) }])
    expect([r.relations, r.mm2Instances, r.unverifiedMm2, r.directedPairs]).toEqual([0, 0, 0, 0]); expect(r.failedScopes).toEqual([])
  })
  it('failure with history keeps old counts and the real success time', () => {
    const r = inspect([{ scope: { id: 'a', synthetic: false }, snapshot: snap([link(1)], { stale: true, issues: [{ code: 'TIMEOUT' }] }) }])
    expect(r.relations).toBe(1); expect(r.failedScopes[0]).toMatchObject({ hasHistory: true, observedAt: '2026-10-27T02:20:00Z' }); expect(r.staleScopes).toEqual(['a'])
  })
})

describe('collect message', () => {
  const zero = '0001-01-01T00:00:00Z'
  it('branches on success history', () => {
    const none = inspect([{ scope: { id: 'a', synthetic: false }, snapshot: snap([], { observed_at: zero, issues: [{ code: 'X' }] }) }])
    expect(collectMessage(['a：未完整成功'], none)).toContain('这些范围未见成功采集，数量未知'); expect(collectMessage(['a'], none)).not.toContain('保留')
    const hist = inspect([{ scope: { id: 'a', synthetic: false }, snapshot: snap([link(1)], { issues: [{ code: 'X' }] }) }])
    expect(collectMessage(['a'], hist)).toContain('已保留上次成功数据')
    const mixed = inspect([{ scope: { id: 'a', synthetic: false }, snapshot: snap([link(1)], { issues: [{ code: 'X' }] }) }, { scope: { id: 'b', synthetic: false }, snapshot: snap([], { observed_at: zero }) }])
    expect(collectMessage(['a', 'b'], mixed)).toContain('其余范围未见成功采集')
    expect(collectMessage([], hist)).toBe('重新采集完成；同步健康仍未验证。')
  })
})
