import { describe, expect, it } from 'vitest'
import { querySummary } from './query-summary'
import type { Lineage } from '../api/topic-lineage'

const t = (id: string, c: string) => ({ id, kind: 'topic' as const, label: id, cluster_id: c, cluster_name: c, topic: 'commerce_order_payment_settlement_events_apac_production_v2026' })
const g: Lineage = { root_id: 'r', observed_at: '', truncated: false, depth: 1, issues: [],
  nodes: [t('r', 'moon'), t('a', 'mars'), t('b', 'charon'), { id: 'm1', kind: 'mm2', label: 'mm1' }, { id: 'm2', kind: 'mm2', label: 'mm2' }],
  edges: [{ id: '1', source: 'a', target: 'm1', kind: 'mm2', evidence: 'configured' }, { id: '2', source: 'm1', target: 'r', kind: 'mm2', evidence: 'configured' }, { id: '3', source: 'b', target: 'm2', kind: 'mm2', evidence: 'configured' }, { id: '4', source: 'm2', target: 'r', kind: 'mm2', evidence: 'configured' }] as Lineage['edges'] }

describe('query summary', () => {
  it('describes merged upstream branches as boundary, not true source', () => {
    const s = querySummary(g, 'upstream', 1)
    expect(s.confirmed).toBe('返回 2 条配置关系、2 个上游集群')
    expect(s.boundary).toEqual(['mars', 'charon'])
    expect(s.pending).toContain('不等于真实源头')
    expect(s.outside).toBe('下游不在本次查询方向内')
  })
  it('empty downstream is not "no downstream"', () => {
    const s = querySummary({ ...g, nodes: [g.nodes[0]!], edges: [] }, 'downstream', 2)
    expect(s.confirmed).toContain('0 条'); expect(s.pending).toBe('不能据此认定没有下游')
  })
})
