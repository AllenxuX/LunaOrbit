import { describe, expect, it } from 'vitest'
import type { Snapshot } from '../api/topology'
import type { CatalogEndpoint } from '../api/catalog'
import { mergeByCatalog } from './catalog-merge'

const m = (alias: string, addr: string) => ({ alias, status: 'unknown', method: 'unmatched', confidence: 'low', candidate_ids: [], bootstrap_addresses: [addr] })
const link = (id: string, s: string, t: string) => ({ id, source: m(s, `${s}.lab:9092`), target: m(t, `${t}.lab:9092`) })
const edge = (id: string, source: string, target: string, link_id: string, relation: string) => ({ id, source, target, link_id, relation, stale: false, evidence: [] })
const snap = {
  scope_id: 's', nodes: [
    { id: 'alias:a1', kind: 'kafka', label: 'k1' }, { id: 'alias:b1', kind: 'kafka', label: 'k8' }, { id: 'workload:w1', kind: 'workload', label: 'mm2-1' },
    { id: 'alias:b2', kind: 'kafka', label: 'k8' }, { id: 'alias:c2', kind: 'kafka', label: 'k9' }, { id: 'workload:w2', kind: 'workload', label: 'mm2-2' },
  ],
  edges: [edge('e1', 'alias:a1', 'workload:w1', 'L1', 'declared_input'), edge('e2', 'workload:w1', 'alias:b1', 'L1', 'declared_output'), edge('e3', 'alias:b2', 'workload:w2', 'L2', 'declared_input'), edge('e4', 'workload:w2', 'alias:c2', 'L2', 'declared_output')],
  links: [link('L1', 'k1', 'k8'), link('L2', 'k8', 'k9')],
} as unknown as Snapshot
const row = (id: string, alias: string): CatalogEndpoint => ({ id, source: 'auto', aliases: [alias], members: [], addresses: [`${alias}.lab:9092`], resolved: [], access: 'credentials_unavailable', read_via: 'auto', lineage_id: '' })

describe('mergeByCatalog', () => {
  it('draws one node per logical cluster even when several MM2 point at it', () => {
    const out = mergeByCatalog(snap, [row('auto:1', 'k1'), row('auto:8', 'k8'), row('auto:9', 'k9')])
    const kafka = out.nodes.filter(n => n.kind === 'kafka')
    expect(kafka.map(n => n.id).sort()).toEqual(['catalog:auto:1', 'catalog:auto:8', 'catalog:auto:9'])
    expect(out.edges.find(e => e.id === 'e2')!.target).toBe('catalog:auto:8')
    expect(out.edges.find(e => e.id === 'e3')!.source).toBe('catalog:auto:8')
  })
  it('leaves the snapshot untouched without catalog rows', () => {
    expect(mergeByCatalog(snap, [])).toBe(snap)
  })
})
