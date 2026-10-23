import { flushPromises, mount } from '@vue/test-utils'
import { nextTick } from 'vue'
import { afterEach, expect, it, vi } from 'vitest'
import ObservabilityPanel from './ObservabilityPanel.vue'
import type { Snapshot } from '../api/topology'
afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers() })
const snapshot: Snapshot = { scope_id: 'scope', synthetic: true, stale: false, observed_at: '', attempted_at: '', nodes: [{ id: 'source', kind: 'kafka', label: 'Source', replication_factor: null, rf_status: 'unknown' }, { id: 'downstream', kind: 'kafka', label: 'Downstream', replication_factor: null, rf_status: 'unknown' }], edges: [], links: [], issues: [] }
function setup(graph = snapshot) {
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    const url = String(input)
    if (url.includes('/impact')) return new Response(JSON.stringify({ node_id: 'source', items: [{ node_id: 'downstream', link_id: 'link', state: 'candidate' }], synthetic: true, state: 'candidate' }))
    if (url.includes('/lineage-confirmations')) return new Response(JSON.stringify({ items: [] }))
    return new Response(JSON.stringify({ items: [{ link_id: 'link', metric: 'mm2_lag_seconds', value: null, unit: 'seconds', source: 'synthetic-provider', observed_at: '2026-10-19T00:00:00Z', sampled_at: '2026-10-19T00:00:00Z', stale: true, synthetic: true, state: 'unknown' }], synthetic: true, stale: true, state: 'unknown' }))
  }))
  return mount(ObservabilityPanel, { props: { scopeId: 'scope', snapshot: graph } })
}
it('shows stale null metrics as unknown with source and sampling time', async () => {
  const w = setup(); await flushPromises()
  expect(w.text()).toContain('未知')
  expect(w.text()).toContain('已过期')
  expect(w.text()).toContain('synthetic-provider')
  expect(w.text()).toContain('seconds')
  expect(w.text()).toContain('未验证真实指标')
  w.unmount()
})
it('expires displayed samples as time passes and cleans its timer on unmount', async () => {
  vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
  vi.setSystemTime(new Date('2026-10-19T00:00:00Z'))
  vi.stubGlobal('fetch', vi.fn(async (input: string) => new Response(JSON.stringify(String(input).includes('/lineage-confirmations') ? { items: [] } : { items: [{ link_id: 'link', metric: 'mm2_lag_seconds', value: 1, unit: 'seconds', source: 'synthetic', sampled_at: '2026-10-19T00:00:00Z', observed_at: '2026-10-19T00:00:00Z', expires_at: '2026-10-19T00:00:02Z', stale: false, synthetic: true, state: 'observed' }], stale: false, synthetic: true, state: 'observed' }))))
  const w = mount(ObservabilityPanel, { props: { scopeId: 'scope', snapshot } }); await flushPromises()
  expect(w.find('tbody').text()).not.toContain('已过期')
  await vi.advanceTimersByTimeAsync(2000); await nextTick()
  expect(w.find('tbody').text()).toContain('已过期 · 状态未知')
  w.unmount()
  expect(vi.getTimerCount()).toBe(0)
})
it.each([undefined, '0001-01-01T00:00:00Z', 'not-a-date'])('keeps server freshness when expiry is absent or invalid (%s)', async (expires_at) => {
  vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
  vi.stubGlobal('fetch', vi.fn(async (input: string) => new Response(JSON.stringify(String(input).includes('/lineage-confirmations') ? { items: [] } : { items: [{ link_id: 'link', metric: 'mm2_lag_seconds', value: 1, unit: 'seconds', stale: false, state: 'observed', expires_at }], stale: false, state: 'observed' }))))
  const w = mount(ObservabilityPanel, { props: { scopeId: 'scope', snapshot } }); await flushPromises()
  await vi.advanceTimersByTimeAsync(120000); await nextTick()
  expect(w.find('tbody').text()).not.toContain('已过期')
  w.unmount()
})
it('uses server impact as candidates and preserves the input snapshot', async () => {
  const before = JSON.stringify(snapshot), w = setup(); await flushPromises()
  await w.get('[data-impact-node]').setValue('source'); await flushPromises()
  expect(w.emitted('affected')?.at(-1)?.[0]).toEqual(['downstream'])
  expect(w.text()).toContain('可能受影响的资源')
  expect(w.text()).toContain('不代表确定故障影响')
  expect(JSON.stringify(snapshot)).toBe(before)
  w.unmount()
})
it('shows safe authorization errors and clears highlights', async () => {
  vi.stubGlobal('fetch', vi.fn(async () => new Response('sensitive upstream', { status: 403 })))
  const w = mount(ObservabilityPanel, { props: { scopeId: 'scope', snapshot } }); await flushPromises()
  expect(w.get('[role="alert"]').text()).toContain('授权')
  expect(w.text()).not.toContain('sensitive upstream')
  expect(w.emitted('affected')?.at(-1)?.[0]).toEqual([])
  w.unmount()
})
it('drops impact responses for an earlier selected node', async () => {
  const w = setup(); await flushPromises()
  const fetcher = vi.mocked(fetch), original = fetcher.getMockImplementation()!
  let completeOld!: (response: Response) => void
  fetcher.mockImplementation(async (input, init) => {
    const url = String(input)
    if (url.includes('node_id=source')) return new Promise<Response>(resolve => { completeOld = resolve })
    if (url.includes('/impact')) return new Response(JSON.stringify({ items: [], state: 'candidate', synthetic: true }))
    return original(input, init)
  })
  await w.get('[data-impact-node]').setValue('source'); await flushPromises()
  await w.get('[data-impact-node]').setValue('downstream'); await flushPromises()
  completeOld(new Response(JSON.stringify({ items: [{ node_id: 'downstream', state: 'candidate' }], state: 'candidate' }))); await flushPromises()
  expect(w.emitted('affected')?.at(-1)?.[0]).toEqual([])
  w.unmount()
})
it('records explicit business confirmation without changing discovery verified evidence', async () => {
  const mapping = { alias: 'a', status: 'unknown', method: '', confidence: '', candidate_ids: [] }
  const graph: Snapshot = { ...snapshot, links: [{ id: 'link', namespace: 'synthetic', workload_uid: 'uid', workload_name: 'mm2', source_alias: 'a', target_alias: 'b', topics_raw: 'orders', policy: '', enabled: true, confidence: '', declared: true, observed: true, verified: false, stale: false, state: 'observed', observed_at: '', source: mapping, target: mapping, evidence: [], notes: [], runtime: { generation: 1, observed_generation: 1, replicas: 1, ready_replicas: 1, pod_candidates: 0, pod_association: '' } }] }
  const before = JSON.stringify(graph), w = setup(graph); await flushPromises()
  const fetcher = vi.mocked(fetch), original = fetcher.getMockImplementation()!
  fetcher.mockImplementation(async (input, init) => {
    if (init?.method === 'POST') {
      expect(JSON.parse(String(init.body))).toEqual({ link_id: 'link', topic: 'orders', description: 'business owner reviewed' })
      return new Response(JSON.stringify({ id: 'confirmation', scope_id: 'scope', link_id: 'link', topic: 'orders', confirmed_by: 'admin', confirmed_at: 'now', kind: 'manual_business_confirmation', active: true }))
    }
    return original(input, init)
  })
  await w.get('input[name="business_topic"]').setValue('orders')
  await w.get('input[name="business_description"]').setValue('business owner reviewed')
  await w.get('[data-explicit-confirm]').setValue(true)
  await w.get('[data-lineage-form]').trigger('submit'); await flushPromises()
  expect(w.emitted('confirmed')?.at(-1)?.[0]).toEqual(['link'])
  expect(w.text()).toContain('未升级链路 verified')
  expect(JSON.stringify(graph)).toBe(before)
  w.unmount()
})
