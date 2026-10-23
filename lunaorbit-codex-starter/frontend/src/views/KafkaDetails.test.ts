import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import KafkaDetails from './KafkaDetails.vue'
afterEach(() => vi.unstubAllGlobals())
const markers = { synthetic: true, observed_at: '2026-10-19T00:00:00Z', stale: true, state: 'partial', capabilities: [] }
function setup() {
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    const path = String(input).split('?')[0]!
    let result: unknown = { display_name: 'Synthetic cluster', security_protocol: 'SASL_PLAINTEXT' }
    if (path.endsWith('/session')) result = { mode: 'mock', persistent: false }
    else if (path.endsWith('/topics/demo')) result = { item: { name: 'demo', partitions: [{ partition: 0, leader: 1, replicas: [1, 2], isr: [1] }], rf_min: 2, rf_max: 2 }, ...markers }
    else if (path.endsWith('/offsets')) result = { items: [{ group: 'consumer', topic: 'demo', partition: 0, committed: null, log_end: 9, lag: null }], total: 1, ...markers }
    else if (path.endsWith('/topics')) result = { items: [{ name: 'demo', partitions: [], rf_min: null, rf_max: null }], total: 1, ...markers }
    else if (path.endsWith('/groups')) result = { items: [{ id: 'consumer', state: 'Stable', protocol_type: 'consumer' }], total: 1, ...markers }
    else if (path.endsWith('/brokers')) result = { items: [{ id: 1, host: 'broker.example.test', port: 9093 }], total: 1, ...markers }
    return new Response(JSON.stringify(result))
  }))
  return mount(KafkaDetails, { props: { id: 'synthetic' } })
}
it('shows synthetic provenance, RF/ISR and unknown lag only after group selection', async () => {
  const wrapper = setup(); await flushPromises()
  expect(wrapper.text()).toContain('未验证真实 Kafka')
  expect(wrapper.text()).toContain('已过期')
  await wrapper.get('[data-topic="demo"]').trigger('click'); await flushPromises()
  expect(wrapper.text()).toContain('1, 2')
  await wrapper.get('[data-group="consumer"]').trigger('click'); await flushPromises()
  expect(wrapper.get('[data-offsets]').text()).toContain('未知')
  wrapper.unmount()
})
it('rejects stale list responses after switching registration IDs', async () => {
  let resolveOld!: (response: Response) => void
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    const url = String(input)
    if (url.endsWith('/session')) return new Response(JSON.stringify({ mode: 'mock' }))
    if (url.includes('/old/topics')) return new Promise<Response>(resolve => { resolveOld = resolve })
    if (!url.includes('?')) return new Response(JSON.stringify({ display_name: url.includes('/new') ? 'New registration' : 'Old registration' }))
    return new Response(JSON.stringify({ items: [], total: 0, ...markers }))
  }))
  const wrapper = mount(KafkaDetails, { props: { id: 'old' } }); await flushPromises()
  await wrapper.setProps({ id: 'new' }); await flushPromises()
  resolveOld(new Response(JSON.stringify({ items: [{ name: 'stale-secret-topic' }], total: 1, ...markers })))
  await flushPromises()
  expect(wrapper.text()).toContain('New registration')
  expect(wrapper.text()).not.toContain('stale-secret-topic')
  wrapper.unmount()
})
it('maps API failures safely and permits refresh', async () => {
  const wrapper = setup(); await flushPromises()
  vi.mocked(fetch).mockImplementation(async (input) => {
    const url = String(input)
    if (url.endsWith('/session')) return new Response(JSON.stringify({ mode: 'mock' }))
    if (!url.includes('?')) return new Response(JSON.stringify({ display_name: 'Synthetic cluster' }))
    return new Response('sensitive upstream details', { status: 501 })
  })
  await wrapper.findAll('button').find(b => b.text() === '刷新')!.trigger('click'); await flushPromises()
  expect(wrapper.get('[role="alert"]').text()).toContain('不支持')
  expect(wrapper.text()).not.toContain('sensitive')
  wrapper.unmount()
})
