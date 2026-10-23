import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import Overview from './Overview.vue'

afterEach(() => vi.unstubAllGlobals())
function setup(options: { failHealth?: boolean; missingSnapshot?: boolean; total?: number; failInventory?: boolean } = {}) {
  let healthCalls = 0
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    const url = String(input)
    if (url === '/healthz') {
      if (options.failHealth && healthCalls++ === 0) throw new Error('private-password')
      return new Response(JSON.stringify({ status: 'ok', mode: 'foundation', external_connections: 'disabled' }))
    }
    if (options.failInventory && url.includes('/kafka/clusters?')) return new Response('{}', { status: 503 })
    const data = url.endsWith('/session') ? { user_id: 'operator', mode: 'mock' }
      : url.includes('/kafka/clusters?') ? { items: options.total === 0 ? [] : [{ id: 'cluster-1', display_name: 'Lab Kafka', environment: 'lab', room: 'test-room', enabled: true, status: 'unverified' }], total: options.total ?? 1 }
      : url.endsWith('/scopes') ? { items: [{ id: 'scope-1', synthetic: true }] }
      : { total: 3, snapshot: { observed_at: '2026-10-20T00:00:00Z', synthetic: true, stale: false } }
    if (options.missingSnapshot && url.includes('/snapshot?')) return new Response('{}', { status: 404 })
    return new Response(JSON.stringify(data))
  }))
  return mount(Overview, { global: { stubs: { RouterLink: { template: '<a><slot /></a>' } } } })
}

it('shows measured inventory and keeps process health separate from external health', async () => {
  const wrapper = setup(); await flushPromises()
  expect(wrapper.get('[data-registered-total]').text()).toBe('1')
  expect(wrapper.get('[data-enabled-total]').text()).toBe('1')
  expect(wrapper.get('[data-link-total]').text()).toBe('3')
  expect(wrapper.text()).toContain('Lab Kafka')
  expect(wrapper.get('[role="status"]').text()).toContain('进程可用')
  expect(wrapper.find('.source-note').exists()).toBe(false)
  expect(wrapper.text()).not.toContain('外部集群可用')
  expect(vi.mocked(fetch).mock.calls.some(call => call[1]?.method === 'POST')).toBe(false)
  wrapper.unmount()
})

it('shows a safe API failure and recovers when explicitly refreshed', async () => {
  const wrapper = setup({ failHealth: true }); await flushPromises()
  expect(wrapper.text()).toContain('无法连接服务')
  expect(wrapper.text()).not.toContain('private-password')
  await wrapper.get('[data-refresh]').trigger('click'); await flushPromises()
  expect(wrapper.get('[role="status"]').text()).toContain('进程可用')
  wrapper.unmount()
})

it('does not turn unavailable snapshots or a partial registration page into healthy totals', async () => {
  const wrapper = setup({ missingSnapshot: true, total: 20 }); await flushPromises()
  expect(wrapper.get('[data-registered-total]').text()).toBe('20')
  expect(wrapper.get('[data-enabled-total]').text()).toBe('1')
  expect(wrapper.text()).toContain('本页启用集群')
  expect(wrapper.get('[data-link-total]').text()).toBe('未知')
  expect(wrapper.text()).toContain('尚无发现快照')
  expect(wrapper.text()).not.toContain('复制健康')
  wrapper.unmount()
})


it('offers a first registration action for an observed empty inventory', async () => {
  const wrapper = setup({ total: 0 }); await flushPromises()
  expect(wrapper.get('[data-registered-total]').text()).toBe('0')
  expect(wrapper.text()).toContain('添加第一个集群')
  wrapper.unmount()
})

it('keeps unavailable registration counts unknown and offers a retry', async () => {
  const wrapper = setup({ failInventory: true }); await flushPromises()
  expect(wrapper.get('[data-registered-total]').text()).toBe('未知')
  expect(wrapper.get('[data-enabled-total]').text()).toBe('未知')
  expect(wrapper.text()).toContain('重试读取')
  expect(wrapper.get('[data-link-total]').text()).toBe('未知')
  wrapper.unmount()
})
