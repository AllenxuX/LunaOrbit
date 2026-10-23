import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import Changes from './Changes.vue'
import Clusters from './Clusters.vue'
import KafkaDetails from './KafkaDetails.vue'
import Security from './Security.vue'

const runtimeCase = vi.hoisted(() => ({ unknown: false }))

vi.mock('../composables/useRuntime', async () => {
  const { shallowRef } = await import('vue')
  return { useRuntime: () => ({ runtime: shallowRef(runtimeCase.unknown ? null : { mode: 'readonly-lab', synthetic: false, write_enabled: false, capabilities: { discovery: true, kafka_reads: false, acl_reads: false, changes: false, observability: false } }) }) }
})
afterEach(() => { vi.unstubAllGlobals(); runtimeCase.unknown = false })

it('hides unavailable change simulation and makes no change requests in the readonly lab', async () => {
  vi.stubGlobal('fetch', vi.fn())
  const wrapper = mount(Changes)
  await flushPromises()
  expect(wrapper.text()).toContain('当前运行模式未启用变更模拟')
  expect(wrapper.find('[data-change-form]').exists()).toBe(false)
  expect(wrapper.find('[data-execute]').exists()).toBe(false)
  expect(fetch).not.toHaveBeenCalled()
  wrapper.unmount()
})

it('does not query Kafka resources when the runtime disables Kafka reads', async () => {
  vi.stubGlobal('fetch', vi.fn())
  const wrapper = mount(KafkaDetails, { props: { id: 'registration-1' } })
  await flushPromises()
  expect(wrapper.text()).toContain('当前运行模式未启用 Kafka 只读查询')
  expect(wrapper.find('.demo-settings').exists()).toBe(false)
  expect(fetch).not.toHaveBeenCalled()
  wrapper.unmount()
})

it('labels actual registration probes without presenting them as Mock results', async () => {
  const cluster = { id: 'lab-registration', revision: 1, display_name: 'Lab cluster', enabled: true, bootstrap_servers: ['broker.example.test:9093'], status: 'unverified', capabilities: [] }
  vi.stubGlobal('fetch', vi.fn(async (input: string) => new Response(JSON.stringify(input.endsWith('/session') ? { user_id: 'lab', mode: 'readonly-lab' } : { items: [cluster], total: 1 }))))
  const wrapper = mount(Clusters, { global: { stubs: { RouterLink: { template: '<a><slot /></a>' } } } })
  await flushPromises()
  expect(wrapper.text()).not.toContain('数据源已禁用')
  expect(wrapper.text()).not.toContain('Mock')
  expect(wrapper.findAll('button').some(button => button.text() === '连接测试')).toBe(true)
  expect(wrapper.find('.demo-settings').exists()).toBe(false)
  wrapper.unmount()
})


it('keeps identity login usable without requesting unavailable ACL inboxes or rules', async () => {
  vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ user_id: 'lab', mode: 'readonly-lab' }))))
  const wrapper = mount(Security, { props: { id: 'lab-registration' } })
  await flushPromises()
  expect(wrapper.find('[data-login]').exists()).toBe(true)
  expect(wrapper.find('[data-inbox]').exists()).toBe(false)
  expect(wrapper.find('[data-acl-request]').exists()).toBe(false)
  expect(wrapper.text()).toContain('当前运行模式未启用 Kafka ACL 查询与审批')
  expect(vi.mocked(fetch).mock.calls.every(call => String(call[0]).endsWith('/session'))).toBe(true)
  wrapper.unmount()
})

it('does not advertise Mock data or actual connections when runtime metadata is unavailable', async () => {
  runtimeCase.unknown = true
  vi.stubGlobal('fetch', vi.fn(async (input: string) => new Response(JSON.stringify(input.endsWith('/session') ? { user_id: 'operator', mode: 'readonly-lab' } : { items: [], total: 0 }))))
  const wrapper = mount(Clusters, { global: { stubs: { RouterLink: { template: '<a><slot /></a>' } } } })
  await flushPromises()
  expect(wrapper.text()).not.toContain('连接正常')
  expect(wrapper.text()).not.toContain('Mock')
  expect(wrapper.text()).not.toContain('Kafka 查询使用实际连接')
  expect(wrapper.find('.demo-settings').exists()).toBe(false)
  wrapper.unmount()
})
