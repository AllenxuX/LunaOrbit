import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import Changes from './Changes.vue'
import { defineComponent, h } from 'vue'
import { provideRuntime } from '../composables/useRuntime'
afterEach(() => vi.unstubAllGlobals())

it('opens real lab changes and trusts explicit server execution permission', async () => {
  const native = { ...change, applicant: 'admin', synthetic: false, status: 'approved_not_executed', can_execute: true }
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    const url = String(input)
    const data = url.endsWith('/runtime') ? { mode: 'admin-lab', synthetic: false, write_enabled: true, capabilities: { discovery: true, kafka_reads: true, acl_reads: true, changes: true, observability: false } } :
      url.endsWith('/session') ? { user_id: 'admin', role: 'admin', mode: 'mock' } : url.endsWith('/targets') ? { items: [{ ...target, synthetic: false }] } :
      url.includes('/changes?') ? { items: [native], total: 1 } : { ...native, status: 'manual_review', can_execute: false }
    return new Response(JSON.stringify(data))
  }))
  const wrapper = mount(defineComponent({ setup() { const context = provideRuntime(); return () => !context.loading.value ? h(Changes) : null } }))
  await flushPromises()
  expect(wrapper.find('[data-change-form]').exists()).toBe(true)
  expect(wrapper.get('[data-execute]').text()).toBe('执行实验变更')
  expect(wrapper.get('[data-execute]').attributes('disabled')).toBeUndefined()
  await wrapper.get('[data-execute]').trigger('click'); await flushPromises()
  expect(vi.mocked(fetch).mock.calls.some(call => String(call[0]).endsWith('/execute'))).toBe(true)
  expect(wrapper.get('[data-detail]').text()).toContain('需要人工复核')
  expect(wrapper.get('[data-execute]').attributes('disabled')).toBeDefined()
  wrapper.unmount()
})
const target = { id: 'target-1', name: 'Synthetic MM2', workload_uid: 'synthetic-workload', resource_version: '7', synthetic: true }
const change = { id: 'change-1', target_id: 'target-1', topic_rule: 'events.*', expected_resource_version: '7', revision: 1, status: 'pending_approval', applicant: 'operator', can_approve: true, can_execute: false, synthetic: true }
function setup(items: unknown[] = [change]) {
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    const url = String(input)
    const data = url.endsWith('/session') ? { user_id: 'operator', mode: 'mock' } : url.endsWith('/targets') ? { items: [target] } : url.includes('/changes?') ? { items, total: items.length } : change
    return new Response(JSON.stringify(data))
  }))
  return mount(Changes)
}
it('shows synthetic-only production-disabled controls and prevents self approval or unapproved execution', async () => {
  const wrapper = setup(); await flushPromises()
  expect(wrapper.text()).toContain('生产写入已禁用')
  expect(wrapper.text()).toContain('Mock')
  expect(wrapper.get('[data-approve]').attributes('disabled')).toBeDefined()
  expect(wrapper.get('[data-execute]').attributes('disabled')).toBeDefined()
  expect(wrapper.text()).not.toContain('JAAS')
  wrapper.unmount()
})
it('does not execute approved changes without explicit server permission', async () => {
  const wrapper = setup([{ ...change, status: 'approved_not_executed', can_execute: false }]); await flushPromises()
  expect(wrapper.get('[data-execute]').attributes('disabled')).toBeDefined()
  wrapper.unmount()
})
it('maps forbidden and version-conflict operations safely', async () => {
  const wrapper = setup([{ ...change, applicant: 'other-applicant' }]); await flushPromises()
  const fetcher = vi.mocked(fetch), original = fetcher.getMockImplementation()!
  fetcher.mockImplementation(async (input, init) => String(input).endsWith('/approve') ? new Response('sensitive upstream', { status: 403 }) : original(input, init))
  await wrapper.get('[data-approve]').trigger('click'); await flushPromises()
  expect(wrapper.get('[role="alert"]').text()).toContain('授权')
  fetcher.mockImplementation(async (input, init) => String(input).endsWith('/approve') ? new Response('sensitive upstream', { status: 409 }) : original(input, init))
  await wrapper.get('[data-approve]').trigger('click'); await flushPromises()
  expect(wrapper.get('[role="alert"]').text()).toContain('版本冲突')
  expect(wrapper.text()).not.toContain('sensitive upstream')
  wrapper.unmount()
})
it('requires manual review when replication evidence is missing despite workload readiness', async () => {
  const wrapper = setup([{ ...change, status: 'approved_not_executed', can_execute: true }]); await flushPromises()
  const fetcher = vi.mocked(fetch), original = fetcher.getMockImplementation()!
  fetcher.mockImplementation(async (input, init) => {
    if (String(input).endsWith('/execute')) {
      expect(JSON.parse(String(init?.body))).toEqual({ revision: 1, scenario: 'metrics_missing' })
      return new Response(JSON.stringify({ ...change, status: 'manual_review', reason: 'metrics_missing', can_execute: false, evidence: { generation: 2, observed_generation: 2, ready: true, replication_healthy: null } }))
    }
    return original(input, init)
  })
  await wrapper.get('select').setValue('target-1')
  const scenarioSelect = wrapper.findAll('select').find(s => s.text().includes('metrics_missing'))!
  await scenarioSelect.setValue('metrics_missing')
  await wrapper.get('[data-execute]').trigger('click'); await flushPromises()
  expect(wrapper.get('[data-detail]').text()).toContain('需要人工复核')
  expect(wrapper.get('[data-detail]').text()).toContain('ready：是')
  expect(wrapper.get('[data-detail]').text()).toContain('复制健康：未知')
  expect(wrapper.get('[data-execute]').attributes('disabled')).toBeDefined()
  wrapper.unmount()
})
it('never renders arbitrary raw configuration fields from change responses', async () => {
  const wrapper = setup([{ ...change, status: 'unexpected_status', raw_configmap: 'DO-NOT-RENDER', jaas: 'DO-NOT-RENDER' }]); await flushPromises()
  expect(wrapper.text()).toContain('未知状态')
  expect(wrapper.text()).not.toContain('DO-NOT-RENDER')
  wrapper.unmount()
})
it('previews typed target metadata before submitting a pending request', async () => {
  const wrapper = setup([]); await flushPromises()
  const fetcher = vi.mocked(fetch), original = fetcher.getMockImplementation()!
  fetcher.mockImplementation(async (input, init) => {
    if (String(input).endsWith('/changes') && init?.method === 'POST') {
      const body = JSON.parse(String(init.body))
      expect(Object.keys(body).sort()).toEqual(['expected_resource_version', 'preview', 'target_id', 'topic_rule'])
      expect(body.expected_resource_version).toBe('7')
      return new Response(JSON.stringify({ ...change, topic_rule: body.topic_rule, status: body.preview ? 'preview' : 'pending_approval' }))
    }
    return original(input, init)
  })
  expect(wrapper.text()).toContain('synthetic-workload')
  await wrapper.get('input[name="topic_rule"]').setValue('events.*')
  await wrapper.get('[data-change-form]').trigger('submit'); await flushPromises()
  expect(wrapper.get('[data-preview]').text()).toContain('未提交')
  await wrapper.get('[data-submit]').trigger('click'); await flushPromises()
  expect(wrapper.get('[data-detail]').text()).toContain('等待审批，未执行')
  expect(wrapper.get('[data-execute]').attributes('disabled')).toBeDefined()
  wrapper.unmount()
})
it('discards pending detail responses after refresh', async () => {
  const wrapper = setup(); await flushPromises()
  const fetcher = vi.mocked(fetch), original = fetcher.getMockImplementation()!
  let resolveDetail!: (response: Response) => void
  fetcher.mockImplementation(async (input, init) => String(input).endsWith('/changes/change-1') ? new Promise<Response>(resolve => { resolveDetail = resolve }) : original(input, init))
  await wrapper.findAll('button').find(b => b.text() === '详情')!.trigger('click'); await flushPromises()
  await wrapper.findAll('button').find(b => b.text() === '刷新')!.trigger('click'); await flushPromises()
  resolveDetail(new Response(JSON.stringify({ ...change, topic_rule: 'stale-detail' }))); await flushPromises()
  expect(wrapper.find('[data-detail]').exists()).toBe(false)
  expect(wrapper.text()).not.toContain('stale-detail')
  wrapper.unmount()
})
