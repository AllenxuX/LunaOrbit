import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import { createMemoryHistory, createRouter } from 'vue-router'
import App from './App.vue'

afterEach(() => vi.unstubAllGlobals())

it('shows shared information navigation without an identity header or governance links', async () => {
  vi.stubGlobal('fetch', vi.fn().mockImplementation(() => Promise.resolve(new Response(JSON.stringify({ mode: 'information', information_mode: true, workspace_id: 'shared', synthetic: false, write_enabled: false,
    capabilities: { discovery: true, kafka_reads: true, acl_reads: false, changes: false, observability: false } })))))
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: { template: '<p>共享信息</p>' } }] })
  await router.push('/')
  const wrapper = mount(App, { global: { plugins: [router] } })
  await flushPromises()
  expect(wrapper.find('a[href="/applications"]').exists()).toBe(true)
  expect(wrapper.find('a[href="/kafka/clusters"]').exists()).toBe(false)
  expect(wrapper.find('a[href="/topic-lineage"]').exists()).toBe(true)
  for (const path of ['/security', '/access-control', '/changes']) expect(wrapper.find(`a[href="${path}"]`).exists()).toBe(false)
  expect(wrapper.find('.account-link').exists()).toBe(false)
  wrapper.unmount()
})

it('keeps cluster navigation active for nested details and opens accessible navigation', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ mode: 'readonly-lab', synthetic: false, write_enabled: false,
    capabilities: { discovery: true, kafka_reads: true, acl_reads: false, changes: false, observability: false }, kite_version: 'v0.16.0' }))))
  const router = createRouter({ history: createMemoryHistory(), routes: [
    { path: '/kafka/clusters/:id/details', component: { template: '<h1>集群详情内容</h1>' } },
    { path: '/topology', component: { template: '<h1>拓扑内容</h1>' } },
  ] })
  await router.push('/kafka/clusters/example/details')
  const wrapper = mount(App, { global: { plugins: [router] } })
  await flushPromises()
  expect(wrapper.get('nav[aria-label="主导航"] a[aria-current="page"]').attributes('href')).toBe('/kafka/clusters')
  expect(wrapper.text()).toContain('实验环境 · 只读')
  expect(wrapper.text()).toContain('集群详情内容')
  const toggle = wrapper.get('button[aria-controls="admin-sidebar"]')
  expect(toggle.attributes('aria-expanded')).toBe('false')
  await toggle.trigger('click')
  expect(toggle.attributes('aria-expanded')).toBe('true')
  await wrapper.trigger('keydown', { key: 'Escape' })
  expect(toggle.attributes('aria-expanded')).toBe('false')
  await router.push('/topology')
  expect(wrapper.get('nav[aria-label="主导航"] a[aria-current="page"]').attributes('href')).toBe('/topology')
  wrapper.unmount()
})

it('does not label a failed runtime response as a connected environment', async () => {
  vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('private-key')))
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: { template: '<p>overview</p>' } }] })
  await router.push('/')
  const wrapper = mount(App, { global: { plugins: [router] } })
  await flushPromises()
  expect(wrapper.text()).toContain('数据源未连接')
  expect(wrapper.text()).not.toContain('private-key')
  wrapper.unmount()
})
