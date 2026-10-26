import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, describe, expect, it, vi } from 'vitest'
import CatalogTopic from './CatalogTopic.vue'

const RouterLinkStub = { props: ['to'], template: '<a :href="String(to)"><slot /></a>' }
afterEach(() => vi.unstubAllGlobals())

describe('CatalogTopic (direct topic details of an auto endpoint)', () => {
  it('shows the partition / replica / ISR table', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ name: 'orders', partitions: [{ partition: 0, leader: 1, replicas: 3, isr: 3 }, { partition: 1, leader: 2, replicas: 3, isr: 2 }] }))))
    const w = mount(CatalogTopic, { props: { id: 'auto:1', topicName: 'orders' }, global: { stubs: { RouterLink: RouterLinkStub } } })
    await flushPromises()
    expect(w.get('h1').text()).toBe('orders')
    expect(w.findAll('[data-partition]').length).toBe(2)
    expect(w.text()).toContain('ISR 不足分区 1/2')
  })
  it('explains an unreadable endpoint safely and offers the way back', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ error: { code: 'CREDENTIALS_OFF', message: 'raw' } }), { status: 409 })))
    const w = mount(CatalogTopic, { props: { id: 'auto:1', topicName: 'orders' }, global: { stubs: { RouterLink: RouterLinkStub } } })
    await flushPromises()
    expect(w.text()).toContain('凭据未开启或不可读，无法读取该集群')
    expect(w.get('[data-back]').attributes('href')).toBe('/topology')
    expect(w.find('form').exists()).toBe(false)
  })
})
