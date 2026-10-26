import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, describe, expect, it, vi } from 'vitest'
import ClusterDrawer from './ClusterDrawer.vue'
import type { CatalogEndpoint } from '../api/catalog'

const RouterLinkStub = { props: ['to'], template: '<a :href="String(to)"><slot /></a>' }
const row = (over: Partial<CatalogEndpoint> = {}): CatalogEndpoint => ({ id: 'auto:1', source: 'merged', aliases: ['k1'], members: [], addresses: ['kafka-k1.lab:9092', 'kafka-k1b.lab:9092', 'kafka-k1c.lab:9092'], resolved: ['10.0.0.1:9092'], registration_id: 'reg-1', display_name: 'luna-k1', room: 'bj', environment: 'prod', access: 'readable', read_via: 'auto', lineage_id: 'reg-1', description: '交易主集群', credential_source: 'MM2 配置（只读）', checked_at: '2026-10-26T08:00:00Z', ...over })

function api(topicCount = 45) {
  const names = Array.from({ length: topicCount }, (_, i) => `t-${String(i).padStart(2, '0')}`)
  const calls: string[] = []
  vi.stubGlobal('fetch', vi.fn(async (url: string) => {
    calls.push(url)
    const u = new URL(url, 'http://x')
    if (u.pathname.endsWith('/groups')) return new Response(JSON.stringify({ items: ['g1', 'g2'] }))
    if (u.pathname.includes('/topics/')) return new Response(JSON.stringify({ name: 't-00', partitions: [{ partition: 0, leader: 1, replicas: 3, isr: 3 }, { partition: 1, leader: 1, replicas: 3, isr: 1 }] }))
    const q = u.searchParams.get('query') ?? '', page = Number(u.searchParams.get('page')), hits = names.filter(n => n.includes(q))
    return new Response(JSON.stringify({ items: hits.slice((page - 1) * 20, page * 20).map(name => ({ name, partitions: 2, internal: false })), total: hits.length, page, page_size: 20 }))
  }))
  return calls
}
afterEach(() => vi.unstubAllGlobals())

describe('ClusterDrawer', () => {
  it('shows alias, addresses, access, collection time and only the credential source wording', async () => {
    api()
    const w = mount(ClusterDrawer, { props: { endpoint: row() }, global: { stubs: { RouterLink: RouterLinkStub } } })
    await flushPromises()
    const text = w.text()
    expect(text).toContain('luna-k1')
    expect(text).toContain('k1')
    expect(w.get('[data-addresses]').text()).toContain('等 3 个')
    expect(w.get('[data-access]').text()).toContain('✓ 可读')
    expect(text).toContain('自动发现 · 已合并登记信息')
    expect(text).toContain('凭据来源：MM2 配置（只读）')
    expect(text).toContain('bj')
    expect(w.get('[data-business]').text()).toContain('prod · bj')
    expect(text).toContain('交易主集群')
    expect(text).not.toMatch(/SCRAM|PLAIN|sasl|jaas|用户名|password/i)
    await w.get('[data-expand-addresses]').trigger('click')
    expect(w.get('[data-addresses]').text()).toContain('kafka-k1c.lab:9092')
  })
  it('pages topics 20 at a time and search returns to page 1', async () => {
    const calls = api()
    const w = mount(ClusterDrawer, { props: { endpoint: row() }, global: { stubs: { RouterLink: RouterLinkStub } } })
    await flushPromises()
    expect(w.findAll('[data-topic]').length).toBe(20)
    expect(w.get('[data-page]').text()).toBe('1 / 3')
    await w.get('[data-next]').trigger('click'); await flushPromises()
    expect(w.get('[data-page]').text()).toBe('2 / 3')
    await w.get('[data-topic-search]').setValue('t-4'); await w.get('[data-topic-search-form]').trigger('submit'); await flushPromises()
    expect(w.get('[data-page]').text()).toBe('1 / 1')
    expect(w.findAll('[data-topic]').length).toBe(5)
    expect(calls.at(-1)).toContain('page=1')
  })
  it('summarises the selected topic and links to topic details', async () => {
    api()
    const w = mount(ClusterDrawer, { props: { endpoint: row() }, global: { stubs: { RouterLink: RouterLinkStub } } })
    await flushPromises()
    await w.get('[data-topic]').trigger('click'); await flushPromises()
    const s = w.get('[data-topic-summary]').text()
    expect(s).toContain('分区 2')
    expect(s).toContain('副本 3')
    expect(s).toContain('ISR 不足分区 1/2')
    expect(w.get('[data-topic-detail-link]').attributes('href')).toBe('/endpoints/auto%3A1/topics/t-00')
  })
  it('queries consumer groups only on first open of the tab', async () => {
    const calls = api()
    const w = mount(ClusterDrawer, { props: { endpoint: row() }, global: { stubs: { RouterLink: RouterLinkStub } } })
    await flushPromises()
    expect(calls.some(c => c.endsWith('/groups'))).toBe(false)
    await w.get('[data-tab-groups]').trigger('click'); await flushPromises()
    await w.get('[data-tab-topics]').trigger('click'); await w.get('[data-tab-groups]').trigger('click'); await flushPromises()
    expect(calls.filter(c => c.endsWith('/groups')).length).toBe(1)
    expect(w.text()).toContain('g2')
  })
  it('does not query or claim credentials when the endpoint is unreadable', async () => {
    const calls = api()
    const w = mount(ClusterDrawer, { props: { endpoint: row({ access: 'credentials_unavailable', credential_source: undefined, source: 'auto', registration_id: undefined, room: undefined, environment: undefined, description: undefined }) }, global: { stubs: { RouterLink: RouterLinkStub } } })
    await flushPromises()
    expect(calls.length).toBe(0)
    expect(w.text()).toContain('不可读 · 凭据未开启或不可读')
    expect(w.text()).not.toContain('凭据来源')
    expect(w.text()).toContain('尚未成功采集')
    expect(w.find('[data-business]').exists()).toBe(false)
  })
  it('resets paging and selection when switching nodes', async () => {
    api()
    const w = mount(ClusterDrawer, { props: { endpoint: row() }, global: { stubs: { RouterLink: RouterLinkStub } } })
    await flushPromises()
    await w.get('[data-next]').trigger('click'); await w.get('[data-topic]').trigger('click'); await flushPromises()
    await w.setProps({ endpoint: row({ id: 'auto:2', aliases: ['k2'], display_name: 'luna-k2' }) }); await flushPromises()
    expect(w.get('[data-page]').text()).toBe('1 / 3')
    expect(w.find('[data-topic-summary]').exists()).toBe(false)
  })
})
