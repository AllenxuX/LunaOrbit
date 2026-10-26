import { afterEach, describe, expect, it, vi } from 'vitest'
import { accessLabel, addressSummary, catalogFor, readCatalog, readTopics, replicaSummary, sourceLabel, type CatalogEndpoint } from './catalog'

const row = (over: Partial<CatalogEndpoint> = {}): CatalogEndpoint => ({ id: 'auto:1', source: 'auto', aliases: ['k1'], members: [], addresses: ['kafka-k1.lab:9092'], resolved: ['10.0.0.1:9092'], access: 'readable', read_via: 'auto', lineage_id: 'endpoint-1', ...over })

afterEach(() => vi.unstubAllGlobals())

describe('catalog api', () => {
  it('labels every access state with text, not only colour', () => {
    expect(accessLabel(row())).toEqual({ text: '可读', symbol: '✓', tone: 'ok' })
    expect(accessLabel(row({ access: 'connect_failed', access_reason: 'timeout' }))).toEqual({ text: '只读失败 · 连接失败（超时）', symbol: '!', tone: 'warn' })
    expect(accessLabel(row({ access: 'connect_failed', access_reason: 'auth' })).tone).toBe('bad')
    expect(accessLabel(row({ access: 'connect_failed', access_reason: 'authorization' })).text).toBe('只读失败 · 授权不足')
    expect(accessLabel(row({ access: 'not_connected' })).text).toBe('未连接')
    expect(accessLabel(row({ access: 'credentials_unavailable' })).text).toBe('不可读 · 凭据未开启或不可读')
    expect(accessLabel(row({ access: 'address_unresolvable' })).text).toBe('只读失败 · 地址无法解析')
    expect(accessLabel(row({ source: 'manual', access: 'registered' })).text).toBe('按登记读取')
  })
  it('summarises addresses as at most two plus 等 N 个', () => {
    expect(addressSummary(['a:1'])).toBe('a:1')
    expect(addressSummary(['a:1', 'b:1', 'c:1'])).toBe('a:1, b:1 等 3 个')
  })
  it('names the source of each endpoint', () => {
    expect(sourceLabel(row())).toBe('自动发现')
    expect(sourceLabel(row({ source: 'merged' }))).toBe('自动发现 · 已合并登记信息')
    expect(sourceLabel(row({ source: 'manual' }))).toBe('手工补充')
    expect(sourceLabel(row({ access: 'connect_failed', read_via: 'registration', source: 'merged' }))).toBe('自动发现 · 已合并登记信息 · 登记回退')
  })
  it('summarises replicas and ISR without faking a single value', () => {
    expect(replicaSummary([{ partition: 0, leader: 1, replicas: 3, isr: 3 }, { partition: 1, leader: 1, replicas: 3, isr: 2 }])).toEqual({ partitions: 2, replicas: '3', isr: 'ISR 不足分区 1/2' })
    expect(replicaSummary([{ partition: 0, leader: 1, replicas: 2, isr: 2 }, { partition: 1, leader: 1, replicas: 3, isr: 3 }])).toEqual({ partitions: 2, replicas: '2–3', isr: '全部同步' })
    expect(replicaSummary([]).replicas).toBe('未知')
  })
  it('finds a catalog row by registration id, lineage id or address', () => {
    const rows = [row({ id: 'auto:a', registration_id: 'reg-1', source: 'merged' }), row({ id: 'auto:b', addresses: ['x:9092'], lineage_id: 'endpoint-x' })]
    expect(catalogFor(rows, { registrationId: 'reg-1' })?.id).toBe('auto:a')
    expect(catalogFor(rows, { addresses: ['X:9092'] })?.id).toBe('auto:b')
    expect(catalogFor(rows, { lineageId: 'endpoint-x' })?.id).toBe('auto:b')
    expect(catalogFor(rows, { addresses: ['none:1'] })).toBeUndefined()
  })
  it('requests a paged topic search and keeps credential-free rows', async () => {
    const fetchMock = vi.fn(async (url: string) => new Response(JSON.stringify(url.includes('/topics') ? { items: [{ name: 'orders', partitions: 3, internal: false }], total: 1, page: 2, page_size: 20 } : { items: [row()] }), { status: 200 }))
    vi.stubGlobal('fetch', fetchMock)
    expect((await readCatalog()).length).toBe(1)
    const page = await readTopics('auto:1', 'ord', 2)
    expect(page.total).toBe(1)
    expect(fetchMock.mock.calls[1][0]).toBe('/api/v1/discovery/catalog/auto%3A1/topics?query=ord&page=2&page_size=20')
  })
  it('turns the fallback and switch-off conflicts into readable errors', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ error: { code: 'CREDENTIALS_OFF' } }), { status: 409 })))
    await expect(readTopics('auto:1', '', 1)).rejects.toMatchObject({ code: 'CREDENTIALS_OFF' })
  })
})
