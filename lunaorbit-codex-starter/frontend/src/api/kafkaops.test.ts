import { afterEach, expect, it, vi } from 'vitest'
import { kafkaList, kafkaError } from './kafkaops'
import { ApiError } from './client'
afterEach(() => vi.unstubAllGlobals())
it('encodes registration, filters and explicit mock scenario using registry session transport', async () => {
  const fetcher = vi.fn(async (url: string) => { expect(url).toContain('/api/v1/'); return new Response(JSON.stringify({ items: [], total: 0 })) })
  vi.stubGlobal('fetch', fetcher)
  await kafkaList('a/b', 'topics', { page: 2, query: 'a b', scenario: 'kafka28' })
  expect(String(fetcher.mock.calls[0]?.[0])).toBe('/api/v1/kafka/clusters/a%2Fb/topics?page=2&page_size=20&query=a+b&scenario=kafka28')
})
it('never surfaces upstream text in safe HTTP error messages', () => {
  expect(kafkaError(new ApiError('secret upstream', 'x', 501))).toContain('不支持')
  expect(kafkaError(new Error('secret upstream'))).not.toContain('secret')
})
