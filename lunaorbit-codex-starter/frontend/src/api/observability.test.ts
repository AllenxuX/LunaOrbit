import { afterEach, expect, it, vi } from 'vitest'
import { confirmLineage, metrics, observationError } from './observability'
import { ApiError } from './client'
afterEach(() => vi.unstubAllGlobals())
it('encodes scope/scenario and sends only typed business confirmation fields', async () => {
  const fetcher = vi.fn(async (url: string) => { expect(url).toContain('/api/v1/observability/scopes/scope%2F1'); return new Response('{}') })
  vi.stubGlobal('fetch', fetcher)
  await metrics('scope/1', 'stale')
  await confirmLineage('scope/1', { link_id: 'link', topic: 'orders', description: 'business confirmed', ...{ metrics_url: 'never-send', verified: true } })
  expect(vi.mocked(fetch).mock.calls[0]?.[0]).toContain('?scenario=stale')
  const call = vi.mocked(fetch).mock.calls[1]!
  expect(JSON.parse(String(call[1]?.body))).toEqual({ link_id: 'link', topic: 'orders', description: 'business confirmed' })
})
it('maps authorization failures safely', () => expect(observationError(new ApiError('upstream secret', 'x', 403))).toContain('授权'))
