import { afterEach, expect, it, vi } from 'vitest'
import { allACLRules, aclCapabilities } from './acl-rules'
afterEach(() => vi.unstubAllGlobals())
it('queries all native identities without requiring an application principal', async () => {
  const fetcher = vi.fn(async (url: string) => { expect(url).toContain('/workspace/'); return new Response('{}') }); vi.stubGlobal('fetch', fetcher)
  await allACLRules('a/b'); await aclCapabilities('a/b')
  expect(String(fetcher.mock.calls[0]?.[0])).toContain('/a%2Fb/workspace/acls?principal=&page=1&page_size=20')
  expect(String(fetcher.mock.calls[1]?.[0])).toContain('/a%2Fb/workspace/acl-capabilities')
})
