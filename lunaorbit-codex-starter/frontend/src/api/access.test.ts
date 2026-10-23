import { afterEach, expect, it, vi } from 'vitest'
import { login, accessError, requestACL } from './access'
import { ApiError } from './client'
afterEach(() => vi.unstubAllGlobals())
it('posts explicit credentials only to platform auth endpoint', async () => {
  const fetcher = vi.fn(async (url: string, options?: RequestInit) => { expect(url).toBe('/api/v1/auth/login'); expect(JSON.parse(String(options?.body))).toEqual({ username: 'entered-user', password: 'entered-secret' }); return new Response(JSON.stringify({ user_id: 'entered-user', mode: 'ldap' })) })
  vi.stubGlobal('fetch', fetcher)
  await login('entered-user', 'entered-secret')
  expect(fetcher).toHaveBeenCalledOnce()
})
it('maps authentication and authorization errors without upstream text', () => {
  expect(accessError(new ApiError('sensitive', 'AUTHORIZER_UNVERIFIED', 501))).toContain('Broker')
  expect(accessError(new ApiError('sensitive', 'AUTHORIZER_UNVERIFIED', 501))).not.toContain('LDAP')
  expect(accessError(new ApiError('sensitive', 'x', 401))).toContain('认证')
  expect(accessError(new ApiError('sensitive', 'x', 403))).toContain('授权')
  expect(accessError(new Error('sensitive'))).not.toContain('sensitive')
})
it('submits only whitelisted ACL fields and lets the server determine registration scope', async () => {
  const fetcher = vi.fn(async (url: string, options?: RequestInit) => {
    expect(url).toBe('/api/v1/kafka/clusters/id/acl-requests')
    expect(JSON.parse(String(options?.body))).toEqual({ rule: { principal: 'User:synthetic', host: '*', resource_type: 'TOPIC', resource_name: 'demo', pattern_type: 'LITERAL', operation: 'READ', permission: 'ALLOW' }, preview: true })
    return new Response(JSON.stringify({ status: 'preview' }))
  })
  vi.stubGlobal('fetch', fetcher)
  await requestACL('id', { principal: 'User:synthetic', host: '*', resource_type: 'TOPIC', resource_name: 'demo', pattern_type: 'LITERAL', operation: 'READ', permission: 'ALLOW', source_registration_id: 'forged-scope', ...{ password: 'must-not-submit', applicant: 'forged-user', status: 'approved' } }, true)
})
