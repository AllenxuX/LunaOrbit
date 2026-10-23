import { afterEach, expect, it, vi } from 'vitest'
import { changesError, createChange } from './changes'
import { ApiError } from './client'
afterEach(() => vi.unstubAllGlobals())
it('sends only typed change fields, never arbitrary ConfigMap or credentials', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url: string, init?: RequestInit) => {
    expect(url).toBe('/api/v1/changes')
    expect(JSON.parse(String(init?.body))).toEqual({ target_id: 'synthetic-target', topic_rule: 'events.*', expected_resource_version: '7', preview: true })
    return new Response(JSON.stringify({ status: 'preview' }))
  }))
  await createChange({ target_id: 'synthetic-target', topic_rule: 'events.*', expected_resource_version: '7', ...{ raw_configmap: { secret: 'never-send' }, jaas: 'never-send' } }, true)
})
it('maps scope and CAS failures without upstream details', () => {
  expect(changesError(new ApiError('sensitive', 'x', 403))).toContain('授权')
  expect(changesError(new ApiError('sensitive', 'x', 409))).toContain('版本冲突')
  expect(changesError(new Error('sensitive'))).not.toContain('sensitive')
})
