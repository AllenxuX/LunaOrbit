import { afterEach, expect, it, vi } from 'vitest'
import { previewPlan, workspaceError } from './workspace'
import { ApiError } from './client'

afterEach(() => vi.unstubAllGlobals())
it('previews business intent for the selected cluster without sending forged principal fields', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url: string, init?: RequestInit) => {
    expect(url).toBe('/api/v1/kafka/clusters/cluster%2Fone/workspace/plans')
    const data = JSON.parse(String(init?.body))
    expect(data.kind).toBe('grant'); expect(data.application_id).toBe('app')
    expect(data.topics).toEqual([{ name: 'orders', pattern_type: 'LITERAL' }])
    expect(data).not.toHaveProperty('principal'); expect(data).not.toHaveProperty('password')
    return new Response(JSON.stringify({ fingerprint: 'preview', additions: [], existing: [], removals: [], input: data }))
  }))
  await previewPlan('cluster/one', { kind: 'grant', application_id: 'app', mode: 'produce', topics: [{ name: 'orders', pattern_type: 'LITERAL' }], ...{ principal: 'User:forged', password: 'do-not-send' } })
})
it('describes conflict and unsupported authorizer without revealing upstream text', () => {
  expect(workspaceError(new ApiError('private upstream', 'CONFLICT', 409))).toContain('重新预览')
  expect(workspaceError(new ApiError('private upstream', 'AUTHORIZER_UNVERIFIED', 501))).toContain('authorizer')
  expect(workspaceError(new Error('private upstream'))).not.toContain('private upstream')
})
