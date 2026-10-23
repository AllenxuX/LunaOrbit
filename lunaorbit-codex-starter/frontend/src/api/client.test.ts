import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, getHealth } from './client'

afterEach(() => vi.unstubAllGlobals())
describe('health API client', () => {
  it('reads process health without assuming external services are healthy', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ status: 'ok', mode: 'foundation', external_connections: 'disabled' }))))
    expect(await getHealth()).toEqual({ status: 'ok', mode: 'foundation', external_connections: 'disabled' })
  })
  it('never exposes proxy HTML or upstream credential errors', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('private-password', { status: 502 })))
    await expect(getHealth()).rejects.toMatchObject({ message: '服务暂时不可用', code: 'HTTP_ERROR', status: 502 })
  })
  it('classifies network errors safely', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('private-password')))
    await expect(getHealth()).rejects.toBeInstanceOf(ApiError)
    await expect(getHealth()).rejects.toMatchObject({ message: '无法连接服务', code: 'NETWORK_ERROR' })
  })
  it('rejects malformed successful responses', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{}')))
    await expect(getHealth()).rejects.toMatchObject({ code: 'INVALID_RESPONSE' })
  })
  it('aborts a stalled request', async () => {
    vi.stubGlobal('fetch', vi.fn((_url, options: RequestInit) => new Promise((_resolve, reject) => {
      options.signal?.addEventListener('abort', () => reject(new DOMException('timeout', 'AbortError')))
    })))
    await expect(getHealth(10)).rejects.toMatchObject({ code: 'TIMEOUT' })
  })
})
