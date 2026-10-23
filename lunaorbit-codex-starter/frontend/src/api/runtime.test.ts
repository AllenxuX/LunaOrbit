import { afterEach, expect, it, vi } from 'vitest'
import { getRuntime } from './runtime'

afterEach(() => vi.unstubAllGlobals())
const valid = { mode: 'readonly-lab', synthetic: false, write_enabled: false,
  capabilities: { discovery: true, kafka_reads: true, acl_reads: false, changes: false, observability: false } }

it('accepts explicit server capabilities and rejects malformed or contradictory markers', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(valid))))
  expect(await getRuntime()).toMatchObject(valid)
  for (const invalid of [{}, { ...valid, write_enabled: true }, { ...valid, mode: 'production' }, { ...valid, synthetic: true }]) {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(invalid))))
    await expect(getRuntime()).rejects.toMatchObject({ code: 'INVALID_RESPONSE' })
  }
})

it('never shows upstream body or secret error context', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('private-key', { status: 502 })))
  await expect(getRuntime()).rejects.toMatchObject({ message: '数据源信息暂时不可用' })
})

it('accepts real explicitly enabled lab management and rejects synthetic lab claims', async () => {
  const admin = { ...valid, mode: 'admin-lab', write_enabled: true, capabilities: { ...valid.capabilities, acl_reads: true, changes: true } }
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(admin))))
  expect(await getRuntime()).toEqual(admin)
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ ...admin, synthetic: true }))))
  await expect(getRuntime()).rejects.toMatchObject({ code: 'INVALID_RESPONSE' })
})
