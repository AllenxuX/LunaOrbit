import { afterEach, expect, it, vi } from 'vitest'
import { ensureSession, setSessionRuntime } from './registry'
import type { RuntimeInfo } from './runtime'

afterEach(() => { setSessionRuntime(undefined); vi.unstubAllGlobals() })
it('uses the current shared workspace without any session request, including runtime changes', async () => {
  let runtime = { information_mode: true, workspace_id: 'shared-a' } as RuntimeInfo
  setSessionRuntime(() => runtime)
  const fetcher = vi.fn()
  vi.stubGlobal('fetch', fetcher)
  expect((await ensureSession()).user_id).toBe('shared-a')
  runtime = { ...runtime, workspace_id: 'shared-b' }
  expect((await ensureSession()).user_id).toBe('shared-b')
  expect(fetcher).not.toHaveBeenCalled()
})
