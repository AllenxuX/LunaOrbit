import { inject, onMounted, onUnmounted, provide, readonly, shallowRef, type InjectionKey, type Ref } from 'vue'
import { getRuntime, type RuntimeInfo } from '../api/runtime'
import { setSessionRuntime } from '../api/registry'

interface RuntimeContext {
  runtime: Readonly<Ref<RuntimeInfo | null>>
  loading: Readonly<Ref<boolean>>
  refresh: () => Promise<void>
}
const runtimeKey: InjectionKey<RuntimeContext> = Symbol('runtime')

export function provideRuntime(): RuntimeContext {
  const runtime = shallowRef<RuntimeInfo | null>(null)
  const loading = shallowRef(true)
  setSessionRuntime(() => runtime.value)
  let request: AbortController | undefined
  let disposed = false
  async function refresh() {
    request?.abort()
    const current = new AbortController()
    request = current
    loading.value = true
    try {
      const result = await getRuntime(current.signal)
      if (!disposed && request === current) runtime.value = result
    } catch {
      if (!disposed && request === current) runtime.value = null
    } finally {
      if (!disposed && request === current) loading.value = false
    }
  }
  const context = { runtime: readonly(runtime), loading: readonly(loading), refresh }
  provide(runtimeKey, context)
  onMounted(refresh)
  onUnmounted(() => { disposed = true; request?.abort(); setSessionRuntime(undefined) })
  return context
}

export function useRuntime(): RuntimeContext {
  // Standalone components retain the project's offline demo defaults. The App
  // always provides its own context, including null when metadata is unavailable.
  const offline: RuntimeInfo = { mode: 'mock', synthetic: true, write_enabled: false,
    capabilities: { discovery: true, kafka_reads: true, acl_reads: true, changes: true, observability: true } }
  return inject(runtimeKey, { runtime: readonly(shallowRef<RuntimeInfo | null>(offline)), loading: readonly(shallowRef(false)), refresh: async () => {} })
}
