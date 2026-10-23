export class ApiError extends Error {
  constructor(message: string, public readonly code: string, public readonly status = 0) {
    super(message)
    this.name = 'ApiError'
  }
}

export interface Health {
  status: 'ok'
  mode: 'foundation'
  external_connections: 'disabled'
}

export async function getHealth(timeoutMs = 5000): Promise<Health> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const response = await fetch('/healthz', { signal: controller.signal, credentials: 'same-origin', headers: { Accept: 'application/json' } })
    if (!response.ok) throw new ApiError('服务暂时不可用', 'HTTP_ERROR', response.status)
    let data: Partial<Health> | null
    try { data = await response.json() as Partial<Health> | null }
    catch { throw new ApiError('服务响应格式错误', 'INVALID_RESPONSE') }
    if (data?.status !== 'ok' || data.mode !== 'foundation' || data.external_connections !== 'disabled') {
      throw new ApiError('服务响应格式错误', 'INVALID_RESPONSE')
    }
    return data as Health
  } catch (error) {
    if (error instanceof ApiError) throw error
    if (controller.signal.aborted) throw new ApiError('请求超时', 'TIMEOUT')
    throw new ApiError('无法连接服务', 'NETWORK_ERROR')
  } finally { clearTimeout(timer) }
}
