import { ApiError } from './client'

export interface RuntimeInfo {
  information_mode?: boolean
  workspace_id?: string
  mode: 'mock' | 'readonly-lab' | 'admin-lab' | 'disabled' | 'information'
  synthetic: boolean
  write_enabled: boolean
  kite_version?: string
  capabilities: { discovery: boolean; kafka_reads: boolean; acl_reads: boolean; changes: boolean; observability: boolean }
}

export async function getRuntime(signal?: AbortSignal): Promise<RuntimeInfo> {
  const controller = new AbortController()
  const abort = () => controller.abort()
  signal?.addEventListener('abort', abort, { once: true })
  if (signal?.aborted) controller.abort()
  const timer = setTimeout(abort, 5000)
  try {
    const response = await fetch('/api/runtime', { signal: controller.signal, credentials: 'same-origin', headers: { Accept: 'application/json' } })
    if (!response.ok) throw new ApiError('数据源信息暂时不可用', 'HTTP_ERROR', response.status)
    const data = await response.json() as RuntimeInfo
    if (!data || !['mock', 'readonly-lab', 'admin-lab', 'disabled', 'information'].includes(data.mode) || typeof data.synthetic !== 'boolean' ||
      typeof data.write_enabled !== 'boolean' || (data.mode !== 'admin-lab' && data.write_enabled) || !data.capabilities ||
      ['discovery', 'kafka_reads', 'acl_reads', 'changes', 'observability'].some(key => typeof data.capabilities[key as keyof RuntimeInfo['capabilities']] !== 'boolean') ||
      ((data.mode === 'readonly-lab' || data.mode === 'admin-lab') && data.synthetic)) throw new ApiError('数据源响应格式错误', 'INVALID_RESPONSE')
    if ((data.mode === 'information' && data.information_mode !== true) || (data.information_mode === true && (typeof data.workspace_id !== 'string' || !data.workspace_id.trim() || data.write_enabled))) throw new ApiError('信息工作空间响应格式错误', 'INVALID_RESPONSE')
    return data
  } catch (error) {
    if (error instanceof ApiError) throw error
    throw new ApiError('数据源信息暂时不可用', 'NETWORK_ERROR')
  } finally {
    clearTimeout(timer)
    signal?.removeEventListener('abort', abort)
  }
}
