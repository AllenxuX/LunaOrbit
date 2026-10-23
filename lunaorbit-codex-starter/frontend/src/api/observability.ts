import { ApiError } from './client'
import { registryRequest } from './registry'
export interface Metric { link_id: string; metric: string; value: number | null; unit: string; source: string; observed_at: string; sampled_at: string; expires_at?: string; stale: boolean; synthetic: boolean; state: string; reason?: string }
export interface Observation { items: Metric[]; synthetic: boolean; observed_at: string; stale: boolean; state: string }
export interface Impact { node_id: string; items: { node_id: string; link_id: string; state: string; reason?: string }[]; synthetic: boolean; state: string }
export interface Confirmation { id: string; scope_id: string; link_id: string; topic: string; description?: string; confirmed_by: string; confirmed_at: string; kind: 'manual_business_confirmation'; active: boolean }
export interface ConfirmationInput { link_id: string; topic: string; description?: string }
const path = (scope: string) => `/observability/scopes/${encodeURIComponent(scope)}`
export const metrics = (scope: string, scenario: string) => registryRequest<Observation>(`${path(scope)}?scenario=${encodeURIComponent(scenario)}`)
export const impact = (scope: string, node: string) => registryRequest<Impact>(`${path(scope)}/impact?node_id=${encodeURIComponent(node)}`)
export const confirmations = (scope: string) => registryRequest<{ items: Confirmation[] }>(`${path(scope)}/lineage-confirmations`)
export function confirmLineage(scope: string, input: ConfirmationInput) {
  const { link_id, topic, description } = input
  return registryRequest<Confirmation>(`${path(scope)}/lineage-confirmations`, 'POST', { link_id, topic, ...(description ? { description } : {}) })
}
export function observationError(error: unknown) {
  const messages: Record<number, string> = { 400: '查询范围或业务确认字段无效', 401: '会话已失效，请重新登录', 403: '当前身份未获此范围授权，业务确认仅限授权管理员', 404: '查询范围或关系不存在或未获授权', 409: '发现数据已变化，请刷新后重试', 429: '请求过于频繁，请稍后重试', 503: '指标服务暂不可用，状态未知', 504: '指标查询超时，状态未知' }
  return error instanceof ApiError ? messages[error.status] ?? '指标查询失败，状态未知' : '指标查询失败，状态未知'
}
