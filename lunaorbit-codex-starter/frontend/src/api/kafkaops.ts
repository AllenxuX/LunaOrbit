import { ApiError } from './client'
import { registryRequest, isInformationMode } from './registry'
export interface Capability { feature: string; supported: boolean; reason: string }
export interface Markers { synthetic: boolean; observed_at: string; stale: boolean; state: string; reason?: string; capabilities: Capability[] }
export interface Page<T> extends Markers { items: T[]; total: number; page: number; page_size: number }
export interface Broker { id: number; host: string; port: number }
export interface Partition { partition: number; leader: number; replicas: number[]; isr: number[]; error_code?: string }
export interface Topic { name: string; internal: boolean; partitions: Partition[]; rf_min: number | null; rf_max: number | null; error_code?: string }
export interface Group { id: string; state: string; protocol_type: string; error_code?: string }
export interface Offset { group: string; topic: string; partition: number; committed: number | null; log_end: number | null; lag: number | null; error_code?: string }
export interface Options { page?: number; pageSize?: number; query?: string; scenario?: string }
export const scenarios = ['success', 'authorization_denied', 'broker_unreachable', 'unsupported_api', 'timeout', 'kafka28', 'kafka36', 'kafka37', 'kafka392']
export function kafkaPath(id: string) { return `/kafka/clusters/${encodeURIComponent(id)}` }
function params(options: Options) {
  const p = new URLSearchParams({ page: String(options.page ?? 1), page_size: String(options.pageSize ?? 20), query: options.query ?? '' })
  if (options.scenario) p.set('scenario', options.scenario)
  return p.toString()
}
export function kafkaList<T>(id: string, resource: string, options: Options = {}) { return registryRequest<Page<T>>(`${kafkaPath(id)}/${resource}?${params(options)}`) }
export function kafkaTopic(id: string, topic: string, scenario: string) { return registryRequest<{ item: Topic } & Markers>(`${kafkaPath(id)}/topics/${encodeURIComponent(topic)}?scenario=${encodeURIComponent(scenario)}`) }
export function kafkaError(error: unknown) {
  if (isInformationMode() && error instanceof ApiError && [401, 403].includes(error.status)) return 'Kafka 查询失败，请检查集群连接配置。'
  const messages: Record<number, string> = { 401: '开发会话已失效，请刷新页面', 403: '当前集群查询未获授权', 404: '集群或资源不存在', 409: '添加已更新，请刷新后重试', 429: '请求过于频繁，请稍后重试', 501: '当前 Kafka API 不支持此查询', 503: 'Broker 不可达或服务暂不可用', 504: 'Kafka 查询超时，请重试' }
  return error instanceof ApiError ? messages[error.status] ?? 'Kafka 查询失败，请稍后重试' : 'Kafka 查询失败，请稍后重试'
}
export function reasonLabel(reason?: string) {
  const labels: Record<string, string> = { unsupported_api: 'API 不支持（实际能力探测结果）', authorization_denied: '未获 Kafka 查询授权', broker_unreachable: 'Broker 不可达', timeout: '查询超时', 'Synthetic protocol fixture; no Kafka connection': '合成协议样例，未连接 Kafka', 'synthetic API capability fixture': '模拟 API 能力结果；此场景不支持该查询' }
  return reason ? labels[reason] ?? '能力受限或数据未知，请检查实际 API 探测结果' : ''
}
