import { ApiError } from './client'
import type { RuntimeInfo } from './runtime'

let sessionRuntime: (() => RuntimeInfo | null) | undefined
export function setSessionRuntime(getter: (() => RuntimeInfo | null) | undefined) { sessionRuntime = getter }
export function isInformationMode() { return sessionRuntime?.()?.information_mode === true }

export type Protocol = 'PLAINTEXT' | 'SSL' | 'SASL_PLAINTEXT' | 'SASL_SSL'
export interface Credentials {
  security_protocol: Protocol
  sasl_mechanism: '' | 'PLAIN' | 'SCRAM-SHA-256' | 'SCRAM-SHA-512'
  username?: string
  password?: string
  tls_ca_pem?: string
  tls_cert_pem?: string
  tls_key_pem?: string
  tls_server_name: string
}
export interface Cluster {
  id: string; revision: number; display_name: string; room: string; environment: string
  description: string; bootstrap_servers: string[]; enabled: boolean
  security_protocol: Protocol; sasl_mechanism: Credentials['sasl_mechanism']; username_masked: string
  credentials_configured: boolean; tls_ca_configured: boolean; tls_client_configured: boolean
  tls_server_name: string; credential_revision: number; status: string; last_probe_at?: string
  last_probe_error_code?: string; discovered_cluster_id?: string; version_reported: string; version_source: string
  capabilities: { feature: string; supported: boolean; reason: string }[]
  brokers: { id: number; host: string; port: number }[]; created_at: string; updated_at: string
}
export interface Session { user_id: string; mode: 'mock' | 'information'; persistent: boolean; lab_catalog?: boolean }
export interface ClusterPage { items: Cluster[]; total: number; page: number; page_size: number }
export const safeMessages: Record<string, string> = {
  authentication_failed: '认证失败', tls_failed: 'TLS 证书验证失败', broker_unreachable: 'Broker 不可达',
  advertised_unreachable: 'advertised.listeners 不可达', authorization_denied: '无管理权限', unsupported_api: 'API 不支持', timeout: '探测超时',
}
const serverCodeAliases: Record<string, string> = {
  authenticationfailed: 'authentication_failed', tlshandshakefailed: 'tls_failed',
  brokerunreachable: 'broker_unreachable', advertisedbrokerunreachable: 'advertised_unreachable',
  authorizationdenied: 'authorization_denied', unsupportedapi: 'unsupported_api',
}
export function probeMessage(code?: string, isMock = true) {
  if (!code) return isMock ? '模拟结果待确认' : '连接结果待确认'
  const normalized = code.toLowerCase()
  return safeMessages[serverCodeAliases[normalized] ?? normalized] ?? (isMock ? '模拟探测失败' : '连接探测失败')
}
export function probeStatus(status: string, isMock = true) {
  if (!isMock) {
    const actual: Record<string, string> = { connected: '连接正常', unverified: '未验证', failed: '连接失败', pending: '排队中', running: '探测中', disabled: '已停用' }
    return actual[status] ?? '状态待确认'
  }
  const labels: Record<string, string> = {
    mock_connected: '模拟连接成功（未验证真实 Kafka）', unverified: '未验证', failed: '模拟探测失败',
    pending: '模拟探测排队中', running: '模拟探测进行中', disabled: '已停用',
  }
  return labels[status] ?? '模拟状态待确认'
}
export function capabilityStatus(capability: { supported: boolean; reason: string }) {
  if (capability.supported) return '支持'
  const reason = capability.reason.trim().toLowerCase()
  if (reason === 'not probed' || reason === 'not_probed') return '未探测'
  if (reason === 'mock unsupported' || reason === 'synthetic unsupported response fixture') return '不支持'
  return '待确认'
}
export async function registryRequest<T>(path: string, method = 'GET', body?: unknown, timeoutMs = 10000): Promise<T> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const response = await fetch(`/api/v1${path}`, { method, credentials: 'same-origin', signal: controller.signal,
      headers: { Accept: 'application/json', ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }) })
    if (!response.ok) {
      const messages: Record<number, string> = { 401: '开发会话已失效，请刷新页面', 403: '当前操作未获授权', 409: '添加已更新，请关闭表单并刷新后重试', 429: '请求过于频繁，请稍后重试' }
      // Forward only explicitly recognized machine codes, never upstream text.
      let code = 'REGISTRY_ERROR'
      try { const error = await response.json(); if (response.status === 501 && error?.error?.code === 'AUTHORIZER_UNVERIFIED') code = 'AUTHORIZER_UNVERIFIED' } catch { /* non-JSON errors retain safe generic text */ }
      throw new ApiError(isInformationMode() && [401, 403].includes(response.status) ? '信息服务暂不可用，请检查服务配置后刷新。' : messages[response.status] ?? '操作失败，请稍后重试', code, response.status)
    }
    if (response.status === 204) return undefined as T
    return await response.json() as T
  } catch (error) {
    if (error instanceof ApiError) throw error
    throw new ApiError(controller.signal.aborted ? '请求超时，请稍后重试' : '无法连接服务或响应格式错误', 'NETWORK_ERROR')
  } finally { clearTimeout(timer) }
}
export async function ensureSession(): Promise<Session> {
  const runtime = sessionRuntime?.()
  if (runtime?.information_mode === true) {
    if (!runtime.workspace_id) throw new ApiError('信息工作空间未配置', 'INVALID_RESPONSE')
    return { user_id: runtime.workspace_id, mode: 'information', persistent: true }
  }
  let session: Session
  try { session = await registryRequest<Session>('/session') }
  catch (error) {
    if (error instanceof ApiError && error.status === 401) session = await registryRequest<Session>('/dev/session', 'POST', {})
    else throw error
  }
  if (session.lab_catalog === true) {
    let rounds = 16
    for (let round = 0; round < rounds; round++) {
      const initialized = await registryRequest<{ items: Pick<Cluster, 'id' | 'status'>[] }>('/lab/registrations', 'POST', {})
      if (!Array.isArray(initialized?.items) || initialized.items.length < 2 || initialized.items.length > 16) throw new ApiError('实验集群响应格式错误', 'INVALID_RESPONSE')
      rounds = Math.min(rounds, initialized.items.length)
      const completed = await Promise.all(initialized.items.filter(item => item.status === 'probing').map(async item => {
        for (let attempt = 0; attempt < 20; attempt++) {
          const job = await registryRequest<{ status: string }>(`/kafka/clusters/${encodeURIComponent(item.id)}/probe`)
          if (job.status === 'done') return true
          await new Promise(resolve => globalThis.setTimeout(resolve, 250))
        }
        return false
      }))
      // Seed again only for registrations still waiting for a probe slot. The
      // server dispatches pending registrations; failed probes are not retried.
      // Still-running probes and their actual status remain unchanged.
      if (!initialized.items.some(item => item.status === 'unverified') || completed.some(done => !done)) break
    }
  }
  return session
}
