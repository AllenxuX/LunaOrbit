/** Endpoint Catalog (auto discovery first). Rows never carry user names, mechanisms or credentials. */
export interface CatalogEndpoint {
  id: string
  source: 'auto' | 'merged' | 'manual'
  aliases: string[]
  members: string[]
  addresses: string[]
  resolved: string[]
  cluster_id?: string
  registration_id?: string
  display_name?: string
  room?: string
  environment?: string
  ambiguous?: string[]
  access: string
  access_reason?: string
  checked_at?: string
  credential_source?: string
  lineage_id: string
  read_via: 'auto' | 'registration'
}
export interface TopicSummary { name: string; partitions: number; internal: boolean }
export interface TopicPage { items: TopicSummary[]; total: number; page: number; page_size: number }
export interface PartitionSummary { partition: number; leader: number; replicas: number; isr: number }
export interface TopicDetail { name: string; partitions: PartitionSummary[] }
export class CatalogError extends Error { constructor(public code: string, public status: number, public registrationId?: string) { super(code) } }

export const PAGE_SIZE = 20
const base = '/api/v1/discovery/catalog'

async function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { signal, credentials: 'same-origin', headers: { Accept: 'application/json' } })
  const body = await response.json().catch(() => ({})) as { error?: { code?: string }; registration_id?: string }
  if (!response.ok) throw new CatalogError(body.error?.code ?? 'REQUEST_FAILED', response.status, body.registration_id)
  return body as T
}

export async function readCatalog(signal?: AbortSignal): Promise<CatalogEndpoint[]> {
  const body = await get<{ items?: CatalogEndpoint[] }>(base, signal)
  return Array.isArray(body.items) ? body.items.filter(r => typeof r?.read_via === 'string' && Array.isArray(r.addresses) && Array.isArray(r.resolved) && Array.isArray(r.aliases)) : []
}
export function readTopics(id: string, query: string, page: number, signal?: AbortSignal): Promise<TopicPage> {
  const q = new URLSearchParams({ query, page: String(page), page_size: String(PAGE_SIZE) })
  return get<TopicPage>(`${base}/${encodeURIComponent(id)}/topics?${q}`, signal)
}
export function readTopic(id: string, topic: string, signal?: AbortSignal): Promise<TopicDetail> {
  return get<TopicDetail>(`${base}/${encodeURIComponent(id)}/topics/${encodeURIComponent(topic)}`, signal)
}
export async function readGroups(id: string, signal?: AbortSignal): Promise<string[]> {
  return (await get<{ items?: string[] }>(`${base}/${encodeURIComponent(id)}/groups`, signal)).items ?? []
}

const reasons: Record<string, string> = { timeout: '连接失败（超时）', refused: '连接失败（拒绝）', blocked: '连接失败（出站未允许）', tls: '连接失败（TLS）', unresolvable: '解析失败', auth: '认证失败', authorization: '授权不足' }
export function accessLabel(row: Pick<CatalogEndpoint, 'access' | 'access_reason' | 'source'>): { text: string; symbol: string; tone: 'ok' | 'warn' | 'bad' | 'gray' | 'lock' } {
  switch (row.access) {
    case 'readable': return { text: '可读', symbol: '✓', tone: 'ok' }
    case 'connect_failed': {
      const reason = row.access_reason ?? ''
      return { text: `只读失败 · ${reasons[reason] ?? '连接失败'}`, symbol: '!', tone: reason === 'auth' || reason === 'authorization' ? 'bad' : 'warn' }
    }
    case 'address_unresolvable': return { text: '只读失败 · 地址无法解析', symbol: '!', tone: 'warn' }
    case 'not_connected': return { text: '未连接', symbol: '○', tone: 'gray' }
    case 'registered': return { text: '按登记读取', symbol: '○', tone: 'gray' }
    default: return { text: '不可读 · 凭据未开启或不可读', symbol: '■', tone: 'lock' }
  }
}
export function readable(row: CatalogEndpoint) { return row.access === 'readable' }
export function addressSummary(addresses: string[]) {
  if (addresses.length <= 2) return addresses.join(', ') || 'MM2 配置中未解析出地址'
  return `${addresses.slice(0, 2).join(', ')} 等 ${addresses.length} 个`
}
export function sourceLabel(row: Pick<CatalogEndpoint, 'source' | 'read_via' | 'access'>) {
  const label = row.source === 'manual' ? '手工补充' : row.source === 'merged' ? '自动发现 · 已合并登记信息' : '自动发现'
  return row.source !== 'manual' && row.read_via === 'registration' ? `${label} · 登记回退` : label
}
export function nodeTitle(row: CatalogEndpoint) {
  const name = row.display_name || row.aliases.join(' / ') || '手工补充'
  return `${name} · ${addressSummary(row.resolved.length ? row.resolved : row.addresses)}`
}
export function replicaSummary(parts: PartitionSummary[]) {
  if (!parts.length) return { partitions: 0, replicas: '未知', isr: '未知' }
  const rs = parts.map(p => p.replicas), lo = Math.min(...rs), hi = Math.max(...rs)
  const short = parts.filter(p => p.isr < p.replicas).length
  return { partitions: parts.length, replicas: lo === hi ? String(lo) : `${lo}–${hi}`, isr: short ? `ISR 不足分区 ${short}/${parts.length}` : '全部同步' }
}
const norm = (a: string) => a.trim().toLowerCase()
export function catalogFor(rows: CatalogEndpoint[], key: { registrationId?: string; lineageId?: string; addresses?: string[] }) {
  if (key.registrationId) { const hit = rows.find(r => r.registration_id === key.registrationId); if (hit) return hit }
  if (key.lineageId) { const hit = rows.find(r => r.lineage_id === key.lineageId); if (hit) return hit }
  const want = new Set((key.addresses ?? []).map(norm))
  return want.size ? rows.find(r => [...r.addresses, ...r.resolved].some(a => want.has(norm(a)))) : undefined
}
export function errorText(error: unknown) {
  const code = error instanceof CatalogError ? error.code : ''
  return ({ CREDENTIALS_OFF: '凭据未开启或不可读，无法读取该集群', USE_REGISTRATION: '自动发现读取失败，请从登记的集群查看', ENDPOINT_UNREADABLE: '只读查询失败，请稍后刷新', NOT_FOUND: '该集群已不在发现结果中' } as Record<string, string>)[code] ?? '读取失败，请稍后刷新'
}
