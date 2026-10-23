import { ApiError } from './client'
import { registryRequest } from './registry'
import { kafkaPath, type Page } from './kafkaops'
export interface AccessSession { user_id: string; mode: string; role?: string; roles?: string[]; groups?: string[]; persistent?: boolean; ldap_configured?: boolean }
export interface ACLRule { principal: string; host: string; resource_type: string; resource_name: string; pattern_type: string; operation: string; permission: string; source_registration_id?: string }
export interface ACLRequest { id: string; source_registration_id: string; applicant: string; approver?: string; revision: number; rule: ACLRule; status: 'preview' | 'pending_approval' | 'approved_not_executed' | 'executing' | 'executed' | 'execution_failed'; created_at: string; can_approve?: boolean; can_execute?: boolean }
export const login = (username: string, password: string) => registryRequest<AccessSession>('/auth/login', 'POST', { username, password })
export const authSession = () => registryRequest<AccessSession>('/auth/session')
export const logout = () => registryRequest('/auth/logout', 'POST', {})
export function listACLs(id: string, page: number, query: string, scenario: string) { return registryRequest<Page<ACLRule>>(`${kafkaPath(id)}/acls?${new URLSearchParams({ page: String(page), page_size: '20', query, scenario })}`) }
export function listACLRequests(id: string) { return registryRequest<Page<ACLRequest>>(`${kafkaPath(id)}/acl-requests?page=1&page_size=100`) }
export const approvalInbox = (page = 1) => registryRequest<{ items: ACLRequest[]; total: number; page: number; page_size: number }>(`/acl-requests/inbox?page=${page}&page_size=20`)
export function requestACL(id: string, rule: ACLRule, preview: boolean) {
  const { principal, host, resource_type, resource_name, pattern_type, operation, permission } = rule
  return registryRequest<ACLRequest>(`${kafkaPath(id)}/acl-requests`, 'POST', { rule: { principal, host, resource_type, resource_name, pattern_type, operation, permission }, preview })
}
export const approveACL = (request: ACLRequest) => registryRequest<ACLRequest>(`/acl-requests/${encodeURIComponent(request.id)}/approve`, 'POST', { revision: request.revision })
export const executeACL = (request: ACLRequest) => registryRequest<ACLRequest>(`/acl-requests/${encodeURIComponent(request.id)}/execute`, 'POST', { revision: request.revision })
export function accessError(error: unknown) {
  if (error instanceof ApiError && error.code === 'AUTHORIZER_UNVERIFIED') return 'Broker 未启用或尚未验证 authorizer，Kafka ACL 不可用；平台管理员权限已开放'
  const messages: Record<number, string> = { 401: '认证失败或会话已失效，请填写登录信息', 403: '当前身份未获授权，或不满足独立审批约束', 404: '集群或申请不存在', 409: '申请版本已更新，请刷新', 429: '请求过于频繁，请稍后重试', 501: 'LDAP 或 API 尚未配置 / 不支持', 503: '认证服务尚未配置或暂时不可用', 504: '认证或查询超时' }
  return error instanceof ApiError ? messages[error.status] ?? '安全服务请求失败，请重试' : '安全服务请求失败，请重试'
}
