import { registryRequest, isInformationMode } from './registry'
import { ApiError } from './client'
import type { ACLRule } from './access'

export interface Application { request_id?:string; id: string; name: string; principal: string; owner: string; description: string; revision: number }
export interface ResourcePattern { name: string; pattern_type: 'LITERAL' | 'PREFIXED' }
export interface PlanInput {
  kind: 'grant' | 'revoke' | 'topic_create' | 'topic_expand' | 'topic_retention' | 'acl_custom'
  principal?: string; desired_rules?: ACLRule[]; account_id?: string
  application_id?: string; mode?: 'produce' | 'consume'; topics?: ResourcePattern[]; group?: ResourcePattern
  selected_rules?: ACLRule[]; topic?: string; partitions?: number; replication_factor?: number; retention_ms?: number
}
export interface Plan { retained?:ACLRule[]; input: PlanInput; fingerprint: string; application_revision: number; additions: ACLRule[]; existing: ACLRule[]; removals: ACLRule[]; before: Record<string, string>; after: Record<string, string> }
export interface WorkspaceRequest { id: string; revision: number; status: string; plan: Plan; created_by: string; approved_by: string; results: { rule?: ACLRule; state: string; code?: string }[]; created_at: string }
export interface WorkspaceMeta { synthetic: boolean; write_enabled: boolean; approval_required: boolean; permissions: Record<'read' | 'topic_manage' | 'applications_manage' | 'acl_manage' | 'audit_read' | 'team_manage', boolean> & { account_manage?: boolean; credentials_read?: boolean }; capabilities?: Record<string, boolean> }
export interface WorkspacePage<T> { items: T[]; total: number; page: number; page_size: number }
export interface WorkspaceAudit { actor: string; action: string; target: string; at: string }
export const workspacePath = (id: string) => `/kafka/clusters/${encodeURIComponent(id)}/workspace`
export const workspaceMeta = (id: string) => registryRequest<WorkspaceMeta>(`${workspacePath(id)}/meta`)
export const applications = (id: string, page = 1, query = '') => registryRequest<WorkspacePage<Application>>(`${workspacePath(id)}/apps?${new URLSearchParams({ page: String(page), page_size: '20', query })}`)
export const saveApplication = (id: string, input: Omit<Application, 'id' | 'revision'>, app?: Application) => registryRequest<Application>(`${workspacePath(id)}/apps${app ? '/' + encodeURIComponent(app.id) : ''}`, app ? 'PATCH' : 'POST', { ...(input.request_id?{request_id:input.request_id}:{}), name: input.name, principal: input.principal, owner: input.owner, description: input.description, ...(app ? { revision: app.revision } : {}) })
export const removeApplication = (id: string, app: Application) => registryRequest(`${workspacePath(id)}/apps/${encodeURIComponent(app.id)}?revision=${app.revision}`, 'DELETE')
export function safePlanInput(input: PlanInput): PlanInput {
  const { kind, application_id, mode, topics, group, topic, partitions, replication_factor, retention_ms, selected_rules } = input
  return { kind, application_id, ...(input.account_id ? {account_id:input.account_id}:{}), ...(kind==='acl_custom'?{principal:input.principal,desired_rules:input.desired_rules?.map(({ principal, host, resource_type, resource_name, pattern_type, operation, permission })=>({ principal, host, resource_type, resource_name, pattern_type, operation, permission }))}:{}), mode, topics: topics?.map(({ name, pattern_type }) => ({ name, pattern_type })), group: group ? { name: group.name, pattern_type: group.pattern_type } : undefined,
    topic, partitions, replication_factor, retention_ms, selected_rules: selected_rules?.map(({ principal, host, resource_type, resource_name, pattern_type, operation, permission }) => ({ principal, host, resource_type, resource_name, pattern_type, operation, permission })) }
}
export const previewPlan = (id: string, input: PlanInput) => registryRequest<Plan>(`${workspacePath(id)}/plans`, 'POST', safePlanInput(input), 35000)
export const submitPlan = (id: string, plan: Plan) => registryRequest<WorkspaceRequest>(`${workspacePath(id)}/requests`, 'POST', { ...safePlanInput(plan.input), fingerprint: plan.fingerprint }, 35000)
export const workspaceRequests = (id: string, page = 1) => registryRequest<WorkspacePage<WorkspaceRequest>>(`${workspacePath(id)}/requests?page=${page}&page_size=20`)
export const approveRequest = (id: string, request: WorkspaceRequest) => registryRequest<WorkspaceRequest>(`${workspacePath(id)}/requests/${encodeURIComponent(request.id)}/approve`, 'POST', { revision: request.revision })
export const executeRequest = (id: string, request: WorkspaceRequest) => registryRequest<WorkspaceRequest>(`${workspacePath(id)}/requests/${encodeURIComponent(request.id)}/execute`, 'POST', { revision: request.revision }, 35000)
export const workspaceACLs = (id: string, principal: string, page = 1) => registryRequest<WorkspacePage<ACLRule>>(`${workspacePath(id)}/acls?${new URLSearchParams({ principal, page: String(page), page_size: '100' })}`)
export const workspaceAudit = (id: string, page = 1) => registryRequest<WorkspacePage<WorkspaceAudit>>(`${workspacePath(id)}/audit?page=${page}&page_size=20`)
export function workspaceError(error: unknown): string {
  if (isInformationMode()) return error instanceof ApiError && error.status === 404 ? '集群或资源不存在，请返回列表刷新。' : '集群信息暂不可用，请刷新或检查连接配置。'
  if (error instanceof ApiError && error.code === 'AUTHORIZER_UNVERIFIED') return 'Broker 尚未启用或未验证 authorizer，无法查询或更改 Kafka ACL'
  const messages: Record<number, string> = { 400: '请检查名称、资源范围和操作参数', 401: '会话已失效，请重新登录', 403: '当前团队未获此集群或资源范围的操作权限', 404: '集群、应用或操作记录不存在', 409: '资源或权限已变化，请重新预览后提交', 429: '操作过于频繁，请稍后重试', 501: '当前 Kafka 尚不支持此操作，请检查集群能力', 503: 'Kafka 或服务暂不可用，操作结果请以记录为准', 504: '请求超时，请先查看操作记录，避免重复执行' }
  return error instanceof ApiError ? messages[error.status] ?? '操作未完成，请查看记录并重试' : '无法连接工作空间，请稍后重试'
}
export const requestKind = (kind: string) => ({ acl_custom:'修改权限', grant: '应用授权', revoke: '撤销权限', topic_create: '创建 Topic', topic_expand: '增加分区', topic_retention: '调整保留时间' })[kind] ?? '集群操作'
export const requestState = (state: string) => ({ pending_approval: '等待审批', approved: '已批准，待执行', executing: '执行中', executed: '执行成功', partial_failure: '部分执行，需复核', execution_unknown: '结果待确认', failed: '执行失败' })[state] ?? state
