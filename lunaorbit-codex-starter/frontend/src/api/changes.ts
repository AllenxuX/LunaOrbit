import { ApiError } from './client'
import { registryRequest } from './registry'
export interface ChangeTarget { id: string; name: string; workload_uid: string; resource_version: string; synthetic: boolean }
export interface ChangeInput { target_id: string; topic_rule: string; expected_resource_version: string }
export interface ChangeEvidence { generation?: number; observed_generation?: number; ready?: boolean; replication_healthy?: boolean | null }
export interface Change extends ChangeInput { id: string; revision: number; status: string; applicant: string; approver?: string; can_approve?: boolean; can_execute?: boolean; synthetic: boolean; reason?: string; created_at?: string; workload_uid?: string; current_resource_version?: string; evidence?: ChangeEvidence }
export const changeTargets = () => registryRequest<{ items: ChangeTarget[] }>('/changes/targets')
export const listChanges = (page = 1) => registryRequest<{ items: Change[]; total: number; page: number; page_size: number }>(`/changes?page=${page}&page_size=20`)
export const changeDetails = (id: string) => registryRequest<Change>(`/changes/${encodeURIComponent(id)}`)
export function createChange(input: ChangeInput, preview: boolean) {
  const { target_id, topic_rule, expected_resource_version } = input
  return registryRequest<Change>('/changes', 'POST', { target_id, topic_rule, expected_resource_version, preview })
}
export const approveChange = (change: Change) => registryRequest<Change>(`/changes/${encodeURIComponent(change.id)}/approve`, 'POST', { revision: change.revision })
export const executeChange = (change: Change, scenario: string) => registryRequest<Change>(`/changes/${encodeURIComponent(change.id)}/execute`, 'POST', { revision: change.revision, scenario })
export function changesError(error: unknown) {
  const messages: Record<number, string> = { 400: '目标或 Topic 规则不符合变更管理要求', 401: '会话已失效，请登录后重试', 403: '当前身份未获目标授权，或不满足独立审批条件', 404: '变更或目标不存在', 409: '版本冲突或状态已变化，请刷新后重新预览', 429: '请求过于频繁，请稍后重试', 501: '真实变更适配器尚未验证，生产写入已禁用', 503: '变更服务暂不可用', 504: '变更查询超时' }
  return error instanceof ApiError ? messages[error.status] ?? '变更操作失败，请重试' : '变更操作失败，请重试'
}
export function changeStatus(status: string) {
  const labels: Record<string, string> = { preview: '预览，未提交', pending_approval: '等待审批，未执行', approved_not_executed: '已批准，尚未执行', executing: '模拟执行中，结果待确认', succeeded: '合成验证通过，未验证真实复制', manual_review: '需要人工复核', conflict: '版本冲突，未自动覆盖' }
  return labels[status] ?? '未知状态，未确认成功'
}
export function changeReason(reason?: string) {
  const labels: Record<string, string> = {
    metrics_missing: '缺少复制指标，需要人工复核', rollout_failed: '工作负载未就绪，需要人工复核', conflict: 'resourceVersion 已变化，未自动覆盖', resource_version_conflict: 'resourceVersion 已变化，未自动覆盖',
    'Independent approval required; execution is synthetic only': '需要独立审批；执行仅限合成环境',
    'Approved; no action executed': '已批准，未执行任何操作',
    'Rollout or replication evidence incomplete; manual review required': '工作负载或复制指标不完整，需要人工复核',
    'Resource identity or version changed; no write attempted': '资源 UID 或版本已变化，未尝试写入',
    'Deployment identity or version changed; no write attempted': 'Deployment UID 或版本已变化，未尝试写入',
    'Resource version conflict; update stopped': 'resourceVersion 冲突，更新已停止',
    'Synthetic rollout and replication evidence verified': '合成 rollout 与复制指标验证通过，未验证真实生产环境',
  }
  return reason ? labels[reason] ?? '检查结果不完整或操作未确认，需要人工复核' : ''
}
