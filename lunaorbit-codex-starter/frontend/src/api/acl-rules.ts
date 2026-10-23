import { registryRequest } from './registry'
import { workspacePath, type WorkspacePage, type PlanInput } from './workspace'
import type { ACLRule } from './access'

export interface ACLCapabilities { resources: Record<string, string[]>; pattern_types: string[]; permissions: string[] }
export const aclCapabilities = (id: string) => registryRequest<ACLCapabilities>(`${workspacePath(id)}/acl-capabilities`)
export const allACLRules = (id: string, principal = '', page = 1) => registryRequest<WorkspacePage<ACLRule>>(`${workspacePath(id)}/acls?${new URLSearchParams({ principal, page: String(page), page_size: '20' })}`)
export function customACLIntent(principal: string, additions: ACLRule[], removals: ACLRule[]): PlanInput {
  return { kind: 'acl_custom', principal, desired_rules: additions, selected_rules: removals }
}
