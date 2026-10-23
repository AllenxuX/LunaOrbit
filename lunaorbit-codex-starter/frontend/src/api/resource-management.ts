import { registryRequest } from './registry'
import { workspacePath, type WorkspacePage } from './workspace'
export interface Resource { type: 'TOPIC' | 'BROKER'; name: string }
export interface ConfigEntry { name: string; value: string | null; source: string; sensitive: boolean; read_only: boolean; type: string; documentation: string; default_value: string | null; synonyms: { name: string; value: string | null; source: string }[] }
export interface ConfigSnapshot { resource: Resource; entries: ConfigEntry[] }
export interface ResourceIntent { kind: 'configs' | 'topic_delete' | 'reassign' | 'reassign_cancel'; resource: Resource; changes?: {name:string;operation:'SET'|'DELETE';value?:string}[]; assignments?: {partition:number;replicas:number[]}[]; partitions?:number[]; confirmation_name?:string }
export interface ResourcePlan { input: ResourceIntent; fingerprint: string; before?: ConfigSnapshot; after?: ConfigSnapshot; topic?:unknown; assignments?:unknown }
export interface ResourceOperation { id:string;request_id:string;revision:number;status:string;plan:ResourcePlan;created_by:string;approved_by:string;executed_by:string;code?:string;created_at:string }
export const resourceConfigs = (id:string,resource:Resource) => registryRequest<ConfigSnapshot>(`${workspacePath(id)}/resource-configs?${new URLSearchParams({resource_type:resource.type,resource_name:resource.name})}`)
export const previewResource = (id:string,input:ResourceIntent) => registryRequest<ResourcePlan>(`${workspacePath(id)}/resource-plans`,'POST',input,35000)
export const submitResource = (id:string,plan:ResourcePlan,requestId:string) => registryRequest<ResourceOperation>(`${workspacePath(id)}/resource-operations`,'POST',{intent:plan.input,fingerprint:plan.fingerprint,request_id:requestId},35000)
export const resourceOperations = (id:string,page=1,resource?:Resource) => registryRequest<WorkspacePage<ResourceOperation>>(`${workspacePath(id)}/resource-operations?${new URLSearchParams({page:String(page),page_size:'20',...(resource?{resource_type:resource.type,resource_name:resource.name}:{})})}`)
export const resourceTransition = (id:string,op:ResourceOperation,action:'approve'|'execute'|'refresh') => registryRequest<ResourceOperation>(`${workspacePath(id)}/resource-operations/${encodeURIComponent(op.id)}/${action}`,'POST',{revision:op.revision},35000)
export const resourceState = (state:string) => ({pending_approval:'等待审批',approved:'已批准，待执行',submitting:'请求提交中，结果待确认',observing:'已受理，等待实际完成',succeeded:'执行成功',partial_failure:'部分完成，需复核',unknown:'结果未知，请检查结果',failed:'操作失败'}[state] ?? state)
export const resourceKind = (kind: string) => ({configs:'修改资源配置',topic_delete:'删除 Topic',reassign:'调整分区副本',reassign_cancel:'取消副本迁移'}[kind] ?? '资源操作')
export const resourceLabel = (resource: Resource) => resource.type === 'BROKER' ? resource.name ? `Broker ${resource.name}` : '集群 Broker 默认配置' : resource.name
