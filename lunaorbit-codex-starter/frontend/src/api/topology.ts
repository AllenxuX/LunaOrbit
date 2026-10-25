import { registryRequest } from './registry'
import { ApiError } from './client'
export interface Mapping { alias:string; status:string; method:string; confidence:string; registration_id?:string; candidate_ids:string[]|null; cluster_id?:string }
export interface Evidence { scope_id:string;namespace:string;deployment_uid:string;deployment_name:string;configmap_name:string;configmap_uid:string;resource_version:string;config_key:string;reference_kind:string;fingerprint:string;observed_at:string }
export interface Link {
 id:string;namespace:string;workload_uid:string;workload_name:string;source_alias:string;target_alias:string;topics_raw:string;policy:string;enabled:boolean
 confidence:string;declared:boolean;observed:boolean;verified:boolean;stale:boolean;state:string;observed_at:string
 source:Mapping;target:Mapping;evidence:Evidence[];notes:string[]
 runtime:{generation:number;observed_generation:number;replicas:number;ready_replicas:number;pod_candidates:number;pod_association:string}
 /** Auto-discovery only: Kubernetes cluster name; scope_id is added client-side when scopes are merged. */
 k8s_cluster?:string;scope_id?:string
}
export interface GraphNode { id:string;kind:'kafka'|'workload';label:string;registration_id?:string;replication_factor:number|null;rf_status:string }
export interface GraphEdge { id:string;source:string;target:string;link_id:string;relation:string;stale:boolean;evidence:Evidence[] }
export interface WorkloadClass { k8s_cluster:string;namespace:string;name:string;workload_uid:string;status:'confirmed'|'suspected';signals:string[] }
export interface ScanSummary { k8s_cluster:string;namespaces:number;deployments:number;namespaces_with_mm2:number;confirmed:number;suspected:number }
export interface Snapshot { scope_id:string;synthetic:boolean;observed_at:string;attempted_at:string;stale:boolean;nodes:GraphNode[];edges:GraphEdge[];links:Link[];issues:{code:string;namespace?:string;workload_uid?:string}[];workloads?:WorkloadClass[];scan?:ScanSummary }
/** /discovery/scopes row; auto-only fields are absent for the legacy single scope. */
export interface ScopeInfo { id:string;synthetic:boolean;k8s_cluster?:string;auto?:boolean;removed?:boolean;removed_at?:string;kite_error?:string }
/** One scope with its latest snapshot, or why none could be read (NO_SNAPSHOT = never collected). */
export interface ScopeEntry { scope:ScopeInfo;snapshot?:Snapshot;error?:string }
export interface Filters { room:string;environment:string }
export interface Layers { kafka:boolean;workloads:boolean }
export interface Position {x:number;y:number}
export interface View { revision:number;positions:Record<string,Position>;filters:Filters;layers:Layers }
export interface Job {job_id:string;status:string;error_code?:string}
export const scopePath=(id:string)=>`/discovery/scopes/${encodeURIComponent(id)}`
export const viewPath=(id:string)=>`/topology/scopes/${encodeURIComponent(id)}/view`
export async function readSnapshot(scope:string):Promise<Snapshot> {
 let result:Snapshot|undefined;let total=0
 for(let page=1;page<=11;page++) {
  const data=await registryRequest<{snapshot:Snapshot;total:number;page:number;page_size:number}>(`${scopePath(scope)}/snapshot?page=${page}&page_size=100`)
  if(!Number.isInteger(data.total)||data.total<0||data.total>1024||data.page!==page||data.page_size!==100||!Array.isArray(data.snapshot.links)||!Array.isArray(data.snapshot.nodes)||!Array.isArray(data.snapshot.edges)) throw new ApiError('拓扑响应不完整，请重试','INVALID_RESPONSE')
  const s=data.snapshot
  if(!result) {total=data.total;result={...s,links:[],nodes:[],edges:[]}}
  if(data.total!==total||s.scope_id!==scope||s.observed_at!==result.observed_at||s.attempted_at!==result.attempted_at) throw new ApiError('发现快照已更新，请重新加载','SNAPSHOT_CHANGED')
  result.links.push(...s.links);result.nodes.push(...s.nodes);result.edges.push(...s.edges)
  if(result.links.length>=total) {
   if(result.links.length!==total||new Set(result.links.map(l=>l.id)).size!==total) throw new ApiError('拓扑响应不完整，请重试','INVALID_RESPONSE')
   result.nodes=[...new Map(result.nodes.map(n=>[n.id,n])).values()];result.edges=[...new Map(result.edges.map(e=>[e.id,e])).values()]
   const nodes=new Map(result.nodes.map(n=>[n.id,n])),links=new Set(result.links.map(l=>l.id))
   if(result.edges.length!==total*2||result.edges.some(e=>!nodes.has(e.source)||!nodes.has(e.target)||!links.has(e.link_id))) throw new ApiError('拓扑响应不完整，请重试','INVALID_RESPONSE')
   for(const link of result.links) {
    const edges=result.edges.filter(e=>e.link_id===link.id),input=edges.find(e=>e.relation==='declared_input'),output=edges.find(e=>e.relation==='declared_output')
    if(edges.length!==2||!input||!output||input.target!==output.source||nodes.get(input.source)?.kind!=='kafka'||nodes.get(input.target)?.kind!=='workload'||nodes.get(output.target)?.kind!=='kafka') throw new ApiError('拓扑响应不完整，请重试','INVALID_RESPONSE')
   }
   return result
  }
  if(s.links.length!==100) throw new ApiError('拓扑响应不完整，请重试','INVALID_RESPONSE')
 }
 throw new ApiError('拓扑超过显示上限','INVALID_RESPONSE')
}
