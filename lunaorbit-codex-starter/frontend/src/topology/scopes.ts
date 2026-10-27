import { validTime } from './time'
import type { ScopeEntry, Snapshot, WorkloadClass } from '../api/topology'

export interface ScopeOptions { includeRemoved:boolean }
export interface ScopeFailure { scopeId:string;cluster:string;reason:string;lastSuccess:string;hasHistory:boolean }
export interface ScopeRemoved { scopeId:string;cluster:string;removedAt:string;lastSuccess:string }
export interface ScopeSummary {
 clusters:number;namespaces:number;withMM2:number;partial:boolean
 failures:ScopeFailure[];fresh:string[];removed:ScopeRemoved[];suspected:WorkloadClass[]
}

const ZERO=/^0001-01-01/
const REASONS:Record<string,string>={TIMEOUT:'采集超时',NOT_FOUND:'Kite 已找不到该集群',PERMISSION_DENIED:'当前账号无权读取',UNAUTHENTICATED:'Kite 认证失败',UPSTREAM_UNAVAILABLE:'Kite 暂不可用',RATE_LIMITED:'Kite 限流',COLLECTION_FAILED:'采集失败',NETWORK_ERROR:'网络错误'}
export const isAutoMode=(entries:ScopeEntry[])=>entries.some(e=>e.scope.auto)
const clusterOf=(e:ScopeEntry)=>e.scope.k8s_cluster??e.scope.id
const active=(entries:ScopeEntry[])=>entries.filter(e=>!e.scope.removed)
const lastSuccess=(s?:Snapshot)=>s&&validTime(s.observed_at)?s.observed_at:''

function failureOf(e:ScopeEntry):ScopeFailure|undefined {
 const base={scopeId:e.scope.id,cluster:clusterOf(e)}
 if(e.error&&e.error!=='NO_SNAPSHOT')return {...base,reason:e.error,lastSuccess:'',hasHistory:false}
 const s=e.snapshot
 if(!s?.stale)return undefined
 const scopeIssue=s.issues.find(i=>!i.namespace&&!i.workload_uid)
 const nsIssue=s.issues.find(i=>i.namespace&&!i.workload_uid)
 const code=scopeIssue?.code??nsIssue?.code
 if(!code)return undefined
 const reason=(REASONS[code]??`采集失败（${code}）`)+(scopeIssue?'':`：命名空间 ${nsIssue!.namespace}`)
 const last=lastSuccess(s)
 return {...base,reason,lastSuccess:last,hasHistory:!!last}
}

export function scopeSummary(entries:ScopeEntry[]):ScopeSummary {
 const live=active(entries),confirmedNs=new Set<string>(),suspected:WorkloadClass[]=[]
 let namespaces=0
 for(const e of live){
  namespaces+=e.snapshot?.scan?.namespaces??0
  for(const w of e.snapshot?.workloads??[]){if(w.status==='confirmed')confirmedNs.add(`${w.k8s_cluster}/${w.namespace}`);else suspected.push(w)}
 }
 suspected.sort((a,b)=>`${a.k8s_cluster}/${a.namespace}/${a.name}`.localeCompare(`${b.k8s_cluster}/${b.namespace}/${b.name}`))
 const failures=live.map(failureOf).filter((f):f is ScopeFailure=>!!f)
 return {clusters:live.length,namespaces,withMM2:confirmedNs.size,partial:failures.length>0,failures,
  fresh:live.filter(e=>e.error==='NO_SNAPSHOT').map(clusterOf),
  removed:entries.filter(e=>e.scope.removed).map(e=>({scopeId:e.scope.id,cluster:clusterOf(e),removedAt:e.scope.removed_at??'',lastSuccess:lastSuccess(e.snapshot)})),suspected}
}

/** Bar and scope prefixes appear only when there is more than one physical scope or anything unusual. */
export function needsScopeBar(entries:ScopeEntry[]):boolean {
 if(!isAutoMode(entries))return false
 const s=scopeSummary(entries),live=active(entries)
 const mm2Ns=new Set(live.flatMap(e=>(e.snapshot?.workloads??[]).map(w=>`${w.k8s_cluster}/${w.namespace}`)))
 return live.length>1||mm2Ns.size>1||s.failures.length>0||s.fresh.length>0||s.removed.length>0||s.suspected.length>0
}

/** Merge per-scope snapshots; links keep their scope. Kafka nodes (registrations) are shared, workloads are per scope. */
export function mergeEntries(entries:ScopeEntry[],opts:ScopeOptions):Snapshot {
 const used=entries.filter(e=>e.snapshot&&(opts.includeRemoved||!e.scope.removed))
 const nodes=new Map<string,Snapshot['nodes'][number]>(),links:Snapshot['links']=[],edges:Snapshot['edges']=[],issues:Snapshot['issues']=[],workloads:WorkloadClass[]=[]
 const times=used.map(e=>e.snapshot!.observed_at).filter(t=>!ZERO.test(t)).sort()
 for(const e of used){
  const s=e.snapshot!
  for(const n of s.nodes)if(!nodes.has(n.id))nodes.set(n.id,n)
  for(const l of s.links)links.push({...l,scope_id:e.scope.id,k8s_cluster:l.k8s_cluster??e.scope.k8s_cluster})
  edges.push(...s.edges);issues.push(...s.issues);workloads.push(...(s.workloads??[]))
 }
 return {scope_id:used[0]?.scope.id??'',synthetic:used.some(e=>e.snapshot!.synthetic),observed_at:times[0]??'',attempted_at:used.map(e=>e.snapshot!.attempted_at).sort().at(-1)??'',
  stale:used.some(e=>e.snapshot!.stale),nodes:[...nodes.values()],links,edges,issues,workloads}
}

export const fullName=(l:{k8s_cluster?:string;namespace:string;workload_name:string})=>l.k8s_cluster?`${l.k8s_cluster} / ${l.namespace} / ${l.workload_name}`:l.workload_name

/** Prefix MM2 names with "K8s / namespace" so same-named workloads in different scopes are never indistinguishable. */
export function withScopeNames(s:Snapshot,on:boolean):Snapshot {
 if(!on)return s
 const workloadName=new Map<string,string>()
 const links=s.links.map(l=>{const name=fullName(l);for(const e of s.edges)if(e.link_id===l.id&&e.relation==='declared_input')workloadName.set(e.target,name);return {...l,workload_name:name}})
 return {...s,links,nodes:s.nodes.map(n=>n.kind==='workload'&&workloadName.has(n.id)?{...n,label:workloadName.get(n.id)!}:n)}
}

/** K8s cluster / namespace filter (physical scope); room and environment filters apply on top. */
export function filterByScope(s:Snapshot,cluster:string,namespace:string):Snapshot {
 if(!cluster&&!namespace)return s
 const links=s.links.filter(l=>(!cluster||l.k8s_cluster===cluster)&&(!namespace||l.namespace===namespace))
 const ids=new Set(links.map(l=>l.id)),edges=s.edges.filter(e=>ids.has(e.link_id)),used=new Set(edges.flatMap(e=>[e.source,e.target]))
 return {...s,links,edges,nodes:s.nodes.filter(n=>used.has(n.id)),workloads:(s.workloads??[]).filter(w=>(!cluster||w.k8s_cluster===cluster)&&(!namespace||w.namespace===namespace))}
}

export interface ScopeGroup { cluster:string;namespaces:{namespace:string;links:{id:string;name:string}[]}[];confirmed:number;suspected:number }
/** MM2 grouped by K8s cluster, then namespace (only scopes with confirmed links or suspects). */
export function groupByScope(links:Snapshot['links'],suspected:WorkloadClass[]):ScopeGroup[] {
 const groups=new Map<string,ScopeGroup>()
 const group=(c:string)=>groups.get(c)??groups.set(c,{cluster:c,namespaces:[],confirmed:0,suspected:0}).get(c)!
 const seen=new Set<string>()
 for(const l of links){
  const g=group(l.k8s_cluster??'未确认'),key=`${g.cluster}/${l.namespace}/${l.workload_uid}`
  let ns=g.namespaces.find(n=>n.namespace===l.namespace);if(!ns){ns={namespace:l.namespace,links:[]};g.namespaces.push(ns)}
  ns.links.push({id:l.id,name:l.workload_name})
  if(!seen.has(key)){seen.add(key);g.confirmed++}
 }
 for(const w of suspected)group(w.k8s_cluster).suspected++
 return [...groups.values()].sort((a,b)=>a.cluster.localeCompare(b.cluster))
}
