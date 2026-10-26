import type { Mapping, Snapshot } from '../api/topology'
import type { RegistrationIdentity } from './model'

/** Kafka endpoint that discovery could not tie to exactly one registered cluster. */
export interface EndpointState {
 nodeId:string
 alias:string
 /** unregistered: no Cluster ID match and no address candidate; candidate: one address match; ambiguous: several. */
 state:'unregistered'|'candidate'|'ambiguous'
 candidateIds:string[]
 /** Enabled links touching this endpoint (all links if none is enabled). */
 linkIds:string[]
}

function endpointMappings(snapshot:Snapshot,nodeId:string){
 return snapshot.edges.filter(e=>e.source===nodeId||e.target===nodeId).flatMap(edge=>{
  const link=snapshot.links.find(l=>l.id===edge.link_id)
  return link?[{link,mapping:edge.source===nodeId?link.source:link.target}]:[]
 })
}

export function endpointStates(snapshot:Snapshot,rows:RegistrationIdentity[]):Map<string,EndpointState>{
 const known=new Set(rows.map(r=>r.id)),out=new Map<string,EndpointState>()
 for(const node of snapshot.nodes){
  if(node.kind!=='kafka')continue
  const touching=endpointMappings(snapshot,node.id)
  if(!touching.length||touching.some(({mapping})=>mapping.status==='confirmed'&&!!mapping.registration_id&&known.has(mapping.registration_id)))continue
  const candidateIds=[...new Set(touching.flatMap(({mapping})=>mapping.candidate_ids??[]).filter(id=>known.has(id)))].sort()
  const enabled=touching.filter(({link})=>link.enabled),links=enabled.length?enabled:touching
  out.set(node.id,{nodeId:node.id,alias:touching[0]!.mapping.alias||node.label,state:candidateIds.length===0?'unregistered':candidateIds.length===1?'candidate':'ambiguous',candidateIds,linkIds:[...new Set(links.map(({link})=>link.id))].sort()})
 }
 return out
}

export interface UnregisteredSummary {registered:number;unregistered:number;aliases:string[];linkIds:string[];banner:string}
export function unregisteredSummary(snapshot:Snapshot,rows:RegistrationIdentity[]):UnregisteredSummary|undefined{
 const states=[...endpointStates(snapshot,rows).values()].filter(s=>s.state==='unregistered')
 if(!states.length)return undefined
 const aliases=[...new Set(states.map(s=>s.alias))].sort(),linkIds=[...new Set(states.flatMap(s=>s.linkIds))].sort()
 const kafka=snapshot.nodes.filter(n=>n.kind==='kafka').length
 return {registered:kafka-states.length,unregistered:states.length,aliases,linkIds,banner:`${linkIds.length} 条链路的对端 ${aliases.join('、')} 为自动发现端点，尚未合并登记信息；点击节点查看访问状态`}
}

export const manualReviewKey=(scope:string,workloadUid:string,alias:string)=>[scope,workloadUid,alias].join('|')
const REVIEW_STORE='lunaorbit.manualBindingReview'
export function loadManualReview():Set<string>{
 try{const raw=globalThis.localStorage?.getItem(REVIEW_STORE);const list=raw?JSON.parse(raw) as unknown:[];return new Set(Array.isArray(list)?list.filter((x):x is string=>typeof x==='string'):[])}catch{return new Set()}
}
export function saveManualReview(keys:Set<string>){try{globalThis.localStorage?.setItem(REVIEW_STORE,JSON.stringify([...keys]))}catch{/* storage unavailable: mark lasts for this page only */}}

/** Node badges: "未登记" for unregistered endpoints, "人工绑定 · 需复核" for endpoints bound through the no-candidate manual flow. */
export function endpointMarks(snapshot:Snapshot,rows:RegistrationIdentity[],reviewed:Set<string>):Record<string,string>{
 const marks:Record<string,string>={}
 for(const s of endpointStates(snapshot,rows).values())if(s.state==='unregistered')marks[s.nodeId]='自动发现'
 for(const link of snapshot.links)for(const m of [link.source,link.target] as Mapping[]){
  if(m.status!=='confirmed'||!m.registration_id||!reviewed.has(manualReviewKey(snapshot.scope_id,link.workload_uid,m.alias)))continue
  const nodeId='kafka:'+m.registration_id
  if(snapshot.nodes.some(n=>n.id===nodeId))marks[nodeId]='人工绑定 · 需复核'
 }
 return marks
}

/** host:port only: scheme, credentials and paths are dropped so nothing secret reaches the URL. */
export function sanitizeAddress(value:string):string{
 const noScheme=value.trim().replace(/^[a-z][a-z0-9+_.-]*:\/\//i,'')
 return noScheme.slice(noScheme.lastIndexOf('@')+1).split('/')[0]!
}
export function registerHref(alias:string,bootstrap:string[]=[]):string{
 const params=new URLSearchParams({register:'1',name:alias})
 const addresses=bootstrap.map(sanitizeAddress).filter(Boolean)
 if(addresses.length)params.set('bootstrap',addresses.join(','))
 return '/kafka/clusters?'+params.toString()
}
export function addressesOverlap(a:string[],b:string[]):boolean{
 const set=new Set(a.map(x=>sanitizeAddress(x).toLowerCase()))
 return b.some(x=>set.has(sanitizeAddress(x).toLowerCase()))
}
