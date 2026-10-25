import type { Presentation, PresentationEdge, PresentationNode } from './presentation'
import type { LayoutPlan } from './routing-types'

export interface ClusterMeta {environment?:string;room?:string}
export interface ClusterGroup {id:string;label:string;clusterIds:string[];internalLinkIds:string[];card:string}
export interface Grouping {basis:string[];groups:ClusterGroup[]}
export interface ScaleResult {presentation:Presentation;grouped:boolean;groups?:Grouping}

export const MAX_CLUSTERS=30,MAX_LINKS=50,MAX_GROUP_SIZE=12,LABEL_CAP=12
export function needsGrouping(clusters:number,links:number):boolean {return clusters>MAX_CLUSTERS||links>MAX_LINKS}

const active=(p:Presentation)=>p.edges.filter(e=>e.state!=='disabled')

/** Root SCCs of the enabled graph that reach each node (a node in a root SCC reaches itself). */
function rootSets(p:Presentation):Map<string,string[]> {
 const ids=p.nodes.map(n=>n.id).sort(),out=new Map(ids.map(id=>[id,[] as string[]]))
 for(const e of active(p))out.get(e.source)?.push(e.target)
 const reach=new Map<string,Set<string>>()
 for(const id of ids){const seen=new Set<string>([id]),stack=[id];while(stack.length){for(const n of out.get(stack.pop()!)??[])if(!seen.has(n)){seen.add(n);stack.push(n)}}reach.set(id,seen)}
 // A node is a root when every node reaching it is also reached by it (it sits in a source SCC).
 const roots=ids.filter(id=>ids.every(o=>!reach.get(o)!.has(id)||reach.get(id)!.has(o)))
 const rep=new Map(roots.map(r=>[r,roots.filter(o=>reach.get(r)!.has(o)&&reach.get(o)!.has(r)).sort()[0]!]))
 return new Map(ids.map(id=>[id,[...new Set(roots.filter(r=>reach.get(r)!.has(id)).map(r=>rep.get(r)!))].sort()]))
}

export function groupClusters(p:Presentation,meta:Record<string,ClusterMeta>):Grouping {
 const edges=active(p),linked=new Set(edges.flatMap(e=>[e.source,e.target]))
 const hasEnv=Object.values(meta).some(m=>m.environment),hasRoom=Object.values(meta).some(m=>m.room)
 const basis=[...(hasEnv?['环境']:[]),...(hasRoom?['机房']:[]),'上游根集合'],roots=rootSets(p)
 // Keys are applied level by level: a bucket is only split by the next key while it still exceeds MAX_GROUP_SIZE.
 const levels:((id:string)=>string)[]=[...(hasEnv?[(id:string)=>meta[id]?.environment||'环境未知']:[]),...(hasRoom?[(id:string)=>meta[id]?.room||'机房未知']:[]),(id:string)=>{const r=roots.get(id)??[];return r.length===1?`源 ${r[0]}`:`多源（${r.length}）`}]
 const buckets=new Map<string,string[]>()
 const split=(prefix:string[],ids:string[],level:number)=>{
  if(ids.length<=MAX_GROUP_SIZE&&prefix.length||level>=levels.length){buckets.set(prefix.join(' · '),ids);return}
  const parts=new Map<string,string[]>()
  for(const id of ids){const k=levels[level]!(id),l=parts.get(k)??[];l.push(id);parts.set(k,l)}
  for(const [k,l] of parts)split([...prefix,k],l,level+1)
 }
 const sorted=[...p.nodes].map(n=>n.id).sort(),isolated=sorted.filter(id=>!linked.has(id))
 split([],sorted.filter(id=>linked.has(id)),0)
 if(isolated.length)buckets.set('孤立集群',isolated)
 const groups:ClusterGroup[]=[]
 for(const [key,ids] of [...buckets.entries()].sort((a,b)=>a[0].localeCompare(b[0]))){
  const pages=Math.ceil(ids.length/MAX_GROUP_SIZE)
  for(let i=0;i<pages;i++){
   const clusterIds=ids.slice(i*MAX_GROUP_SIZE,(i+1)*MAX_GROUP_SIZE),members=new Set(clusterIds)
   const internalLinkIds=edges.filter(e=>members.has(e.source)&&members.has(e.target)).flatMap(e=>e.linkIds)
   // No runtime cluster status is collected yet, so every cluster counts as unknown rather than healthy.
   groups.push({id:`group:${key}#${i+1}`,label:pages>1?`${key}（${i+1}/${pages}）`:key,clusterIds,internalLinkIds,card:`集群 ${clusterIds.length} · 组内 MM2 ${internalLinkIds.length} · 失败 0 · 未知 ${clusterIds.length}`})
  }
 }
 return {basis,groups}
}

function merge(a:PresentationEdge['state'],b:PresentationEdge['state']):PresentationEdge['state'] {
 return a==='unknown'||b==='unknown'?'unknown':a==='unverified'||b==='unverified'?'unverified':a
}

/** Folds clusters into groups above the thresholds; counts of clusters and enabled MM2 are conserved. */
export function applyScaleRules(p:Presentation,meta:Record<string,ClusterMeta>,options:{expanded?:Set<string>;force?:boolean}={}):ScaleResult {
 const edges=active(p)
 if(!options.force&&!needsGrouping(p.nodes.length,edges.length))return {presentation:p,grouped:false}
 const grouping=groupClusters(p,meta),expanded=options.expanded??new Set<string>(),owner=new Map<string,string>(),nodes:PresentationNode[]=[]
 const byId=new Map(p.nodes.map(n=>[n.id,n]))
 for(const g of grouping.groups){
  if(expanded.has(g.id)){for(const id of g.clusterIds){owner.set(id,id);nodes.push(byId.get(id)!)}continue}
  for(const id of g.clusterIds)owner.set(id,g.id)
  nodes.push({id:g.id,kind:'kafka',label:`${g.label} · ${g.clusterIds.length} 集群`,replication_factor:null,rf_status:'unknown'})
 }
 const folded=new Map<string,PresentationEdge>()
 for(const e of edges){
  const s=owner.get(e.source)!,t=owner.get(e.target)!
  if(s===t&&s.startsWith('group:'))continue
  const key=JSON.stringify([s,t]),hit=folded.get(key)
  if(hit){hit.linkIds.push(...e.linkIds);hit.state=merge(hit.state,e.state);hit.stale||=e.stale;hit.label=`MM2 ×${hit.linkIds.length}`}
  else folded.set(key,{...e,id:'fold:'+key,source:s,target:t,linkIds:[...e.linkIds],label:s===e.source&&t===e.target?e.label:'MM2 ×1'})
 }
 return {presentation:{nodes,edges:[...folded.values()].sort((a,b)=>a.id.localeCompare(b.id))},grouped:true,groups:grouping}
}

/** Edges the layout had to draw right-to-left (inside cycles) get an explicit return label. */
export function markReturnEdges(p:Presentation,plan:LayoutPlan):Presentation {
 return {...p,edges:p.edges.map(e=>{const s=plan.positions[e.source],t=plan.positions[e.target];return s&&t&&t.x<=s.x?{...e,label:`${e.label??''} · 回流/环路`}:e})}
}

export interface LabelBudget {shown:string[];total:number;notice:string}
/** Collision-free label set: selected labels always win, focus shows at most LABEL_CAP labels. */
export function labelBudget(p:Presentation,plan:LayoutPlan,options:{focusEdgeIds?:string[];selectedEdgeIds?:string[];cap?:number}):LabelBudget {
 const cap=options.cap??LABEL_CAP,selected=new Set(options.selectedEdgeIds??[]),focusing=!!options.focusEdgeIds?.length
 const candidates=(focusing?p.edges.filter(e=>options.focusEdgeIds!.includes(e.id)):p.edges).filter(e=>e.label).map(e=>e.id)
 const box=(id:string)=>plan.labels?.[id],margin=4
 const hit=(a:string,b:string)=>{const x=box(a),y=box(b);return !!x&&!!y&&x.x-margin<y.x+y.width+margin&&y.x-margin<x.x+x.width+margin&&x.y-margin<y.y+y.height+margin&&y.y-margin<x.y+x.height+margin}
 let shown=candidates.filter(id=>selected.has(id)||!candidates.some(o=>o!==id&&hit(id,o)))
 shown=[...shown.filter(id=>selected.has(id)),...shown.filter(id=>!selected.has(id))]
 const capped=focusing&&shown.length>cap
 if(capped)shown=shown.slice(0,Math.max(cap,[...selected].filter(id=>shown.includes(id)).length))
 return {shown,total:candidates.length,notice:capped?`已显示 ${shown.length}/${candidates.length} 个标签`:''}
}
