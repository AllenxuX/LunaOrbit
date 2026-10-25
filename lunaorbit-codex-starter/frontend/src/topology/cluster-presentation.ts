import type { Snapshot } from '../api/topology'
import { buildPresentation, type Presentation, type PresentationEdge, type PresentationNode, type TopologyMode } from './presentation'
import type { Position } from '../api/topology'

export interface ClusterOptions {
 showDisabled?:boolean
 /** Extension point for scale rules (grouping/folding); identity by default. */
 transform?:(presentation:Presentation)=>Presentation
}

/** A link is disabled when its rule is off, whatever runtime state was collected. */
export function isDisabledLink(link:Pick<Snapshot['links'][number],'state'|'enabled'>):boolean {return link.state==='disabled'||link.enabled===false}

function edgeState(link:Snapshot['links'][number],stale:boolean):PresentationEdge['state'] {
 if(isDisabledLink(link))return 'disabled'
 if(stale||link.state==='unknown')return 'unknown'
 return link.verified?'verified':'unverified'
}

/** Cluster-only view: every Kafka cluster is a node, every MM2 replication link is one labelled edge. */
export function buildClusterPresentation(snapshot:Snapshot,options:ClusterOptions={}):Presentation {
 const kinds=new Map(snapshot.nodes.map(n=>[n.id,n.kind])),byLink=new Map<string,Snapshot['edges']>()
 for(const edge of snapshot.edges){const list=byLink.get(edge.link_id)??[];list.push(edge);byLink.set(edge.link_id,list)}
 const edges:PresentationEdge[]=[]
 for(const link of [...snapshot.links].sort((a,b)=>a.id.localeCompare(b.id))){
  if(isDisabledLink(link)&&!options.showDisabled)continue
  const rows=byLink.get(link.id)??[],input=rows.filter(e=>e.relation==='declared_input'),output=rows.filter(e=>e.relation==='declared_output')
  if(input.length!==1||output.length!==1||kinds.get(input[0]!.source)!=='kafka'||kinds.get(output[0]!.target)!=='kafka')continue
  const stale=!!(snapshot.stale||link.stale||input[0]!.stale||output[0]!.stale)
  edges.push({id:'link:'+link.id,source:input[0]!.source,target:output[0]!.target,linkIds:[link.id],state:edgeState(link,stale),stale,label:link.workload_name||link.id})
 }
 const nodes:PresentationNode[]=snapshot.nodes.filter(n=>n.kind==='kafka').sort((a,b)=>a.id.localeCompare(b.id))
 const presentation={nodes,edges}
 return options.transform?options.transform(presentation):presentation
}

export function enabledLinkCount(snapshot:Snapshot):number {return buildClusterPresentation(snapshot).edges.length}

export function directNeighbors(p:Presentation,id:string):{upstream:number;downstream:number} {
 const active=p.edges.filter(e=>e.state!=='disabled')
 return {upstream:new Set(active.filter(e=>e.target===id&&e.source!==id).map(e=>e.source)).size,downstream:new Set(active.filter(e=>e.source===id&&e.target!==id).map(e=>e.target)).size}
}

export function middleEllipsis(text:string,max=18):string {
 if(text.length<=max)return text
 const keep=max-1,head=Math.ceil(keep/2)
 return text.slice(0,head)+'…'+text.slice(text.length-(keep-head))
}

export const clusterNodeWidth=180,clusterNodeHeight=64
export type CanvasMode=TopologyMode|'clusters'|'pairs'
export function presentationFor(snapshot:Snapshot,mode:CanvasMode,showDisabled=false):Presentation {
 return mode==='clusters'?buildClusterPresentation(snapshot,{showDisabled}):mode==='pairs'?buildRelationPresentation(snapshot,{showDisabled}):buildPresentation(snapshot,mode)
}

/** Distance in px along an orthogonal route to the point nearest the centre of a reserved label box. */
export function labelDistance(route:Position[],box:Position&{width:number;height:number}):number {
 const cx=box.x+box.width/2,cy=box.y+box.height/2
 let best=Infinity,at=0,walked=0
 for(let i=1;i<route.length;i++){
  const a=route[i-1]!,b=route[i]!,len=Math.abs(b.x-a.x)+Math.abs(b.y-a.y)
  const t=len?Math.max(0,Math.min(1,((cx-a.x)*(b.x-a.x)+(cy-a.y)*(b.y-a.y))/(len*len))):0
  const px=a.x+(b.x-a.x)*t,py=a.y+(b.y-a.y)*t,d=Math.hypot(px-cx,py-cy)
  if(d<best){best=d;at=walked+len*t}
  walked+=len
 }
 return at
}

/** Every cluster reachable upstream / downstream over enabled links; BFS with a visited set, so cycles terminate and nobody is counted twice. */
export function lineage(p:Presentation,id:string):{upstream:string[];downstream:string[];edgeIds:string[]} {
 const active=p.edges.filter(e=>e.state!=='disabled')
 const walk=(forward:boolean)=>{
  const seen=new Set<string>([id]),queue=[id]
  for(let i=0;i<queue.length;i++){const current=queue[i]!;for(const e of active){const [from,to]=forward?[e.source,e.target]:[e.target,e.source];if(from===current&&!seen.has(to)){seen.add(to);queue.push(to)}}}
  seen.delete(id);return seen
 }
 const up=walk(false),down=walk(true)
 const edgeIds=active.filter(e=>up.has(e.source)&&(up.has(e.target)||e.target===id)||down.has(e.target)&&(down.has(e.source)||e.source===id)).map(e=>e.id)
 return {upstream:[...up].sort(),downstream:[...down].sort(),edgeIds:edgeIds.sort()}
}

const rank:Record<PresentationEdge['state'],number>={verified:0,unverified:1,unknown:2,disabled:3}
/** One edge per ordered cluster pair; parallel MM2 links are merged and labelled ×N. Disabled pairs never merge with active ones. */
export function buildRelationPresentation(snapshot:Snapshot,options:ClusterOptions={}):Presentation {
 const base=buildClusterPresentation(snapshot,{showDisabled:options.showDisabled}),pairs=new Map<string,PresentationEdge>()
 for(const e of base.edges){
  const key=JSON.stringify([e.source,e.target,e.state==='disabled']),hit=pairs.get(key)
  if(hit){hit.linkIds.push(...e.linkIds);hit.stale||=e.stale;if(rank[e.state]>rank[hit.state])hit.state=e.state}
  else pairs.set(key,{id:'pair:'+key,source:e.source,target:e.target,linkIds:[...e.linkIds],state:e.state,stale:e.stale})
 }
 const edges=[...pairs.values()].map(e=>e.linkIds.length>1?{...e,label:`×${e.linkIds.length}`}:e).sort((a,b)=>a.id.localeCompare(b.id))
 const presentation={nodes:base.nodes,edges}
 return options.transform?options.transform(presentation):presentation
}

type EvidenceLink=Pick<Snapshot['links'][number],'declared'|'observed'|'verified'|'state'|'enabled'|'stale'>
/** Evidence source only; health is reported separately by statusText. */
export function evidenceType(link:EvidenceLink):string {return link.observed?'配置关系 · 运行资源已发现':'配置关系'}
export function statusText(link:EvidenceLink,snapshotStale:boolean):string {
 if(snapshotStale||link.stale)return '数据已过期'
 if(isDisabledLink(link))return '已禁用'
 return link.verified?'已验证':'状态未知'
}
