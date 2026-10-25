import type { Snapshot } from '../api/topology'
import { createElkLayout } from './elk-layout'
import type { Presentation, PresentationEdge, PresentationNode } from './presentation'
import type { LayoutPlan } from './routing-types'

export interface ClusterOptions {
 showDisabled?:boolean
 /** Extension point for scale rules (grouping/folding); identity by default. */
 transform?:(presentation:Presentation)=>Presentation
}

function edgeState(link:Snapshot['links'][number],stale:boolean):PresentationEdge['state'] {
 if(link.state==='disabled')return 'disabled'
 if(stale||link.state==='unknown')return 'unknown'
 return link.verified?'verified':'unverified'
}

/** Cluster-only view: every Kafka cluster is a node, every MM2 replication link is one labelled edge. */
export function buildClusterPresentation(snapshot:Snapshot,options:ClusterOptions={}):Presentation {
 const kinds=new Map(snapshot.nodes.map(n=>[n.id,n.kind])),byLink=new Map<string,Snapshot['edges']>()
 for(const edge of snapshot.edges){const list=byLink.get(edge.link_id)??[];list.push(edge);byLink.set(edge.link_id,list)}
 const edges:PresentationEdge[]=[]
 for(const link of [...snapshot.links].sort((a,b)=>a.id.localeCompare(b.id))){
  if(link.state==='disabled'&&!options.showDisabled)continue
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
export function labelSize(text:string){return {width:Math.min(18,text.length)*7+16,height:20}}

export function layoutClusters(p:Presentation,workerUrl?:string):Promise<LayoutPlan> {
 return createElkLayout(p,clusterNodeWidth,clusterNodeHeight,workerUrl)
}

export interface LayoutMetrics {crossings:number;overlaps:number;edgeNodeIntersections:number;backwardNonCycleEdges:number;cycleEdges:number}

function reach(p:Presentation):Map<string,Set<string>> {
 const out=new Map<string,string[]>()
 for(const e of p.edges)if(e.state!=='disabled'){const l=out.get(e.source)??[];l.push(e.target);out.set(e.source,l)}
 const result=new Map<string,Set<string>>()
 for(const n of new Set([...p.nodes.map(n=>n.id),...p.edges.flatMap(e=>[e.source,e.target])])){
  const seen=new Set<string>(),stack=[...(out.get(n)??[])]
  while(stack.length){const c=stack.pop()!;if(seen.has(c))continue;seen.add(c);stack.push(...(out.get(c)??[]))}
  result.set(n,seen)
 }
 return result
}

/** Algorithmic quality checks: exact counts, never visual judgement. */
export function layoutMetrics(p:Presentation,plan:LayoutPlan):LayoutMetrics {
 const w=plan.nodeWidth,h=plan.nodeHeight,boxes=Object.values(plan.positions)
 let overlaps=0
 for(let i=0;i<boxes.length;i++)for(let j=i+1;j<boxes.length;j++){const a=boxes[i]!,b=boxes[j]!;if(a.x<b.x+w&&b.x<a.x+w&&a.y<b.y+h&&b.y<a.y+h)overlaps++}
 const segments=new Map(p.edges.map(e=>{const r=plan.routes[e.id]??[];return [e.id,r.slice(1).map((b,i)=>[r[i]!,b] as const)]}))
 let through=0
 for(const segs of segments.values())for(const [a,b] of segs)for(const n of boxes){
  const hit=a.x===b.x?a.x>n.x&&a.x<n.x+w&&Math.max(a.y,b.y)>n.y&&Math.min(a.y,b.y)<n.y+h:a.y>n.y&&a.y<n.y+h&&Math.max(a.x,b.x)>n.x&&Math.min(a.x,b.x)<n.x+w
  if(hit)through++
 }
 let crossings=0
 const ids=[...segments.keys()]
 for(let i=0;i<ids.length;i++)for(let j=i+1;j<ids.length;j++)for(const [a,b] of segments.get(ids[i]!)!)for(const [c,d] of segments.get(ids[j]!)!){
  const [hz,vt]=a.y===b.y&&c.x===d.x?[[a,b],[c,d]]:c.y===d.y&&a.x===b.x?[[c,d],[a,b]]:[undefined,undefined]
  if(!hz||!vt)continue
  const y=hz[0]!.y,x=vt[0]!.x
  if(x>Math.min(hz[0]!.x,hz[1]!.x)&&x<Math.max(hz[0]!.x,hz[1]!.x)&&y>Math.min(vt[0]!.y,vt[1]!.y)&&y<Math.max(vt[0]!.y,vt[1]!.y))crossings++
 }
 const r=reach(p);let backward=0,cycle=0
 for(const e of p.edges){
  if(e.state==='disabled')continue
  const inCycle=e.source===e.target||!!r.get(e.target)?.has(e.source)
  if(inCycle){cycle++;continue}
  const s=plan.positions[e.source],t=plan.positions[e.target]
  if(s&&t&&t.x<=s.x)backward++
 }
 return {crossings,overlaps,edgeNodeIntersections:through,backwardNonCycleEdges:backward,cycleEdges:cycle}
}
