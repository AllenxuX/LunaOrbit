import { createElkLayout } from './elk-layout'
import type { Presentation } from './presentation'
export * from './cluster-presentation'
import type { LayoutPlan } from './routing-types'

import { clusterNodeHeight, clusterNodeWidth } from './cluster-presentation'
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
