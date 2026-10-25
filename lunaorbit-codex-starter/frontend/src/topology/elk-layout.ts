import ELK, { type ElkNode } from 'elkjs/lib/elk.bundled.js'
import type { Presentation } from './presentation'
import type { LayoutPlan } from './routing-types'

export async function createElkLayout(presentation:Presentation,nodeWidth:number,nodeHeight=76,workerUrl?:string):Promise<LayoutPlan>{
 const plan:LayoutPlan={positions:{},ports:{},routes:{},nodeWidth,nodeHeight}
 if(!presentation.nodes.length)return plan
 const nodes=[...presentation.nodes].sort((a,b)=>a.id<b.id?-1:a.id>b.id?1:0),edges=[...presentation.edges].sort((a,b)=>a.id<b.id?-1:a.id>b.id?1:0)
 const portId=(edge:string,side:'source'|'target')=>JSON.stringify([edge,side])
 const graph:ElkNode={id:'layout-root',layoutOptions:{'elk.algorithm':'layered','elk.direction':'RIGHT','elk.edgeRouting':'ORTHOGONAL','elk.spacing.nodeNode':'50','elk.layered.spacing.nodeNodeBetweenLayers':'70','elk.spacing.edgeNode':'20','elk.layered.spacing.edgeNodeBetweenLayers':'20','elk.spacing.edgeEdge':'16','elk.randomSeed':'1','elk.layered.considerModelOrder.strategy':'NODES_AND_EDGES','elk.layered.feedbackEdges':'true'},children:nodes.map(node=>{
  const outgoing=edges.filter(e=>e.source===node.id),incoming=edges.filter(e=>e.target===node.id)
  return {id:node.id,width:nodeWidth,height:nodeHeight,layoutOptions:{'elk.portConstraints':'FIXED_SIDE'},ports:[...outgoing.map(edge=>{return {id:portId(edge.id,'source'),width:0,height:0,layoutOptions:{'elk.port.side':'EAST'}}}),...incoming.map(edge=>({id:portId(edge.id,'target'),width:0,height:0,layoutOptions:{'elk.port.side':'WEST'}}))]}
 }),edges:edges.map(edge=>({id:edge.id,sources:[portId(edge.id,'source')],targets:[portId(edge.id,'target')],...(edge.label?{labels:[{id:edge.id+':label',text:edge.label,width:Math.min(18,edge.label.length)*7+16,height:20}],layoutOptions:{'elk.edgeLabels.placement':'CENTER'}}:{})}))}
 const engine=new ELK(workerUrl?{workerFactory:()=>new globalThis.Worker(workerUrl)}:{})
 const output=await engine.layout(graph).finally(()=>{if(workerUrl)engine.terminateWorker()})
 // Binary-grid normalization removes ELK floating-point tails while keeping orthogonal segments and endpoints exact.
 const snap=(n:number)=>Math.round(n*1024)/1024
 for(const node of output.children??[])plan.positions[node.id]={x:snap(node.x??0),y:snap(node.y??0)}
 for(const edge of output.edges??[]){const sections=edge.sections??[];if(sections.length!==1)throw new Error('拓扑路径计算失败，请重试');const section=sections[0]!;plan.routes[edge.id]=[section.startPoint,...section.bendPoints??[],section.endPoint].map(p=>({x:snap(p.x),y:snap(p.y)}))
  const label=edge.labels?.[0];if(label){plan.labels??={};plan.labels[edge.id]={x:snap(label.x??0),y:snap(label.y??0),width:label.width??0,height:label.height??0}}}
 for(const edge of edges){const route=plan.routes[edge.id]!,source=plan.positions[edge.source]!,target=plan.positions[edge.target]!;plan.ports[edge.id]={source:{side:'right',offset:route[0]!.y-source.y},target:{side:'left',offset:route.at(-1)!.y-target.y}}}
 return plan
}
