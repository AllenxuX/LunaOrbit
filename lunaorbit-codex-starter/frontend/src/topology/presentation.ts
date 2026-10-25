import type { GraphNode, Link, Position, Snapshot } from '../api/topology'

export type TopologyMode='relations'|'deployment'|'chain'
export interface PresentationEdge {id:string;source:string;target:string;linkIds:string[];state:'verified'|'unverified'|'unknown'|'disabled';stale:boolean;label?:string}
export type PresentationNode=GraphNode&{linkId?:string}
export interface Presentation {nodes:PresentationNode[];edges:PresentationEdge[]}

function state(link:Link|undefined,stale:boolean):PresentationEdge['state'] {
 if(link?.state==='disabled')return 'disabled'
 if(!link||stale||link.state==='unknown')return 'unknown'
 return link.verified?'verified':'unverified'
}

export function buildPresentation(snapshot:Snapshot,mode:TopologyMode):Presentation {
 const links=new Map(snapshot.links.map(link=>[link.id,link]))
 if(mode==='deployment')return {nodes:[...snapshot.nodes],edges:snapshot.edges.map(edge=>{
  const link=links.get(edge.link_id),stale=snapshot.stale||edge.stale||!!link?.stale
  return {id:edge.id,source:edge.source,target:edge.target,linkIds:[edge.link_id],state:state(link,stale),stale}
 })}
 const nodes=new Map(snapshot.nodes.map(node=>[node.id,node])),groups=new Map<string,PresentationEdge>()
 const tasks:PresentationNode[]=[],taskEdges:PresentationEdge[]=[],allocated=new Set(nodes.keys())
 const byLink=new Map<string,Snapshot['edges']>()
 for(const edge of snapshot.edges){const group=byLink.get(edge.link_id)??[];group.push(edge);byLink.set(edge.link_id,group)}
 for(const link of [...snapshot.links].sort((a,b)=>a.id.localeCompare(b.id))){
  const edges=byLink.get(link.id)??[],inputs=edges.filter(e=>e.relation==='declared_input'),outputs=edges.filter(e=>e.relation==='declared_output')
  if(inputs.length!==1||outputs.length!==1)continue
  const input=inputs[0]!,output=outputs[0]!
  if(input.target!==output.source||nodes.get(input.source)?.kind!=='kafka'||nodes.get(input.target)?.kind!=='workload'||nodes.get(output.target)?.kind!=='kafka')continue
  const stale=!!(snapshot.stale||link.stale||input.stale||output.stale),status=state(link,stale)
  if(mode==='chain'){
   let id='task:'+link.id
   while(allocated.has(id))id='task:'+id
   allocated.add(id)
   tasks.push({id,kind:'workload',label:link.workload_name||nodes.get(input.target)!.label,linkId:link.id,replication_factor:null,rf_status:'unknown'})
   taskEdges.push({id:id+':input',source:input.source,target:id,linkIds:[link.id],state:status,stale},{id:id+':output',source:id,target:output.target,linkIds:[link.id],state:status,stale})
   continue
  }
  // Disabled declarations remain independently inspectable and cannot hide an active route.
  const key=JSON.stringify([input.source,output.target,status==='disabled'?'disabled':'active']),existing=groups.get(key)
  if(existing){
   existing.linkIds.push(link.id);existing.stale ||= stale
   if(existing.state==='unknown'||status==='unknown')existing.state='unknown'
   else if(existing.state==='unverified'||status==='unverified')existing.state='unverified'
  }else groups.set(key,{id:'relation:'+key,source:input.source,target:output.target,linkIds:[link.id],state:status,stale})
 }
 if(mode==='chain'){const referenced=new Set(taskEdges.flatMap(e=>[e.source,e.target]));return {nodes:[...snapshot.nodes.filter(n=>n.kind==='kafka'&&referenced.has(n.id)),...tasks],edges:taskEdges}}
 const edges=[...groups.values()].sort((a,b)=>a.id.localeCompare(b.id)),referenced=new Set(edges.flatMap(edge=>[edge.source,edge.target]))
 return {nodes:snapshot.nodes.filter(node=>node.kind==='kafka'&&referenced.has(node.id)).sort((a,b)=>a.id.localeCompare(b.id)),edges}
}

// Condense strongly connected components before assigning longest-path layers.
// Iterative traversal prevents stack overflow on large or cyclic snapshots.
function ranks(ids:string[],edges:PresentationEdge[]):Map<string,number> {
 const forward=new Map(ids.map(id=>[id,new Set<string>()])),reverse=new Map(ids.map(id=>[id,new Set<string>()]))
 for(const edge of edges)if(forward.has(edge.source)&&forward.has(edge.target)){forward.get(edge.source)!.add(edge.target);reverse.get(edge.target)!.add(edge.source)}
 const seen=new Set<string>(),order:string[]=[]
 for(const id of ids){
  if(seen.has(id))continue
  const stack:[string,boolean][]=[[id,false]]
  while(stack.length){const [current,done]=stack.pop()!;if(done){order.push(current);continue}if(seen.has(current))continue;seen.add(current);stack.push([current,true]);for(const next of [...forward.get(current)!].sort().reverse())if(!seen.has(next))stack.push([next,false])}
 }
 const component=new Map<string,number>();let count=0
 for(const id of order.reverse()){
  if(component.has(id))continue
  component.set(id,count);const stack=[id]
  while(stack.length){const current=stack.pop()!;for(const next of reverse.get(current)!)if(!component.has(next)){component.set(next,count);stack.push(next)}}
  count++
 }
 const outgoing=Array.from({length:count},()=>new Set<number>()),indegree=Array<number>(count).fill(0),level=Array<number>(count).fill(0)
 for(const edge of edges){const from=component.get(edge.source),to=component.get(edge.target);if(from!==undefined&&to!==undefined&&from!==to&&!outgoing[from]!.has(to)){outgoing[from]!.add(to);indegree[to]=indegree[to]!+1}}
 const queue=indegree.flatMap((n,i)=>n===0?[i]:[])
 for(let i=0;i<queue.length;i++){const from=queue[i]!;for(const to of outgoing[from]!){level[to]=Math.max(level[to]!,level[from]!+1);indegree[to]=indegree[to]!-1;if(indegree[to]===0)queue.push(to)}}
 return new Map(ids.map(id=>[id,level[component.get(id)!]!]))
}

export function presentationPositions(presentation:Presentation):Record<string,Position> {
 const nodes=[...presentation.nodes].sort((a,b)=>a.label.localeCompare(b.label)||a.id.localeCompare(b.id)),ids=nodes.map(node=>node.id)
 const edges=presentation.edges.filter(edge=>edge.state!=='disabled'),levels=ranks(ids,edges),layers=new Map<number,string[]>()
 for(const id of ids){const level=levels.get(id)??0,layer=layers.get(level)??[];layer.push(id);layers.set(level,layer)}
 const ordered=[...layers.keys()].sort((a,b)=>a-b),index=new Map<string,number>()
 const update=()=>{for(const layer of layers.values())layer.forEach((id,i)=>index.set(id,i))};update()
 // Barycentric sweeps reduce crossings without changing the actual relationships.
 for(let pass=0;pass<4;pass++)for(const downstream of [true,false]){
  for(const level of downstream?ordered:[...ordered].reverse()){
   const layer=layers.get(level)!,scores=new Map<string,number>()
   for(const id of layer){const neighbors=edges.filter(e=>downstream?e.target===id&&(levels.get(e.source)??0)<level:e.source===id&&(levels.get(e.target)??0)>level).map(e=>index.get(downstream?e.source:e.target)!).filter(n=>n!==undefined);scores.set(id,neighbors.length?neighbors.reduce((sum,n)=>sum+n,0)/neighbors.length:index.get(id)!)}
   layer.sort((a,b)=>scores.get(a)!-scores.get(b)!||index.get(a)!-index.get(b)!)
   update()
  }
 }
 const positions:Record<string,Position>={}
 for(const [level,layer] of layers)layer.forEach((id,i)=>positions[id]={x:40+level*(nodes.some(n=>n.linkId)?230:320),y:60+i*140})
 return positions
}

export function focusPresentation(presentation:Presentation,nodeId:string,direction:'neighbors'|'upstream'|'downstream'):{nodeIds:string[];edgeIds:string[]} {
 const allowed=new Set(presentation.nodes.map(node=>node.id))
 if(!allowed.has(nodeId))return {nodeIds:[],edgeIds:[]}
 const nodeIds=new Set([nodeId]),edgeIds=new Set<string>(),queue=[nodeId]
 for(let i=0;i<queue.length;i++){
  const id=queue[i]!
  for(const edge of presentation.edges){
   const next=direction==='upstream'?(edge.target===id?edge.source:undefined):direction==='downstream'?(edge.source===id?edge.target:undefined):edge.source===id?edge.target:edge.target===id?edge.source:undefined
   if(next===undefined||!allowed.has(next))continue
   edgeIds.add(edge.id)
   if(!nodeIds.has(next)){nodeIds.add(next);if(direction!=='neighbors'||presentation.nodes.find(n=>n.id===next)?.linkId)queue.push(next)}
  }
 }
 return {nodeIds:[...nodeIds].sort(),edgeIds:[...edgeIds].sort()}
}


export interface ConfigurationPath {key:string;nodeIds:string[];linkIds:string[]}
export function tracePaths(presentation:Presentation,source:string,target:string,limit=12,maxHops=8):{paths:ConfigurationPath[];truncated:boolean}{
 const paths:ConfigurationPath[]=[],nodes=new Map(presentation.nodes.map(n=>[n.id,n]))
 if(source===target||nodes.get(source)?.kind!=='kafka'||nodes.get(target)?.kind!=='kafka')return {paths,truncated:false}
 const outgoing=new Map<string,PresentationEdge[]>()
 for(const edge of presentation.edges)if(edge.state!=='disabled'){const list=outgoing.get(edge.source)??[];list.push(edge);outgoing.set(edge.source,list)}
 const queue=[{nodeIds:[source],linkIds:[] as string[]}];let truncated=false
 search:for(let i=0;i<queue.length;i++){
  const current=queue[i]!,last=current.nodeIds.at(-1)!
  for(const edge of outgoing.get(last)??[]){
   if(current.nodeIds.includes(edge.target))continue
   if(current.linkIds.length>=maxHops&&!edge.linkIds.every(id=>current.linkIds.includes(id))){truncated=true;continue}
   const linkIds=[...new Set([...current.linkIds,...edge.linkIds])],nodeIds=[...current.nodeIds,edge.target]
   if(edge.target===target){paths.push({key:JSON.stringify(linkIds),nodeIds,linkIds});if(paths.length>limit){truncated=true;break search}}
   else{if(queue.length>=4096){truncated=true;break search}queue.push({nodeIds,linkIds})}
  }
 }
 return {paths:paths.slice(0,limit),truncated}
}
