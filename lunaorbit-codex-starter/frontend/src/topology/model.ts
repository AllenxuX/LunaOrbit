import type { Filters, GraphEdge, GraphNode, Layers, Link, Mapping, Position, Snapshot } from '../api/topology'
export interface Location {id:string;room:string;environment:string}
export const unknown='__unknown__'
function endpointMatches(mapping:Mapping,locations:Location[],filters:Filters) {
 const row=mapping.status==='confirmed' ? locations.find(c=>c.id===mapping.registration_id) : undefined
 const room=row?.room || unknown, env=row?.environment || unknown
 return (!filters.room||filters.room===room)&&(!filters.environment||filters.environment===env)
}
export function filterGraph(snapshot:Snapshot,locations:Location[],filters:Filters,layers:Layers,includeDisabled=false):Snapshot {
 const links=snapshot.links.filter(l=>(includeDisabled||l.state!=='disabled')&&(endpointMatches(l.source,locations,filters)||endpointMatches(l.target,locations,filters)))
 const ids=new Set(links.map(l=>l.id))
 const related=snapshot.edges.filter(e=>ids.has(e.link_id));const referenced=new Set(related.flatMap(e=>[e.source,e.target]))
 const nodes=snapshot.nodes.filter(n=>referenced.has(n.id)&&(n.kind==='kafka'?layers.kafka:layers.workloads))
 const visible=new Set(nodes.map(n=>n.id));const edges=related.filter(e=>visible.has(e.source)&&visible.has(e.target))
 return {...snapshot,links,nodes,edges}
}
export function defaultPositions(nodes:GraphNode[],edges:GraphEdge[]=[],links:Pick<Link,'id'|'state'>[]=[]):Record<string,Position> {
 // Disabled declarations remain visible evidence, but do not determine flow direction.
 const disabled=new Set(links.filter(link=>link.state==='disabled').map(link=>link.id))
 const layoutEdges=edges.filter(edge=>!disabled.has(edge.link_id))
 const positions:Record<string,Position>={}
 const ordered=[...nodes].sort((a,b)=>(a.label??a.id).localeCompare(b.label??b.id)||a.id.localeCompare(b.id))
 const kafka=ordered.filter(n=>n.kind==='kafka'),kafkaIDs=new Set(kafka.map(n=>n.id))
 const adjacency=new Map(kafka.map(n=>[n.id,new Set<string>()]))
 const byLink=new Map<string,GraphEdge[]>()
 for(const edge of layoutEdges){const group=byLink.get(edge.link_id)??[];group.push(edge);byLink.set(edge.link_id,group)}
 // A workload may serve many connectors: derive Kafka flows per link rather
 // than interpreting the shared process as a Kafka-to-Kafka cycle.
 for(const group of byLink.values())for(const input of group){
  if(!kafkaIDs.has(input.source))continue
  if(kafkaIDs.has(input.target))adjacency.get(input.source)!.add(input.target)
  else for(const output of group)if(output.source===input.target&&kafkaIDs.has(output.target))adjacency.get(input.source)!.add(output.target)
 }
 const ranks=kafkaRanks(adjacency)
 const place=(id:string,x:number,y:number)=>{
  while(Object.values(positions).some(p=>Math.abs(p.x-x)<280&&Math.abs(p.y-y)<100))y+=150
  positions[id]={x,y}
 }
 kafka.forEach((n,i)=>place(n.id,40+(layoutEdges.length?(ranks.get(n.id)??0)*680:(i%2)*680),layoutEdges.length?60:60+Math.floor(i/2)*150))
 for(const n of ordered.filter(n=>n.kind==='workload')){
  const associated=[...new Set(layoutEdges.filter(e=>e.source===n.id||e.target===n.id).map(e=>e.source===n.id?e.target:e.source))].map(id=>positions[id]).filter((p):p is Position=>!!p)
  place(n.id,associated.length?associated.reduce((sum,p)=>sum+p.x,0)/associated.length:380,associated.length?associated.reduce((sum,p)=>sum+p.y,0)/associated.length:60)
 }
 return positions
}

// Collapse strongly connected Kafka components before assigning longest-path
// ranks. Iterative walks remain bounded for cyclic and large discovery graphs.
function kafkaRanks(adjacency:Map<string,Set<string>>):Map<string,number>{
 const reverse=new Map([...adjacency.keys()].map(id=>[id,new Set<string>()]))
 for(const [from,targets] of adjacency)for(const to of targets)reverse.get(to)!.add(from)
 const visited=new Set<string>(),order:string[]=[]
 for(const id of adjacency.keys()){
  if(visited.has(id))continue
  const stack:[string,boolean][]=[[id,false]]
  while(stack.length){const [node,finished]=stack.pop()!;if(finished){order.push(node);continue}if(visited.has(node))continue;visited.add(node);stack.push([node,true]);for(const next of adjacency.get(node)!)if(!visited.has(next))stack.push([next,false])}
 }
 const component=new Map<string,number>();let count=0
 for(const id of order.reverse()){
  if(component.has(id))continue
  const stack=[id];component.set(id,count)
  while(stack.length){const node=stack.pop()!;for(const next of reverse.get(node)!)if(!component.has(next)){component.set(next,count);stack.push(next)}}
  count++
 }
 const outgoing=Array.from({length:count},()=>new Set<number>()),indegree=Array<number>(count).fill(0),ranks=Array<number>(count).fill(0)
 for(const [from,targets] of adjacency)for(const to of targets){const a=component.get(from)!,b=component.get(to)!;if(a!==b&&!outgoing[a]!.has(b)){outgoing[a]!.add(b);indegree[b] = indegree[b]!+1}}
 const queue=indegree.flatMap((degree,id)=>degree===0?[id]:[])
 for(let i=0;i<queue.length;i++){const id=queue[i]!;for(const next of outgoing[id]!){ranks[next]=Math.max(ranks[next]!,ranks[id]!+1);indegree[next]=indegree[next]!-1;if(indegree[next]===0)queue.push(next)}}
 return new Map([...component].map(([id,index])=>[id,ranks[index]!]))
}
export function linkStatus(link:Link) {return link.stale?'数据已过期 · 状态未知':link.state==='disabled'?'同步已禁用':link.state==='unknown'?'状态未知 · 链路未验证':link.verified?'链路已验证':'链路未验证'}
export function mappingLabel(mapping:Mapping) {
 if(mapping.status==='confirmed') return mapping.method==='manual'?'人工确认映射':'已确认映射'
 return mapping.status==='candidate'?'地址候选 · 待确认':mapping.status==='ambiguous'?'映射歧义 · 待确认':'映射未知 · 待确认'
}


export interface RegistrationIdentity { id:string; display_name:string; bootstrap_servers:string[]; discovered_cluster_id?:string }
export function mappingRegistrations<T extends RegistrationIdentity>(mapping:Mapping,rows:T[]):T[] {
 const ids=mapping.status==='confirmed'?[mapping.registration_id]:mapping.candidate_ids??[]
 return rows.filter(row=>ids.includes(row.id))
}
export function mappingName(mapping:Mapping,rows:RegistrationIdentity[]):string {
 const matches=mappingRegistrations(mapping,rows)
 if(matches.length===1)return matches[0]!.display_name+(mapping.status==='confirmed'?'':'（候选，待确认）')
 return matches.length>1?`${mapping.alias}（多个候选）`:mapping.alias
}
export function nodeIdentity(snapshot:Snapshot,nodeId:string,rows:RegistrationIdentity[]):string {
 const node=snapshot.nodes.find(n=>n.id===nodeId)
 if(node?.kind!=='kafka')return node?.label??nodeId
 const mappings=snapshot.edges.filter(e=>e.source===nodeId||e.target===nodeId).flatMap(edge=>{
  const link=snapshot.links.find(l=>l.id===edge.link_id)
  return link?[edge.source===nodeId?link.source:link.target]:[]
 })
 const labels=mappings.filter(mapping=>mappingRegistrations(mapping,rows).length>0).map(mapping=>mappingName(mapping,rows))
 return labels.length?[...new Set(labels)].join('\n'):node?.label??'Kafka'
}
