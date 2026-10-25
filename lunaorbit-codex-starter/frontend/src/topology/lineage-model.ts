import type { Direction, Lineage, LineageEdge, LineageNode } from '../api/topic-lineage'

export interface RuleLink {
 id:string;workload_name:string;enabled:boolean;topics_raw:string;topics_exclude_raw:string
 source:{registration_id?:string;status:string};target:{registration_id?:string;status:string}
 semantics_known:boolean;stale:boolean
}
export interface LineageView {nodes:LineageNode[];edges:(LineageEdge&{label?:string;linkId?:string;mm2Id?:string})[]}
export interface Reason {kind:'unreadable'|'no_rule'|'excluded'|'untrusted'|'budget'|'unmapped';title:string;facts:string[];actions:{label:string;to?:string;action?:'refresh'|'deeper'|'retry'}[]}
export interface Conditions {cluster:string;topic:string;direction:Direction;depth:number}

const policyName=(p?:string)=>p?.endsWith('IdentityReplicationPolicy')?'Identity':p?.endsWith('DefaultReplicationPolicy')?'Default':p?p.split('.').at(-1)!:'策略未知'

/** MM2 nodes become labelled edges between topic nodes; consumer groups/applications are optional. */
export function collapseLineage(data:Pick<Lineage,'nodes'|'edges'>,options:{showConsumers:boolean}):LineageView {
 const byId=new Map(data.nodes.map(n=>[n.id,n])),edges:LineageView['edges']=[]
 const inbound=new Map<string,LineageEdge[]>(),outbound=new Map<string,LineageEdge[]>()
 for(const e of data.edges){if(byId.get(e.target)?.kind==='mm2')(inbound.get(e.target)??inbound.set(e.target,[]).get(e.target)!).push(e);if(byId.get(e.source)?.kind==='mm2')(outbound.get(e.source)??outbound.set(e.source,[]).get(e.source)!).push(e)}
 for(const node of data.nodes)if(node.kind==='mm2')for(const i of inbound.get(node.id)??[])for(const o of outbound.get(node.id)??[]){
  const policy=i.policy??o.policy
  edges.push({...i,id:`${i.id}|${o.id}`,source:i.source,target:o.target,label:`${node.label} · ${policyName(policy)}`,linkId:node.link_id??i.link_id,mm2Id:node.id,policy,rule:i.rule??o.rule})
 }
 const extra=new Set(['consumer_group','application'])
 const nodes=data.nodes.filter(n=>n.kind!=='mm2'&&(options.showConsumers||!extra.has(n.kind)))
 const kept=new Set(nodes.map(n=>n.id))
 for(const e of data.edges)if(byId.get(e.source)?.kind!=='mm2'&&byId.get(e.target)?.kind!=='mm2'&&kept.has(e.source)&&kept.has(e.target))edges.push(e)
 return {nodes,edges}
}

/** Same semantics as backend matches(): comma separated, trimmed, each anchored; undefined when the syntax is not portable. */
export function matchesRule(raw:string,topic:string):boolean|undefined {
 let hit=false
 for(const part of raw.split(',')){
  const p=part.trim()
  if(/\(\?[<P'!=]/.test(p))return undefined
  try{if(new RegExp(`^(?:${p})$`).test(topic))hit=true}catch{return undefined}
 }
 return hit
}

const BUDGET=new Set(['READ_BUDGET','TRAVERSAL_TRUNCATED','DEADLINE_REACHED','TRUNCATED','LINKS_TRUNCATED','CLUSTERS_TRUNCATED'])
const UNTRUSTED=new Set(['SEMANTICS_UNKNOWN','FILTER_UNKNOWN','POLICY_UNKNOWN','CONFLICTING_DECLARATIONS','DISCOVERY_UNAVAILABLE','HEARTBEAT_POLICY_UNKNOWN','TARGET_NAME_UNKNOWN'])
const linkPath=(id:string)=>`/topology/readonly-lab/links/${encodeURIComponent(id)}`

/** Hop distance (cross-cluster) from the root over MM2 edges, both directions. */
function hops(view:LineageView,root:string):Map<string,number> {
 const d=new Map([[root,0]]),q=[root]
 for(let i=0;i<q.length;i++){const c=q[i]!;for(const e of view.edges){if(!e.mm2Id)continue;const n=e.source===c?e.target:e.target===c?e.source:undefined;if(n&&!d.has(n)){d.set(n,d.get(c)!+1);q.push(n)}}}
 return d
}

export function classifyLineage(input:Conditions&{result:Lineage;links:RuleLink[];snapshotStale:boolean;configAt?:string}):Reason[] {
 const {result,links,cluster,topic,direction}=input,reasons:Reason[]=[],codes=new Set(result.issues.map(i=>i.code))
 const view=collapseLineage(result,{showConsumers:false}),replication=view.edges.filter(e=>e.mm2Id)
 if(codes.has('TOPICS_UNAVAILABLE'))reasons.push({kind:'unreadable',title:'无法读取 Topic',facts:[`无法读取所选集群的 Topic「${topic}」，当前不能确认是否存在同步关系。`],actions:[{label:'重试读取',action:'retry'},{label:'查看数据源连接状态',to:`/kafka/clusters/${encodeURIComponent(cluster)}/details`}]})
 const stale=input.snapshotStale||links.some(l=>l.stale)
 const untrustedIssues=result.issues.filter(i=>UNTRUSTED.has(i.code))
 if(stale||untrustedIssues.length){
  const facts=[...(stale?['数据已过期，不代表没有关系；配置采集时间之后的变化未反映。']:[]),...untrustedIssues.filter(i=>i.code!=='SEMANTICS_UNKNOWN'||!stale).map(i=>{const l=links.find(x=>x.id===i.link_id);return l?`MM2 ${l.workload_name} 的规则暂无法解析：${l.topics_raw}${l.topics_exclude_raw?'；排除 '+l.topics_exclude_raw:''}`:i.message})]
  reasons.push({kind:'untrusted',title:stale?'配置暂不可信：快照过期':'配置暂不可信：规则无法解析',facts:[...new Set(facts)],actions:[{label:'刷新发现快照',action:'refresh'},{label:'查看规则与任务',to:'/topology?view=tasks'}]})
 }
 const unmapped=result.issues.filter(i=>i.code==='UNRESOLVED_MAPPING')
 if(unmapped.length)reasons.push({kind:'unmapped',title:'集群映射未确认',facts:[...new Set(unmapped.map(i=>{const l=links.find(x=>x.id===i.link_id);return l?`MM2 ${l.workload_name} 的集群别名尚未确认对应哪个 Kafka 集群，已保留已知路径。`:i.message}))],actions:[{label:'查看集群映射',to:'/topology?view=mm2'},...unmapped.flatMap(i=>i.link_id?[{label:'查看 MM2 配置',to:linkPath(i.link_id)}]:[]).slice(0,1)]})
 // Depth: a boundary node at the maximum hop that still has a matching enabled rule onwards.
 const d=hops(view,result.root_id),boundary=view.nodes.filter(n=>n.kind==='topic'&&d.get(n.id)===result.depth)
 const onward=boundary.some(n=>links.some(l=>l.enabled&&((direction!=='upstream'&&l.source.registration_id===n.cluster_id)||(direction!=='downstream'&&l.target.registration_id===n.cluster_id))&&matchesRule(l.topics_raw,n.topic??n.label)===true&&!view.edges.some(e=>e.linkId===l.id&&(e.source===n.id||e.target===n.id))))
 if(result.truncated||result.issues.some(i=>BUDGET.has(i.code))||onward)reasons.push({kind:'budget',title:onward&&!result.truncated?'已达到深度上限':'已达到读取预算',facts:[onward?`当前查询深度 ${result.depth}，已展示至第 ${result.depth} 跳；更深关系尚未查询，不代表不存在。`:'已达到查询读取预算或时限，部分关系未展开。'],actions:[...(result.depth<4?[{label:'增加一层并重新查询',action:'deeper' as const}]:[]),{label:'以边界节点为新起点',action:'retry'}]})
 if(!replication.length&&!reasons.some(r=>r.kind==='unreadable'||r.kind==='untrusted'||r.kind==='unmapped')){
  const relevant=links.filter(l=>l.enabled&&((direction!=='upstream'&&l.source.registration_id===cluster)||(direction!=='downstream'&&l.target.registration_id===cluster)))
  const excluded=relevant.filter(l=>matchesRule(l.topics_raw,topic)===true&&!!l.topics_exclude_raw&&matchesRule(l.topics_exclude_raw,topic)===true)
  if(excluded.length)reasons.push({kind:'excluded',title:'Topic 被排除规则排除',facts:excluded.map(l=>`被 MM2 ${l.workload_name} 的排除规则命中：${l.topics_exclude_raw}`),actions:excluded.map(l=>({label:`查看 ${l.workload_name}`,to:linkPath(l.id)}))})
  else if(!relevant.some(l=>matchesRule(l.topics_raw,topic)===true))reasons.push({kind:'no_rule',title:'没有 MM2 规则选中此 Topic',facts:[`当前配置中没有启用的 MM2 包含规则匹配「${topic}」；这是配置结论，不代表没有真实数据流。`],actions:[{label:'查看候选 MM2 与包含规则',to:'/topology?view=tasks'},{label:'调整 Topic 或方向',action:'retry'}]})
 }
 return reasons
}

/** Keep the last configuration graph when the snapshot is stale; never present stale emptiness as "no link". */
export function pickDisplayGraph(input:{current?:Lineage;cached?:Lineage;snapshotStale:boolean}):{graph?:Lineage;banner:string;notice:string} {
 if(!input.snapshotStale)return {graph:input.current,banner:'',notice:''}
 const graph=input.cached??(input.current&&input.current.edges.length?input.current:undefined)
 return {graph,banner:'数据已过期，不代表没有关系',notice:graph?'':'暂无可用配置图'}
}

export function conditionsChanged(applied:Conditions|undefined,form:Conditions):boolean {
 if(!applied)return false
 return applied.cluster!==form.cluster||applied.topic!==form.topic.trim()||applied.direction!==form.direction||applied.depth!==form.depth
}

export function cacheKey(c:Conditions){return 'lunaorbit.lineage.'+JSON.stringify([c.cluster,c.topic,c.direction,c.depth])}
