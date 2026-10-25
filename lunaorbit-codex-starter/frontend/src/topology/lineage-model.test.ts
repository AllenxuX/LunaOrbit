import { describe, expect, it } from 'vitest'
import type { Lineage } from '../api/topic-lineage'
import { classifyLineage, collapseLineage, conditionsChanged, matchesRule, pickDisplayGraph, type RuleLink } from './lineage-model'

const T=(c:string,t='orders',exists:boolean|null=true)=>({id:`topic:${c}:${t}`,kind:'topic' as const,label:t,cluster_id:c,cluster_name:'name-'+c,topic:t,exists,evidence:'observed' as const,observed_at:'2026-10-25T10:00:00Z'})
const M=(id:string,label:string)=>({id:`mm2:${id}`,kind:'mm2' as const,label,link_id:id,exists:null,evidence:'configured' as const})
const E=(s:string,t:string,link:string)=>({id:`e:${s}>${t}`,source:s,target:t,kind:'replicates_to',evidence:'configured' as const,link_id:link,policy:'org.apache.kafka.connect.mirror.IdentityReplicationPolicy',rule:'.*',observed_at:'2026-10-25T09:00:00Z'})
function chain(pairs:[string,string,string][],extra:Lineage['nodes']=[]):Lineage {
 const nodes=new Map<string,Lineage['nodes'][number]>();const edges:Lineage['edges']=[]
 for(const [s,t,l] of pairs){for(const c of [s,t])nodes.set(`topic:${c}:orders`,T(c));nodes.set(`mm2:${l}`,M(l,'mm2-'+l));edges.push(E(`topic:${s}:orders`,`mm2:${l}`,l),E(`mm2:${l}`,`topic:${t}:orders`,l))}
 for(const n of extra)nodes.set(n.id,n)
 return {root_id:`topic:${pairs[0]?.[0]??'a'}:orders`,observed_at:'2026-10-25T10:05:00Z',nodes:[...nodes.values()],edges,issues:[],truncated:false,depth:2}
}
const rule=(p:Partial<RuleLink>):RuleLink=>({id:'L1',workload_name:'mm2-a-b',enabled:true,topics_raw:'.*',topics_exclude_raw:'',source:{registration_id:'a',status:'confirmed'},target:{registration_id:'b',status:'confirmed'},semantics_known:true,stale:false,...p})

describe('collapse MM2 into labelled edges',()=>{
 it('one edge per MM2 between two topic nodes, label = task name + policy, same-name topics in different clusters stay separate',()=>{
  const g=collapseLineage(chain([['a','b','L1'],['a','c','L2'],['b','d','L3']]),{showConsumers:false})
  expect(g.nodes.map(n=>n.id).sort()).toEqual(['topic:a:orders','topic:b:orders','topic:c:orders','topic:d:orders'])
  expect(g.edges).toHaveLength(3)
  expect(g.edges.find(e=>e.linkId==='L1')!.label).toBe('mm2-L1 · Identity')
  expect(new Set(g.nodes.map(n=>n.label))).toEqual(new Set(['orders']))
 })
 it('consumer groups and applications only appear when switched on',()=>{
  const data=chain([['a','b','L1']]);data.nodes.push({id:'cg',kind:'consumer_group',label:'g1',cluster_id:'b'});data.edges.push({id:'ec',source:'topic:b:orders',target:'cg',kind:'consumes',evidence:'observed'})
  expect(collapseLineage(data,{showConsumers:false}).nodes.some(n=>n.id==='cg')).toBe(false)
  expect(collapseLineage(data,{showConsumers:true}).nodes.some(n=>n.id==='cg')).toBe(true)
 })
})

describe('rule matching mirrors the backend (comma list, anchored, trimmed)',()=>{
 it('matches and reports unknown syntax',()=>{
  expect(matchesRule('.*','x')).toBe(true)
  expect(matchesRule('.*[\\-\\.]internal, .*\\.replica, __.*','a.replica')).toBe(true)
  expect(matchesRule('orders','orders-2')).toBe(false)
  expect(matchesRule('(?<x>a)','a')).toBeUndefined()
 })
})

describe('six empty / incomplete reasons',()=>{
 const base={cluster:'a',topic:'orders',depth:2,direction:'downstream' as const,snapshotStale:false,configAt:'2026-10-25T09:00:00Z'}
 const rootOnly=():Lineage=>({...chain([]),root_id:'topic:a:orders',nodes:[T('a')]})
 it('① cannot read the topic',()=>{
  const r=classifyLineage({...base,result:{...rootOnly(),issues:[{code:'TOPICS_UNAVAILABLE',message:'x',cluster_id:'a'}]},links:[rule({})]})
  expect(r.map(x=>x.kind)).toContain('unreadable')
 })
 it('② no MM2 rule selects the topic',()=>{
  const r=classifyLineage({...base,result:rootOnly(),links:[rule({topics_raw:'payments'})]})
  expect(r.map(x=>x.kind)).toEqual(['no_rule'])
  expect(r[0]!.title).toBe('未被同步规则选中')
 })
 it('③ excluded by a task exclude rule, with task name and raw rule',()=>{
  const r=classifyLineage({...base,topic:'orders.replica',result:{...rootOnly(),nodes:[T('a','orders.replica')]},links:[rule({topics_exclude_raw:'.*\\.replica'})]})
  expect(r[0]!.kind).toBe('excluded');expect(r[0]!.facts.join(' ')).toContain('mm2-a-b');expect(r[0]!.facts.join(' ')).toContain('.*\\.replica')
  expect(r[0]!.actions.some(a=>a.to?.includes('/links/L1'))).toBe(true)
 })
 it('④ stale snapshot or unparseable rule',()=>{
  expect(classifyLineage({...base,snapshotStale:true,result:rootOnly(),links:[rule({stale:true})]}).map(x=>x.kind)).toContain('untrusted')
  expect(classifyLineage({...base,result:{...rootOnly(),issues:[{code:'FILTER_UNKNOWN',message:'x',link_id:'L1'}]},links:[rule({topics_raw:'(?<x>a)'})]}).map(x=>x.kind)).toContain('unparsed')
 })
 it('⑤ depth or read budget reached',()=>{
  const deep=chain([['a','b','L1'],['b','c','L2']]);deep.depth=2
  const links=[rule({}),rule({id:'L2',source:{registration_id:'b',status:'confirmed'},target:{registration_id:'c',status:'confirmed'}}),rule({id:'L3',source:{registration_id:'c',status:'confirmed'},target:{registration_id:'d',status:'confirmed'}})]
  expect(classifyLineage({...base,result:deep,links}).map(x=>x.kind)).toEqual(['budget'])
  expect(classifyLineage({...base,result:{...rootOnly(),truncated:true,issues:[{code:'READ_BUDGET',message:'x'}]},links:[rule({})]}).map(x=>x.kind)).toContain('budget')
 })
 it('⑥ cluster mapping not confirmed',()=>{
  const r=classifyLineage({...base,result:{...rootOnly(),issues:[{code:'UNRESOLVED_MAPPING',message:'x',link_id:'L1'}]},links:[rule({target:{registration_id:'',status:'candidate'}})]})
  expect(r.map(x=>x.kind)).toContain('unmapped')
 })
 it('a complete graph without boundary has no reason',()=>{
  const g=chain([['a','b','L1']]);g.depth=2
  expect(classifyLineage({...base,result:g,links:[rule({})]})).toEqual([])
 })
 it('every reason has a next-step action',()=>{
  const r=classifyLineage({...base,snapshotStale:true,result:{...rootOnly(),truncated:true,issues:[{code:'TOPICS_UNAVAILABLE',message:'x',cluster_id:'a'},{code:'UNRESOLVED_MAPPING',message:'x',link_id:'L1'}]},links:[rule({stale:true})]})
  expect(r.length).toBeGreaterThanOrEqual(3);expect(r.every(x=>x.actions.length>0)).toBe(true)
 })
})

describe('stale snapshot keeps the last configuration graph',()=>{
 it('returns the cached graph with a stale banner, never an empty "no link" result',()=>{
  const good=chain([['a','b','L1']]),empty={...good,nodes:[good.nodes[0]!],edges:[]}
  const d=pickDisplayGraph({current:empty,cached:good,snapshotStale:true})
  expect(d.graph).toBe(good);expect(d.banner).toBe('数据已过期，不代表没有关系')
  const none=pickDisplayGraph({current:empty,cached:undefined,snapshotStale:true})
  expect(none.graph).toBeUndefined();expect(none.banner).toBe('数据已过期，不代表没有关系');expect(none.notice).toBe('暂无可用配置图')
  expect(pickDisplayGraph({current:good,cached:undefined,snapshotStale:false})).toEqual({graph:good,banner:'',notice:''})
 })
})

describe('conditions vs applied result',()=>{
 it('flags modified conditions',()=>{
  const applied={cluster:'a',topic:'orders',direction:'both' as const,depth:2}
  expect(conditionsChanged(applied,{...applied})).toBe(false)
  expect(conditionsChanged(applied,{...applied,depth:3})).toBe(true)
  expect(conditionsChanged(undefined,applied)).toBe(false)
 })
})

// Backend-shaped trace over the 9-cluster lab. 'both' reproduces the backend's single mixed BFS (the bug source).
const LAB:[string,string][]=[['k1','k2'],['k1','k3'],['k1','k4'],['k2','k5'],['k3','k6'],['k4','k7'],['k5','k8'],['k6','k8'],['k7','k8'],['k8','k9']]
function trace(root:string,direction:'upstream'|'downstream'|'both',depth:number,exists:(c:string)=>boolean,pairs:[string,string][]=LAB):Lineage {
 const nodes=new Map<string,Lineage['nodes'][number]>(),edges:Lineage['edges']=[],seen=new Set([root]),q:[string,number][]=[[root,0]]
 const add=(c:string)=>nodes.set(`topic:${c}`,{id:`topic:${c}`,kind:'topic',label:'t',cluster_id:c,cluster_name:c,topic:'t',exists:exists(c)})
 add(root)
 while(q.length){const [c,d]=q.shift()!;if(d>=depth)continue
  for(const [s,t] of pairs){const fwd=s===c&&direction!=='upstream',back=t===c&&direction!=='downstream';if(!fwd&&!back)continue
   const n=fwd?t:s,id=`${s}>${t}`;add(n);nodes.set(`mm2:${id}`,{id:`mm2:${id}`,kind:'mm2',label:id,link_id:id})
   if(!edges.some(e=>e.id===`a:${id}`))edges.push({id:`a:${id}`,source:`topic:${s}`,target:`mm2:${id}`,kind:'replicates_to',evidence:'configured',link_id:id},{id:`b:${id}`,source:`mm2:${id}`,target:`topic:${t}`,kind:'replicates_to',evidence:'configured',link_id:id})
   if(!seen.has(n)){seen.add(n);q.push([n,d+1])}}}
 return {root_id:`topic:${root}`,observed_at:'',nodes:[...nodes.values()],edges,issues:[],truncated:false,depth}
}
describe('上下游 = directed ancestors ∪ directed descendants',()=>{
 const clustersOf=(g:Lineage)=>g.nodes.filter(n=>n.kind==='topic').map(n=>n.cluster_id).sort()
 const relations=(g:Lineage)=>collapseLineage(g,{showConsumers:false}).edges.length
 it('the backend mixed BFS leaks sibling branches (documents the defect)',()=>{
  expect(clustersOf(trace('k4','both',4,()=>true))).toHaveLength(9)
 })
 it('k4 exclusive topic: k1 k4 k7 k8 k9, 4 relations',async()=>{
  const {mergeDirections}=await import('./lineage-model'),only=new Set(['k4','k7','k8','k9'])
  const g=mergeDirections(trace('k4','upstream',4,c=>only.has(c)),trace('k4','downstream',4,c=>only.has(c)))
  expect(clustersOf(g)).toEqual(['k1','k4','k7','k8','k9']);expect(relations(g)).toBe(4)
 })
 it('k3 exclusive topic: k1 k3 k6 k8 k9',async()=>{
  const {mergeDirections}=await import('./lineage-model')
  expect(clustersOf(mergeDirections(trace('k3','upstream',4,()=>true),trace('k3','downstream',4,()=>true)))).toEqual(['k1','k3','k6','k8','k9'])
 })
 it('shared topic from k4 is also only ancestors + descendants (5), unlike k1 downstream (9)',async()=>{
  const {mergeDirections}=await import('./lineage-model')
  expect(clustersOf(mergeDirections(trace('k4','upstream',4,()=>true),trace('k4','downstream',4,()=>true)))).toEqual(['k1','k4','k7','k8','k9'])
  expect(clustersOf(trace('k1','downstream',4,()=>true))).toHaveLength(9)
 })
 it('cycles terminate in a merged view',async()=>{
  const {mergeDirections}=await import('./lineage-model'),cyc:[string,string][]=[['a','b'],['b','c'],['c','a'],['c','d']]
  expect(clustersOf(mergeDirections(trace('b','upstream',4,()=>true,cyc),trace('b','downstream',4,()=>true,cyc)))).toEqual(['a','b','c','d'])
 })

})

describe('源头 four-state rule (LUNA-28)',()=>{
 const cl=(g:Lineage)=>g.nodes.filter(n=>n.kind==='topic').map(n=>n.cluster_id).sort()
 const rel=(g:Lineage)=>collapseLineage(g,{showConsumers:false}).edges.length
 const src4=(c:string)=>['k4','k7','k8','k9'].includes(c)
 const kinds=(r:{graph:Lineage;states:Record<string,{kind:string}>})=>Object.fromEntries(r.graph.nodes.filter(n=>n.kind==='topic').map(n=>[n.cluster_id,r.states[n.id]!.kind]))
 const load=()=>import('./lineage-model')
 it('k4 luna-src-k4 upstream 4: only k4, marked 源头',async()=>{
  const {resolveSources}=await load(),r=resolveSources({direction:'upstream',up:trace('k4','upstream',4,src4)})
  expect(cl(r.graph)).toEqual(['k4']);expect(kinds(r)).toEqual({k4:'source'});expect(r.summary).toBe('源头：k4')
 })
 it('k4 luna-src-k4 downstream 4: k4 k7 k8 k9, 3 relations, only k4 is 源头',async()=>{
  const {resolveSources}=await load(),r=resolveSources({direction:'downstream',up:trace('k4','upstream',4,src4),down:trace('k4','downstream',4,src4)})
  expect(cl(r.graph)).toEqual(['k4','k7','k8','k9']);expect(rel(r.graph)).toBe(3)
  expect(kinds(r)).toEqual({k4:'source',k7:'exists',k8:'exists',k9:'exists'})
 })
 it('k8 shared topic upstream 3 and 4: 8 nodes, 9 relations, only k1 is 源头',async()=>{
  const {resolveSources}=await load()
  for(const d of [3,4]){const r=resolveSources({direction:'upstream',up:trace('k8','upstream',d,()=>true)})
   expect(cl(r.graph)).toEqual(['k1','k2','k3','k4','k5','k6','k7','k8']);expect(rel(r.graph)).toBe(9)
   expect(Object.entries(kinds(r)).filter(([,k])=>k==='source').map(([c])=>c)).toEqual(['k1']);expect(r.summary).toBe('源头：k1')}
 })
 it('query cluster without the topic draws nothing',async()=>{
  const {resolveSources}=await load(),r=resolveSources({direction:'both',up:trace('k1','upstream',4,src4),down:trace('k1','downstream',4,src4)})
  expect(r.rootMissing).toBe(true);expect(r.graph.nodes).toHaveLength(0)
 })
 it('downstream target not yet present stays as a pending node and is not expanded',async()=>{
  const {resolveSources}=await load(),P:[string,string][]=[['a','b'],['b','c']],ex=(c:string)=>c!=='b'
  const r=resolveSources({direction:'downstream',up:trace('a','upstream',4,ex,P),down:trace('a','downstream',4,ex,P)})
  expect(cl(r.graph)).toEqual(['a','b']);expect(r.states['topic:b']).toEqual({kind:'pending',text:'已配置同步，目标 Topic 尚未出现'});expect(r.summary).toBe('源头：a · 未出现 1 个')
 })
 it('broken chain in the middle: stop at the gap and mark 上游断点',async()=>{
  const {resolveSources}=await load(),P:[string,string][]=[['a','b'],['b','c']],ex=(c:string)=>c!=='b'
  const r=resolveSources({direction:'upstream',up:trace('c','upstream',4,ex,P)})
  expect(cl(r.graph)).toEqual(['c']);expect(r.states['topic:c']).toEqual({kind:'break',text:'上游断点：b 无此 Topic'});expect(r.summary).toBe('源头 0 个 · 断点 1 处')
 })
 it('two independently created sources are both marked and listed',async()=>{
  const {resolveSources}=await load(),P:[string,string][]=[['x','z'],['y','z'],['w','x']],ex=(c:string)=>c!=='w'
  const r=resolveSources({direction:'upstream',up:trace('z','upstream',4,ex,P)})
  expect(cl(r.graph)).toEqual(['x','y','z']);expect(kinds(r)).toEqual({x:'source',y:'source',z:'exists'});expect(r.summary).toBe('源头 2 个：x、y')
 })
 it('上下游 union still yields k4 k7 k8 k9 and no forbidden wording',async()=>{
  const {resolveSources}=await load(),r=resolveSources({direction:'both',up:trace('k4','upstream',4,src4),down:trace('k4','downstream',4,src4)})
  expect(cl(r.graph)).toEqual(['k4','k7','k8','k9'])
  expect(JSON.stringify(r)).not.toMatch(/上游（Topic 不存在）|配置目标/)
 })
})

describe('same-name topic on an unconnected cluster (LUNA-29)',()=>{
 it('is not pulled into the result and gets no relation, even if present in the raw graph',async()=>{
  const {resolveSources}=await import('./lineage-model')
  const iso=(g:Lineage):Lineage=>({...g,nodes:[...g.nodes,{id:'topic:zz',kind:'topic',label:'t',cluster_id:'zz',cluster_name:'zz',topic:'t',exists:true}]})
  const r=resolveSources({direction:'both',up:iso(trace('k4','upstream',4,()=>true)),down:iso(trace('k4','downstream',4,()=>true))})
  expect(r.graph.nodes.some(n=>n.cluster_id==='zz')).toBe(false)
  expect(r.graph.edges.some(e=>e.source==='topic:zz'||e.target==='topic:zz')).toBe(false)
  expect(r.sources).toEqual(['k1']);expect(r.summary).toBe('源头：k1')
 })
})

describe('LUNA-31B candidates and wording',()=>{
 const load=()=>import('./lineage-model')
 const L=(id:string,s:string,t:string,p:Partial<RuleLink>={}):RuleLink=>rule({id,workload_name:`mm2-${s}-${t}`,source:{registration_id:s,status:'confirmed'},target:{registration_id:t,status:'confirmed'},topics_exclude_raw:'.*[\\-\\.]internal, .*\\.replica, __.*',...p})
 const lab=[['k1','k2'],['k1','k3'],['k1','k4'],['k2','k5'],['k3','k6'],['k4','k7'],['k5','k8'],['k6','k8'],['k7','k8'],['k8','k9']].map(([s,t],i)=>L('L'+i,s!,t!))
 it('classifies candidates: syncing, internal, excluded, pending, none',async()=>{
  const {classifyCandidate}=await load()
  expect(classifyCandidate('k1','luna-lab-events',lab).kind).toBe('syncing')
  for(const t of ['heartbeats','mm2-offset-syncs.k2.internal','__consumer_offsets','k1.checkpoints.internal'])expect(classifyCandidate('k1',t,lab).kind).toBe('internal')
  const ex=classifyCandidate('k1','orders.replica',lab);expect(ex.kind).toBe('excluded');expect(ex.tag).toBe('被规则排除')
  expect(classifyCandidate('k1','x',[L('A','k1','k2',{topics_raw:'(?<n>x)'})]).kind).toBe('pending')
  expect(classifyCandidate('k1','x',[L('A','k1','k2',{topics_raw:'payments'})]).kind).toBe('none')
  expect(classifyCandidate('k1','heartbeats',lab).tag).toBe('MM2 内部');expect(classifyCandidate('k1','x',[L('A','k1','k2',{topics_raw:'(?<n>x)'})]).tag).toBe('同步关系待确认')
 })
 it('coverage = downstream clusters reached by the same snapshot rules: k1 luna-lab-events = 8, k4 = 3, unknown = undefined',async()=>{
  const {syncCoverage}=await load()
  expect(syncCoverage('k1','luna-lab-events',lab)).toBe(8);expect(syncCoverage('k4','luna-src-k4',lab)).toBe(3);expect(syncCoverage('k9','luna-lab-events',lab)).toBe(0)
  expect(syncCoverage('k1','x',[L('A','k1','k2',{topics_raw:'(?<n>x)'})])).toBeUndefined()
  expect(syncCoverage('k1','x',[L('A','k1','k2',{policy:'org.apache.kafka.connect.mirror.DefaultReplicationPolicy'} as Partial<RuleLink>)])).toBeUndefined()
 })
 const base={cluster:'a',topic:'orders',depth:2,direction:'both' as const,snapshotStale:false}
 const root=(t='orders'):Lineage=>({...chain([]),root_id:`topic:a:${t}`,nodes:[T('a',t)]})
 it('three fixed sentences; never 配置暂不可信 or 无法解析',async()=>{
  const none=classifyLineage({...base,result:root(),links:[rule({topics_raw:'payments'})]})
  expect(none[0]!.facts).toEqual(['该 Topic 未被任何同步规则选中，因此没有跨集群链路。']);expect(none[0]!.actions[0]).toEqual({label:'去 MM2 拓扑查看各任务的 Topic 规则',to:'/topology?view=mm2'})
  const internal=classifyLineage({...base,topic:'mm2-offset-syncs.b.internal',result:root('mm2-offset-syncs.b.internal'),links:[rule({topics_exclude_raw:'.*[\\-\\.]internal'})]})
  expect(internal.map(r=>r.kind)).toEqual(['internal']);expect(internal[0]!.facts).toEqual(['这是 MM2/Kafka 内部 Topic，不参与业务同步，通常无需追踪。'])
  const ex=classifyLineage({...base,topic:'x.replica',result:root('x.replica'),links:[rule({topics_exclude_raw:'.*\\.replica'})]})
  expect(ex[0]!.facts).toEqual(['该 Topic 被 mm2-a-b 的排除规则排除：.*\\.replica。'])
  expect(JSON.stringify([none,internal,ex])).not.toMatch(/配置暂不可信|无法解析/)
 })
 it('HEARTBEAT_POLICY_UNKNOWN is classified as internal without raw rules',async()=>{
  const r=classifyLineage({...base,topic:'heartbeats',result:{...root('heartbeats'),issues:[{code:'HEARTBEAT_POLICY_UNKNOWN',message:'x',link_id:'L1'},{code:'HEARTBEAT_POLICY_UNKNOWN',message:'x',link_id:'L2'}]},links:[rule({}),rule({id:'L2',workload_name:'mm2-b-c'})]})
  expect(r.map(x=>x.kind)).toEqual(['internal']);expect(JSON.stringify(r)).not.toMatch(/无法解析|\.\*|配置暂不可信/)
 })
 it('unparsed rules: four parts, one per task, at most 3 shown, rest folded',async()=>{
  const ids=['L1','L1','L2','L3','L4','L5'],links=['L1','L2','L3','L4','L5'].map(id=>rule({id,workload_name:'task-'+id,topics_raw:'(?<n>x),orders'}))
  const r=classifyLineage({...base,result:{...root(),issues:ids.map((link_id,i)=>({code:i%2?'SEMANTICS_UNKNOWN':'FILTER_UNKNOWN',message:'x',link_id}))},links})
  const u=r.find(x=>x.kind==='unparsed')!
  expect(u.title).toBe('5 个同步任务的规则无法解析');expect(u.groups).toHaveLength(3);expect(u.more).toBe(2)
  const g=u.groups![0]!;expect(g.task).toBe('task-L1');expect(g.where).toContain('(?<n>x)');expect(g.impact).toBe('结果可能不完整，不代表没有同步。');expect(g.to).toBe('/topology/readonly-lab/links/L1')
  expect(r.filter(x=>x.kind==='unparsed')).toHaveLength(1)
 })
})
