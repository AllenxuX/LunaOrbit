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
  expect(r[0]!.title).toBe('没有 MM2 规则选中此 Topic')
 })
 it('③ excluded by a task exclude rule, with task name and raw rule',()=>{
  const r=classifyLineage({...base,topic:'orders.internal',result:{...rootOnly(),nodes:[T('a','orders.internal')]},links:[rule({topics_exclude_raw:'.*\\.internal'})]})
  expect(r[0]!.kind).toBe('excluded');expect(r[0]!.facts.join(' ')).toContain('mm2-a-b');expect(r[0]!.facts.join(' ')).toContain('.*\\.internal')
  expect(r[0]!.actions.some(a=>a.to?.includes('/links/L1'))).toBe(true)
 })
 it('④ stale snapshot or unparseable rule',()=>{
  expect(classifyLineage({...base,snapshotStale:true,result:rootOnly(),links:[rule({stale:true})]}).map(x=>x.kind)).toContain('untrusted')
  expect(classifyLineage({...base,result:{...rootOnly(),issues:[{code:'FILTER_UNKNOWN',message:'x',link_id:'L1'}]},links:[rule({topics_raw:'(?<x>a)'})]}).map(x=>x.kind)).toContain('untrusted')
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
