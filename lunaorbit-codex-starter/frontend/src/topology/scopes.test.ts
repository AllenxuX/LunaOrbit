import { describe, expect, it } from 'vitest'
import type { ScopeEntry, ScopeInfo, Snapshot } from '../api/topology'
import { filterGraph } from './model'
import { filterByScope, mergeEntries, needsScopeBar, scopeSummary, withScopeNames } from './scopes'

const at='2026-10-25T12:00:00Z',old='2026-10-25T08:00:00Z'
function snap(scope:string,cluster:string,rows:[string,string,string,string,string?][],extra:Partial<Snapshot>={}):Snapshot {
 const nodes=new Map<string,Snapshot['nodes'][number]>(),links:Snapshot['links']=[],edges:Snapshot['edges']=[]
 for(const [id,ns,name,s,t='kx'] of rows){
  for(const k of [s,t])nodes.set('kafka:'+k,{id:'kafka:'+k,kind:'kafka',label:k,registration_id:k,replication_factor:null,rf_status:'unknown'})
  const w=`workload:${scope}:${ns}:${name}`;nodes.set(w,{id:w,kind:'workload',label:name,replication_factor:null,rf_status:'not_applicable'})
  const m=(r:string)=>({alias:r,status:'confirmed',method:'manual',confidence:'manual',registration_id:r,candidate_ids:[r]})
  links.push({id,namespace:ns,workload_uid:'uid-'+id,workload_name:name,source_alias:s,target_alias:t,topics_raw:'.*',policy:'',enabled:true,confidence:'manual',declared:true,observed:true,verified:false,stale:false,state:'declared',observed_at:at,source:m(s),target:m(t),evidence:[],notes:[],runtime:{generation:1,observed_generation:1,replicas:1,ready_replicas:1,pod_candidates:0,pod_association:'unknown'},k8s_cluster:cluster} as Snapshot['links'][number])
  edges.push({id:id+':in',source:'kafka:'+s,target:w,link_id:id,relation:'declared_input',stale:false,evidence:[]},{id:id+':out',source:w,target:'kafka:'+t,link_id:id,relation:'declared_output',stale:false,evidence:[]})
 }
 const nsWith=new Set(rows.map(r=>r[1]))
 return {scope_id:scope,synthetic:false,observed_at:at,attempted_at:at,stale:false,nodes:[...nodes.values()],edges,links,issues:[],
  workloads:rows.map(r=>({k8s_cluster:cluster,namespace:r[1],name:r[2],workload_uid:'uid-'+r[0],status:'confirmed',signals:['command:connect-mirror-maker.sh']})),
  scan:{k8s_cluster:cluster,namespaces:3,deployments:9,namespaces_with_mm2:nsWith.size,confirmed:rows.length,suspected:0},...extra}
}
const S=(id:string,cluster:string,p:Partial<ScopeInfo>={}):ScopeInfo=>({id,synthetic:false,k8s_cluster:cluster,auto:true,...p})
const two=():ScopeEntry[]=>[{scope:S('k8s-a','edge-a'),snapshot:snap('k8s-a','edge-a',[['A1','luna','mm2','k1','k2']])},{scope:S('k8s-b','edge-b'),snapshot:snap('k8s-b','edge-b',[['B1','luna','mm2','k3','k4']])}]

describe('multi K8s scopes',()=>{
 it('same-named mm2 in two clusters stay distinct and carry their scope',()=>{
  const m=mergeEntries(two(),{includeRemoved:false})
  expect(m.links.map(l=>[l.id,l.scope_id,l.k8s_cluster])).toEqual([['A1','k8s-a','edge-a'],['B1','k8s-b','edge-b']])
  const named=withScopeNames(m,true)
  expect(named.links.map(l=>l.workload_name)).toEqual(['edge-a / luna / mm2','edge-b / luna / mm2'])
  expect(new Set(named.nodes.filter(n=>n.kind==='workload').map(n=>n.label)).size).toBe(2)
 })
 it('a failed scope is isolated: others stay current, failure keeps history with its own time',()=>{
  const e=two();e[1]!.snapshot={...e[1]!.snapshot!,stale:true,observed_at:old,issues:[{code:'TIMEOUT'}]}
  const s=scopeSummary(e)
  expect(s.failures).toEqual([{scopeId:'k8s-b',cluster:'edge-b',reason:'采集超时',lastSuccess:old,hasHistory:true}])
  expect(s.partial).toBe(true)
  const m=mergeEntries(e,{includeRemoved:false})
  expect(m.links.find(l=>l.id==='A1')!.stale).toBe(false);expect(m.links.find(l=>l.id==='B1')).toBeDefined()
 })
 it('a newly listed cluster without snapshot is announced, not counted as 0',()=>{
  const e=[...two(),{scope:S('k8s-c','edge-c'),error:'NO_SNAPSHOT'}]
  const s=scopeSummary(e)
  expect(s.fresh).toEqual(['edge-c']);expect(s.clusters).toBe(3);expect(s.namespaces).toBe(6)
 })
 it('removed scopes are hidden by default and shown with the switch',()=>{
  const e=two();e[1]!.scope={...e[1]!.scope,removed:true,removed_at:at}
  expect(mergeEntries(e,{includeRemoved:false}).links.map(l=>l.id)).toEqual(['A1'])
  expect(mergeEntries(e,{includeRemoved:true}).links.map(l=>l.id)).toEqual(['A1','B1'])
  const s=scopeSummary(e)
  expect(s.clusters).toBe(1);expect(s.removed).toEqual([{scopeId:'k8s-b',cluster:'edge-b',removedAt:at,lastSuccess:at}])
 })
 it('suspected MM2 are grouped outside the graph and not counted',()=>{
  const e=two();e[0]!.snapshot!.workloads!.push({k8s_cluster:'edge-a',namespace:'legacy',name:'replicator',workload_uid:'x',status:'suspected',signals:['image:example/mirrormaker:1']});e[0]!.snapshot!.scan!.suspected=1
  const s=scopeSummary(e)
  expect(s.suspected.map(w=>`${w.k8s_cluster}/${w.namespace}/${w.name}`)).toEqual(['edge-a/legacy/replicator'])
  expect(s.withMM2).toBe(2);expect(mergeEntries(e,{includeRemoved:false}).links.some(l=>l.workload_name==='replicator')).toBe(false)
 })
 it('one cluster with MM2 in one namespace and nothing unusual hides the bar and prefixes',()=>{
  const one=[two()[0]!]
  expect(needsScopeBar(one)).toBe(false)
  expect(needsScopeBar(two())).toBe(true)
  const multiNs=[{scope:S('k8s-a','edge-a'),snapshot:snap('k8s-a','edge-a',[['A1','luna','mm2','k1','k2'],['A2','ops','mm2','k2','k3']])}]
  expect(needsScopeBar(multiNs)).toBe(true)
  const legacy=[{scope:{id:'readonly-lab',synthetic:false},snapshot:{...snap('readonly-lab','',[['L1','luna','mm2','k1','k2']]),workloads:undefined,scan:undefined}}]
  expect(needsScopeBar(legacy)).toBe(false)
 })
 it('K8s / namespace filters combine with room and environment filters',()=>{
  const m=mergeEntries(two(),{includeRemoved:false})
  expect(filterByScope(m,'edge-b','').links.map(l=>l.id)).toEqual(['B1'])
  expect(filterByScope(m,'edge-a','ops').links).toEqual([])
  const rooms=[{id:'k1',room:'bj',environment:'prod'},{id:'k2',room:'bj',environment:'prod'},{id:'k3',room:'sh',environment:'prod'},{id:'k4',room:'sh',environment:'prod'}]
  const both=filterGraph(filterByScope(m,'edge-a',''),rooms as never,{room:'sh',environment:''},{kafka:true,workloads:true})
  expect(both.links).toEqual([])
  expect(filterGraph(filterByScope(m,'',''),rooms as never,{room:'sh',environment:''},{kafka:true,workloads:true}).links.map(l=>l.id)).toEqual(['B1'])
 })
})
