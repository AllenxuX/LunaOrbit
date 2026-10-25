import { flushPromises,mount } from '@vue/test-utils'
import { afterEach,describe,expect,it,vi } from 'vitest'
import Topology from './Topology.vue'
import { routeLocationKey } from 'vue-router'
import type { ScopeInfo, Snapshot } from '../api/topology'
import { scopeSnapshot } from '../topology/fixtures'
vi.mock('../components/TopologyCanvas.vue',()=>({default:{name:'TopologyCanvas',props:['snapshot','positions','mode','focusNodeId','focusDirection','selectedLinkIds','showDisabled','groupMeta'],emits:['move','select','selectNode','selectLinks'],methods:{fit(){},async arrange(){return {}}},template:'<div data-canvas />'}}))
vi.mock('../composables/useRuntime',async()=>{const {ref}=await import('vue');return {useRuntime:()=>({runtime:ref({mode:'readonly-lab',information_mode:true,workspace_id:'w',capabilities:{discovery:true,observability:false}})})}})
afterEach(()=>{vi.unstubAllGlobals()})
const old='2026-10-25T08:00:00Z'
const A=()=>scopeSnapshot('k8s-a','edge-a',[['A1','luna','mm2','k1','k2']])
const B=()=>scopeSnapshot('k8s-b','edge-b',[['B1','luna','mm2','k3','k4']])
const S=(id:string,c:string,p:Partial<ScopeInfo>={}):ScopeInfo=>({id,synthetic:false,k8s_cluster:c,auto:true,...p})
const registry=[{id:'k1',display_name:'luna-k1',room:'bj',environment:'prod'},{id:'k2',display_name:'luna-k2',room:'bj',environment:'prod'},{id:'k3',display_name:'luna-k3',room:'sh',environment:'prod'},{id:'k4',display_name:'luna-k4',room:'sh',environment:'prod'}]
function setup(scopes:ScopeInfo[],snaps:Record<string,Snapshot|undefined>){
 const calls:string[]=[]
 vi.stubGlobal('fetch',vi.fn(async(url:string,init?:RequestInit)=>{
  calls.push(`${init?.method??'GET'} ${url}`)
  const json=(b:unknown,status=200)=>new Response(JSON.stringify(b),{status})
  if(url.endsWith('/session'))return json({user_id:'u',mode:'information'})
  if(url.endsWith('/discovery/scopes'))return json({items:scopes})
  if(url.includes('/kafka/clusters'))return json({items:registry,total:registry.length})
  const m=url.match(/scopes\/([^/]+)\/snapshot/);if(m){const s=snaps[decodeURIComponent(m[1]!)];return s?json({snapshot:s,total:s.links.length,page:1,page_size:100}):json({error:{code:'NOT_FOUND'}},404)}
  return json({})
 }))
 const w=mount(Topology,{global:{provide:{[routeLocationKey as symbol]:{query:{view:'mm2'}}},stubs:{'el-tag':{template:'<span><slot /></span>'},RouterLink:{template:'<a><slot /></a>'}}}})
 return {w,calls}
}
const canvasLinks=(w:ReturnType<typeof mount>)=>(w.findComponent({name:'TopologyCanvas'}).props('snapshot') as Snapshot).links
const settle=async()=>{for(let i=0;i<6;i++)await flushPromises()}

describe('topology with auto-discovered K8s scopes',()=>{
 it('merges scopes, shows the scope bar and keeps same-named MM2 distinguishable',async()=>{
  const {w}=setup([S('k8s-a','edge-a'),S('k8s-b','edge-b')],{'k8s-a':A(),'k8s-b':B()});await settle()
  const bar=w.get('[data-scope-bar]').text()
  expect(bar).toContain('扫描 2 个 K8s 集群 / 6 个命名空间，其中 2 个含 MM2')
  expect(bar).toContain('Kite 只列出当前账号有权限的集群');expect(bar).toContain('未识别到 MM2 不等于没有 MM2')
  expect(canvasLinks(w).map(l=>l.workload_name)).toEqual(['edge-a / luna / mm2','edge-b / luna / mm2'])
  const groups=w.findAll('[data-scope-group]');expect(groups.map(g=>g.attributes('data-scope-group'))).toEqual(['edge-a','edge-b'])
  expect(groups.every(g=>!(g.element as HTMLDetailsElement).open)).toBe(true)
  w.unmount()
 })
 it('a failing scope is reported alone and the others stay',async()=>{
  const b={...B(),stale:true,observed_at:old,issues:[{code:'TIMEOUT'}]}
  const {w,calls}=setup([S('k8s-a','edge-a'),S('k8s-b','edge-b')],{'k8s-a':A(),'k8s-b':b});await settle()
  const f=w.get('[data-scope-failure="k8s-b"]').text();expect(f).toContain('edge-b');expect(f).toContain('采集超时');expect(f).toContain('数据已过期，不代表没有关系')
  expect(canvasLinks(w).map(l=>l.id)).toEqual(['A1','B1'])
  await w.get('[data-scope-failure="k8s-b"] button').trigger('click');await settle()
  expect(calls.some(c=>c==='POST /api/v1/discovery/scopes/k8s-b/runs')).toBe(true)
  expect(calls.some(c=>c==='POST /api/v1/discovery/scopes/k8s-a/runs')).toBe(false)
  w.unmount()
 })
 it('announces a new cluster with a dismissible hint',async()=>{
  const {w}=setup([S('k8s-a','edge-a'),S('k8s-b','edge-b'),S('k8s-c','edge-c')],{'k8s-a':A(),'k8s-b':B()});await settle()
  expect(w.get('[data-scope-new]').text()).toContain('edge-c')
  await w.get('[data-scope-new] button').trigger('click');expect(w.find('[data-scope-new]').exists()).toBe(false)
  w.unmount()
 })
 it('removed scopes are hidden until 包含已移除范围 is switched on',async()=>{
  const {w}=setup([S('k8s-a','edge-a'),S('k8s-b','edge-b',{removed:true,removed_at:old})],{'k8s-a':A(),'k8s-b':B()});await settle()
  expect(canvasLinks(w).map(l=>l.id)).toEqual(['A1']);expect(w.find('[data-scope-removed]').exists()).toBe(false)
  await w.get('[data-include-removed]').setValue(true);await settle()
  expect(canvasLinks(w).map(l=>l.id)).toEqual(['A1','B1']);expect(w.get('[data-scope-removed="k8s-b"]').text()).toContain('已移除')
  w.unmount()
 })
 it('suspected MM2 are listed under 待确认 and never drawn',async()=>{
  const a=A();a.workloads!.push({k8s_cluster:'edge-a',namespace:'legacy',name:'replicator',workload_uid:'x',status:'suspected',signals:['image:example/mirrormaker:1']})
  const {w}=setup([S('k8s-a','edge-a')],{'k8s-a':a});await settle()
  expect(w.get('[data-suspected]').text()).toContain('待确认（1）');expect(w.get('[data-suspected]').text()).toContain('edge-a / legacy / replicator')
  expect(canvasLinks(w).some(l=>l.workload_name.includes('replicator'))).toBe(false)
  w.unmount()
 })
 it('a single cluster with MM2 in one namespace looks exactly like before',async()=>{
  const {w}=setup([S('k8s-a','edge-a')],{'k8s-a':A()});await settle()
  expect(w.find('[data-scope-bar]').exists()).toBe(false);expect(w.find('[data-scope-group]').exists()).toBe(false)
  expect(canvasLinks(w).map(l=>l.workload_name)).toEqual(['mm2'])
  w.unmount()
 })
 it('K8s cluster filter combines with the room filter',async()=>{
  const {w}=setup([S('k8s-a','edge-a'),S('k8s-b','edge-b')],{'k8s-a':A(),'k8s-b':B()});await settle()
  await w.get('[data-scope-cluster]').setValue('edge-b');expect(canvasLinks(w).map(l=>l.id)).toEqual(['B1'])
  await w.get('[aria-label="机房筛选"]').setValue('bj');expect(canvasLinks(w)).toEqual([])
  await w.get('[data-scope-cluster]').setValue('');expect(canvasLinks(w).map(l=>l.id)).toEqual(['A1'])
  w.unmount()
 })
})
