import { flushPromises,mount } from '@vue/test-utils'
import { afterEach,beforeEach,describe,expect,it,vi } from 'vitest'
import Topology from './Topology.vue'
import { routeLocationKey } from 'vue-router'
import type { Snapshot } from '../api/topology'
import { registeredLab, labRegistrations } from '../topology/fixtures'
vi.mock('../components/TopologyCanvas.vue',()=>({default:{name:'TopologyCanvas',props:['snapshot','positions','mode','focusNodeId','focusDirection','selectedLinkIds','showDisabled','groupMeta','endpointMarks','registrations'],emits:['move','select','selectNode','selectLinks'],methods:{fit(){},async arrange(){return {}}},template:'<div data-canvas />'}}))
vi.mock('../composables/useRuntime',async()=>{const {ref}=await import('vue');return {useRuntime:()=>({runtime:ref({mode:'readonly-lab',information_mode:true,workspace_id:'w',capabilities:{discovery:true,observability:false}})})}})
beforeEach(()=>globalThis.localStorage?.clear())
afterEach(()=>{vi.unstubAllGlobals()})
function setup(snap:Snapshot,registry:ReturnType<typeof labRegistrations>,view='relations',catalog:unknown[]=[]){
 const calls:{method:string;url:string;body?:unknown}[]=[]
 vi.stubGlobal('fetch',vi.fn(async(url:string,init?:RequestInit)=>{
  calls.push({method:init?.method??'GET',url,body:init?.body?JSON.parse(init.body as string):undefined})
  const json=(b:unknown,status=200)=>new Response(JSON.stringify(b),{status})
  if(url.includes('/discovery/catalog/'))return json({items:[],total:0,page:1,page_size:20})
  if(url.endsWith('/discovery/catalog'))return json({items:catalog})
  if(url.endsWith('/session'))return json({user_id:'u',mode:'information'})
  if(url.endsWith('/discovery/scopes'))return json({items:[{id:'lab',synthetic:false}]})
  if(url.includes('/kafka/clusters'))return json({items:registry.map(r=>({...r,enabled:true})),total:registry.length})
  if(url.endsWith('/runs/latest'))return json({job_id:'j',status:'done'})
  if(url.endsWith('/runs'))return json({status:'pending',job_id:'j'},202)
  if(url.endsWith('/bindings'))return json({})
  if(url.includes('/snapshot'))return json({snapshot:snap,total:snap.links.length,page:1,page_size:100})
  return json({})
 }))
 const w=mount(Topology,{global:{provide:{[routeLocationKey as symbol]:{query:{view}}},stubs:{'el-tag':{template:'<span><slot /></span>'},RouterLink:{props:['to'],template:'<a :href="to"><slot /></a>'}}}})
 return {w,calls}
}
const settle=async()=>{for(let i=0;i<6;i++)await flushPromises()}
const canvas=(w:ReturnType<typeof mount>)=>w.findComponent({name:'TopologyCanvas'})

const row=(over:Record<string,unknown>)=>({id:'auto:x',source:'auto',aliases:['k9'],members:[],addresses:['luna-kafka-k9.luna-kafka.svc:9092'],resolved:[],access:'credentials_unavailable',read_via:'auto',lineage_id:'endpoint-9',...over})

describe('cluster drawer on the topology page',()=>{
 it('clicking a registered cluster node opens the drawer from the catalog',async()=>{
  const regs=labRegistrations()
  const {w}=setup(registeredLab(),regs,'relations',[row({id:'auto:k1',source:'merged',aliases:['k1'],registration_id:regs[0]!.id,display_name:regs[0]!.display_name,access:'readable',credential_source:'MM2 配置（只读）'})]);await settle()
  canvas(w).vm.$emit('selectNode',regs[0]!.id);await settle()
  const d=w.get('[data-cluster-drawer]').text()
  expect(d).toContain('自动发现 · 已合并登记信息');expect(d).toContain('凭据来源：MM2 配置（只读）');expect(d).toContain('✓ 可读')
  await w.get('[data-close-drawer]').trigger('click');await settle()
  expect(w.find('[data-cluster-drawer]').exists()).toBe(false)
  w.unmount()
 })
 it('an unregistered endpoint node opens the drawer by its MM2 address',async()=>{
  const snap=registeredLab(['k9']);const addr='10.9.0.9:9092';for(const l of snap.links)for(const m of [l.source,l.target])if(m.alias==='k9')m.bootstrap_addresses=[addr]
  const {w}=setup(snap,labRegistrations(['k9']),'relations',[row({addresses:[addr]})]);await settle()
  canvas(w).vm.$emit('selectNode','catalog:auto:x');await settle()
  expect(w.get('[data-cluster-drawer]').text()).toContain('不可读 · 凭据未开启或不可读')
  w.unmount()
 })
 it('several MM2 pointing at one auto cluster draw a single node that opens the drawer',async()=>{
  const snap=registeredLab(['k9']);for(const l of snap.links)for(const m of [l.source,l.target])if(m.alias==='k9')m.bootstrap_addresses=['10.9.0.9:9092']
  const {w}=setup(snap,labRegistrations(['k9']),'relations',[row({id:'auto:k9',addresses:['10.9.0.9:9092']})]);await settle()
  const nodes=(canvas(w).props('snapshot') as {nodes:{id:string}[]}).nodes.map(n=>n.id)
  expect(nodes.filter(id=>id==='catalog:auto:k9').length).toBe(1);expect(nodes.some(id=>id.startsWith('alias:'))).toBe(false)
  canvas(w).vm.$emit('selectNode','catalog:auto:k9');await settle()
  expect(w.get('[data-cluster-drawer]').text()).toContain('k9')
  expect(w.get('.focus-controls strong').text()).toBe('k9')
  w.unmount()
 })
 it('without catalog rows the old behaviour stays',async()=>{
  const regs=labRegistrations()
  const {w}=setup(registeredLab(),regs);await settle()
  canvas(w).vm.$emit('selectNode',regs[0]!.id);await settle()
  expect(w.find('[data-cluster-drawer]').exists()).toBe(false)
  w.unmount()
 })
})
