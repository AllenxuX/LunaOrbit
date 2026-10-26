import { flushPromises,mount } from '@vue/test-utils'
import { afterEach,beforeEach,describe,expect,it,vi } from 'vitest'
import Topology from './Topology.vue'
import { routeLocationKey } from 'vue-router'
import type { Snapshot } from '../api/topology'
import { registeredLab, labRegistrations } from '../topology/fixtures'
import { manualReviewKey } from '../topology/unregistered'
vi.mock('../components/TopologyCanvas.vue',()=>({default:{name:'TopologyCanvas',props:['snapshot','positions','mode','focusNodeId','focusDirection','selectedLinkIds','showDisabled','groupMeta','endpointMarks','registrations'],emits:['move','select','selectNode','selectLinks'],methods:{fit(){},async arrange(){return {}}},template:'<div data-canvas />'}}))
vi.mock('../composables/useRuntime',async()=>{const {ref}=await import('vue');return {useRuntime:()=>({runtime:ref({mode:'readonly-lab',information_mode:true,workspace_id:'w',capabilities:{discovery:true,observability:false}})})}})
beforeEach(()=>globalThis.localStorage?.clear())
afterEach(()=>{vi.unstubAllGlobals()})
function setup(snap:Snapshot,registry:ReturnType<typeof labRegistrations>,view='relations'){
 const calls:{method:string;url:string;body?:unknown}[]=[]
 vi.stubGlobal('fetch',vi.fn(async(url:string,init?:RequestInit)=>{
  calls.push({method:init?.method??'GET',url,body:init?.body?JSON.parse(init.body as string):undefined})
  const json=(b:unknown,status=200)=>new Response(JSON.stringify(b),{status})
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
const targetCard=(w:ReturnType<typeof mount>)=>w.findAll('.link-options .mapping-card')[1]!

describe('unregistered MM2 endpoints on the topology page',()=>{
 it('counts registered clusters and unregistered endpoints separately and keeps a non-dismissable banner',async()=>{
  const {w}=setup(registeredLab(['k9']),labRegistrations(['k9']));await settle()
  expect(w.text()).toContain('8 个已登记集群 · 1 个自动发现端点 · 10 对同步关系')
  const banner=w.get('[data-unregistered-banner]')
  expect(banner.text()).toContain('1 条链路的对端 k9 为自动发现端点，尚未合并登记信息；点击节点查看访问状态')
  expect(banner.find('a').exists()).toBe(false)
  expect(banner.text()).not.toMatch(/关闭|×/)
  expect(canvas(w).props('endpointMarks')).toEqual({'alias:k9':'自动发现'})
  w.unmount()
 })
 it('MM2 view uses the same split count',async()=>{
  const {w}=setup(registeredLab(['k9']),labRegistrations(['k9']),'mm2');await settle()
  expect(w.text()).toContain('8 个已登记集群 · 1 个自动发现端点 · 10 条 MM2 任务')
  w.unmount()
 })
 it('with every cluster registered the page is unchanged: no banner, no badges, old count',async()=>{
  const {w}=setup(registeredLab(),labRegistrations());await settle()
  expect(w.text()).toContain('9 个集群 · 10 对同步关系');expect(w.text()).not.toContain('已登记集群')
  expect(w.find('[data-unregistered-banner]').exists()).toBe(false);expect(canvas(w).props('endpointMarks')).toEqual({})
  w.unmount()
 })
 it('clicking an unregistered node explains the match result, the reason and three next steps',async()=>{
  const {w}=setup(registeredLab(['k9']),labRegistrations(['k9']));await settle()
  canvas(w).vm.$emit('selectNode','alias:k9');await settle()
  const panel=w.get('[data-endpoint-panel]').text()
  for(const t of ['k9','自动发现','MM2 里的地址','没找到 Cluster ID 匹配','地址无候选','为什么没匹配','从已登记集群中手工指定','查看该任务配置','配置关系（未验证）'])expect(panel).toContain(t)
  expect(panel).not.toContain('登记为新集群')
  w.unmount()
 })
 it('empty candidates: dropdown says so, offers register and a folded manual assignment that needs explicit confirmation',async()=>{
  const {w,calls}=setup(registeredLab(['k9']),labRegistrations(['k9']),'mm2');await settle()
  canvas(w).vm.$emit('select','L10');await settle()
  const card=targetCard(w)
  expect(card.text()).toContain('没有匹配的已登记集群');expect(card.text()).not.toContain('去登记')
  expect(card.find('select:not([data-manual-select])').exists()).toBe(false)
  const manual=card.get('[data-manual-assign]');expect(manual.attributes('open')).toBeUndefined()
  expect(manual.findAll('[data-manual-select] option').length).toBe(9)
  await manual.get('[data-manual-select]').setValue('k8')
  const submit=manual.get('[data-manual-submit]');expect(submit.attributes('disabled')).toBeDefined()
  await manual.get('[data-manual-confirm]').setValue(true);expect(submit.attributes('disabled')).toBeUndefined()
  await manual.get('[data-manual-confirm]').setValue(false);expect(submit.attributes('disabled')).toBeDefined()
  await manual.get('[data-manual-confirm]').setValue(true);await submit.trigger('click');await settle()
  expect(calls.filter(c=>c.method==='PUT'&&c.url.endsWith('/bindings')).map(c=>c.body)).toEqual([{namespace:'luna-kafka',workload_uid:'uid-L10',alias:'k9',registration_id:'k8'}])
  expect(globalThis.localStorage.getItem('lunaorbit.manualBindingReview')).toContain(manualReviewKey('lab','uid-L10','k9'))
  w.unmount()
 })
 it('several address candidates: only the candidates are offered, plus the folded manual list',async()=>{
  const {w}=setup(registeredLab(['k9'],['k5']),labRegistrations(['k9']),'mm2');await settle()
  canvas(w).vm.$emit('select','L07');await settle()
  const card=w.findAll('.link-options .mapping-card')[0]!
  expect(card.text()).toContain('地址命中 2 个候选')
  expect(card.findAll('select:not([data-manual-select]) option').map(o=>o.text()).filter(t=>t.startsWith('luna-'))).toEqual(['luna-k1 · lab · lab','luna-k2 · lab · lab'])
  expect(card.find('[data-manual-assign]').exists()).toBe(true)
  w.unmount()
 })
 it('manual assignment shows both addresses and asks again when they do not overlap',async()=>{
  const snap=registeredLab(['k9']);const l=snap.links.find(x=>x.id==='L10')!;l.target.bootstrap_addresses=['k9-kafka.luna-kafka.svc:9092']
  const {w,calls}=setup(snap,labRegistrations(['k9']),'mm2');await settle()
  canvas(w).vm.$emit('select','L10');await settle()
  const manual=targetCard(w).get('[data-manual-assign]')
  await manual.get('[data-manual-select]').setValue('k8')
  expect(manual.text()).toContain('MM2 里的地址');expect(manual.text()).toContain('k9-kafka.luna-kafka.svc:9092');expect(manual.text()).toContain('127.0.0.1:19099')
  expect(manual.get('[data-address-mismatch]').text()).toBe('地址不一致，可能绑错集群')
  await manual.get('[data-manual-confirm]').setValue(true)
  const ask=vi.fn(()=>false);vi.stubGlobal('confirm',ask)
  await manual.get('[data-manual-submit]').trigger('click');await settle()
  expect(ask).toHaveBeenCalledOnce();expect(calls.some(c=>c.method==='PUT'&&c.url.endsWith('/bindings'))).toBe(false)
  w.unmount()
 })
 it('registered endpoints keep the existing binding dropdown',async()=>{
  const {w}=setup(registeredLab(['k9']),labRegistrations(['k9']),'mm2');await settle()
  canvas(w).vm.$emit('select','L10');await settle()
  const card=w.findAll('.link-options .mapping-card')[0]!
  expect(card.find('[data-manual-assign]').exists()).toBe(false);expect(card.findAll('select option').length).toBe(9)
  w.unmount()
 })
})
