import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import TopicLineage from './TopicLineage.vue'
vi.mock('../components/TopicLineageCanvas.vue',()=>({default:{name:'TopicLineageCanvas',props:['view','rootId','states'],emits:['selectNode','selectEdge'],template:'<div data-canvas />'}}))

const rows=[
 {id:'auto:a',source:'merged',aliases:['source'],members:[],addresses:['k-a.lab:9092'],resolved:['10.0.0.11:9092'],registration_id:'a',display_name:'Beijing',access:'readable',read_via:'auto',lineage_id:'a'},
 {id:'auto:t',source:'auto',aliases:['target'],members:[],addresses:['k-t.lab:9092'],resolved:['10.0.0.21:9092'],access:'connect_failed',access_reason:'authorization',read_via:'auto',lineage_id:'endpoint-t'},
]
function stub(){
 const calls:string[]=[]
 vi.stubGlobal('fetch',vi.fn(async(url:string)=>{
  calls.push(url)
  const json=(body:unknown,status=200)=>({ok:status<400,status,json:async()=>body})
  if(url.includes('/discovery/catalog/auto%3Aa/topics'))return json({items:[{name:'orders',partitions:3},{name:'payments',partitions:1}],total:2,page:1,page_size:20})
  if(url.endsWith('/discovery/catalog'))return json({items:rows})
  if(url.includes('/topic-lineage?'))return json({root_id:'topic:x',observed_at:'',depth:2,truncated:false,issues:[],nodes:[{id:'topic:x',kind:'topic',label:'orders',cluster_id:'endpoint-t',cluster_name:'target · 10.0.0.21:9092',topic:'orders',exists:null,evidence:'configured',existence_note:'Topic 是否存在无法确认'}],edges:[]})
  if(url.includes('/snapshot'))return json({snapshot:{stale:false,observed_at:'',links:[]}})
  if(url.includes('/discovery/scopes'))return json({items:[{id:'lab',synthetic:false}]})
  if(url.includes('/kafka/clusters'))return json({items:[{id:'a',display_name:'Beijing'}],total:1})
  return json({})
 }))
 return calls
}
async function page(path='/topic-lineage'){const router=createRouter({history:createMemoryHistory(),routes:[{path:'/topic-lineage',component:TopicLineage},{path:'/:p(.*)',component:{template:'<div/>'}}]});await router.push(path);await router.isReady();const wrapper=mount(TopicLineage,{global:{plugins:[router]}});for(let i=0;i<5;i++)await flushPromises();return {wrapper,router}}
beforeEach(()=>globalThis.localStorage?.clear())
afterEach(()=>vi.unstubAllGlobals())

describe('Topic 链路 · 先选别名再选 Topic',()=>{
 it('lists every catalog alias with address and access, keeping unreadable ones',async()=>{
  stub();const {wrapper}=await page()
  const options=wrapper.findAll('select[aria-label="集群别名"] option').map(o=>o.text())
  expect(options).toContain('source · 10.0.0.11:9092 · 可读')
  expect(options).toContain('target · 10.0.0.21:9092 · 不可读（授权不足）')
  expect(wrapper.get('input[aria-label="Topic"]').attributes('disabled')).toBeDefined()
  expect(wrapper.text()).not.toContain('未关联集群')
  wrapper.unmount()
 })
 it('reads topics of a readable alias through the catalog',async()=>{
  const calls=stub();const {wrapper}=await page()
  await wrapper.get('select[aria-label="集群别名"]').setValue('a');for(let i=0;i<4;i++)await flushPromises()
  expect(calls.some(c=>c.includes('/discovery/catalog/auto%3Aa/topics'))).toBe(true)
  await wrapper.get('[data-show-all-topics]').setValue(true)
  expect(wrapper.findAll('#lineage-topics option').map(o=>o.attributes('value'))).toEqual(['orders','payments'])
  wrapper.unmount()
 })
 it('lets an unreadable alias be queried by a typed topic, existence unknown',async()=>{
  const calls=stub();const {wrapper}=await page()
  await wrapper.get('select[aria-label="集群别名"]').setValue('endpoint-t');for(let i=0;i<4;i++)await flushPromises()
  expect(wrapper.text()).toContain('无法枚举 Topic；仅沿配置关系查询，Topic 是否存在无法确认')
  await wrapper.get('input[aria-label="Topic"]').setValue('orders')
  await wrapper.get('form').trigger('submit');for(let i=0;i<6;i++)await flushPromises()
  expect(calls.some(c=>c.includes('/topic-lineage?cluster_id=endpoint-t'))).toBe(true)
  const canvas=wrapper.findComponent({name:'TopicLineageCanvas'});canvas.vm.$emit('selectNode',(canvas.props('view') as {nodes:unknown[]}).nodes[0]);await flushPromises()
  expect(wrapper.get('.lineage-detail').text()).toContain('Topic 是否存在无法确认')
  expect(wrapper.get('.lineage-detail').text()).toContain('target · 10.0.0.21:9092')
  expect(wrapper.get('.lineage-detail a').attributes('href')).toBe('/endpoints/auto%3At/topics/orders')
  wrapper.unmount()
 })
})
