import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import TopicLineage from './TopicLineage.vue'
vi.mock('../components/TopicLineageCanvas.vue',()=>({default:{name:'TopicLineageCanvas',props:['view','rootId','states'],emits:['selectNode','selectEdge'],template:'<div data-canvas />'}}))

const topicNode=(c:string,name:string)=>({id:`topic:${c}`,kind:'topic',label:'orders',cluster_id:c,cluster_name:name,topic:'orders',exists:true,evidence:'observed',observed_at:'2026-10-25T10:00:00Z'})
const graph={root_id:'topic:a',observed_at:'2026-10-25T10:05:00Z',depth:2,truncated:false,issues:[] as {code:string;message:string}[],
 nodes:[topicNode('a','Beijing'),topicNode('b','Shanghai'),{id:'mm2:L1',kind:'mm2',label:'mm2-ab',link_id:'L1',exists:null,evidence:'configured'}],
 edges:[{id:'e1',source:'topic:a',target:'mm2:L1',kind:'replicates_to',evidence:'configured',link_id:'L1',policy:'org.apache.kafka.connect.mirror.IdentityReplicationPolicy',rule:'.*',observed_at:'2026-10-25T09:00:00Z'},{id:'e2',source:'mm2:L1',target:'topic:b',kind:'replicates_to',evidence:'configured',link_id:'L1',observed_at:'2026-10-25T09:00:00Z'}]}
const rootOnly={...graph,nodes:[graph.nodes[0]],edges:[]}
const link={id:'L1',workload_name:'mm2-ab',enabled:true,topics_raw:'.*',topics_exclude_raw:'',source:{registration_id:'a',status:'confirmed'},target:{registration_id:'b',status:'confirmed'},semantics_known:true,stale:false,state:'declared'}
interface Opts {lineage?:unknown;lineageStatus?:number;stale?:boolean;topics?:Record<string,string[]>;links?:unknown[]}
function stub(o:Opts={}){
 const topics=o.topics??{a:['orders','payments'],b:['orders']}
 const fetch=vi.fn(async(url:string)=>{
  const json=(body:unknown,status=200)=>({ok:status<400,status,json:async()=>body})
  if(url.includes('/topic-lineage?'))return json(o.lineageStatus?{error:{code:'NOT_FOUND'}}:(o.lineage??graph),o.lineageStatus??200)
  if(url.includes('/discovery/scopes/readonly-lab/snapshot'))return json({snapshot:{stale:!!o.stale,observed_at:'2026-10-25T09:00:00Z',links:o.links??[link]}})
  if(url.includes('/discovery/scopes'))return json({items:[{id:'readonly-lab',synthetic:false}]})
  const m=url.match(/\/kafka\/clusters\/([^/]+)\/topics\?/);if(m)return json({items:(topics[m[1]!]??[]).map(name=>({name})),total:(topics[m[1]!]??[]).length})
  if(url.includes('/kafka/clusters'))return json({items:[{id:'a',display_name:'Beijing'},{id:'b',display_name:'Shanghai'}],total:2})
  return json({})
 })
 vi.stubGlobal('fetch',fetch);return fetch
}
async function page(path='/topic-lineage?cluster_id=a&topic=orders'){const router=createRouter({history:createMemoryHistory(),routes:[{path:'/topic-lineage',component:TopicLineage},{path:'/:p(.*)',component:{template:'<div/>'}}]});await router.push(path);await router.isReady();const wrapper=mount(TopicLineage,{global:{plugins:[router]}});for(let i=0;i<4;i++)await flushPromises();return {wrapper,router}}
const view=(w:ReturnType<typeof mount>)=>w.findComponent({name:'TopicLineageCanvas'}).props('view') as {nodes:{id:string;kind:string}[];edges:{label?:string}[]}
beforeEach(()=>globalThis.localStorage?.clear())
afterEach(()=>vi.unstubAllGlobals())

describe('Topic 链路 page',()=>{
 it('is named Topic 链路, shows three separate times and MM2 only as edges',async()=>{
  stub();const {wrapper}=await page()
  expect(wrapper.get('h1').text()).toBe('Topic 链路');expect(wrapper.text()).not.toContain('全链路')
  const times=wrapper.get('[data-lineage-times]').text()
  expect(times).toContain('查询时间');expect(times).toContain('配置采集时间');expect(times).toContain('Topic 元数据时间')
  expect(view(wrapper).nodes.every(n=>n.kind==='topic')).toBe(true);expect(view(wrapper).edges[0]!.label).toBe('mm2-ab · Identity')
  expect(wrapper.text()).not.toMatch(/×\d|关系对|就绪副本/)
  wrapper.unmount()
 })
 it('edge side panel starts with evidence type and keeps freshness separate',async()=>{
  stub();const {wrapper}=await page()
  wrapper.findComponent({name:'TopicLineageCanvas'}).vm.$emit('selectEdge',view(wrapper).edges[0]);await flushPromises()
  const facts=wrapper.findAll('[data-edge-facts] dt').map(d=>d.text())
  expect(facts[0]).toBe('证据类型');expect(wrapper.get('[data-edge-evidence]').text()).toBe('配置关系（未验证）')
  expect(wrapper.get('[data-edge-freshness]').text()).toBe('当前')
  expect(wrapper.get('[data-edge-mm2]').attributes('href')).toContain('/topology/readonly-lab/links/L1')
  expect(wrapper.get('[aria-label="链路详情"]').text()).not.toContain('已验证')
  wrapper.unmount()
 })
 it('topic that does not exist says so instead of blaming the connection',async()=>{
  stub({lineageStatus:404,topics:{a:['payments'],b:[]}});const {wrapper}=await page('/topic-lineage?cluster_id=a&topic=ghost')
  const nf=wrapper.get('[data-not-found]').text()
  expect(nf).toContain('该集群未读取到此 Topic');expect(nf).toMatch(/确实没有.*名称不完全一致.*集群连接不可用/s);expect(wrapper.text()).not.toContain('检查集群连接')
  expect(wrapper.find('[data-not-found] [data-reinput]').exists()).toBe(true);expect(wrapper.find('[data-canvas]').exists()).toBe(false)
  wrapper.unmount()
 })
 it('stale snapshot keeps the last configuration graph with a permanent banner',async()=>{
  stub();const first=await page();first.wrapper.unmount()
  stub({lineage:rootOnly,stale:true,links:[{...link,stale:true}]});const {wrapper}=await page()
  expect(wrapper.get('[data-stale-banner]').text()).toContain('数据已过期，不代表没有关系')
  expect(view(wrapper).edges).toHaveLength(1)
  expect(wrapper.text()).not.toMatch(/没有链路|暂未发现/)
  wrapper.unmount()
 })
 it('stale snapshot without history shows 暂无可用配置图',async()=>{
  stub({lineage:rootOnly,stale:true,links:[{...link,stale:true}]});const {wrapper}=await page()
  expect(wrapper.get('[data-stale-banner]').text()).toContain('数据已过期，不代表没有关系')
  expect(wrapper.text()).toContain('暂无可用配置图')
  expect(wrapper.get('[data-reason="untrusted"]').text()).toContain('刷新发现快照')
  wrapper.unmount()
 })
 it('no matching rule is reported with a reason and next steps',async()=>{
  stub({lineage:rootOnly,links:[{...link,topics_raw:'payments'}]});const {wrapper}=await page()
  const r=wrapper.get('[data-reason="no_rule"]');expect(r.text()).toContain('没有 MM2 规则选中此 Topic');expect(r.findAll('a,button').length).toBeGreaterThan(0)
  wrapper.unmount()
 })
 it('changing conditions marks the old result as not updated; switching cluster revalidates the topic',async()=>{
  stub({topics:{a:['orders'],b:['payments']}});const {wrapper}=await page()
  await wrapper.get('[aria-label="深度"]').setValue(3)
  expect(wrapper.get('[data-stale-conditions]').text()).toContain('条件已修改，结果尚未更新')
  await wrapper.get('[aria-label="集群"]').setValue('b');for(let i=0;i<4;i++)await flushPromises()
  expect(wrapper.get('[data-topic-check]').text()).toContain('该 Topic 在所选集群不存在')
  expect(wrapper.get('button[type="submit"]').attributes('disabled')).toBeDefined()
  wrapper.unmount()
 })
 it('consumer groups and applications are hidden until switched on',async()=>{
  const g={...graph,nodes:[...graph.nodes,{id:'cg',kind:'consumer_group',label:'g1',cluster_id:'b'}],edges:[...graph.edges,{id:'ec',source:'topic:b',target:'cg',kind:'consumes',evidence:'observed'}]}
  stub({lineage:g});const {wrapper}=await page()
  expect(view(wrapper).nodes.some(n=>n.id==='cg')).toBe(false)
  await wrapper.get('[data-show-consumers]').setValue(true)
  expect(view(wrapper).nodes.some(n=>n.id==='cg')).toBe(true)
  wrapper.unmount()
 })
 it('sends selected direction and depth (1-4, default 2) while keeping the root',async()=>{
  stub();const {wrapper,router}=await page()
  expect((wrapper.get('[aria-label="深度"]').element as HTMLSelectElement).value).toBe('2')
  await wrapper.get('[aria-label="方向"]').setValue('upstream');await wrapper.get('[aria-label="深度"]').setValue(4);await wrapper.get('form').trigger('submit');await flushPromises()
  expect(router.currentRoute.value.query).toMatchObject({cluster_id:'a',topic:'orders',direction:'upstream',depth:'4'})
  wrapper.unmount()
 })
 it('reads without session login',async()=>{
  const fetch=stub();const {wrapper}=await page()
  expect(fetch.mock.calls.every(([url])=>!String(url).includes('session'))).toBe(true);wrapper.unmount()
 })
 it('上下游 issues two directed queries and shows their union with neutral existence tags',async()=>{
  const up={...graph,root_id:'topic:b',nodes:[{...topicNode('a','Beijing'),exists:false},topicNode('b','Shanghai'),graph.nodes[2]],edges:graph.edges}
  const down={...graph,root_id:'topic:b',nodes:[topicNode('b','Shanghai')],edges:[]}
  const fetch=vi.fn(async(url:string)=>{const json=(body:unknown)=>({ok:true,status:200,json:async()=>body})
   if(url.includes('/topic-lineage?'))return json(url.includes('direction=upstream')?up:down)
   if(url.includes('/discovery/scopes/readonly-lab/snapshot'))return json({snapshot:{stale:false,observed_at:'x',links:[link]}})
   if(url.includes('/discovery/scopes'))return json({items:[{id:'readonly-lab',synthetic:false}]})
   if(url.includes('/topics?'))return json({items:[{name:'orders'}],total:1})
   return json({items:[{id:'a',display_name:'Beijing'},{id:'b',display_name:'Shanghai'}],total:2})})
  vi.stubGlobal('fetch',fetch)
  const {wrapper}=await page('/topic-lineage?cluster_id=b&topic=orders&direction=both')
  const urls=fetch.mock.calls.map(([u])=>String(u)).filter(u=>u.includes('/topic-lineage?'))
  expect(urls.some(u=>u.includes('direction=both'))).toBe(false)
  expect(urls.some(u=>u.includes('direction=upstream'))&&urls.some(u=>u.includes('direction=downstream'))).toBe(true)
  expect(view(wrapper).nodes.map(n=>n.id)).toEqual(['topic:b'])
  const states=wrapper.findComponent({name:'TopicLineageCanvas'}).props('states') as Record<string,{kind:string}>
  expect(states['topic:b']!.kind).toBe('source');expect(wrapper.get('[data-source-summary]').text()).toBe('源头：Shanghai')
  expect(wrapper.text()).not.toMatch(/配置目标|上游（Topic 不存在）/)
  wrapper.unmount()
 })
 it('root topic reported as absent by the lineage draws no nodes',async()=>{
  stub({lineage:{...rootOnly,nodes:[{...topicNode('a','Beijing'),exists:false}]}});const {wrapper}=await page()
  expect(wrapper.get('[data-not-found]').text()).toContain('该集群未读取到此 Topic');expect(wrapper.find('[data-canvas]').exists()).toBe(false)
  wrapper.unmount()
 })
 it('does not list or draw a same-name topic on an unconnected cluster',async()=>{
  stub({lineage:{...graph,nodes:[...graph.nodes,{...topicNode('z','Isolated-Z')}]}});const {wrapper}=await page()
  expect(view(wrapper).nodes.map(n=>n.id).sort()).toEqual(['topic:a','topic:b'])
  expect(wrapper.text()).not.toContain('Isolated-Z');expect(wrapper.find('[data-lineage-node="topic:z"]').exists()).toBe(false)
  wrapper.unmount()
 })
})
