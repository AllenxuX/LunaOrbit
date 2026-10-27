import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, expect, it, vi } from 'vitest'
import TopicDetails from './TopicDetails.vue'
const meta={synthetic:true,write_enabled:true,permissions:{topic_manage:true}}
const markers={synthetic:true,observed_at:'2026-10-21T00:00:00Z',stale:false,capabilities:[]}
const topic={name:'events',internal:false,partitions:[{partition:0,leader:1,replicas:[1,2],isr:[1]}],rf_min:2,rf_max:2}
const json=(value:unknown)=>new Response(JSON.stringify(value))
function mockFetch(){vi.stubGlobal('fetch',vi.fn(async(input:string)=>{
  const url=String(input)
  if(url.endsWith('/session'))return json({user_id:'operator'})
  if(url.includes('/workspace/meta'))return json(meta)
  if(url.includes('/topics/'))return json({item:topic,...markers})
  return json({display_name:'实验集群'})
}))}
const stubs={ResourceConfigs:{template:'<div data-config-editor />',props:['resource']},TopicLifecycle:true,ResourceOperations:{template:'<div data-topic-operations />',props:['resource']},WorkspacePlanDialog:true}
async function setup(tab='partitions'){
  const router=createRouter({history:createMemoryHistory(),routes:[{path:'/:pathMatch(.*)*',component:{template:'<div />'}}]})
  await router.push(`/kafka/clusters/one/topics/events?tab=${tab}`)
  const wrapper=mount(TopicDetails,{props:{id:'one',topicName:'events'},global:{plugins:[router],stubs}})
  await flushPromises()
  return {wrapper,router}
}
afterEach(()=>vi.unstubAllGlobals())
it('opens a dedicated topic page with real partition state and context navigation',async()=>{
  mockFetch();const {wrapper}=await setup()
  expect(wrapper.get('h1').text()).toBe('events')
  expect(wrapper.get('.topic-breadcrumb').text()).toContain('实验集群')
  expect(wrapper.get('tbody').text()).toContain('副本未同步完整')
  expect(wrapper.find('[data-config-editor]').exists()).toBe(false)
  expect(wrapper.find('[data-topic-operations]').exists()).toBe(false)
  wrapper.unmount()
})
it('supports configuration deep links, resource-scoped records and browser history',async()=>{
  mockFetch();const {wrapper,router}=await setup('configs')
  expect(wrapper.findComponent(stubs.ResourceConfigs).props('resource')).toEqual({type:'TOPIC',name:'events'})
  await wrapper.findAll('.topic-tabs button')[3]!.trigger('click');await flushPromises()
  expect(router.currentRoute.value.query.tab).toBe('operations')
  expect(wrapper.findComponent(stubs.ResourceOperations).props('resource')).toEqual({type:'TOPIC',name:'events'})
  router.back();await flushPromises();await new Promise(resolve=>setTimeout(resolve,0));await flushPromises()
  expect(wrapper.find('[data-config-editor]').exists()).toBe(true)
  wrapper.unmount()
})
it('clears the previous Topic while another Topic loads and ignores late responses',async()=>{
  mockFetch();const {wrapper}=await setup()
  let complete!:(r:Response)=>void
  vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).endsWith('/session')?json({user_id:'operator'}):String(input).includes('/topics/late')?new Promise<Response>(resolve=>complete=resolve):String(input).includes('/topics/current')?json({item:{...topic,name:'current'},...markers}):String(input).includes('/meta')?json(meta):json({display_name:'新集群'})))
  await wrapper.setProps({topicName:'late'});await flushPromises()
  expect(wrapper.find('tbody').exists()).toBe(false)
  await wrapper.setProps({topicName:'current'});await flushPromises()
  complete(json({item:{...topic,name:'late-private'},...markers}));await flushPromises()
  expect(wrapper.text()).not.toContain('late-private')
  expect(wrapper.get('h1').text()).toBe('current')
  wrapper.unmount()
})
it('refreshes partitions after expansion while keeping the execution result dialog open',async()=>{
  mockFetch();const {wrapper}=await setup()
  await wrapper.get('[data-expand-topic]').trigger('click')
  const dialog=wrapper.findComponent({name:'WorkspacePlanDialog'})
  expect(dialog.props('input')).toMatchObject({kind:'topic_expand',topic:'events',partitions:2})
  vi.stubGlobal('fetch',vi.fn(async()=>json({item:{...topic,partitions:[...topic.partitions,{...topic.partitions[0],partition:1}]},...markers})))
  dialog.vm.$emit('completed');await flushPromises()
  expect(wrapper.findAll('tbody tr')).toHaveLength(2)
  expect(dialog.props('input')).toMatchObject({kind:'topic_expand'})
  wrapper.unmount()
})
it('links partition leaders, replicas and ISR to the matching broker detail page',async()=>{
 mockFetch();const{wrapper}=await setup()
 expect(wrapper.get('tbody tr').findAll('a').map(a=>a.attributes('href'))).toEqual(['/kafka/clusters/one/brokers/1','/kafka/clusters/one/brokers/1','/kafka/clusters/one/brokers/2','/kafka/clusters/one/brokers/1'])
 wrapper.unmount()
})
it('does not create broker links for unknown negative IDs or empty replica lists',async()=>{
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).endsWith('/session')?json({user_id:'operator'}):String(input).includes('/workspace/meta')?json(meta):String(input).includes('/topics/')?json({item:{...topic,partitions:[{partition:0,leader:-1,replicas:[-1,2],isr:[]}]},...markers}):json({display_name:'实验集群'})))
 const{wrapper}=await setup()
 expect(wrapper.get('tbody tr').findAll('a').map(a=>a.attributes('href'))).toEqual(['/kafka/clusters/one/brokers/2'])
 const cells=wrapper.get('tbody tr').findAll('td')
 expect(cells[1]!.text()).toBe('未知');expect(cells[2]!.text()).toBe('未知, 2');expect(cells[3]!.text()).toBe('未知')
 wrapper.unmount()
})
it('shows ISR/retention/cleanup on the main layer and keeps partitions when the config sub-query fails',async()=>{
  const e=(name:string,value:string|null,sensitive=false)=>({name,value,source:'DYNAMIC_TOPIC_CONFIG',sensitive,read_only:false,type:'',documentation:'',default_value:null,synonyms:[]})
  let failConfigs=false
  vi.stubGlobal('fetch',vi.fn(async(input:string)=>{const url=String(input)
    if(url.endsWith('/session'))return json({user_id:'operator'})
    if(url.includes('/workspace/meta'))return json(meta)
    if(url.includes('/resource-configs'))return failConfigs?new Response(JSON.stringify({error:{code:'TIMEOUT',message:'查询超时',request_id:'r'}}),{status:503}):json({resource:{type:'TOPIC',name:'events'},entries:[e('retention.ms','604800000'),e('cleanup.policy','delete'),e('min.insync.replicas','2')]})
    if(url.includes('/topics/'))return json({item:topic,...markers})
    return json({display_name:'实验集群'})}))
  let {wrapper}=await setup()
  expect(wrapper.get('[data-min-isr]').text()).toBe('2 个副本');expect(wrapper.get('[data-retention]').text()).toBe('7 天');expect(wrapper.get('[data-cleanup]').text()).toBe('按保留规则删除')
  expect(wrapper.get('thead').text()).toContain('ISR Broker IDs')
  wrapper.unmount();failConfigs=true;({wrapper}=await setup())
  expect(wrapper.get('[data-retention]').text()).toBe('未读取');expect(wrapper.find('[data-config-partial]').exists()).toBe(true)
  expect(wrapper.get('tbody').text()).toContain('0')
})
