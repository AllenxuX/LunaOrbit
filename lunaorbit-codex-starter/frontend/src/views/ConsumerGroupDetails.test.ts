import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, expect, it, vi } from 'vitest'
import ConsumerGroupDetails from './ConsumerGroupDetails.vue'
const json=(value:unknown,status=200)=>new Response(JSON.stringify(value),{status})
const markers={synthetic:true,observed_at:'2026-10-21',stale:false,state:'verified',capabilities:[]}
function mockFetch(){vi.stubGlobal('fetch',vi.fn(async(input:string)=>{
 const url=String(input)
 if(url.endsWith('/session'))return json({user_id:'operator'})
 if(url.includes('/offsets'))return json({items:[{topic:'events',partition:0,committed:null,log_end:12,lag:null}],total:1,...markers})
 if(url.includes('/groups?'))return json({items:[{id:'consumer',state:'Stable',protocol_type:'consumer'}],total:1,...markers})
 return json({display_name:'实验集群'})
}))}
async function setup(){
 const router=createRouter({history:createMemoryHistory(),routes:[{path:'/:pathMatch(.*)*',component:{template:'<div />'}}]})
 await router.push('/kafka/clusters/one/groups/consumer')
 const wrapper=mount(ConsumerGroupDetails,{props:{id:'one',groupName:'consumer'},global:{plugins:[router]}});await flushPromises()
 return {wrapper,router}
}
afterEach(()=>vi.unstubAllGlobals())
it('shows unknown offsets honestly and links back to consumer groups and topics',async()=>{
 mockFetch();const {wrapper}=await setup()
 expect(wrapper.get('h1').text()).toBe('consumer')
 expect(wrapper.get('tbody').findAll('td').map(td=>td.text())).toEqual(['events','0','未知','12','未知','位点信息不完整'])
 expect(wrapper.get('tbody a').attributes('href')).toBe('/kafka/clusters/one/topics/events')
 expect(wrapper.get('.group-breadcrumb').html()).toContain('tab=groups')
 await wrapper.findAll('.group-tabs button')[1]!.trigger('click');await flushPromises()
 expect(wrapper.text()).toContain('暂不支持查看成员信息')
 wrapper.unmount()
})
it('does not mistake a similarly named group for the requested group',async()=>{
 mockFetch();vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).includes('/groups?')?json({items:[{id:'consumer-other'}],total:1,...markers}):json({})))
 const {wrapper}=await setup();expect(wrapper.get('[role=alert]').text()).toContain('消费组不存在')
 expect(wrapper.find('tbody').exists()).toBe(false);wrapper.unmount()
})
it('checks subsequent filtered pages before declaring a group missing',async()=>{
 mockFetch();const fetcher=vi.fn(async(input:string)=>{
  const url=String(input)
  if(url.includes('/groups?'))return json({items:url.includes('page=2&')?[{id:'consumer',state:'Empty'}]:Array.from({length:100},(_,i)=>({id:`consumer-${i}`})),total:101,...markers})
  if(url.includes('/offsets'))return json({items:[],total:0,...markers})
  return json({})
 });vi.stubGlobal('fetch',fetcher)
 const {wrapper}=await setup()
 expect(wrapper.find('[role=alert]').exists()).toBe(false)
 expect(wrapper.text()).toContain('无在线成员')
 expect(fetcher.mock.calls.some(([url])=>url.includes('page=2&'))).toBe(true)
 wrapper.unmount()
})
it('shows authorization failures without displaying stale resource details',async()=>{
 mockFetch();const {wrapper}=await setup()
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).includes('/groups?')?json({},403):json({})))
 await wrapper.setProps({groupName:'denied'});await flushPromises()
 expect(wrapper.get('[role=alert]').text()).toContain('未获授权');expect(wrapper.find('tbody').exists()).toBe(false)
 wrapper.unmount()
})
it('ignores late offset responses after switching groups',async()=>{
 mockFetch();const {wrapper}=await setup();let finish!:(r:Response)=>void
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>{
  const url=String(input)
  if(url.includes('/groups?'))return json({items:[{id:url.includes('late')?'late':'current',state:'Empty'}],total:1,...markers})
  if(url.includes('/late/offsets'))return new Promise<Response>(resolve=>finish=resolve)
  if(url.includes('/offsets'))return json({items:[],total:0,...markers})
  return json({})
 }))
 await wrapper.setProps({groupName:'late'});await flushPromises();await wrapper.setProps({groupName:'current'});await flushPromises()
 finish(json({items:[{topic:'private-old-topic'}],total:1,...markers}));await flushPromises()
 expect(wrapper.text()).not.toContain('private-old-topic');expect(wrapper.get('h1').text()).toBe('current')
 wrapper.unmount()
})
