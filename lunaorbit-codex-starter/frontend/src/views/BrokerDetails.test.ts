import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, expect, it, vi } from 'vitest'
import BrokerDetails from './BrokerDetails.vue'
const meta={synthetic:true,write_enabled:true,permissions:{topic_manage:true}}
const json=(value:unknown)=>new Response(JSON.stringify(value))
function mockFetch(){vi.stubGlobal('fetch',vi.fn(async(input:string)=>{
 const url=String(input)
 if(url.endsWith('/session'))return json({user_id:'operator'})
 if(url.includes('/workspace/meta'))return json(meta)
 if(url.includes('/brokers?'))return json({items:[{id:1,host:'broker.synthetic',port:9092}],total:1,page_size:100})
 return json({display_name:'实验集群'})
}))}
const configs={template:'<div data-config-editor />',props:['resource']}
const operations={template:'<div data-broker-operations />',props:['resource']}
async function setup(brokerId='1',tab='overview'){
 const router=createRouter({history:createMemoryHistory(),routes:[{path:'/:pathMatch(.*)*',component:{template:'<div />'}}]})
 await router.push(`/kafka/clusters/one/brokers/${brokerId}?tab=${tab}`)
 const wrapper=mount(BrokerDetails,{props:{id:'one',brokerId},global:{plugins:[router],stubs:{ResourceConfigs:configs,ResourceOperations:operations}}})
 await flushPromises();return{wrapper,router}
}
afterEach(()=>vi.unstubAllGlobals())
it('shows broker overview with list breadcrumb instead of opening cluster defaults',async()=>{
 mockFetch();const{wrapper}=await setup()
 expect(wrapper.get('h1').text()).toBe('Broker 1')
 expect(wrapper.get('dl').text()).toContain('broker.synthetic')
 expect(wrapper.find('[data-config-editor]').exists()).toBe(false)
 expect(wrapper.get('.broker-breadcrumb').findAll('a')[2]!.attributes('href')).toContain('tab=brokers')
 wrapper.unmount()
})
it('deep links configurations and scopes operation records to this broker',async()=>{
 mockFetch();const{wrapper,router}=await setup('1','configs')
 expect(wrapper.findComponent(configs).props('resource')).toEqual({type:'BROKER',name:'1'})
 await wrapper.findAll('.broker-tabs button')[2]!.trigger('click');await flushPromises()
 expect(router.currentRoute.value.query.tab).toBe('operations')
 expect(wrapper.findComponent(operations).props('resource')).toEqual({type:'BROKER',name:'1'})
 wrapper.unmount()
})
it('labels cluster defaults separately and does not query broker metadata',async()=>{
 mockFetch();const{wrapper}=await setup('default','configs')
 expect(wrapper.get('h1').text()).toBe('集群默认配置')
 expect(wrapper.text()).toContain('实际生效值')
 expect(wrapper.findComponent(configs).props('resource')).toEqual({type:'BROKER',name:''})
 expect(vi.mocked(fetch).mock.calls.some(([url])=>String(url).includes('/brokers?'))).toBe(false)
 wrapper.unmount()
})
it('refuses to edit a broker no longer in this cluster',async()=>{
 mockFetch();const{wrapper}=await setup('9','configs')
 expect(wrapper.get('[role=alert]').text()).toContain('不在当前集群')
 expect(wrapper.find('[data-config-editor]').exists()).toBe(false)
 wrapper.unmount()
})
it('clears the old broker and ignores a late result after changing route props',async()=>{
 mockFetch();const{wrapper}=await setup()
 let complete!:(response:Response)=>void
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).endsWith('/session')?json({user_id:'operator'}):String(input).includes('/brokers?')?new Promise<Response>(resolve=>complete=resolve):String(input).includes('/meta')?json(meta):json({display_name:'另一集群'})))
 await wrapper.setProps({id:'late'});await flushPromises()
 expect(wrapper.find('dl').exists()).toBe(false)
 mockFetch();await wrapper.setProps({id:'current'});await flushPromises()
 complete(json({items:[{id:1,host:'late.synthetic',port:9092}],total:1,page_size:100}));await flushPromises()
 expect(wrapper.text()).not.toContain('late.synthetic')
 expect(wrapper.get('dl').text()).toContain('broker.synthetic')
 wrapper.unmount()
})
