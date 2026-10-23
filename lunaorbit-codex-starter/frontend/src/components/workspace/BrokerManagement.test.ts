import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, expect, it, vi } from 'vitest'
import BrokerManagement from './BrokerManagement.vue'
afterEach(()=>vi.unstubAllGlobals())
it('lists brokers as detail links and keeps cluster defaults a separate explicit entry',async()=>{
 vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({items:[{id:2,host:'broker.synthetic',port:9092}],total:1}))))
 const router=createRouter({history:createMemoryHistory(),routes:[{path:'/:pathMatch(.*)*',component:{template:'<div />'}}]});await router.push('/')
 const wrapper=mount(BrokerManagement,{props:{clusterId:'synthetic',actorId:'actor',meta:{} as never},global:{plugins:[router]}});await flushPromises()
 expect(wrapper.get('[data-broker="2"]').attributes('href')).toBe('/kafka/clusters/synthetic/brokers/2')
 expect(wrapper.findAll('a').find(a=>a.text()==='集群默认配置')?.attributes('href')).toBe('/kafka/clusters/synthetic/brokers/default')
 expect(wrapper.find('select').exists()).toBe(false)
 expect(wrapper.find('.resource-configs').exists()).toBe(false)
 wrapper.unmount()
})
