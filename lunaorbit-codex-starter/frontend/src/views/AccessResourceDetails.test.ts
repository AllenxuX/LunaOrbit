import { flushPromises,mount } from '@vue/test-utils'
import { createMemoryHistory,createRouter } from 'vue-router'
import { afterEach,expect,it,vi } from 'vitest'
import AccessResourceDetails from './AccessResourceDetails.vue'
const meta={synthetic:true,write_enabled:true,permissions:{account_manage:true}}
const json=(value:unknown)=>new Response(JSON.stringify(value))
const applications={template:'<div data-application-detail/>',props:['applicationId']}
const accounts={template:'<div data-account-detail/>',props:['accountId','applicationId']}
function mockFetch(){vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).endsWith('/session')?json({user_id:'actor'}):String(input).includes('/meta')?json(meta):json({display_name:'实验集群'})))}
async function setup(props:{id:string;applicationId?:string;accountId?:string},tab=''){
 const router=createRouter({history:createMemoryHistory(),routes:[{path:'/:pathMatch(.*)*',component:{template:'<div/>'}}]});await router.push('/resource'+(tab?'?tab='+tab:''))
 const wrapper=mount(AccessResourceDetails,{props,global:{plugins:[router],stubs:{WorkspaceApplications:applications,WorkspaceAccounts:accounts,WorkspacePlanDialog:true}}});await flushPromises();return{wrapper,router}
}
afterEach(()=>vi.unstubAllGlobals())
it('opens only the requested application and links its accounts as a deep-link tab',async()=>{
 mockFetch();const{wrapper,router}=await setup({id:'one',applicationId:'app'})
 expect(wrapper.findComponent(applications).props('applicationId')).toBe('app')
 expect(wrapper.find('[data-account-detail]').exists()).toBe(false)
 await wrapper.findAll('.access-tabs button')[1]!.trigger('click');await flushPromises()
 expect(router.currentRoute.value.query.tab).toBe('accounts')
 expect(wrapper.findComponent(accounts).props('applicationId')).toBe('app')
 wrapper.unmount()
})
it('opens an existing Kafka username by exact external identifier without revealing its password',async()=>{
 mockFetch();const{wrapper}=await setup({id:'one',accountId:'external:synthetic-user'})
 expect(wrapper.findComponent(accounts).props('accountId')).toBe('external:synthetic-user')
 expect(wrapper.get('.access-breadcrumb').findAll('a')[2]!.attributes('href')).toContain('access=accounts')
 expect(vi.mocked(fetch).mock.calls.some(([url])=>String(url).includes('reveal'))).toBe(false)
 wrapper.unmount()
})
it('clears the old resource while another cluster loads and ignores late metadata',async()=>{
 mockFetch();const{wrapper}=await setup({id:'one',accountId:'account'})
 let finish!:(response:Response)=>void
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).endsWith('/session')?json({user_id:'actor'}):String(input).includes('/meta')?new Promise<Response>(resolve=>finish=resolve):json({display_name:'迟到集群'})))
 await wrapper.setProps({id:'late'});await flushPromises()
 expect(wrapper.find('[data-account-detail]').exists()).toBe(false)
 mockFetch();await wrapper.setProps({id:'current'});await flushPromises()
 finish(json(meta));await flushPromises()
 expect(wrapper.text()).not.toContain('迟到集群')
 expect(wrapper.find('[data-account-detail]').exists()).toBe(true)
 wrapper.unmount()
})
