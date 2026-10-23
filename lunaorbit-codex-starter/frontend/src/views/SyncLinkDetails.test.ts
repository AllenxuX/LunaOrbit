import { flushPromises,mount } from '@vue/test-utils'
import { createMemoryHistory,createRouter } from 'vue-router'
import { afterEach,expect,it,vi } from 'vitest'
import SyncLinkDetails from './SyncLinkDetails.vue'
import { readSnapshot,type Snapshot } from '../api/topology'
vi.mock('../api/topology',()=>({readSnapshot:vi.fn()}))
const link={id:'link',namespace:'synthetic',workload_name:'synthetic-mm2',source:{alias:'source',status:'confirmed',registration_id:'source'},target:{alias:'target',status:'confirmed',registration_id:'target'},topics_raw:'events.*',policy:'DefaultReplicationPolicy',declared:true,observed:true,verified:false,stale:false,state:'enabled',observed_at:'2026-10-21T00:00:00Z',runtime:{ready_replicas:1,replicas:1}}
const snapshot={scope_id:'scope',synthetic:true,stale:false,links:[link]} as unknown as Snapshot
const json=(value:unknown)=>new Response(JSON.stringify(value))
function mockFetch(){vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).endsWith('/session')?json({user_id:'actor'}):String(input).includes('page=2')?json({items:[{id:'target',display_name:'目标实验集群',bootstrap_servers:[]}],total:2}):json({items:[{id:'source',display_name:'源实验集群',bootstrap_servers:[]}],total:2})))}
async function setup(linkId='link'){
 const router=createRouter({history:createMemoryHistory(),routes:[{path:'/:pathMatch(.*)*',component:{template:'<div />'}}]});await router.push('/topology/scope/links/'+linkId)
 const wrapper=mount(SyncLinkDetails,{props:{scopeId:'scope',linkId},global:{plugins:[router]}});await flushPromises();return{wrapper,router}
}
afterEach(()=>{vi.unstubAllGlobals();vi.mocked(readSnapshot).mockReset()})
it('shows cluster names, clickable endpoints and only declared topic rules',async()=>{
 mockFetch();vi.mocked(readSnapshot).mockResolvedValue(snapshot)
 const{wrapper}=await setup()
 expect(wrapper.get('h1').text()).toBe('源实验集群 → 目标实验集群')
 expect(wrapper.get('.sync-endpoints').findAll('a').map(a=>a.attributes('href'))).toEqual(['/kafka/clusters/source/details','/kafka/clusters/target/details'])
 expect(wrapper.get('.sync-topics').text()).toContain('events.*')
 expect(wrapper.get('.sync-topics').text()).toContain('实际同步 Topic 尚未验证')
 expect(wrapper.get('.sync-status').text()).toBe('链路未验证')
 expect(wrapper.get('.sync-runtime').text()).toContain('1 / 1')
 expect(wrapper.get('.sync-breadcrumb a').attributes('href')).toBe('/topology?scope=scope')
 expect(vi.mocked(fetch).mock.calls.some(([url])=>String(url).includes('/topics'))).toBe(false)
 await wrapper.get('.page-header button').trigger('click');await flushPromises();expect(readSnapshot).toHaveBeenCalledTimes(2)
 wrapper.unmount()
})
it('reports missing links without showing another link from the same scope',async()=>{
 mockFetch();vi.mocked(readSnapshot).mockResolvedValue(snapshot)
 const{wrapper}=await setup('missing')
 expect(wrapper.get('[role=alert]').text()).toContain('不在当前发现结果中')
 expect(wrapper.find('.sync-runtime').exists()).toBe(false)
 expect(wrapper.text()).not.toContain('synthetic-mm2')
 wrapper.unmount()
})
it('keeps stale runtime information from implying a healthy synchronization',async()=>{
 mockFetch();vi.mocked(readSnapshot).mockResolvedValue({...snapshot,stale:true})
 const{wrapper}=await setup();expect(wrapper.get('.sync-status').text()).toContain('数据已过期')
 expect(wrapper.text()).not.toContain('同步正常');wrapper.unmount()
})
it('clears the old link and ignores a late response after navigating to another scope',async()=>{
 mockFetch();vi.mocked(readSnapshot).mockResolvedValue(snapshot)
 const{wrapper}=await setup();let finish!:(value:Snapshot)=>void
 vi.mocked(readSnapshot).mockImplementationOnce(()=>new Promise(resolve=>finish=resolve))
 await wrapper.setProps({scopeId:'late'});await flushPromises();expect(wrapper.find('.sync-runtime').exists()).toBe(false)
 vi.mocked(readSnapshot).mockResolvedValue({...snapshot,scope_id:'current'})
 await wrapper.setProps({scopeId:'current'});await flushPromises()
 finish({...snapshot,links:[{...snapshot.links[0]!,workload_name:'late-mm2'}]});await flushPromises()
 expect(wrapper.text()).not.toContain('late-mm2');expect(wrapper.text()).toContain('synthetic-mm2');wrapper.unmount()
})
