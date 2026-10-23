import { flushPromises,mount } from '@vue/test-utils'
import { createMemoryHistory,createRouter } from 'vue-router'
import { afterEach,expect,it,vi } from 'vitest'
import WorkspaceLinks from './WorkspaceLinks.vue'
import { readSnapshot,type Snapshot } from '../../api/topology'
vi.mock('../../api/topology',()=>({readSnapshot:vi.fn()}))
afterEach(()=>{vi.unstubAllGlobals();vi.mocked(readSnapshot).mockReset()})
it('opens the exact scope and link even when different scopes share a link identifier',async()=>{
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>new Response(JSON.stringify(String(input).includes('/discovery/scopes')?{items:[{id:'scope-one'},{id:'scope-two'}]}:{items:[{id:'cluster',display_name:'实验集群',bootstrap_servers:[]}],total:1}))))
 vi.mocked(readSnapshot).mockResolvedValue({links:[{id:'shared-link',state:'enabled',source:{status:'confirmed',registration_id:'cluster'},target:{status:'unknown',alias:'target'},workload_name:'synthetic-mm2',topics_raw:'events.*',verified:false}]} as unknown as Snapshot)
 const router=createRouter({history:createMemoryHistory(),routes:[{path:'/:pathMatch(.*)*',component:{template:'<div />'}}]});await router.push('/')
 const wrapper=mount(WorkspaceLinks,{props:{clusterId:'cluster'},global:{plugins:[router]}});await flushPromises()
 const detailLinks=wrapper.findAll('a').filter(a=>a.text()==='查看详情')
 expect(detailLinks.map(a=>a.attributes('href'))).toEqual(['/topology/scope-one/links/shared-link','/topology/scope-two/links/shared-link'])
 expect(wrapper.findAll('tbody tr')).toHaveLength(2)
 expect(wrapper.get('tbody tr').findAll('td')[0]!.get('a').attributes('href')).toBe('/kafka/clusters/cluster/details')
 expect(wrapper.get('tbody tr').findAll('td')[1]!.find('a').exists()).toBe(false)
 wrapper.unmount()
})
