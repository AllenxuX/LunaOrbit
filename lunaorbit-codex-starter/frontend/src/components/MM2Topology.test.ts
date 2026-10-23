import { mount } from '@vue/test-utils'
import { expect,it } from 'vitest'
import MM2Topology from './MM2Topology.vue'
import type { Snapshot,Link } from '../api/topology'
function fixture():Snapshot{
 const routes=[['ab','a','b','first'],['ac','a','c','mesh'],['db','d','b','mesh'],['bc','b','c','mesh'],['ba','b','a','mesh']]
 const links=routes.map(([id,a,b,w])=>({id,source_alias:a,target_alias:b,policy:'',enabled:id!=='ba',confidence:'high',declared:true,observed:true,observed_at:'2026-10-22T00:00:00Z',evidence:[],notes:[],namespace:'lab',workload_uid:w,workload_name:'mm2-'+w,source:{alias:a,method:'explicit',confidence:'high',status:'confirmed',registration_id:a,candidate_ids:[]},target:{alias:b,method:'explicit',confidence:'high',status:'confirmed',registration_id:b,candidate_ids:[]},topics_raw:id+'.*',state:id==='ba'?'disabled':'observed',verified:false,stale:false,runtime:{replicas:1,ready_replicas:1,generation:1,observed_generation:1,pod_candidates:1,pod_association:'labels'}} as Link))
 return {stale:false,nodes:['a','b','c','d'].map(id=>({id,kind:'kafka',label:id})),links,edges:routes.flatMap(([id,a,b,w])=>[{id:id+'i',source:a,target:w,link_id:id,relation:'declared_input'},{id:id+'o',source:w,target:b,link_id:id,relation:'declared_output'}])} as Snapshot
}
const registrations=['a','b','c','d'].map(id=>({id,display_name:'集群 '+id.toUpperCase(),bootstrap_servers:[]}))
it('draws exact task paths inside real MM2 groups without mixing shared-instance inputs and outputs',async()=>{
 const snapshot=fixture(),before=JSON.stringify(snapshot)
 const w=mount(MM2Topology,{props:{snapshot,registrations}})
 expect(w.findAll('[data-mm2-instance]')).toHaveLength(2)
 expect(w.findAll('[data-mm2-task]')).toHaveLength(4)
 await w.get('[data-expand-tasks]').trigger('click')
 expect(w.findAll('[data-mm2-task]')).toHaveLength(5)
 const row=w.get('[data-mm2-task=bc]')
 expect(row.get('[data-source]').text()).toContain('集群 B')
 expect(row.get('[data-target]').text()).toContain('集群 C')
 expect(row.findAll('svg')).toHaveLength(2)
 await row.get('[data-mm2-select]').trigger('click')
 expect(w.emitted('select')).toEqual([['bc']])
 await row.get('[data-source]').trigger('click')
 expect(w.emitted('selectNode')).toEqual([['b']])
 expect(JSON.stringify(snapshot)).toBe(before)
})
it('highlights a single task, preserves disabled direction and never equates readiness with health',async()=>{
 const w=mount(MM2Topology,{props:{snapshot:fixture(),registrations,selectedId:'ba'}})
 expect(w.get('[data-mm2-task=ba]').classes()).toContain('path-selected')
 expect(w.get('[data-mm2-task=ba]').text()).toContain('同步已禁用')
 expect(w.text()).toContain('就绪 1 / 1');expect(w.text()).not.toContain('链路已验证')
 await w.setProps({snapshot:{...fixture(),stale:true}})
 expect(w.text()).toContain('运行数据已过期');expect(w.text()).not.toContain('就绪 1 / 1')
})
it('collapses instances and treats same-name distinct UIDs as separate instances',async()=>{
 const s=fixture();s.links.forEach(l=>l.workload_name='same-name')
 const w=mount(MM2Topology,{props:{snapshot:s,registrations}})
 expect(w.findAll('[data-mm2-instance]')).toHaveLength(2)
 await w.findAll('[data-toggle-instance]')[0]!.trigger('click')
 expect(w.findAll('[data-mm2-task]')).toHaveLength(3)
})

it('reveals a selected task in a collapsed instance and lets the user collapse an auto-expanded preview',async()=>{
 const w=mount(MM2Topology,{props:{snapshot:fixture(),registrations}})
 await w.findAll('[data-toggle-instance]')[0]!.trigger('click')
 await w.setProps({selectedId:'ab'})
 expect(w.find('[data-mm2-task=ab]').exists()).toBe(true)
 await w.setProps({selectedId:'ba'})
 expect(w.find('[data-mm2-task=ba]').exists()).toBe(true)
 await w.get('[data-expand-tasks]').trigger('click')
 expect(w.find('[data-mm2-task=ba]').exists()).toBe(false)
})
