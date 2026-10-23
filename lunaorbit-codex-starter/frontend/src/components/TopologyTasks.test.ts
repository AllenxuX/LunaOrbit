import { mount } from '@vue/test-utils'
import { expect,it } from 'vitest'
import TopologyTasks from './TopologyTasks.vue'
import type { Link } from '../api/topology'
const task=(id:string,uid:string):Link=>({id,source_alias:'A',target_alias:'B',enabled:true,confidence:'high',declared:true,observed:true,observed_at:'2026-10-22T00:00:00Z',evidence:[],notes:[],namespace:'synthetic',workload_uid:uid,workload_name:'shared-mm2',topics_raw:'events.*',policy:'DefaultReplicationPolicy',source:{alias:'A',method:'explicit',confidence:'high',status:'confirmed',registration_id:'a',candidate_ids:[]},target:{alias:'B',method:'explicit',confidence:'high',status:'confirmed',registration_id:'b',candidate_ids:[]},verified:false,state:'observed',stale:false,runtime:{ready_replicas:1,replicas:1,generation:1,observed_generation:1,pod_candidates:1,pod_association:'labels'}} as Link)
const registrations=[{id:'a',display_name:'源集群',bootstrap_servers:[]},{id:'b',display_name:'目标集群',bootstrap_servers:[]}]
it('groups exact deployment identities and never merges same-name instances',()=>{
 const w=mount(TopologyTasks,{props:{links:[task('one','uid1'),task('two','uid1'),task('three','uid2')],registrations,scope:'synthetic',grouped:true},global:{stubs:{RouterLink:true}}})
 expect(w.findAll('[data-deployment]')).toHaveLength(2)
 expect(w.findAll('[data-task-row]')).toHaveLength(3)
 expect(w.get('[data-deployment]').text()).toContain('2 个任务')
 expect(w.text()).toContain('就绪 1 / 1');expect(w.text()).toContain('链路未验证')
 expect(w.text()).not.toContain('uid1');expect(w.text()).not.toContain('uid2')
})
it('selects an exact task and labels topic rules without claiming actual matched topics',async()=>{
 const w=mount(TopologyTasks,{props:{links:[task('one','uid')],registrations,scope:'synthetic',selectionMode:true},global:{stubs:{RouterLink:true}}})
 await w.get('[data-route-choice=one]').trigger('click')
 expect(w.emitted('select')).toEqual([['one']])
 expect(w.text()).toContain('Topic 匹配规则');expect(w.text()).toContain('events.*');expect(w.text()).toContain('源集群');expect(w.text()).toContain('目标集群')
})

it('does not present stale snapshot readiness as current',()=>{
 const link=task('one','uid');link.verified=true
 const w=mount(TopologyTasks,{props:{links:[link],registrations,scope:'synthetic',grouped:true,stale:true},global:{stubs:{RouterLink:true}}})
 expect(w.text()).toContain('运行数据已过期')
 expect(w.text()).not.toContain('就绪 1 / 1');expect(w.text()).not.toContain('链路已验证')
})
