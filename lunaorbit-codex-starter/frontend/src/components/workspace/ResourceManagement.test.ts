import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import ResourceConfigs from './ResourceConfigs.vue'
import TopicLifecycle from './TopicLifecycle.vue'
import ResourceOperations from './ResourceOperations.vue'
const meta={synthetic:true,write_enabled:true,approval_required:true,permissions:{read:true,topic_manage:true,applications_manage:false,acl_manage:false,audit_read:false,team_manage:false}}
const entry={name:'retention.ms',value:'1000',source:'DYNAMIC_TOPIC_CONFIG',sensitive:false,read_only:false,type:'LONG',documentation:'Retention limit',default_value:'7000',synonyms:[]}
const props={clusterId:'one',meta,actorId:'operator'}
const json=(v:unknown)=>new Response(JSON.stringify(v))
afterEach(()=>vi.unstubAllGlobals())
it.each(['TOPIC','BROKER'] as const)('shows only Chinese explanations for %s even when Kafka returns HTML documentation',async(type)=>{
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).includes('resource-configs')?json({entries:[{...entry,name:'min.insync.replicas',documentation:'When a producer <code>raw-server-html</code><br>English documentation'},{...entry,name:'future.config',documentation:'Unknown raw English documentation'}]}):json({items:[],total:0})))
 const w=mount(ResourceConfigs,{props:{...props,resource:{type,name:type==='TOPIC'?'events':'1'},showOperations:false}});await flushPromises()
 expect(w.text()).toContain('同步副本')
 expect(w.text()).not.toMatch(/When a producer|raw-server-html|English documentation|<code>|<br>/)
 await w.get('.config-view-switch button:last-child').trigger('click')
 expect(w.text()).toContain('暂无中文说明。')
 expect(w.text()).not.toContain('Unknown raw English')
 w.unmount()
})
it('starts with common settings and exposes the full configuration list on demand',async()=>{
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).includes('resource-configs')?json({entries:[entry,{...entry,name:'segment.jitter.ms'}]}):json({items:[],total:0})))
 const w=mount(ResourceConfigs,{props:{...props,resource:{type:'TOPIC',name:'events'},showOperations:false}});await flushPromises()
 expect(w.find('[data-edit-config="retention.ms"]').exists()).toBe(true)
 expect(w.find('[data-edit-config="segment.jitter.ms"]').exists()).toBe(false)
 await w.get('.config-view-switch button:last-child').trigger('click')
 expect(w.find('[data-edit-config="segment.jitter.ms"]').exists()).toBe(true)
 expect(w.find('.resource-operations').exists()).toBe(false)
 w.unmount()
})
it('uses selected resource and deletes overrides without sending a default value',async()=>{
 const fetcher=vi.fn(async(input:string,options?:RequestInit)=>{if(String(input).includes('resource-configs'))return json({resource:{type:'BROKER',name:'12'},entries:[entry]});if(String(input).includes('resource-plans'))return json({input:JSON.parse(String(options?.body)),fingerprint:'fp'});return json({items:[],total:0})});vi.stubGlobal('fetch',fetcher)
 const w=mount(ResourceConfigs,{props:{...props,resource:{type:'BROKER',name:'12'}}});await flushPromises();expect(fetcher.mock.calls.some(([url])=>String(url).includes('resource_type=BROKER&resource_name=12'))).toBe(true)
 await w.get('[data-edit-config="retention.ms"]').trigger('click');await w.get('.config-editor select').setValue('DELETE');await w.get('form').trigger('submit');await flushPromises()
 const call=fetcher.mock.calls.find(([url])=>String(url).includes('resource-plans'));expect(JSON.parse(String(call?.[1]?.body))).toEqual({kind:'configs',resource:{type:'BROKER',name:'12'},changes:[{name:'retention.ms',operation:'DELETE'}]});w.unmount()
})
it('never renders sensitive values or defaults and disables sensitive and read only editing',async()=>{
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).includes('resource-configs')?json({entries:[{...entry,name:'secret',sensitive:true,value:'hidden-secret',default_value:'hidden-default',synonyms:[{name:'secret',value:'hidden-synonym',source:'DEFAULT_CONFIG'}]},{...entry,name:'immutable',read_only:true}]}):json({items:[],total:0})))
 const w=mount(ResourceConfigs,{props:{...props,resource:{type:'TOPIC',name:'events'}}});await flushPromises();expect(w.text()).not.toMatch(/hidden-secret|hidden-default|hidden-synonym/);expect(w.get('[data-edit-config="secret"]').attributes('disabled')).toBeDefined();expect(w.get('[data-edit-config="immutable"]').attributes('disabled')).toBeDefined();w.unmount()
})
it('discards config responses from a previous cluster',async()=>{
 let finish!:(r:Response)=>void;vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).includes('/one/workspace/resource-configs')?new Promise<Response>(r=>finish=r):String(input).includes('resource-configs')?json({entries:[{...entry,name:'current'}]}):json({items:[],total:0})))
 const w=mount(ResourceConfigs,{props:{...props,resource:{type:'TOPIC',name:'events'}}});await flushPromises();await w.setProps({clusterId:'two'});await flushPromises();finish(json({entries:[{...entry,name:'stale'}]}));await flushPromises();expect(w.text()).toContain('current');expect(w.text()).not.toContain('stale');w.unmount()
})
it('requires exact topic name for deletion and blocks internal topics',async()=>{
 vi.stubGlobal('fetch',vi.fn(async()=>json({items:[],total:0})));const w=mount(TopicLifecycle,{props:{...props,topic:{name:'events',internal:false,partitions:[],rf_min:1,rf_max:1}}});await flushPromises();await w.get('select').setValue('topic_delete');expect(w.get('form button').attributes('disabled')).toBeDefined();await w.get('input').setValue('event');expect(w.get('form button').attributes('disabled')).toBeDefined();await w.get('input').setValue('events');expect(w.get('form button').attributes('disabled')).toBeUndefined();await w.setProps({topic:{name:'__internal',internal:true,partitions:[],rf_min:1,rf_max:1}});expect(w.get('form button').attributes('disabled')).toBeDefined();w.unmount()
})
it('keeps accepted asynchronous operations awaiting actual completion',async()=>{
 const op={id:'op',revision:1,status:'observing',plan:{input:{kind:'reassign',resource:{type:'TOPIC',name:'events'}},fingerprint:'fp'},created_by:'operator',approved_by:'reviewer'};vi.stubGlobal('fetch',vi.fn(async()=>json({items:[op],total:1})));const w=mount(ResourceOperations,{props});await flushPromises();expect(w.text()).toContain('已受理，等待实际完成');expect(w.text()).not.toContain('执行成功');w.unmount()
})
it('retains selected asynchronous detail through parent refresh and updates only with newer revisions',async()=>{
 let revision=1,status='approved';const make=()=>({id:'operation',revision,status,plan:{input:{kind:'reassign',resource:{type:'TOPIC',name:'events'}},fingerprint:'fp'},created_by:'creator',approved_by:'reviewer'});
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>{if(String(input).endsWith('/execute')){revision=2;status='observing';return json(make())}if(String(input).endsWith('/refresh')){revision=3;status='succeeded';return json(make())}return json({items:[make()],total:1})}));
 const w=mount(ResourceOperations,{props:{...props,refreshVersion:0}});await flushPromises();await w.get('tbody button').trigger('click');await w.get('.operation-detail input').setValue(true);await w.findAll('.operation-detail button').find(b=>b.text()==='执行变更')!.trigger('click');await flushPromises();expect(w.get('.operation-detail').text()).toContain('已受理，等待实际完成');await w.setProps({refreshVersion:1});await flushPromises();expect(w.get('.operation-detail').text()).toContain('已受理，等待实际完成');expect(w.get('.operation-detail button').text()).toBe('检查结果');revision=1;status='approved';await w.setProps({refreshVersion:2});await flushPromises();expect(w.get('.operation-detail').text()).toContain('已受理，等待实际完成');await w.get('.operation-detail button').trigger('click');await flushPromises();expect(w.get('.operation-detail').text()).toContain('执行成功');w.unmount()
})
