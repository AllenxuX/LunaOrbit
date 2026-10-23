import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import WorkspaceTeams from './WorkspaceTeams.vue'
afterEach(() => vi.unstubAllGlobals())
it('previews explicit scopes and sends revision for confirmed revocation', async () => {
 const calls: { url: string; init?: RequestInit }[] = []
 vi.stubGlobal('fetch',vi.fn(async (input: string,init?: RequestInit)=>{calls.push({url:String(input),init});return new Response(JSON.stringify({items:[{id:'grant',revision:3,subject_type:'user',subject_id:'trusted-subject',role:'viewer',topic_prefix:'app.',group_prefix:'app-'}]}))}))
 const w=mount(WorkspaceTeams,{props:{clusterId:'cluster',canManage:true}});await flushPromises()
 await w.get('[data-subject]').setValue('trusted-new');await w.get('[data-topic-prefix]').setValue('team.');await w.get('[data-group-prefix]').setValue('team-');await w.get('form').trigger('submit');expect(calls.filter(c=>c.init?.method==='POST')).toHaveLength(0)
 await w.get('[data-confirm-grant]').trigger('click');await flushPromises();const posted=calls.find(c=>c.init?.method==='POST')!;expect(JSON.parse(String(posted.init?.body))).toEqual({subject_type:'user',subject_id:'trusted-new',role:'viewer',topic_prefix:'team.',group_prefix:'team-'})
 await w.get('[data-revoke]').trigger('click');expect(calls.filter(c=>c.init?.method==='DELETE')).toHaveLength(0);await w.get('[data-confirm-revoke]').trigger('click');await flushPromises();expect(JSON.parse(String(calls.find(c=>c.init?.method==='DELETE')!.init?.body))).toEqual({revision:3});w.unmount()
})
it('hides mutation controls and discards stale cluster responses',async()=>{
 let old!:(r:Response)=>void;vi.stubGlobal('fetch',vi.fn(async(input:string)=>String(input).includes('/old/')?new Promise<Response>(r=>{old=r}):new Response(JSON.stringify({items:[]}))))
 const w=mount(WorkspaceTeams,{props:{clusterId:'old',canManage:false}});await flushPromises();await w.setProps({clusterId:'new'});await flushPromises();old(new Response(JSON.stringify({items:[{id:'old',subject_id:'stale-subject',role:'admin'}]})));await flushPromises();expect(w.text()).not.toContain('stale-subject');expect(w.find('form').exists()).toBe(false);w.unmount()
})
it('shows denied access without raw server messages',async()=>{
 vi.stubGlobal('fetch',vi.fn(async()=>new Response('sensitive upstream',{status:403})));const w=mount(WorkspaceTeams,{props:{clusterId:'cluster',canManage:false}});await flushPromises();expect(w.get('[role="alert"]').text()).toContain('权限');expect(w.text()).not.toContain('sensitive');w.unmount()
})
it('explains duplicate grant conflicts without adding a second row', async () => {
 vi.stubGlobal('fetch',vi.fn(async(_input: string,init?:RequestInit)=>init?.method==='POST'?new Response(JSON.stringify({error:{code:'CONFLICT'}}),{status:409}):new Response(JSON.stringify({items:[]}))))
 const w=mount(WorkspaceTeams,{props:{clusterId:'cluster',canManage:true}});await flushPromises();await w.get('[data-subject]').setValue('existing');await w.get('form').trigger('submit');await w.get('[data-confirm-grant]').trigger('click');await flushPromises();expect(w.get('[role="alert"]').text()).toContain('授权已存在');expect(w.find('[data-confirm-grant]').exists()).toBe(false);w.unmount()
})
