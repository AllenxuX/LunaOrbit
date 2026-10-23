import { mount, flushPromises } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import WorkspaceAccounts from './WorkspaceAccounts.vue'
import type { WorkspaceMeta } from '../../api/workspace'
const account={id:'account',username:'business',principal:'User:business',mechanism:'SCRAM-SHA-256',source:'import',credential_state:'managed',observation:'verified',revision:1,application_ids:['app']}
const meta:WorkspaceMeta={synthetic:true,write_enabled:true,approval_required:false,permissions:{read:true,topic_manage:true,applications_manage:true,acl_manage:true,audit_read:true,team_manage:true,account_manage:true,credentials_read:true}}
afterEach(()=>{vi.unstubAllGlobals();vi.useRealTimers()})
it('copies only on request and clears the password after thirty seconds',async()=>{
 vi.useFakeTimers()
 const copy=vi.fn(async()=>undefined);vi.stubGlobal('navigator',{clipboard:{writeText:copy}})
 vi.stubGlobal('fetch',vi.fn(async(url:string)=>new Response(JSON.stringify(url.endsWith('/reveal')?{password:'synthetic-secret'}:url.includes('/accounts')?{items:[account],total:1}:{items:[],total:0}))))
 const w=mount(WorkspaceAccounts,{props:{clusterId:'one',meta,actorId:'owner',refreshVersion:0},global:{stubs:{ElDialog:{props:['modelValue'],template:'<section v-if="modelValue"><slot/></section>'}}}});await flushPromises()
 await w.get('[data-reveal-account]').trigger('click');await flushPromises();expect(copy).not.toHaveBeenCalled()
 await w.findAll('button').find(b=>b.text()==='复制密码')!.trigger('click');await flushPromises();expect(copy).toHaveBeenCalledWith('synthetic-secret')
 await vi.advanceTimersByTimeAsync(30000);await flushPromises()
 expect(w.find('input[aria-label=账号密码]').exists()).toBe(false);w.unmount()
})
it('ignores a delayed password response after view permission is removed',async()=>{
 let finish!:(response:Response)=>void
 vi.stubGlobal('fetch',vi.fn(async(url:string)=>url.endsWith('/reveal')?new Promise<Response>(resolve=>finish=resolve):new Response(JSON.stringify(url.includes('/accounts')?{items:[account],total:1}:{items:[],total:0}))))
 const w=mount(WorkspaceAccounts,{props:{clusterId:'one',meta,actorId:'owner',refreshVersion:0}});await flushPromises()
 await w.get('[data-reveal-account]').trigger('click');await flushPromises()
 await w.setProps({meta:{...meta,permissions:{...meta.permissions,credentials_read:false}}})
 finish(new Response(JSON.stringify({password:'synthetic-late-secret'})));await flushPromises()
 expect(w.find('input[aria-label=账号密码]').exists()).toBe(false);expect(w.text()).not.toContain('synthetic-late-secret');w.unmount()
})
it('reveals only by explicit action and clears the secret when the cluster changes',async()=>{
 const fetcher=vi.fn(async(url:string)=>new Response(JSON.stringify(url.endsWith('/reveal')?{password:'synthetic-secret'}:url.includes('/accounts')?{items:[account],total:1}:{items:[],total:0})));vi.stubGlobal('fetch',fetcher)
 const w=mount(WorkspaceAccounts,{props:{clusterId:'one',meta,actorId:'owner',refreshVersion:0}});await flushPromises()
 expect(w.text()).not.toContain('synthetic-secret');expect(fetcher.mock.calls.some(([u])=>u.endsWith('/reveal'))).toBe(false)
 await w.get('[data-reveal-account]').trigger('click');await flushPromises();expect((w.get('input[aria-label=账号密码]').element as HTMLInputElement).value).toBe('synthetic-secret')
 await w.setProps({clusterId:'two'});await flushPromises();expect(w.text()).not.toContain('synthetic-secret');expect(localStorage.length).toBe(0);w.unmount()
})
it('explains why password viewing is disabled without calling the reveal endpoint',async()=>{
 vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({items:[account],total:1}))))
 const w=mount(WorkspaceAccounts,{props:{clusterId:'one',meta:{...meta,permissions:{...meta.permissions,credentials_read:false}},actorId:'reader',refreshVersion:0}});await flushPromises();expect(w.get('[data-reveal-account]').attributes('disabled')).toBeDefined();expect(w.text()).toContain('没有查看密码的权限');w.unmount()
})
it('shows missing-password status and opens a prefilled import form without requiring an application',async()=>{
 const fetcher=vi.fn(async(url:string)=>new Response(JSON.stringify(url.includes('/accounts')?{items:[{...account,id:'external:business',credential_state:'unmanaged',source:'discovered',application_ids:[]}],total:1}:{items:[],total:0})));vi.stubGlobal('fetch',fetcher)
 const w=mount(WorkspaceAccounts,{props:{clusterId:'one',meta,actorId:'owner',refreshVersion:0}});await flushPromises()
 expect(w.get('[data-reveal-account]').attributes('disabled')).toBeDefined();expect(w.text()).toContain('未保存密码')
 await w.get('[data-import-password]').trigger('click');await flushPromises()
 expect((w.get('[name=account_username]').element as HTMLInputElement).value).toBe('business')
 expect(w.get('[name=account_application]').attributes('required')).toBeUndefined()
 expect(fetcher.mock.calls.some(([url])=>url.endsWith('/reveal'))).toBe(false)
 w.unmount()
})
it('opens the application onboarding flow and submits one stable request with the selected ACL intent',async()=>{
 const writes:{url:string;body:Record<string,unknown>}[]=[]
 vi.stubGlobal('fetch',vi.fn(async(url:string,init?:RequestInit)=>{
  if(init?.method==='POST'){const body=JSON.parse(String(init.body));writes.push({url,body});return new Response(JSON.stringify(url.endsWith('/apps')?{id:'created-app',...body}:{account}))}
  return new Response(JSON.stringify({items:[],total:0}))
 }))
 const w=mount(WorkspaceAccounts,{props:{clusterId:'one',meta,actorId:'owner',refreshVersion:0,openVersion:1}});await flushPromises()
 await w.get('[name=new_application_name]').setValue('Synthetic onboarded application');await w.get('[name=new_application_owner]').setValue('Synthetic team')
 await w.get('[name=account_access_mode]').setValue('produce_consume');await w.get('[name=account_topics]').setValue('orders,orders');await w.get('[name=account_group]').setValue('reporting')
 await w.get('[data-account-form]').findAll('input[type=checkbox]').at(-1)!.setValue(true)
 await w.get('[data-account-form]').trigger('submit');await flushPromises()
 expect(writes).toHaveLength(2);expect(writes[1]!.body).toMatchObject({mode:'create',application_id:'created-app',access_mode:'produce_consume',topics:[{name:'orders',pattern_type:'LITERAL'}],group:{name:'reporting',pattern_type:'LITERAL'}})
 expect(writes[0]!.body.request_id).toBe(writes[1]!.body.request_id);expect(writes[1]!.body).not.toHaveProperty('password');w.unmount()
})
it('finds a selected account beyond the first page without showing unrelated accounts',async()=>{
 const fetcher=vi.fn(async(url:string)=>new Response(JSON.stringify(url.includes('/accounts')?url.includes('page=2')?{items:[{...account,id:'external:target',username:'target',credential_state:'unmanaged'}],total:21,page_size:20}:{items:[{...account,username:'unrelated'}],total:21,page_size:20}:{items:[],total:0})))
 vi.stubGlobal('fetch',fetcher)
 const w=mount(WorkspaceAccounts,{props:{clusterId:'one',meta,actorId:'owner',refreshVersion:0,accountId:'external:target'}});await flushPromises()
 expect(w.text()).toContain('target');expect(w.text()).not.toContain('unrelated')
 expect(fetcher.mock.calls.some(([url])=>url.includes('page=2'))).toBe(true)
 expect(w.find('.account-pagination').exists()).toBe(false)
 expect(w.get('[data-reveal-account]').attributes('disabled')).toBeDefined()
 expect(fetcher.mock.calls.some(([url])=>url.includes('reveal'))).toBe(false)
 w.unmount()
})
it('keeps an external username detail available after its password has been saved',async()=>{
 vi.stubGlobal('fetch',vi.fn(async(url:string)=>new Response(JSON.stringify(url.includes('/accounts')?{items:[{...account,id:'saved-account',username:'target'}],total:1}:{items:[],total:0}))))
 const w=mount(WorkspaceAccounts,{props:{clusterId:'one',meta,actorId:'owner',refreshVersion:0,accountId:'external:target'}});await flushPromises()
 expect(w.get('[data-reveal-account]').attributes('disabled')).toBeUndefined()
 expect(w.text()).toContain('target');w.unmount()
})
