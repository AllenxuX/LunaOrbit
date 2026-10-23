import { mount, flushPromises } from '@vue/test-utils'
import { defineComponent } from 'vue'
import { afterEach, expect, it, vi } from 'vitest'
import WorkspaceApplications from './WorkspaceApplications.vue'
import type { WorkspaceMeta } from '../../api/workspace'
const meta: WorkspaceMeta = { synthetic: true, write_enabled: true, approval_required: false, permissions: { read: true, topic_manage: true, applications_manage: true, acl_manage: true, audit_read: true, team_manage: true } }
const app = { id: 'app-1', name: 'Orders service', principal: 'User:orders-service', owner: 'orders-team', description: 'Events processing', revision: 1 }
const dialog = defineComponent({ props: { modelValue: Boolean }, template: '<div v-if="modelValue"><slot/><slot name="footer"/></div>' })
const options = { props: { clusterId: 'lab', meta, refreshVersion: 0 }, global: { stubs: { ElDialog: dialog } } }
afterEach(() => vi.unstubAllGlobals())
function setup() {
  const fetcher = vi.fn(async (input: string) => new Response(JSON.stringify(String(input).includes('/acls?') ? { items: [], total: 0 } : { items: [app], total: 1 })))
  vi.stubGlobal('fetch', fetcher)
  return { fetcher, w: mount(WorkspaceApplications, options) }
}
it('registers only explicit application metadata without credentials or LDAP identity inference', async () => {
  const { w, fetcher } = setup(); await flushPromises()
  await w.get('[data-new-application]').trigger('click')
  expect(w.text()).toContain('不会创建用户或修改密码')
  expect(w.find('input[type="password"]').exists()).toBe(false)
  await w.get('[name="app_name"]').setValue('Billing service'); await w.get('[name="app_owner"]').setValue('billing-team')
  await w.get('[name="app_principal"]').setValue('User:billing-service'); await w.get('[name="app_description"]').setValue('Billing events')
  await w.get('[data-application-form]').trigger('submit'); await flushPromises()
  const calls = fetcher.mock.calls as unknown as [string, RequestInit][]
  const write = calls.find(([, init]) => init?.method === 'POST')!
  expect(write[0]).toContain('/workspace/apps')
  expect(JSON.parse(write[1].body as string)).toEqual({ name: 'Billing service', owner: 'billing-team', principal: 'User:billing-service', description: 'Billing events' })
  expect(calls.every(([url]) => !url.includes('ldap') && !url.includes('credentials'))).toBe(true)
  w.unmount()
})
it('requires a consumer group and emits exact intents without direct ACL writes', async () => {
  const { w, fetcher } = setup(); await flushPromises()
  await w.findAll('button').find(b => b.text() === '授权')!.trigger('click'); await flushPromises()
  await w.get('[name="grant_topics"]').setValue('orders, orders, receipts')
  await w.get('input[value="consume"]').setValue(true)
  await w.get('[data-intent-grant]').trigger('submit')
  expect(w.emitted('intent')).toBeUndefined(); expect(w.get('[role="alert"]').text()).toContain('消费组')
  await w.get('[name="grant_group"]').setValue('reporting')
  await w.get('[data-intent-grant]').trigger('submit')
  expect(w.emitted('intent')).toEqual([[{ kind: 'grant', application_id: app.id, mode: 'consume', topics: [{ name: 'orders', pattern_type: 'LITERAL' }, { name: 'receipts', pattern_type: 'LITERAL' }], group: { name: 'reporting', pattern_type: 'LITERAL' } }]])
  expect((fetcher.mock.calls as unknown as [string, RequestInit][]).every(([, init]) => !init?.method || init.method === 'GET')).toBe(true)
  w.unmount()
})
it('rejects late application lists after changing clusters and hides mutation controls for readers', async () => {
  let finish!: (r: Response) => void
  vi.stubGlobal('fetch', vi.fn(async (input: string) => String(input).includes('/old/') ? new Promise<Response>(resolve => { finish = resolve }) : new Response(JSON.stringify({ items: [{ ...app, name: 'New application' }], total: 1 }))))
  const w = mount(WorkspaceApplications, { ...options, props: { clusterId: 'old', refreshVersion: 0, meta: { ...meta, write_enabled: false, permissions: { ...meta.permissions, applications_manage: false, acl_manage: false } } } }); await flushPromises()
  await w.setProps({ clusterId: 'new' }); await flushPromises()
  finish(new Response(JSON.stringify({ items: [{ ...app, name: 'Private old application' }], total: 1 }))); await flushPromises()
  expect(w.text()).toContain('New application'); expect(w.text()).not.toContain('Private old application')
  expect(w.find('[data-new-application]').exists()).toBe(false); expect(w.findAll('button').some(b => b.text() === '授权')).toBe(false)
  w.unmount()
})

it('opens exactly one application beyond the first page and loads only its permissions',async()=>{
 const fetcher=vi.fn(async(url:string)=>new Response(JSON.stringify(url.includes('/acls?')?{items:[],total:0}:url.includes('page=2')?{items:[app],total:21,page_size:20}:{items:[{...app,id:'unrelated',name:'Unrelated app'}],total:21,page_size:20})))
 vi.stubGlobal('fetch',fetcher)
 const w=mount(WorkspaceApplications,{...options,props:{...options.props,applicationId:app.id}});await flushPromises()
 expect(w.text()).toContain(app.name);expect(w.text()).not.toContain('Unrelated app')
 expect(w.find('.filter-bar').exists()).toBe(false);expect(w.find('[data-new-application]').exists()).toBe(false)
 expect(w.find('.application-access').exists()).toBe(true)
 expect(fetcher.mock.calls.some(([url])=>url.includes('page=2'))).toBe(true)
 expect(fetcher.mock.calls.find(([url])=>url.includes('/acls?'))?.[0]).toContain('principal=User%3Aorders-service')
 w.unmount()
})
