import { mount,flushPromises } from '@vue/test-utils'
import { createMemoryHistory,createRouter } from 'vue-router'
import { expect, it } from 'vitest'
import WorkspaceAccess from './WorkspaceAccess.vue'
import type { WorkspaceMeta } from '../../api/workspace'

it('opens accounts for the selected application and allows returning to all accounts', async () => {
  const meta: WorkspaceMeta = { synthetic: true, write_enabled: true, approval_required: false, permissions: { read: true, topic_manage: true, applications_manage: true, acl_manage: true, audit_read: true, team_manage: true, account_manage: true, credentials_read: true } }
  const w = mount(WorkspaceAccess, { props: { clusterId: 'one', meta, actorId: 'owner', refreshVersion: 0 }, global: { stubs: { WorkspaceApplications: true, WorkspaceAccounts: true, WorkspaceACLRules: true } } })
  const app = { id: 'app', name: '订单应用', principal: 'User:orders', owner: 'team', description: '', revision: 1 }
  w.findComponent({ name: 'WorkspaceApplications' }).vm.$emit('accounts', app)
  await w.vm.$nextTick()
  expect(w.findComponent({ name: 'WorkspaceAccounts' }).props()).toMatchObject({ applicationId: 'app', applicationName: '订单应用' })
  w.findComponent({ name: 'WorkspaceAccounts' }).vm.$emit('clearFilter')
  await w.vm.$nextTick()
  expect(w.findComponent({ name: 'WorkspaceAccounts' }).props('applicationId')).toBeUndefined()
  w.unmount()
})

it('restores the account list from the URL and keeps navigation in browser history',async()=>{
 const meta:WorkspaceMeta={synthetic:true,write_enabled:true,approval_required:false,permissions:{read:true,topic_manage:true,applications_manage:true,acl_manage:true,audit_read:true,team_manage:true,account_manage:true}}
 const router=createRouter({history:createMemoryHistory(),routes:[{path:'/:pathMatch(.*)*',component:{template:'<div/>'}}]})
 await router.push('/kafka/clusters/one/details?tab=applications&access=accounts')
 const w=mount(WorkspaceAccess,{props:{clusterId:'one',meta,actorId:'owner',refreshVersion:0},global:{plugins:[router],stubs:{WorkspaceApplications:true,WorkspaceAccounts:true,WorkspaceACLRules:true}}})
 expect(w.findComponent({name:'WorkspaceAccounts'}).exists()).toBe(true)
 await w.findAll('.access-views button')[0]!.trigger('click');await flushPromises()
 expect(router.currentRoute.value.query.access).toBe('applications')
 await router.push('/kafka/clusters/one/details?tab=applications&access=accounts');await flushPromises()
 expect(w.findComponent({name:'WorkspaceAccounts'}).exists()).toBe(true)
 w.unmount()
})
