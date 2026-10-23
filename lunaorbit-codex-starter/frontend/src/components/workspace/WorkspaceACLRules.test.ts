import { mount, flushPromises } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import WorkspaceACLRules from './WorkspaceACLRules.vue'
import type { WorkspaceMeta } from '../../api/workspace'

const rule = { principal: 'User:external', host: '10.0.0.1', resource_type: 'TOPIC', resource_name: 'orders', pattern_type: 'PREFIXED', operation: 'WRITE', permission: 'DENY' }
const meta: WorkspaceMeta = { synthetic: true, write_enabled: true, approval_required: true, permissions: { read: true, topic_manage: true, applications_manage: true, acl_manage: true, audit_read: true, team_manage: true } }
afterEach(() => vi.unstubAllGlobals())
function setup(writes = true) {
  const fetcher = vi.fn(async (url: string) => new Response(JSON.stringify(url.includes('acl-capabilities') ? { resources: { TOPIC: ['READ', 'WRITE'], CLUSTER: ['ALL'] }, pattern_types: ['LITERAL', 'PREFIXED'], permissions: ['ALLOW', 'DENY'] } : { items: [rule], total: 1 })))
  vi.stubGlobal('fetch', fetcher)
  return { fetcher, w: mount(WorkspaceACLRules, { props: { clusterId: 'lab', meta: { ...meta, write_enabled: writes }, refreshVersion: 0 } }) }
}
it('includes unassociated identities and deletes only explicitly selected complete native rules', async () => {
  const { w, fetcher } = setup(); await flushPromises()
  expect(w.text()).toContain('User:external'); expect(w.text()).toContain('10.0.0.1'); expect(w.text()).toContain('DENY')
  expect(w.get('[data-revoke-native]').attributes('disabled')).toBeDefined()
  await w.get('input[type="checkbox"]').setValue(true); await w.get('[data-revoke-native]').trigger('click')
  expect(w.emitted('intent')).toEqual([[{ kind: 'acl_custom', principal: rule.principal, desired_rules: [], selected_rules: [rule] }]])
  expect(fetcher.mock.calls.every(([url]) => !url.includes('/requests'))).toBe(true); w.unmount()
})
it('builds native DENY/host intent and constrains operations using the capability table', async () => {
  const { w } = setup(); await flushPromises()
  await w.get('[name="acl_principal"]').setValue(rule.principal); await w.get('[name="acl_host"]').setValue(rule.host)
  await w.get('[name="acl_resource"]').setValue(rule.resource_name); await w.get('[name="acl_pattern"]').setValue(rule.pattern_type)
  await w.get('[name="acl_operation"]').setValue(rule.operation); await w.get('[name="acl_permission"]').setValue(rule.permission)
  await w.get('[data-add-native]').trigger('submit')
  expect(w.emitted('intent')?.[0]).toEqual([{ kind: 'acl_custom', principal: rule.principal, desired_rules: [rule], selected_rules: [] }])
  await w.get('[name="acl_type"]').setValue('CLUSTER')
  expect(w.get('[name="acl_resource"]').element).toHaveProperty('value', 'kafka-cluster')
  expect(w.get('[name="acl_operation"]').text()).toBe('ALL'); w.unmount()
})
it('shows rules with write controls disabled when the write gate is closed', async () => {
  const { w } = setup(false); await flushPromises(); expect(w.text()).toContain('User:external'); expect(w.find('[data-add-native]').exists()).toBe(false); expect(w.find('[data-revoke-native]').exists()).toBe(false); w.unmount()
})
