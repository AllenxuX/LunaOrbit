import { mount, flushPromises } from '@vue/test-utils'
import { defineComponent } from 'vue'
import { afterEach, expect, it, vi } from 'vitest'
import WorkspacePlanDialog from './WorkspacePlanDialog.vue'
import type { Plan, PlanInput, WorkspaceMeta, WorkspaceRequest } from '../../api/workspace'
const meta: WorkspaceMeta = { synthetic: false, write_enabled: true, approval_required: false, permissions: { read: true, topic_manage: true, applications_manage: true, acl_manage: true, audit_read: true, team_manage: true } }
const input: PlanInput = { kind: 'topic_create', topic: 'events', partitions: 1, replication_factor: 1, retention_ms: 86400000 }
const plan: Plan = { input, fingerprint: 'exact-observed-fingerprint', application_revision: 0, additions: [], existing: [], removals: [], before: {}, after: { topic: 'events' } }
const request = (status: string, revision = 1): WorkspaceRequest => ({ id: 'req-1', revision, status, plan, created_by: 'actor', approved_by: '', results: [], created_at: '2026-10-20T00:00:00Z' })
const dialog = defineComponent({ props: { modelValue: Boolean }, template: '<div v-if="modelValue"><slot/><slot name="footer"/></div>' })
const options = { props: { clusterId: 'lab', meta, input, actorId: 'actor' }, global: { stubs: { ElDialog: dialog } } }
afterEach(() => vi.unstubAllGlobals())
it.each(['executed', 'execution_unknown'])('requires confirmation, submits fingerprint, executes approved revision and reports %s accurately', async status => {
  let finish!: (r: Response) => void
  const fetcher = vi.fn(async (url: string) => {
    if (String(url).endsWith('/plans')) return new Response(JSON.stringify(plan))
    if (String(url).endsWith('/execute')) return new Promise<Response>(resolve => { finish = resolve })
    return new Response(JSON.stringify(request('approved', 3)))
  }); vi.stubGlobal('fetch', fetcher)
  const w = mount(WorkspacePlanDialog, options)
  await w.get('form').trigger('submit'); await flushPromises()
  expect(w.get('[data-plan-diff]').text()).toContain('events')
  expect(w.get('[data-workspace-submit]').attributes('disabled')).toBeDefined()
  await w.get('[data-workspace-submit]').trigger('click'); expect(fetcher).toHaveBeenCalledTimes(1)
  await w.get('input[type="checkbox"]').setValue(true); await w.get('[data-workspace-submit]').trigger('click'); await flushPromises()
  const calls = fetcher.mock.calls as unknown as [string, RequestInit][]
  expect(JSON.parse(calls[1]![1].body as string)).toMatchObject({ ...input, fingerprint: plan.fingerprint })
  await w.get('[data-workspace-execute]').trigger('click'); await flushPromises()
  expect(w.get('[data-workspace-execute]').attributes('disabled')).toBeDefined()
  await w.get('[data-workspace-execute]').trigger('click'); expect(fetcher).toHaveBeenCalledTimes(3)
  expect(JSON.parse(calls[2]![1].body as string)).toEqual({ revision: 3 })
  const done = { ...request(status, 4), results: [{ state: status === 'executed' ? 'verified' : 'unknown', code: status === 'executed' ? '' : 'TIMEOUT' }] }
  finish(new Response(JSON.stringify(done))); await flushPromises()
  expect(w.emitted('completed')).toEqual([[done]])
  const result = w.get('[data-operation-result]').text()
  if (status === 'executed') expect(result).toContain('执行成功')
  else { expect(result).toContain('结果待确认'); expect(result).not.toContain('执行成功'); expect(result).toContain('TIMEOUT') }
  w.unmount()
})
it('keeps pending requests separate from execution and prevents self approval', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url: string) => new Response(JSON.stringify(String(url).endsWith('/plans') ? plan : request('pending_approval')))))
  const w = mount(WorkspacePlanDialog, { ...options, props: { ...options.props, meta: { ...meta, approval_required: true } } })
  await w.get('form').trigger('submit'); await flushPromises(); await w.get('input[type="checkbox"]').setValue(true)
  expect(w.get('[data-workspace-submit]').text()).toBe('提交审批')
  await w.get('[data-workspace-submit]').trigger('click'); await flushPromises()
  expect(w.text()).toContain('等待审批人'); expect(w.find('[data-workspace-execute]').exists()).toBe(false)
  expect(w.findAll('button').some(b => b.text() === '批准变更')).toBe(false); expect(w.emitted('completed')).toBeUndefined()
  w.unmount()
})
it('discards late preview after switching cluster and invalidates confirmation after editing', async () => {
  let finish!: (r: Response) => void
  const fetcher = vi.fn(async (url: string) => String(url).includes('/old/') ? new Promise<Response>(resolve => { finish = resolve }) : new Response(JSON.stringify(plan)))
  vi.stubGlobal('fetch', fetcher)
  const w = mount(WorkspacePlanDialog, { ...options, props: { ...options.props, clusterId: 'old' } })
  await w.get('form').trigger('submit'); await flushPromises(); await w.setProps({ clusterId: 'new' })
  finish(new Response(JSON.stringify({ ...plan, after: { topic: 'old-private-topic' } }))); await flushPromises()
  expect(w.find('[data-plan-diff]').exists()).toBe(false); expect(w.text()).not.toContain('old-private-topic')
  await w.get('form').trigger('submit'); await flushPromises(); await w.get('input[type="checkbox"]').setValue(true)
  await w.get('[name="topic_name"]').setValue('changed-events')
  expect(w.find('[data-plan-diff]').exists()).toBe(false); expect(w.find('[data-workspace-submit]').exists()).toBe(false)
  w.unmount()
})
it('does not enable execution when writes are disabled', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url: string) => new Response(JSON.stringify(String(url).endsWith('/plans') ? plan : request('approved')))))
  const w = mount(WorkspacePlanDialog, { ...options, props: { ...options.props, meta: { ...meta, write_enabled: false } } })
  await w.get('form').trigger('submit'); await flushPromises(); await w.get('input[type="checkbox"]').setValue(true)
  await w.get('[data-workspace-submit]').trigger('click'); await flushPromises()
  expect(w.get('[data-workspace-execute]').attributes('disabled')).toBeDefined()
  w.unmount()
})
