import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import Security from './Security.vue'
afterEach(() => { vi.unstubAllGlobals(); sessionStorage.clear(); localStorage.clear() })
function setup() {
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    const path = String(input).split('?')[0]!
    if (path.endsWith('/auth/login')) return new Response('sensitive server error', { status: 401 })
    if (path.endsWith('/session')) return new Response(JSON.stringify({ user_id: 'synthetic-operator', mode: 'mock', roles: ['operator'], persistent: false }))
    if (path.endsWith('/acl-requests')) return new Response(JSON.stringify({ items: [], total: 0, page: 1, page_size: 100, can_approve: false }))
    if (path.endsWith('/acls')) return new Response(JSON.stringify({ items: [{ principal: 'User:synthetic', host: '*', resource_type: 'TOPIC', name: 'demo', pattern_type: 'LITERAL', operation: 'READ', permission: 'ALLOW', source_registration_id: 'test' }], total: 1, page: 1, page_size: 20, synthetic: true, state: 'synthetic' }))
    return new Response(JSON.stringify({ display_name: 'Synthetic registration' }))
  }))
  return mount(Security, { props: { id: 'test' } })
}
it('clears login password on failure and never persists it or creates a replacement mock login', async () => {
  const wrapper = setup(); await flushPromises()
  await wrapper.get('input[name="username"]').setValue('entered-user')
  await wrapper.get('input[name="password"]').setValue('entered-secret')
  const before = vi.mocked(fetch).mock.calls.length
  await wrapper.get('[data-login]').trigger('submit'); await flushPromises()
  expect((wrapper.get('input[name="password"]').element as HTMLInputElement).value).toBe('')
  expect(wrapper.text()).toContain('认证')
  expect(wrapper.text()).not.toContain('sensitive server error')
  expect(vi.mocked(fetch).mock.calls.slice(before).some(c => String(c[0]).includes('/dev/session'))).toBe(false)
  expect(sessionStorage.length + localStorage.length).toBe(0)
  wrapper.unmount()
})
it('marks synthetic ACLs and separates platform identity from Kafka principal', async () => {
  const wrapper = setup(); await flushPromises()
  expect(wrapper.text()).toContain('synthetic-operator')
  expect(wrapper.text()).toContain('Mock')
  expect(wrapper.text()).toContain('LDAP 登录与 Kafka 集群认证分别授权')
  expect(wrapper.text()).toContain('未验证真实 Kafka')
  expect(wrapper.text()).toContain('User:synthetic')
  wrapper.unmount()
})
it('previews before submission and never presents approval as an executed ACL', async () => {
  const wrapper = setup(); await flushPromises()
  const fetcher = vi.mocked(fetch)
  const original = fetcher.getMockImplementation()!
  const aclRule = { principal: 'User:synthetic-applicant', host: '*', resource_type: 'TOPIC', resource_name: 'demo', pattern_type: 'LITERAL', operation: 'READ', permission: 'ALLOW' }
  fetcher.mockImplementation(async (input, init) => {
    if (String(input).endsWith('/acl-requests') && init?.method === 'POST') {
      const body = JSON.parse(String(init.body))
      return new Response(JSON.stringify({ id: 'request-1', applicant: 'synthetic-operator', revision: 1, rule: aclRule, status: body.preview ? 'preview' : 'pending_approval', can_approve: true }))
    }
    return original(input, init)
  })
  await wrapper.get('input[name="principal"]').setValue(aclRule.principal)
  await wrapper.get('input[name="resource_name"]').setValue('demo')
  await wrapper.get('[data-acl-request]').trigger('submit'); await flushPromises()
  expect(wrapper.get('[data-preview]').text()).toContain('尚未提交')
  await wrapper.get('[data-submit-request]').trigger('click'); await flushPromises()
  expect(wrapper.text()).toContain('等待审批，未执行')
  const approveButton = wrapper.findAll('button').find(b => b.text() === '独立审批')!
  expect(approveButton.attributes('disabled')).toBeDefined()
  expect(wrapper.text()).not.toContain('ACL 已生效')
  wrapper.unmount()
})
it('never queries ACLs for a forbidden registration', async () => {
  vi.stubGlobal('fetch', vi.fn(async (input: string) => String(input).endsWith('/session') ? new Response(JSON.stringify({ user_id: 'limited', mode: 'mock' })) : new Response('private upstream', { status: 403 })))
  const wrapper = mount(Security, { props: { id: 'forbidden' } }); await flushPromises()
  expect(wrapper.get('[role="alert"]').text()).toContain('授权')
  expect(vi.mocked(fetch).mock.calls.some(c => String(c[0]).includes('/acls'))).toBe(false)
  expect(wrapper.text()).not.toContain('private upstream')
  wrapper.unmount()
})
it('discards an old registration response when the cluster changes', async () => {
  let completeOld!: (response: Response) => void
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    const url = String(input)
    if (url.endsWith('/session')) return new Response(JSON.stringify({ user_id: 'synthetic', mode: 'mock' }))
    if (url.endsWith('/old')) return new Promise<Response>(resolve => { completeOld = resolve })
    if (!url.includes('?')) return new Response(JSON.stringify({ display_name: 'New registration' }))
    return new Response(JSON.stringify({ items: [], total: 0, synthetic: true, state: 'pending' }))
  }))
  const wrapper = mount(Security, { props: { id: 'old' } }); await flushPromises()
  await wrapper.setProps({ id: 'new' }); await flushPromises()
  completeOld(new Response(JSON.stringify({ display_name: 'Old registration' }))); await flushPromises()
  expect(wrapper.text()).toContain('New registration')
  expect(wrapper.text()).not.toContain('Old registration')
  expect(vi.mocked(fetch).mock.calls.some(c => String(c[0]).includes('/old/acls'))).toBe(false)
  wrapper.unmount()
})
it('approves only server-authorized inbox requests and labels approved as not executed', async () => {
  const request = { id: 'other-request', source_registration_id: 'other-cluster', applicant: 'different-user', revision: 2, rule: { principal: 'User:reviewed', resource_name: 'demo' }, status: 'pending_approval', can_approve: true }
  vi.stubGlobal('fetch', vi.fn(async (input: string, init?: RequestInit) => {
    const url = String(input)
    if (url.includes('/inbox')) return new Response(JSON.stringify({ items: [request], total: 1, page: 1, page_size: 20 }))
    if (url.endsWith('/approve')) { expect(init?.method).toBe('POST'); expect(JSON.parse(String(init?.body))).toEqual({ revision: 2 }); return new Response(JSON.stringify({ ...request, status: 'approved_not_executed', can_approve: false })) }
    return new Response(JSON.stringify({ user_id: 'reviewer', mode: 'mock', role: 'admin' }))
  }))
  const wrapper = mount(Security); await flushPromises()
  const approve = wrapper.get('[data-inbox] button')
  expect(approve.attributes('disabled')).toBeUndefined()
  await approve.trigger('click'); await flushPromises()
  expect(wrapper.get('[data-inbox]').text()).toContain('approved_not_executed')
  expect(wrapper.get('[data-inbox]').text()).toContain('未执行 Kafka ACL 写入')
  expect(wrapper.get('[data-inbox] button').attributes('disabled')).toBeDefined()
  wrapper.unmount()
})
it('pages through the authorized approval inbox using server totals', async () => {
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    const url = String(input)
    if (url.includes('/inbox')) return new Response(JSON.stringify({ items: [], total: 21, page: new URL(url, 'https://synthetic.test').searchParams.get('page') === '2' ? 2 : 1, page_size: 20 }))
    return new Response(JSON.stringify({ user_id: 'reviewer', mode: 'mock' }))
  }))
  const w = mount(Security); await flushPromises()
  expect(w.get('[data-inbox-total]').text()).toContain('21')
  await w.get('[data-inbox-next]').trigger('click'); await flushPromises()
  expect(vi.mocked(fetch).mock.calls.some(c => String(c[0]).endsWith('/inbox?page=2&page_size=20'))).toBe(true)
  expect(w.get('[data-inbox-next]').attributes('disabled')).toBeDefined()
  await w.get('[data-inbox-prev]').trigger('click'); await flushPromises()
  expect(w.get('[data-inbox-prev]').attributes('disabled')).toBeDefined()
  w.unmount()
})
