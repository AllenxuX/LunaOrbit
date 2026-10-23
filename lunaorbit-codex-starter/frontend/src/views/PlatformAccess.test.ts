import { mount, flushPromises } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import { createMemoryHistory, createRouter } from 'vue-router'
import PlatformAccess from './PlatformAccess.vue'

afterEach(() => vi.unstubAllGlobals())
it('validates query selection and resets team drafts while ignoring late cluster permission responses', async () => {
  let finish!: (value: Response) => void
  const fetcher = vi.fn(async (url: string) => {
    if (url.endsWith('/session')) return new Response(JSON.stringify({ user_id: 'owner' }))
    if (url.includes('/permissions')) return url.includes('/private-id/') ? new Promise<Response>(resolve => { finish = resolve }) : new Response(JSON.stringify({ can_manage_team: true }))
    if (url.includes('team-grants')) return new Response(JSON.stringify({ items: [] }))
    return new Response(JSON.stringify({ items: [{ id: 'private-id', display_name: '源集群' }, { id: 'second-id', display_name: '目标集群' }], total: 2 }))
  })
  vi.stubGlobal('fetch', fetcher)
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/access-control', component: PlatformAccess }] })
  await router.push('/access-control?cluster=unknown-private-id'); await router.isReady()
  const w = mount(PlatformAccess, { global: { plugins: [router] } }); await flushPromises()
  expect(w.text()).not.toContain('unknown-private-id')
  await w.get('[name=access_cluster]').setValue('second-id'); await flushPromises()
  await w.get('[data-subject]').setValue('second-cluster-draft')
  finish(new Response(JSON.stringify({ can_manage_team: false }))); await flushPromises()
  expect(w.get('[data-subject]').element).toHaveProperty('value', 'second-cluster-draft')
  await w.get('[name=access_cluster]').setValue('private-id'); await flushPromises()
  expect(w.find('[data-subject]').exists()).toBe(false)
  finish(new Response(JSON.stringify({ can_manage_team: true }))); await flushPromises()
  expect(w.get('[data-subject]').element).toHaveProperty('value', '')
  expect(w.text()).not.toContain('private-id'); w.unmount()
})
it('does not query team memberships when the cluster permission denies team management', async () => {
  const fetcher = vi.fn(async (url: string) => new Response(JSON.stringify(url.endsWith('/session') ? { user_id: 'viewer' } : url.includes('/permissions') ? { can_manage_team: false } : { items: [{ id: 'one', display_name: '受限集群' }], total: 1 })))
  vi.stubGlobal('fetch', fetcher)
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/access-control', component: PlatformAccess }] })
  await router.push('/access-control?cluster=one'); await router.isReady()
  const w = mount(PlatformAccess, { global: { plugins: [router] } }); await flushPromises()
  expect(w.text()).toContain('当前身份没有此集群的团队授权管理权限')
  expect(fetcher.mock.calls.some(([url]) => url.includes('team-grants'))).toBe(false); w.unmount()
})
