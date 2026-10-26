import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
vi.mock('../composables/useRuntime', async () => { const { ref } = await import('vue'); return { useRuntime: () => ({ runtime: ref({ mode: 'readonly-lab', information_mode: true, workspace_id: 'w', capabilities: { discovery: true } }) }) } })
import Clusters from './Clusters.vue'

afterEach(() => vi.unstubAllGlobals())
const cluster = { id: 'c1', revision: 1, display_name: 'luna-k1', bootstrap_servers: ['k1:9092'], enabled: true, security_protocol: 'SASL_SSL', sasl_mechanism: 'PLAIN', status: 'unverified', version_reported: '3.7', version_source: 'manual', capabilities: [], credentials_configured: true }

it('auto discovery hides every management entry but keeps the data readable', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url: string) => new Response(JSON.stringify(url.endsWith('/session') ? { user_id: 'u', mode: 'information', persistent: true } : { items: [cluster], total: 1, page: 1, page_size: 20 }))))
  const w = mount(Clusters, { global: { stubs: { RouterLink: { props: ['to'], template: '<a :href="String(to)"><slot /></a>' } } } })
  await flushPromises()
  expect(w.text()).toContain('luna-k1')
  expect(w.text()).toContain('只读详情')
  for (const label of ['新增 Kafka 集群', '编辑', '连接测试', '停用', '启用', '删除', '授权']) expect(w.findAll('button').some(b => b.text() === label)).toBe(false)
  expect(w.text()).toContain('集群由 MM2 配置自动发现')
  w.unmount()
})
