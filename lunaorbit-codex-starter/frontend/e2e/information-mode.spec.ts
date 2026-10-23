import { expect, test } from '@playwright/test'

test('information workspace hides governance, avoids sessions, and shares application records', async ({ page }) => {
  const requests: string[] = []
  let items: Record<string, unknown>[] = []
  const runtime = { mode: 'information', synthetic: false, information_mode: true, workspace_id: 'shared-e2e', write_enabled: false, capabilities: { discovery: false, kafka_reads: true, acl_reads: false, changes: false, observability: false } }
  await page.route('**/api/**', async route => {
    const request = route.request(), url = new URL(request.url())
    if (!url.pathname.startsWith('/api/')) return route.continue()
    requests.push(`${request.method()} ${url.pathname}`)
    if (url.pathname === '/api/runtime') return route.fulfill({ json: runtime })
    if (url.pathname === '/api/health') return route.fulfill({ json: { status: 'ok' } })
    if (url.pathname === '/api/v1/kafka/clusters') return route.fulfill({ json: { items: [{ id: 'cluster-a', display_name: 'East Kafka', enabled: true, bootstrap_servers: ['kafka:9092'], capabilities: [] }], total: 1, page: 1, page_size: 20 } })
    if (url.pathname === '/api/v1/information/applications') {
      if (request.method() === 'POST') { const item = { ...request.postDataJSON(), id: 'app-a', revision: 1 }; items = [item]; return route.fulfill({ status: 201, json: item }) }
      return route.fulfill({ json: { items, total: items.length, page: 1, page_size: 20 } })
    }
    if (url.pathname === '/api/v1/information/applications/app-a') {
      if (request.method() === 'DELETE') { expect(url.searchParams.get('revision')).toBe('2'); items = []; return route.fulfill({ status: 204 }) }
      const item = { ...request.postDataJSON(), id: 'app-a', revision: 2 }; items = [item]; return route.fulfill({ json: item })
    }
    return route.fulfill({ status: 404, json: {} })
  })
  await page.goto('/applications')
  await expect(page.getByRole('heading', { name: '应用信息', exact: true })).toBeVisible()
  await expect(page.locator('.account-link')).toHaveCount(0)
  for (const path of ['/security', '/access-control', '/changes']) await expect(page.locator(`a[href="${path}"]`)).toHaveCount(0)
  await page.getByRole('button', { name: '新增应用' }).click()
  await page.getByLabel('应用名称', { exact: true }).fill('Orders producer')
  await page.getByLabel('负责人', { exact: true }).fill('订单团队')
  await page.getByRole('button', { name: '添加关联' }).click()
  await page.getByLabel('关联集群').selectOption('cluster-a')
  await page.getByLabel('Topic', { exact: true }).fill('orders')
  await page.getByRole('button', { name: '保存应用' }).click()
  await expect(page.getByRole('table')).toContainText('Orders producer')
  await expect(page.getByRole('table')).toContainText('人工登记')
  await page.reload()
  await expect(page.getByRole('table')).toContainText('Orders producer')
  await page.getByRole('button', { name: '编辑', exact: true }).click()
  await page.getByLabel('应用名称', { exact: true }).fill('Orders updated')
  await page.getByRole('button', { name: '保存应用' }).click()
  await expect(page.getByRole('table')).toContainText('Orders updated')
  await page.getByRole('button', { name: '删除', exact: true }).click()
  await page.getByRole('button', { name: '确认删除' }).click()
  await expect(page.getByRole('table')).not.toContainText('Orders updated')
  await page.goto('/security')
  await expect(page).toHaveURL(/\/applications$/)
  expect(requests.some(path => /session|auth|acl|team|operation/.test(path))).toBe(false)
})

test('legacy deep links cannot expose account tabs or resource mutations in information mode', async ({ page }) => {
  const requests: string[] = []
  await page.route('**/api/**', async route => {
    const url = new URL(route.request().url())
    if (!url.pathname.startsWith('/api/')) return route.continue()
    requests.push(url.pathname)
    if (url.pathname === '/api/runtime') return route.fulfill({ json: { mode: 'information', synthetic: false, information_mode: true, workspace_id: 'shared', write_enabled: false, capabilities: { discovery: false, kafka_reads: true, acl_reads: false, changes: false, observability: false } } })
    if (url.pathname.endsWith('/workspace/meta')) return route.fulfill({ json: { synthetic: false, write_enabled: false, permissions: { read: true, topic_manage: false } } })
    if (url.pathname.endsWith('/workspace/resource-configs')) return route.fulfill({ json: { entries: [{ name: 'retention.ms', value: '1000', read_only: false, source: 'DYNAMIC_TOPIC_CONFIG', synonyms: [] }] } })
    if (url.pathname.endsWith('/topics/orders')) return route.fulfill({ json: { item: { name: 'orders', internal: false, partitions: [], rf_min: 1, rf_max: 1 } } })
    if (url.pathname === '/api/v1/kafka/clusters/one') return route.fulfill({ json: { id: 'one', display_name: 'East Kafka' } })
    return route.fulfill({ json: { items: [], total: 0, page: 1, page_size: 20 } })
  })
  await page.goto('/kafka/clusters/one/details?tab=teams')
  const tabs = page.getByRole('navigation', { name: '集群工作台' })
  await expect(tabs).toBeVisible()
  await expect(tabs.getByRole('button')).toHaveCount(5)
  await expect(page.getByText('团队授权现属于平台访问控制。')).toHaveCount(0)
  await page.goto('/kafka/clusters/one/topics/orders?tab=delete')
  await expect(page.getByRole('navigation', { name: 'Topic 详情' }).getByRole('button')).toHaveCount(2)
  await expect(page.locator('[data-delete-topic], [data-expand-topic]')).toHaveCount(0)
  await page.getByRole('button', { name: '配置', exact: true }).click()
  await expect(page.locator('.resource-configs')).toContainText('1 秒')
  await expect(page.locator('[data-edit-config], .editable-filter')).toHaveCount(0)
  await page.goto('/kafka/clusters/one/brokers/default?tab=configs')
  await expect(page.locator('.resource-configs')).toContainText('1 秒')
  await expect(page.getByRole('navigation', { name: 'Broker 详情' }).getByRole('button')).toHaveCount(2)
  await expect(page.locator('[data-edit-config]')).toHaveCount(0)
  expect(requests.some(path => /session|auth|acl|team|operation/.test(path))).toBe(false)
})
