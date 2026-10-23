import { expect, test } from '@playwright/test'

test.skip(process.env.LUNAORBIT_LAB_E2E !== '1', 'Explicit isolated lab tunnel required')
test.use({ baseURL: 'http://127.0.0.1:8088' })
test.beforeEach(async ({ request }) => {
  const runtime = await (await request.get('/api/runtime')).json()
  test.skip(runtime.mode !== 'admin-lab', 'Requires the explicitly authorized management deployment')
})

test('single enabled direction hides disabled reverse edges until explicitly inspected', async ({ page }) => {
  await page.goto('/topology?view=relations')
  await expect(page.locator('[data-task-row]').filter({hasText:'实验源集群'}).filter({hasText:'实验目标集群'}).locator('[data-link]')).toBeVisible()
  const response = await page.request.get('/api/v1/discovery/scopes/readonly-lab/snapshot?page_size=100')
  const links = (await response.json()).snapshot.links
  const enabledCount=links.filter((link:{state:string})=>link.state!=='disabled').length
  await expect(page.locator('[data-link]')).toHaveCount(enabledCount)
  await expect(page.locator('.x6-edge')).toHaveCount(enabledCount)
  expect(links.filter((link: { state: string }) => link.state === 'disabled')).toHaveLength(1)
  await page.locator('.topology-more summary').click()
  await page.locator('[data-show-disabled]').check()
  await expect(page.locator('[data-link]')).toHaveCount(links.length)
  await expect(page.locator('.x6-edge')).toHaveCount(links.length)
  await page.locator('[data-show-disabled]').uncheck()
  await expect(page.locator('[data-link]')).toHaveCount(enabledCount)
  await expect(page.locator('.x6-edge')).toHaveCount(enabledCount)
  await page.locator('.topology-more summary').click()
  await page.locator('[data-task-row]').filter({hasText:'实验源集群'}).filter({hasText:'实验目标集群'}).locator('[data-link]').click()
  await expect(page.locator('[data-link-summary]')).toContainText('实验源集群')
  await expect(page.locator('[data-link-summary]')).toContainText('实验目标集群')
  const active = links.find((link: {source_alias:string;target_alias:string}) => link.source_alias==='source'&&link.target_alias==='target')
  for (const mapping of [active.source, active.target]) {
    await expect(page.locator('body')).not.toContainText(mapping.registration_id)
    await expect(page.locator('body')).not.toContainText(mapping.cluster_id)
  }
  await expect(page.locator('[data-diagnostics]')).not.toHaveAttribute('open')
  await page.screenshot({ path: 'test-results/console-lab-topology.png', fullPage: true })
})

test('new owner sees real Kafka mapped to MM2 and can execute an audited lab change', async ({ page }) => {
  test.setTimeout(90000)
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await page.goto('/topology?view=relations')
  await expect(page.locator('.environment-badge')).toHaveText('实验环境 · 管理操作已开放')
  await expect(page.locator('[data-link]').first()).toBeVisible({ timeout: 15000 })
  const response = await page.request.get('/api/v1/discovery/scopes/readonly-lab/snapshot?page_size=100')
  expect(response.status()).toBe(200)
  const snapshot = (await response.json()).snapshot
  const forward = snapshot.links.find((link: { source_alias: string; target_alias: string }) => link.source_alias === 'source' && link.target_alias === 'target')
  expect(forward.source.status).toBe('confirmed')
  expect(forward.target.status).toBe('confirmed')
  expect(forward.source.cluster_id).not.toBe(forward.target.cluster_id)
  expect(forward.verified).toBe(false)
  await page.locator('[data-link]').filter({ hasText: '实验源集群 → 实验目标集群' }).click()
  await expect(page.locator('.evidence-panel')).toContainText('实验源集群')
  await expect(page.locator('.evidence-panel')).toContainText('实验目标集群')
  await expect(page.locator('[data-link-summary]')).not.toContainText(forward.source.cluster_id)
  await expect(page.locator('[data-link-summary]')).not.toContainText(forward.source.registration_id)
  await page.screenshot({ path: 'test-results/console-lab-topology.png', fullPage: true })
  await page.goto('/changes')
  await expect(page.locator('[data-change-form]')).toBeVisible()
  const original = forward.topics_raw as string
  const changed = `(${original})|lunaorbit-ui-${Date.now()}`
  // Test the ordinary UI through preview, submit, approval and actual execution.
  try {
  await page.locator('input[name="topic_rule"]').fill(changed)
  await page.getByRole('button', { name: '脱敏预览', exact: true }).click()
  await expect(page.locator('[data-preview]')).toBeVisible()
  await page.locator('[data-submit]').click()
  await expect(page.locator('[data-approve]').first()).toBeEnabled()
  await page.locator('[data-approve]').first().click()
  await expect(page.locator('[data-execute]').first()).toBeEnabled()
  await page.locator('[data-execute]').first().click()
  await expect(page.locator('[data-detail]')).toContainText('需要人工复核')
  const changes = (await (await page.request.get('/api/v1/changes')).json()).items
  const executed = changes.find((item: { topic_rule: string }) => item.topic_rule === changed)
  expect(executed.synthetic).toBe(false)
  expect(executed.evidence.generation).toBeGreaterThan(1)
  expect(executed.evidence.replication_healthy).toBeNull()
  } finally {
  // Restore the exact original rule using the same authenticated, audited API.
  // A restore failure fails this test and remains visible in the change records.
  const targets = (await (await page.request.get('/api/v1/changes/targets')).json()).items
  let restore = await page.request.post('/api/v1/changes', { data: { target_id: targets[0].id, topic_rule: original, expected_resource_version: targets[0].resource_version, preview: false } })
  expect(restore.status()).toBe(200)
  let row = await restore.json()
  restore = await page.request.post(`/api/v1/changes/${row.id}/approve`, { data: { revision: row.revision } })
  expect(restore.status()).toBe(200); row = await restore.json()
  restore = await page.request.post(`/api/v1/changes/${row.id}/execute`, { data: { revision: row.revision, scenario: 'success' } })
  expect(restore.status()).toBe(200)
  expect((await restore.json()).status).toBe('manual_review')
  }
  await page.screenshot({ path: 'test-results/console-lab-changes.png', fullPage: true })
  const registrations = (await (await page.request.get('/api/v1/kafka/clusters')).json()).items
  await page.goto(`/kafka/clusters/${registrations[0].id}/security`)
  await expect(page.locator('[data-acl-request]')).toBeVisible()
  const aclResponse = await page.request.get(`/api/v1/kafka/clusters/${registrations[0].id}/acls`)
  // Older synthetic fixtures deliberately omitted an authorizer. Newer lab
  // fixtures exercise positive ACL writes in workspace-lab.spec.ts instead.
  if (aclResponse.status() === 501) {
  expect((await aclResponse.json()).error.code).toBe('AUTHORIZER_UNVERIFIED')
  await expect(page.getByRole('alert')).toContainText('authorizer')
  await page.locator('input[name="principal"]').fill('User:lunaorbit-synthetic-acl-probe')
  await page.locator('input[name="resource_name"]').fill('lunaorbit-lab-events')
  await page.getByRole('button', { name: '预览申请', exact: true }).click()
  await expect(page.locator('[data-preview]')).toBeVisible()
  await page.locator('[data-submit-request]').click()
  await expect(page.locator('[data-acl-approve]').last()).toBeEnabled()
  await page.locator('[data-acl-approve]').last().click()
  await expect(page.locator('[data-acl-execute]').last()).toBeEnabled()
  await page.locator('[data-acl-execute]').last().click()
  await expect(page.getByRole('alert')).toContainText('authorizer')
  await expect(page.getByText('实际执行失败，请检查 Broker authorizer 与权限', { exact: false })).toBeVisible()
  } else {
    expect(aclResponse.status()).toBe(200)
    expect(Array.isArray((await aclResponse.json()).items)).toBe(true)
  }
  expect((await (await page.request.get('/api/runtime')).json()).write_enabled).toBe(true)
  expect(errors).toEqual([])
})

test('another fresh mobile owner receives its own Kafka registrations and topology', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 })
  await page.goto('/topology?view=relations')
  await expect(page.locator('[data-link]').first()).toBeVisible({ timeout: 15000 })
  const menu = page.getByRole('button', { name: '打开导航菜单', exact: true })
  await menu.click()
  await page.getByRole('navigation', { name: '主导航', exact: true }).getByRole('link', { name: 'Kafka 集群' }).click()
  await expect(menu).toHaveAttribute('aria-expanded', 'false')
  await expect(page.locator('.data-table')).toContainText('实验源集群')
  await expect(page.locator('.data-table')).toContainText('实验目标集群')
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 1)).toBe(false)
  await page.screenshot({ path: 'test-results/console-lab-mobile.png', fullPage: true })
})
