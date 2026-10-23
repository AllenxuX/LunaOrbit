import { expect, test, type Page } from '@playwright/test'

test.skip(process.env.LUNAORBIT_LAB_E2E !== '1', 'Explicit synthetic laboratory required')
test.use({ baseURL: 'http://127.0.0.1:8088' })

async function executeDialog(page: Page, acl = false) {
  const dialog = page.getByRole('dialog').filter({ visible: true })
  await dialog.getByRole('button', { name: acl ? '生成授权差异' : '预览变更', exact: true }).click()
  await dialog.getByRole('checkbox').check()
  await dialog.locator('[data-workspace-submit]').click()
  await expect(dialog.locator('[data-operation-result]')).toContainText('已批准')
  await dialog.locator('[data-workspace-execute]').click()
  await expect(dialog.locator('[data-operation-result]')).toContainText('执行成功', { timeout: 35000 })
  await dialog.getByRole('button', { name: '关闭', exact: true }).click()
}

test('real Kafka workbench Topic changes and application ACL lifecycle', async ({ page }) => {
  test.setTimeout(150000)
  const runtime = await (await page.request.get('/api/runtime')).json()
  expect(runtime.mode).toBe('admin-lab')
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await page.goto('/kafka/clusters')
  await expect(page.getByRole('button', { name: '新增 Kafka 集群' })).toBeEnabled()
  const clusters = (await (await page.request.get('/api/v1/kafka/clusters')).json()).items as { id: string; display_name: string }[]
  const cluster = clusters.find(row => row.display_name === '实验源集群')!
  expect(cluster).toBeTruthy()
  const url = `/kafka/clusters/${cluster.id}/details`
  const base = `/api/v1/kafka/clusters/${cluster.id}/workspace`
  const topic = `lunaorbit-workspace-${Date.now()}`
  const principal = `User:${topic}`
  await page.goto(url + '?tab=topics')
  await page.locator('[data-create-topic]').click()
  let dialog = page.getByRole('dialog').filter({ visible: true })
  await dialog.locator('[name=topic_name]').fill(topic)
  await dialog.locator('[name=partitions]').fill('1')
  await dialog.locator('[name=replication_factor]').fill('1')
  await executeDialog(page)
  await page.getByRole('textbox', { name: '搜索 Topic', exact: true }).fill(topic)
  await page.getByRole('button', { name: '查询', exact: true }).click()
  await page.locator(`[data-topic="${topic}"]`).click()
  await page.locator('[data-expand-topic]').click()
  dialog = page.getByRole('dialog').filter({ visible: true })
  await dialog.locator('[name=partitions]').fill('2')
  await executeDialog(page)
  await page.getByRole('button', { name: '配置', exact: true }).click()
  await page.locator('[data-edit-config="retention.ms"]').click()
  await page.getByRole('textbox', { name: '新配置值' }).fill('172800000')
  await page.getByRole('button', { name: '预览差异并校验' }).click()
  await page.getByRole('checkbox', { name: '我已核对目标资源和配置差异' }).check()
  await page.getByRole('button', { name: '提交变更申请', exact: true }).click()
  const resources=page.locator('.resource-operations')
  await resources.locator('tbody tr').filter({hasText:topic}).filter({hasText:'已批准，待执行'}).first().getByRole('button',{name:'查看'}).click()
  await resources.getByRole('checkbox',{name:'我已核对范围，确认执行'}).check()
  await resources.getByRole('button',{name:'执行变更',exact:true}).click()
  await resources.getByRole('button',{name:'检查结果',exact:true}).click()
  await expect(resources.locator('.operation-detail')).toContainText('执行成功')
  const config = await (await page.request.get(`${base}/topic-configs?topic=${topic}`)).json()
  expect(config.synthetic).toBe(false)
  expect(config.configs).toMatchObject({ partitions: '2', 'retention.ms': '172800000' })
  await page.goto(url + '?tab=applications')
  await page.locator('[data-new-application]').click()
  await page.locator('[name=app_name]').fill('工作台实验应用')
  await page.locator('[name=app_owner]').fill('合成测试团队')
  await page.locator('[name=app_principal]').fill(principal)
  await page.locator('[data-save-application]').click()
  await expect(page.getByRole('dialog').filter({ visible: true })).toHaveCount(0)
  for (const consume of [false, true]) {
    await page.getByRole('button', { name: '授权', exact: true }).click()
    if (consume) {
      await page.getByRole('radio', { name: '消费消息' }).check()
      await page.locator('[name=grant_group]').fill(topic + '-group')
    }
    await page.locator('[name=grant_topics]').fill(topic)
    await page.locator('[data-preview-grant]').click()
    await executeDialog(page, true)
  }
  await page.getByRole('button', { name: '查看权限', exact: true }).click()
  const selected = page.locator('.application-access input[type=checkbox]')
  await expect(selected).toHaveCount(4)
  for (const checkbox of await selected.all()) await checkbox.check()
  await page.getByRole('button', { name: '预览撤销选中权限' }).click()
  await executeDialog(page, true)
  expect((await (await page.request.get(`${base}/acls?principal=${encodeURIComponent(principal)}`)).json()).total).toBe(0)
  await page.goto(url + '?tab=operations')
  await expect(page.locator('.workspace-operations')).toContainText('执行成功')
  await page.screenshot({ path: 'test-results/workspace-lab-operations.png', fullPage: true })
  await page.goto(url + '?tab=topics')
  await expect(page.locator('[data-create-topic]')).toBeVisible()
  await page.screenshot({ path: 'test-results/workspace-lab-topics.png', fullPage: true })
  await page.setViewportSize({ width: 390, height: 844 })
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
  expect(errors).toEqual([])
})
