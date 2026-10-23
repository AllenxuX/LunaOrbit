import { expect, test } from '@playwright/test'

test.use({baseURL:process.env.LUNAORBIT_LAB_E2E==='1'?'http://127.0.0.1:8088':'http://127.0.0.1:5173'})

test('platform team access selects named clusters and clears the prior cluster draft', async ({ page }) => {
  await page.goto('/access-control?cluster=unrecognized-private-id')
  const selector = page.locator('[name=access_cluster]')
  await expect(selector).toBeEnabled()
  await expect(page.locator('h1')).toHaveText('平台访问控制')
  await expect(page.locator('body')).not.toContainText('unrecognized-private-id')
  const options = await selector.locator('option').evaluateAll(rows => rows.map(row => ({ value: row.value, label: row.textContent })))
  expect(options.length).toBeGreaterThan(0)
  const subject = page.locator('[data-subject]')
  await expect(subject).toBeVisible()
  await subject.fill('draft-for-first-cluster')
  if (options.length > 1) {
    await selector.selectOption(options[1]!.value)
    await expect(subject).toHaveValue('')
    await expect(page).toHaveURL(new RegExp(`cluster=${encodeURIComponent(options[1]!.value)}`))
  } else {
    await page.getByRole('button', { name: '刷新集群', exact: true }).click()
    await expect(subject).toHaveValue('')
  }
  const clusterId = await selector.inputValue()
  await page.goto(`/kafka/clusters/${encodeURIComponent(clusterId)}/details?tab=teams`)
  await expect(page.getByRole('navigation', { name: '集群工作台' }).getByRole('button', { name: '团队授权', exact: true })).toHaveCount(0)
  await page.getByRole('link', { name: '前往平台访问控制', exact: true }).click()
  await expect(page.locator('h1')).toHaveText('平台访问控制')
  await expect(selector).toHaveValue(clusterId)
})
