import { expect, test } from '@playwright/test'

test.skip(process.env.LUNAORBIT_LAB_E2E !== '1', 'Explicit isolated laboratory required')
test.use({ baseURL: 'http://127.0.0.1:8088', trace: 'off', screenshot: 'off', video: 'off' })

test('discovered Kafka accounts explain missing passwords and offer a prefilled input on desktop and mobile', async ({ page }) => {
  expect((await (await page.request.get('/api/runtime')).json()).mode).toBe('admin-lab')
  await page.goto('/kafka/clusters')
  await expect(page.getByText('实验目标集群', { exact: true }).first()).toBeVisible()
  const clusters = (await (await page.request.get('/api/v1/kafka/clusters')).json()).items as { id: string; display_name: string }[]
  const cluster = clusters.find(c => c.display_name === '实验目标集群')!
  expect(Boolean(cluster)).toBe(true)
  let revealCalls = 0
  page.on('request', request => { if (request.url().endsWith('/reveal')) ++revealCalls })
  await page.goto(`/kafka/clusters/${cluster.id}/details?tab=applications`)
  await page.getByRole('button', { name: '账号', exact: true }).click()
  const rows = page.locator('.account-table tbody tr').filter({ has: page.locator('[data-reveal-account]') })
  await expect(rows).toHaveCount(3)
  for (const row of await rows.all()) {
    await expect(row).toContainText('未保存密码')
    await expect(row.locator('[data-reveal-account]')).toBeVisible()
    await expect(row.locator('[data-reveal-account]')).toBeDisabled()
    await expect(row.locator('[data-import-password]')).toBeEnabled()
  }
  await expect(page.locator('main')).not.toContainText('托管')
  await page.screenshot({ path: 'test-results/account-password-desktop.png', fullPage: true })
  await rows.first().locator('[data-import-password]').click()
  await expect(page.locator('[name=account_username]')).not.toHaveValue('')
  await expect(page.locator('[name=account_application]')).not.toHaveAttribute('required')
  await expect(page.locator('[name=account_password]')).toHaveValue('')
  await page.locator('[data-account-form]').getByRole('button', { name: '取消', exact: true }).click()
  await page.setViewportSize({ width: 390, height: 844 })
  await expect(rows.first().locator('[data-reveal-account]')).toBeVisible()
  await expect(rows.first().locator('[data-import-password]')).toBeVisible()
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
  await page.screenshot({ path: 'test-results/account-password-mobile.png', fullPage: true })
  expect(revealCalls).toBe(0)
})
