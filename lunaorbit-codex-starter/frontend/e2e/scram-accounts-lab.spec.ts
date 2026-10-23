import { expect, test } from '@playwright/test'

test.skip(process.env.LUNAORBIT_LAB_E2E !== '1', 'Explicit synthetic laboratory required')
// Credential viewing must never produce a Playwright trace, screenshot or video.
test.use({ baseURL: 'http://127.0.0.1:8088', trace: 'off', screenshot: 'off', video: 'off' })

test('real SCRAM account provisioning, controlled reveal, application sharing and native ACL preview', async ({ page }) => {
  test.setTimeout(150000)
  const runtime = await (await page.request.get('/api/runtime')).json()
  expect(runtime.mode).toBe('admin-lab')
  await page.goto('/kafka/clusters')
  await expect(page.getByRole('button', { name: '新增 Kafka 集群' })).toBeEnabled()
  const clusters = (await (await page.request.get('/api/v1/kafka/clusters')).json()).items as { id: string; display_name: string }[]
  const cluster = clusters.find(row => row.display_name === '实验源集群')
  expect(Boolean(cluster)).toBe(true)
  const base = `/api/v1/kafka/clusters/${cluster!.id}/workspace`
  const suffix = Date.now(), appName = `SCRAM浏览器实验-${suffix}`, secondAppName = `共享身份实验-${suffix}`
  const group = `lunaorbit-scram-browser-${suffix}`
  await page.goto(`/kafka/clusters/${cluster!.id}/details?tab=applications`)
  await page.locator('[data-onboard-application]').click()
  const form = page.locator('[data-account-form]')
  await form.getByRole('checkbox', { name: '同时添加新应用' }).check()
  await form.locator('[name=new_application_name]').fill(appName)
  await form.locator('[name=new_application_owner]').fill('合成浏览器实验团队')
  await form.locator('[name=account_access_mode]').selectOption('produce_consume')
  await form.locator('[name=account_topics]').fill('lunaorbit-lab-events')
  await form.locator('[name=account_group]').fill(group)
  await form.getByRole('checkbox', { name: '我已确认账号来源与应用关联' }).check()
  const saved = page.waitForResponse(r => r.url().endsWith(`${base}/accounts`) && r.request().method() === 'POST')
  await form.getByRole('button', { name: '创建账号', exact: true }).click()
  const createdResponse = await saved
  expect(createdResponse.ok()).toBe(true)
  const created = (await createdResponse.json()).account as { id: string; username: string; principal: string; application_ids: string[] }
  await expect(form).toHaveCount(0)
  const row = page.getByRole('row').filter({ hasText: created.username })
  await expect(row).toContainText('已批准，待创建')
  await page.getByRole('checkbox', { name: '已核对待执行账号与关联应用' }).check()
  await row.getByRole('button', { name: '创建账号' }).click()
  await expect(row).toContainText('账号已创建', { timeout: 45000 })

  let secret: string
  try {
    await row.locator('[data-reveal-account]').click()
    const panel = page.locator('.password-dialog')
    await expect(panel).toBeVisible()
    secret = await panel.locator('input[aria-label=账号密码]').inputValue()
    // Boolean assertions avoid printing the credential if validation fails.
    expect(secret.length >= 32).toBe(true)
    await panel.getByRole('button', { name: '关闭', exact: true }).click()
    await expect(panel).toHaveCount(0)
    expect(await page.locator('body').evaluate((body, password) => body.textContent?.includes(password) ?? false, secret)).toBe(false)
    const accountResponse = await page.request.get(`${base}/accounts?page=1&page_size=100`)
    const ordinary = await accountResponse.text()
    expect(ordinary.includes(secret)).toBe(false)
    expect(/"(?:password|ciphertext|account_secrets)"\s*:/.test(ordinary)).toBe(false)
    const account = JSON.parse(ordinary).items.find((item: { id: string }) => item.id === created.id)
    expect(account.operation.status).toBe('executed')
    const aclResponse = await page.request.get(`${base}/acls?${new URLSearchParams({ principal: created.principal, page_size: '100' })}`)
    const acls = (await aclResponse.json()).items as { resource_type: string; resource_name: string; operation: string; permission: string; host: string }[]
    expect(acls.map(r => `${r.resource_type}:${r.resource_name}:${r.operation}:${r.permission}:${r.host}`).sort()).toEqual([
      `GROUP:${group}:READ:ALLOW:*`, 'TOPIC:lunaorbit-lab-events:DESCRIBE:ALLOW:*', 'TOPIC:lunaorbit-lab-events:READ:ALLOW:*', 'TOPIC:lunaorbit-lab-events:WRITE:ALLOW:*',
    ].sort())
  } finally {
    const clear = page.locator('.password-dialog').getByRole('button', { name: '关闭', exact: true })
    if (await clear.count()) await clear.click()
    secret = ''
  }
  expect(secret.length).toBe(0)

  // Sharing uses reference mode: it must neither replace the managed password nor write SCRAM/ACLs.
  await page.locator('[data-new-account]').click()
  await form.getByRole('checkbox', { name: '同时添加新应用' }).check()
  await form.locator('[name=new_application_name]').fill(secondAppName)
  await form.locator('[name=new_application_owner]').fill('合成共享应用团队')
  await form.locator('[name=account_mode]').selectOption('reference')
  await form.locator('[name=account_username]').fill(created.username)
  await form.getByRole('checkbox', { name: '我已确认账号来源与应用关联' }).check()
  await form.getByRole('button', { name: '保存账号', exact: true }).click()
  await expect(form).toHaveCount(0)
  await expect(row).toContainText(secondAppName)
  const shared = (await (await page.request.get(`${base}/accounts?page=1&page_size=100`)).json()).items.filter((item: { username: string }) => item.username === created.username)
  expect(shared.length).toBe(1)
  expect(shared[0].application_ids.length).toBe(2)
  expect(shared[0].credential_state).toBe('managed')

  await page.getByRole('button', { name: '权限规则', exact: true }).click()
  const nativeForm = page.locator('[data-add-native]')
  await nativeForm.locator('[name=acl_principal]').fill(created.principal)
  await nativeForm.locator('[name=acl_resource]').fill('lunaorbit-lab-events')
  await nativeForm.locator('[name=acl_operation]').selectOption('READ')
  await nativeForm.locator('[name=acl_permission]').selectOption('DENY')
  await nativeForm.getByRole('button', { name: '预览添加规则', exact: true }).click()
  const dialog = page.getByRole('dialog').filter({ visible: true })
  await dialog.getByRole('button', { name: '生成授权差异', exact: true }).click()
  await expect(dialog).toContainText('DENY')
  await expect(dialog).toContainText(created.principal)
  await dialog.getByRole('button', { name: '取消', exact: true }).click()
  const unchanged = (await (await page.request.get(`${base}/acls?${new URLSearchParams({ principal: created.principal, page_size: '100' })}`)).json()).items
  expect(unchanged.length).toBe(4)
  expect(unchanged.some((r: { permission: string }) => r.permission === 'DENY')).toBe(false)
})
