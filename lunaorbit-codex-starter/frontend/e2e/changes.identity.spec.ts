import {expect,test} from '@playwright/test'
test.skip(process.env.LUNAORBIT_IDENTITY_E2E!=='1','Requires isolated identity and changes Mock server on loopback18080')
test('independent approval precedes synthetic mutation and missing replication evidence requires review',async({page})=>{
 await page.route('**/api/v1/**',async route=>{const url=new URL(route.request().url());url.port='18080';await route.fulfill({response:await route.fetch({url:url.toString()})})})
 async function login(user:string){await page.goto('/security');await page.locator('input[name=username]').fill(user);await page.locator('input[name=password]').fill('synthetic-identity-test-password');await page.locator('[data-login]').getByRole('button',{name:'登录',exact:true}).click();await expect(page.getByRole('status')).toContainText('平台身份已更新')}
 async function logout(){expect((await page.request.post('http://127.0.0.1:18080/api/v1/auth/logout',{headers:{Origin:'http://127.0.0.1:5173'},data:{}})).status()).toBe(204)}
 await login('synthetic-admin');await page.goto('/topology');await expect(page.locator('[data-discover]')).toBeEnabled();await page.locator('[data-discover]').click();await expect(page.locator('[data-link]')).toHaveCount(3)
 await page.goto('/changes');await expect(page.locator('select[name=target_id] option')).toHaveCount(1)
 await page.locator('input[name=topic_rule]').fill('synthetic.events.*');await page.getByRole('button',{name:'脱敏预览',exact:true}).click();await expect(page.locator('[data-preview]')).toContainText('未提交')
 await page.locator('[data-submit]').click();await expect(page.locator('[data-detail]')).toContainText('等待审批');await expect(page.locator('[data-execute]').first()).toBeDisabled();await expect(page.locator('[data-approve]').first()).toBeDisabled()
 await logout();await login('synthetic-approver');await page.goto('/changes');await expect(page.locator('[data-approve]').first()).toBeEnabled();await page.locator('[data-approve]').first().click();await expect(page.locator('[data-detail]')).toContainText('已批准，尚未执行')
 await logout();await login('synthetic-admin');await page.goto('/changes');await page.locator('.demo-settings summary').click();await page.getByLabel('合成执行场景').selectOption('metrics_missing');await expect(page.locator('[data-execute]').first()).toBeEnabled();await page.locator('[data-execute]').first().click()
 await expect(page.locator('[data-detail]')).toContainText('需要人工复核');await expect(page.locator('[data-detail]')).toContainText('复制健康：未知');await expect(page.locator('[data-detail]')).not.toContainText('DO_NOT_EXPOSE')
})
