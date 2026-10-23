import {expect,test} from '@playwright/test'
test('Kafka read-only details expose partition RF/ISR and unknown offsets with classified failures',async({page})=>{
 await page.goto('/kafka/clusters');await expect(page.getByRole('button',{name:'新增 Kafka 集群'})).toBeEnabled()
 const created=await page.request.post('/api/v1/kafka/clusters',{data:{display_name:'Synthetic read-only E2E',bootstrap_servers:['broker.synthetic.test:9092'],enabled:true,security_protocol:'PLAINTEXT',sasl_mechanism:'',tls_server_name:''}})
 expect(created.status()).toBe(201);const cluster=await created.json()
 await page.goto(`/kafka/clusters/${cluster.id}/read-only`)
 await expect(page.getByRole('heading',{name:/Kafka 只读详情/})).toContainText('Synthetic read-only E2E')
 await page.getByRole('button',{name:'orders',exact:true}).click();await expect(page.getByRole('heading',{name:/orders 分区/})).toContainText('RF 3')
 await page.getByRole('button',{name:'payments',exact:true}).click();await expect(page.locator('[data-offsets]')).toContainText('25');await expect(page.locator('[data-offsets]')).toContainText('未知')
 await page.locator('.demo-settings summary').click()
 await page.getByLabel('Mock 场景').selectOption('authorization_denied');await expect(page.getByRole('alert')).toContainText('未获授权')
 await page.getByLabel('Mock 场景').selectOption('unsupported_api');await expect(page.getByRole('alert')).toContainText('API 不支持')
 expect((await page.request.delete(`/api/v1/kafka/clusters/${cluster.id}`,{data:{revision:cluster.revision}})).status()).toBe(204)
})
