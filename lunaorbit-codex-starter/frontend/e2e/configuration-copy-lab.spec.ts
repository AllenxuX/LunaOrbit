import { expect, test } from '@playwright/test'

test.skip(process.env.LUNAORBIT_LAB_E2E !== '1', 'Explicit synthetic laboratory required')
test.use({baseURL:'http://127.0.0.1:8088'})

test('Topic and Broker configuration explanations are Chinese without raw Kafka HTML',async({page})=>{
  await page.goto('/kafka/clusters')
  await expect(page.getByText('实验源集群',{exact:true}).first()).toBeVisible()
  const clusters=(await(await page.request.get('/api/v1/kafka/clusters')).json()).items as {id:string;display_name:string}[]
  const cluster=clusters.find(c=>c.display_name==='实验源集群')!
  expect(cluster).toBeTruthy()
  const topics=(await(await page.request.get(`/api/v1/kafka/clusters/${cluster.id}/topics?query=lunaorbit-lab-events&page_size=100`)).json()).items as {name:string}[]
  const topic=topics.find(t=>t.name==='lunaorbit-lab-events')!
  expect(topic).toBeTruthy()
  const brokers=(await(await page.request.get(`/api/v1/kafka/clusters/${cluster.id}/brokers?page_size=100`)).json()).items as {id:number}[]
  expect(brokers.length).toBeGreaterThan(0)
  for(const broker of [false,true]){
    await page.goto(broker?`/kafka/clusters/${cluster.id}/brokers/${brokers[0]!.id}?tab=configs`:`/kafka/clusters/${cluster.id}/topics/${encodeURIComponent(topic.name)}?tab=configs`)
    const row=page.locator('.resource-configs tbody tr').filter({has:page.locator('[data-edit-config="min.insync.replicas"]')})
    await row.locator('summary').click()
    await expect(row.locator('.config-documentation')).toContainText('同步副本数不能低于此值')
    await expect(row.locator('.config-documentation')).toContainText('acks=all')
    await page.getByRole('button',{name:/^全部配置/}).click()
    const paragraphs=page.locator('.config-documentation>p')
    expect(await paragraphs.count()).toBeGreaterThan(0)
    expect(await paragraphs.evaluateAll(nodes=>nodes.every(node=>/[\u3400-\u9fff]/.test(node.textContent??'')&&!/<\/?(?:code|br|p|ul|li)\b|When a producer/.test(node.textContent??'')))).toBe(true)
    await expect(page.locator('.resource-configs')).not.toContainText('When a producer')
    await page.screenshot({path:`test-results/config-chinese-${broker?'broker':'topic'}.png`,fullPage:true})
  }
})
