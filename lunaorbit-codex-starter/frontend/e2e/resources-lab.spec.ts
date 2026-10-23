import { randomUUID } from 'node:crypto'
import { expect, test } from '@playwright/test'

test.skip(process.env.LUNAORBIT_LAB_E2E !== '1', 'Explicit synthetic laboratory required')
test.use({ baseURL: 'http://127.0.0.1:8088', actionTimeout:15000 })
const headers = { Origin: 'http://127.0.0.1:8088' }

test('native resource configuration, replica migration and deletion on both lab Kafka versions', async ({ page }) => {
  test.setTimeout(240000)
  const pageErrors:string[]=[]
  page.on('pageerror',error=>pageErrors.push(error.message))
  await page.goto('/kafka/clusters')
  await expect(page.getByRole('button',{name:'新增 Kafka 集群'})).toBeEnabled()
  await expect(page.getByText('实验源集群',{exact:true}).first()).toBeVisible()
  expect((await (await page.request.get('/api/runtime')).json()).mode).toBe('admin-lab')
  const clusters = (await (await page.request.get('/api/v1/kafka/clusters')).json()).items as {id:string;display_name:string}[]
  const selected = clusters.filter(c => ['实验源集群', '实验目标集群'].includes(c.display_name))
  expect(selected).toHaveLength(2)
  for (const cluster of selected) {
    console.info('Verifying synthetic cluster:',cluster.display_name)
    await page.goto(`/kafka/clusters/${cluster.id}/details?tab=brokers`)
    const firstBroker=page.locator('[data-broker]').first()
    await expect(firstBroker).toBeVisible()
    await firstBroker.click()
    await expect(page.locator('.broker-overview')).toBeVisible()
    await page.reload()
    await expect(page.locator('.broker-overview')).toBeVisible()
    await page.getByRole('button',{name:'配置',exact:true}).click()
    await expect(page.locator('.resource-configs')).toBeVisible()
    await page.locator('.broker-breadcrumb').getByRole('link',{name:'Brokers',exact:true}).click()
    await expect(page.locator('[data-broker]').first()).toBeVisible()
    const base = `/api/v1/kafka/clusters/${cluster.id}/workspace`
    const topic = `lunaorbit-resources-${Date.now()}`
    async function post(path:string, data:unknown) {
      const response = await page.request.post(base + path, { headers, data, timeout:35000 })
      if (!response.ok()) console.info('Synthetic resource failure',path,JSON.stringify(data),await response.text())
      expect(response.ok(), `${path}: ${await response.text()}`).toBe(true)
      return response.json()
    }
    async function operate(intent:unknown) {
      const plan = await post('/resource-plans', intent)
      let op = await post('/resource-operations', { intent, fingerprint:plan.fingerprint, request_id:randomUUID() })
      expect(op.status).toBe('approved')
      op = await post(`/resource-operations/${op.id}/execute`, { revision:op.revision })
      expect(op.status).toBe('observing')
      await expect.poll(async () => {
        op = await post(`/resource-operations/${op.id}/refresh`, { revision:op.revision })
        return op.status
      }, { timeout:60000, intervals:[500,1000,2000] }).toBe('succeeded')
      return op
    }
    async function configs(type:string,name:string) {
      const r = await page.request.get(`${base}/resource-configs?${new URLSearchParams({resource_type:type,resource_name:name})}`)
      expect(r.ok(), await r.text()).toBe(true)
      return (await r.json()).entries as {name:string;value:string|null;source:string;sensitive:boolean;read_only:boolean;synonyms:{value:string|null}[]}[]
    }
    // Existing creation workflow is deliberately reused; no account/ACL changes.
    const input = {kind:'topic_create',topic,partitions:1,replication_factor:1}
    const createPlan = await post('/plans', input)
    const request = await post('/requests', {...input,fingerprint:createPlan.fingerprint})
    expect((await post(`/requests/${request.id}/execute`,{revision:request.revision})).status).toBe('executed')
    try {
      const resource = {type:'TOPIC',name:topic}
      const original = (await configs('TOPIC',topic)).find(e=>e.name==='retention.ms')!
      expect(original.source).not.toBe('DYNAMIC_TOPIC_CONFIG')
      await operate({kind:'configs',resource,changes:[{name:'retention.ms',operation:'SET',value:'172800000'}]})
      expect((await configs('TOPIC',topic)).find(e=>e.name==='retention.ms')).toMatchObject({value:'172800000',source:'DYNAMIC_TOPIC_CONFIG'})
      await operate({kind:'configs',resource,changes:[{name:'retention.ms',operation:'DELETE'}]})
      expect((await configs('TOPIC',topic)).find(e=>e.name==='retention.ms')).toMatchObject({value:original.value,source:original.source})
      for (const name of ['', '1', ...(cluster.display_name==='实验源集群'?['2']:[])]) {
        const before = await configs('BROKER',name)
        const entry = before.find(e=>e.name==='log.retention.ms')!
        expect(entry).toBeTruthy()
        const override = name?'DYNAMIC_BROKER_CONFIG':'DYNAMIC_DEFAULT_BROKER_CONFIG'
        try {
          await operate({kind:'configs',resource:{type:'BROKER',name},changes:[{name:entry.name,operation:'SET',value:'604800001'}]})
          expect((await configs('BROKER',name)).find(e=>e.name===entry.name)).toMatchObject({value:'604800001',source:override})
        } finally {
          await operate({kind:'configs',resource:{type:'BROKER',name},changes:[entry.source===override?{name:entry.name,operation:'SET',value:entry.value}:{name:entry.name,operation:'DELETE'}]})
        }
        const sensitive = before.filter(e=>e.sensitive)
        expect(sensitive.length).toBeGreaterThan(0)
        expect(sensitive.every(e=>e.value===null&&e.synonyms.every(s=>s.value===null))).toBe(true)
        const readonly = before.find(e=>e.read_only&&!e.sensitive)!
        expect(readonly).toBeTruthy()
        expect((await page.request.post(base+'/resource-plans',{headers,data:{kind:'configs',resource:{type:'BROKER',name},changes:[{name:readonly.name,operation:'SET',value:readonly.value??'x'}]}})).ok()).toBe(false)
      }
      if (cluster.display_name==='实验源集群') {
        await operate({kind:'reassign',resource,assignments:[{partition:0,replicas:[1,2]}]})
        await operate({kind:'reassign',resource,assignments:[{partition:0,replicas:[2]}]})
        await operate({kind:'reassign',resource,assignments:[{partition:0,replicas:[1]}]})
      }
      await page.goto(`/kafka/clusters/${cluster.id}/details?tab=topics`)
      await page.getByRole('textbox',{name:'搜索 Topic',exact:true}).fill(topic)
      await page.getByRole('button',{name:'查询',exact:true}).click()
      await page.locator(`[data-topic="${topic}"]`).click()
      await expect(page).toHaveURL(new RegExp('/topics/'+topic+'$'))
      await expect(page.getByRole('heading',{name:topic,exact:true})).toBeVisible()
      await expect(page.locator('.topic-partitions tbody tr')).toHaveCount(1)
      await page.reload()
      await expect(page.locator('.topic-partitions tbody tr')).toHaveCount(1)
      await page.goBack()
      await expect(page.locator(`[data-topic="${topic}"]`)).toBeVisible()
      await page.locator(`[data-topic="${topic}"]`).click()
      await page.getByRole('button',{name:'配置',exact:true}).click()
      await expect(page.getByRole('button',{name:'常用配置',exact:true})).toHaveAttribute('aria-pressed','true')
      await page.getByRole('button',{name:/^全部配置/}).click()
      await expect(page.locator('[data-edit-config="segment.jitter.ms"]')).toBeVisible()
      await page.getByRole('button',{name:'常用配置',exact:true}).click()
      await expect(page.locator('[data-edit-config="retention.ms"]')).toBeEnabled()
      await page.locator('.config-filters input').first().fill('retention.ms')
      await expect(page.locator('.resource-configs > .table-scroll tbody tr').first()).toBeVisible()
      expect(await page.locator('.resource-configs > .table-scroll [data-edit-config]').evaluateAll(elements=>elements.every(e=>e.getAttribute('data-edit-config')?.includes('retention.ms')))).toBe(true)
      await page.locator('[data-edit-config="retention.ms"]').click()
      await page.getByRole('textbox',{name:'新配置值'}).fill('259200000')
      await page.getByRole('button',{name:'预览差异并校验'}).click()
      await page.getByRole('checkbox',{name:'我已核对目标资源和配置差异'}).check()
      await expect(page.getByRole('dialog').filter({visible:true})).toBeVisible()
      await page.screenshot({path:`test-results/topic-editor-${cluster.display_name}.png`,fullPage:true})
      await page.getByRole('button',{name:'提交变更申请',exact:true}).click()
      const operations = page.locator('.resource-operations')
      await operations.locator('tbody tr').filter({hasText:topic}).filter({hasText:'已批准，待执行'}).first().getByRole('button',{name:'查看'}).click()
      await operations.getByRole('checkbox',{name:'我已核对范围，确认执行'}).check()
      await operations.getByRole('button',{name:'执行变更',exact:true}).click()
      await expect(operations.locator('.operation-detail')).toContainText('已受理，等待实际完成')
      await operations.getByRole('button',{name:'检查结果',exact:true}).click()
      await expect(operations.locator('.operation-detail')).toContainText('执行成功')
      expect((await configs('TOPIC',topic)).find(e=>e.name==='retention.ms')?.value).toBe('259200000')
      await page.getByRole('button',{name:'配置',exact:true}).click()
      await expect(page.locator('[data-edit-config="retention.ms"]')).toBeVisible()
      await page.screenshot({path:`test-results/topic-configs-${cluster.display_name}.png`,fullPage:true})
      await page.setViewportSize({width:390,height:844})
      await expect.poll(()=>page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true)
      await page.screenshot({path:`test-results/topic-mobile-${cluster.display_name}.png`,fullPage:true})
      await expect(page.locator('[data-edit-config="retention.ms"]')).toBeInViewport()
      await page.locator('[data-edit-config="retention.ms"]').click()
      await expect(page.getByRole('dialog').filter({visible:true})).toBeVisible()
      await expect(page.getByRole('textbox',{name:'新配置值'})).toBeInViewport()
      await page.getByRole('textbox',{name:'新配置值'}).fill('259200000')
      await page.screenshot({path:`test-results/topic-editor-mobile-${cluster.display_name}.png`,fullPage:true,animations:'disabled'})
      await page.keyboard.press('Escape')
      await expect(page.getByRole('dialog').filter({visible:true})).toHaveCount(0)
      await page.setViewportSize({width:1500,height:1050})
    } finally {
      await operate({kind:'topic_delete',resource:{type:'TOPIC',name:topic},confirmation_name:topic})
    }
  }
  expect(pageErrors).toEqual([])
})
