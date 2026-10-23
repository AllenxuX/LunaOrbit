import { buildPresentation } from '../src/topology/presentation'
import { expect, test } from '@playwright/test'

test.skip(process.env.LUNAORBIT_LAB_E2E !== '1', 'Explicit synthetic laboratory required')
test.use({ baseURL: 'http://127.0.0.1:8088', trace: 'off', screenshot: 'off', video: 'off' })

test('a fresh session opening topology first confirms all four configured clusters', async ({ page }) => {
  await page.goto('/topology')
  await expect(page.locator('.x6-node')).toHaveCount(8)
  await expect(page.locator('.x6-edge')).toHaveCount(8)
  await page.setViewportSize({width:1150,height:850})
  await page.screenshot({path:'test-results/continuous-mm2-overview.png',fullPage:true})
  const raw=(await (await page.request.get('/api/v1/discovery/scopes/readonly-lab/snapshot?page_size=100')).json()).snapshot
  const ab=raw.links.find((l:{source_alias:string;target_alias:string})=>l.source_alias==='source'&&l.target_alias==='target')
  const bc=raw.links.find((l:{source_alias:string;target_alias:string})=>l.source_alias==='target'&&l.target_alias==='archive')
  const source=raw.edges.find((e:{link_id:string;relation:string})=>e.link_id===ab.id&&e.relation==='declared_input').source
  const middle=raw.edges.find((e:{link_id:string;relation:string})=>e.link_id===ab.id&&e.relation==='declared_output').target
  const target=raw.edges.find((e:{link_id:string;relation:string})=>e.link_id===bc.id&&e.relation==='declared_output').target
  await expect(page.locator(`[data-cell-id="${middle}"]`)).toHaveCount(1)
  await page.locator('[data-path-source]').selectOption(source)
  await page.locator('[data-path-target]').selectOption(target)
  await page.locator('[data-path-choice]').selectOption(JSON.stringify([ab.id,bc.id]))
  await expect(page.locator('[data-path-task]')).toHaveCount(2)
  await expect(page.locator('.x6-node')).toHaveCount(5)
  await expect(page.locator('.x6-edge')).toHaveCount(4)
  await page.setViewportSize({width:1150,height:850})
  await expect(async()=>{
    let previousRight=-Infinity
    for(const id of [source,'task:'+ab.id,middle,'task:'+bc.id,target]){
      const box=await page.locator(`[data-cell-id="${id}"]`).boundingBox()
      expect(box).not.toBeNull();expect(box!.width).toBeGreaterThan(110);expect(box!.x).toBeGreaterThan(previousRight)
      previousRight=box!.x+box!.width
    }
  }).toPass()
  await page.screenshot({path:'test-results/continuous-mm2-real.png',fullPage:true})
  await page.locator(`[data-path-task="${bc.id}"]`).click()
  await expect(page.locator('[data-link-summary]')).toContainText('实验目标集群')
  await page.getByRole('button',{name:'关闭链路详情'}).click()
  await page.locator('[data-clear-path]').click()
  await page.locator('[data-mode=instances]').click()
  await expect(page.locator('[data-mm2-instance]')).toHaveCount(2)
  await expect(page.locator('[data-mm2-task]')).toHaveCount(4)
  await expect(page.locator('.path-arrow')).toHaveCount(8)
  await page.setViewportSize({width:1150,height:850})
  await page.screenshot({path:'test-results/mm2-topology-real.png',fullPage:true})
  const path=page.locator('[data-mm2-task]').filter({has:page.locator('[data-source]',{hasText:'实验目标集群'})})
  await expect(path.locator('[data-target]')).toContainText('实验归档集群 C')
  await path.locator('[data-mm2-select]').click()
  await expect(page.locator('[data-link-summary]')).toContainText('实验目标集群')
  await page.getByRole('button',{name:'关闭链路详情'}).click()
  await page.locator('[data-mode=relations]').click()
  await expect(page.locator('[data-link]')).toHaveCount(4)
  await expect(page.locator('.x6-node')).toHaveCount(4)
  await expect(page.locator('.x6-edge')).toHaveCount(4)
  await expect(page.locator('.x6-edge-label')).toHaveCount(0)
  await page.setViewportSize({width:1150,height:850})
  const host=page.locator('.topology-graph-host')
  await expect(async()=>{
    const area=await host.boundingBox()
    for(const node of await page.locator('.x6-node').all()){
      const b=await node.boundingBox();expect(b!.x).toBeGreaterThanOrEqual(area!.x);expect(b!.x+b!.width).toBeLessThanOrEqual(area!.x+area!.width)
    }
  }).toPass()
  await page.screenshot({path:'test-results/four-cluster-relations.png',fullPage:true})
  const rows=(await (await page.request.get('/api/v1/kafka/clusters?page_size=100')).json()).items as {display_name:string;room:string}[]
  await page.getByLabel('机房筛选',{exact:true}).selectOption(rows.find(c=>c.display_name==='实验源集群')!.room)
  await expect(page.locator('.x6-node').filter({hasText:'其他机房'})).toHaveCount(2)
  await page.screenshot({path:'test-results/four-cluster-external-room.png',fullPage:true})
  await page.getByLabel('机房筛选',{exact:true}).selectOption('')
  await page.locator('[data-mode=tasks]').click()
  await expect(page.locator('.x6-node')).toHaveCount(0)
  await expect(page.locator('[data-mm2-instance]')).toHaveCount(0)
  await expect(page.locator('[data-task-row]')).toHaveCount(4)
  await page.screenshot({path:'test-results/four-cluster-deployment.png',fullPage:true})
  await page.locator('[data-mode=relations]').click()
  await expect(page.locator('.x6-node')).toHaveCount(4)
  const response=await page.request.get('/api/v1/discovery/scopes/readonly-lab/snapshot?page_size=100')
  const active=(await response.json()).snapshot.links.filter((link:{enabled:boolean})=>link.enabled)
  expect(active).toHaveLength(4)
  expect(active.every((link:{source:{status:string};target:{status:string}})=>link.source.status==='confirmed'&&link.target.status==='confirmed')).toBe(true)
})

test('Kafka resources open independent detail pages and the real mesh contains fan-out, fan-in and a two-hop path', async ({ page }) => {
  test.setTimeout(90000)
  const errors: string[] = []
  page.on('pageerror', e => errors.push(e.message))
  await page.goto('/kafka/clusters')
  await expect(page.getByText('实验源集群', { exact: true }).first()).toBeVisible()
  const clusters = (await (await page.request.get('/api/v1/kafka/clusters?page_size=100')).json()).items as {id:string;display_name:string}[]
  expect(clusters).toHaveLength(4)
  const source = clusters.find(c => c.display_name === '实验源集群')!, target = clusters.find(c => c.display_name === '实验目标集群')!
  await page.goto(`/kafka/clusters/${source.id}/details?tab=brokers`)
  await page.locator('[data-broker]').first().click()
  await expect(page.locator('.broker-overview')).toBeVisible()
  await page.getByRole('button', { name: '配置', exact: true }).click()
  const config = page.locator('.resource-configs tr').filter({ has: page.locator('[data-edit-config="min.insync.replicas"]') })
  await expect(config.locator('.config-current-value')).not.toHaveText(/未知|未设置|Kafka 未返回/)
  await expect(page.locator('.resource-configs tr').filter({ has: page.locator('[data-edit-config="log.retention.ms"]') }).locator('.config-current-value')).toContainText('log.retention.hours')
  await page.screenshot({ path: 'test-results/broker-effective-config.png', fullPage: true })
  await page.setViewportSize({ width: 390, height: 844 })
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
  await page.screenshot({ path: 'test-results/broker-mobile-config.png', fullPage: true })
  await page.setViewportSize({ width: 1500, height: 1050 })
  await page.reload()
  await expect(config).toBeVisible()
  await page.locator('.broker-breadcrumb').getByRole('link', { name: 'Brokers', exact: true }).click()
  await page.getByRole('link', { name: '集群默认配置', exact: true }).click()
  await expect(page.getByRole('heading', { name: '集群默认配置', exact: true }).first()).toBeVisible()
  await expect(page.locator('thead')).toContainText('集群覆盖值')
  await expect(page.locator('.resource-configs')).not.toContainText('未知')

  await page.goto(`/kafka/clusters/${target.id}/details?tab=groups`)
  const group = page.locator('[data-group]').first()
  await expect(group).toBeVisible()
  await group.click()
  await expect(page.locator('.group-summary')).toBeVisible()
  await page.reload()
  await expect(page.locator('.group-tabs')).toBeVisible()
  await page.locator('.group-breadcrumb').getByRole('link', { name: '消费组', exact: true }).click()
  await expect(page.locator('[data-group]').first()).toBeVisible()

  await page.goto(`/kafka/clusters/${target.id}/details?tab=applications&access=accounts`)
  const account = page.locator('.account-table tbody tr td:first-child a').first()
  await expect(account).toBeVisible()
  await account.click()
  await expect(page).toHaveURL(/\/accounts\//)
  await expect(page.locator('[data-reveal-account]')).toHaveCount(1)
  await page.reload()
  await expect(page.locator('[data-reveal-account]')).toHaveCount(1)
  await page.locator('.access-breadcrumb').getByRole('link', { name: '账号', exact: true }).click()
  await expect(page).toHaveURL(/access=accounts/)
  await expect(page.locator('.account-table tbody tr')).toHaveCount(3)

  // Application metadata only: no SCRAM credential or ACL mutation.
  const base = `/api/v1/kafka/clusters/${target.id}/workspace`
  const create = await page.request.post(base + '/apps', { headers: { Origin: 'http://127.0.0.1:8088' }, data: { name: '合成详情导航应用', owner: '实验验证', principal: 'User:synthetic-navigation' } })
  expect(create.status()).toBe(200)
  const app = await create.json()
  try {
    await page.goto(`/kafka/clusters/${target.id}/details?tab=applications`)
    await page.getByRole('link', { name: '合成详情导航应用', exact: true }).click()
    await expect(page).toHaveURL(/\/applications\//)
    await expect(page.locator('.applications-workspace')).toContainText('合成详情导航应用')
    await page.reload()
    await expect(page.locator('.applications-workspace')).toContainText('合成详情导航应用')
  } finally {
    const removed = await page.request.delete(`${base}/apps/${app.id}?revision=${app.revision}`, { headers: { Origin: 'http://127.0.0.1:8088' } })
    expect(removed.status()).toBe(204)
  }

  await page.goto('/topology?view=relations')
  await expect(page.locator('[data-link]')).toHaveCount(4)
  const response = await page.request.get('/api/v1/discovery/scopes/readonly-lab/snapshot?page_size=100')
  const snapshot = (await response.json()).snapshot
  const active = snapshot.links.filter((l:{enabled:boolean}) => l.enabled)
  expect(active.map((l:{source_alias:string;target_alias:string}) => `${l.source_alias}->${l.target_alias}`).sort()).toEqual(['source->archive','source->target','target->archive','upstream->target'])
  expect(snapshot.nodes.filter((n:{kind:string}) => n.kind === 'kafka')).toHaveLength(4)
  expect(active.every((l:{source:{status:string};target:{status:string}}) => l.source.status === 'confirmed' && l.target.status === 'confirmed')).toBe(true)
  await expect(page.locator('.evidence-panel')).toHaveCount(0)
  await expect.poll(() => page.locator('.topology-graph-host').evaluate(host => {
    const area=host.getBoundingClientRect()
    return [...host.querySelectorAll('.x6-node')].every(node => {const box=node.getBoundingClientRect();return box.left>=area.left&&box.right<=area.right&&box.top>=area.top&&box.bottom<=area.bottom})
  })).toBe(true)
  await page.screenshot({ path: 'test-results/four-cluster-topology.png', fullPage: true })
  await page.locator('[data-link-detail]').first().click()
  await expect(page.locator('.sync-endpoints')).toBeVisible()
  await page.reload()
  await expect(page.locator('.sync-topics')).toBeVisible()
  await page.locator('.sync-breadcrumb a').click()
  await expect(page.locator('[data-link]')).toHaveCount(4)
  expect(errors).toEqual([])
})


test('actual MM2 topology remains stable under small and cross-layer dragging',async({page})=>{
 const errors:string[]=[];page.on('pageerror',e=>errors.push(e.message))
 await page.goto('/topology');await expect(page.locator('.x6-node')).toHaveCount(8)
 await expect(page.locator('.layout-notice')).toHaveCount(0)
 const raw=(await (await page.request.get('/api/v1/discovery/scopes/readonly-lab/snapshot?page_size=100')).json()).snapshot
 const link=raw.links.find((l:{source_alias:string;target_alias:string})=>l.source_alias==='source'&&l.target_alias==='target')
 const id=raw.edges.find((e:{link_id:string;relation:string})=>e.link_id===link.id&&e.relation==='declared_input').source
 const node=page.locator(`[data-cell-id="${id}"]`),viewport=page.locator('.x6-graph-svg-viewport')
 await node.scrollIntoViewIfNeeded();const transform=await viewport.getAttribute('transform')
 await node.evaluate(n=>n.setAttribute('data-stability','original'))
 for(const delta of [10,30,100,280,-280]){
  const box=await node.boundingBox();expect(box).not.toBeNull()
  await page.mouse.move(box!.x+box!.width/2,box!.y+box!.height/2);await page.mouse.down();await page.mouse.move(box!.x+box!.width/2+delta,box!.y+box!.height/2+12,{steps:12});await page.mouse.up()
  expect(await node.getAttribute('data-stability')).toBe('original');expect(await viewport.getAttribute('transform')).toBe(transform)
  await expect(page.locator('.dirty-dot')).toHaveCount(1)
  expect(await page.locator('.layout-notice').allTextContents()).not.toEqual(expect.arrayContaining([expect.stringMatching(/无法|不可用/)]))
  await expect.poll(()=>page.locator('.topology-graph-host').evaluate(host=>{
   const boxes=[...host.querySelectorAll('.x6-node')].map(n=>n.getBoundingClientRect());let total=0
   for(const e of host.querySelectorAll('.x6-edge')){const p=e.querySelector('path:last-child') as SVGPathElement|null,m=p?.getScreenCTM();if(!p||!m)continue;for(let d=0;d<=p.getTotalLength();d+=3){const point=p.getPointAtLength(d).matrixTransform(m);if(boxes.some(b=>point.x>b.left+3&&point.x<b.right-3&&point.y>b.top+3&&point.y<b.bottom-3))total++}}
   return total
  })).toBe(0)
 }
 await page.screenshot({path:'test-results/stable-routing-real-drag.png',fullPage:true})
 // Leave the persisted personal layout unchanged.
 await page.reload();await expect(page.locator('.x6-node')).toHaveCount(8);await expect(page.locator('.dirty-dot')).toHaveCount(0)
 expect(errors).toEqual([])
})

test('default fan-in arrows follow source order without inverted target ports',async({page})=>{
 await page.goto('/topology');await expect(page.locator('.x6-node')).toHaveCount(8)
 await expect(page.locator('.layout-notice')).toHaveCount(0)
 const raw=(await (await page.request.get('/api/v1/discovery/scopes/readonly-lab/snapshot?page_size=100')).json()).snapshot
 const edges=buildPresentation(raw,'chain').edges
 const geometry=await page.locator('.x6-edge').evaluateAll(items=>items.map(el=>{const path=el.querySelector('path:last-child') as SVGPathElement;return {id:el.getAttribute('data-cell-id'),start:path.getPointAtLength(0).y,end:path.getPointAtLength(path.getTotalLength()).y}}))
 let pairs=0
 for(const target of new Set(edges.map(e=>e.target))){const incoming=edges.filter(e=>e.target===target).map(e=>geometry.find(g=>g.id===e.id)!).sort((a,b)=>a.start-b.start);for(let i=1;i<incoming.length;i++){pairs++;expect(incoming[i-1]!.end).toBeLessThan(incoming[i]!.end)}}
 expect(pairs).toBeGreaterThanOrEqual(2)
 await page.screenshot({path:'test-results/optimized-fanin-real.png',fullPage:true})
})
