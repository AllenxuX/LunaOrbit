import { expect,test } from '@playwright/test'

test.skip(process.env.LUNAORBIT_TOPIC_LINEAGE_LAB_E2E!=='1','Requires explicitly authorized dedicated information lab')
test.use({baseURL:'http://127.0.0.1:8088',trace:'off',video:'off'})
const headers={Origin:'http://127.0.0.1:8088'}
interface LNode {id:string;kind:string;cluster_id?:string;topic?:string;label:string;evidence?:string;exists?:boolean|null;application_id?:string;group?:string}
interface LEdge {id:string;source:string;target:string;evidence:string;kind:string}
interface Lineage {root_id:string;nodes:LNode[];edges:LEdge[];issues:{code:string}[];truncated:boolean}

test('actual MM2 paths keep the same Topic name across A B C without merging clusters',async({request})=>{
 const runtime=await (await request.get('/api/runtime')).json();expect(runtime.information_mode).toBe(true)
 const clusters=(await (await request.get('/api/v1/kafka/clusters?page_size=100')).json()).items as {id:string;display_name:string}[]
 const a=clusters.find(c=>c.display_name==='实验源集群')!,b=clusters.find(c=>c.display_name==='实验目标集群')!,c=clusters.find(c=>c.display_name==='实验归档集群 C')!
 const query=new URLSearchParams({cluster_id:a.id,topic:'lunaorbit-lab-events',depth:'2',direction:'both'})
 const response=await request.get('/api/v1/topic-lineage?'+query);expect(response.status()).toBe(200)
 const graph=await response.json() as Lineage
 expect(graph.nodes.find(n=>n.id===graph.root_id)).toMatchObject({kind:'topic',cluster_id:a.id,topic:'lunaorbit-lab-events',exists:true})
 for(const expected of [{cluster_id:b.id,topic:'lunaorbit-lab-events'},{cluster_id:c.id,topic:'lunaorbit-lab-events'}])expect(graph.nodes.find(n=>n.kind==='topic'&&n.cluster_id===expected.cluster_id&&n.topic===expected.topic)).toMatchObject({...expected,exists:true})
 expect(graph.nodes.filter(n=>n.kind==='topic')).toHaveLength(3)
 expect(graph.nodes.some(n=>n.topic?.includes('lunaorbit-mesh-events'))).toBe(false)
 expect(graph.nodes.filter(n=>n.kind==='mm2').length).toBeGreaterThanOrEqual(3)
 expect(graph.edges.filter(e=>e.kind==='replicates_to').every(e=>e.evidence==='configured')).toBe(true)
 expect(graph.truncated).toBe(false)
 expect(graph.issues.filter(i=>['GROUP_EVIDENCE_UNKNOWN','REPLICATION_CYCLE'].includes(i.code))).toEqual([])
 const shallow=await (await request.get('/api/v1/topic-lineage?'+new URLSearchParams({...Object.fromEntries(query),depth:'1'}))).json() as Lineage
 expect(shallow.nodes.filter(n=>n.kind==='mm2')).toHaveLength(2);expect(graph.nodes.filter(n=>n.kind==='mm2')).toHaveLength(3)
 const reverse=await (await request.get('/api/v1/topic-lineage?'+new URLSearchParams({cluster_id:c.id,topic:'lunaorbit-lab-events',depth:'2',direction:'upstream'}))).json() as Lineage
 expect(reverse.nodes.some(n=>n.cluster_id===a.id&&n.topic==='lunaorbit-lab-events')).toBe(true)
})

test('actual graph includes registered producer and consumer apps with manual evidence',async({page})=>{
 const clusters=(await (await page.request.get('/api/v1/kafka/clusters?page_size=100')).json()).items as {id:string;display_name:string}[]
 const a=clusters.find(c=>c.display_name==='实验源集群')!,c=clusters.find(c=>c.display_name==='实验归档集群 C')!
 const input={name:'合成Topic链路应用',owner:'实验验证',environment:'测试',description:'验证人工关系，不代表实际流量',relations:[{cluster_id:a.id,topic:'lunaorbit-lab-events',mode:'produce'},{cluster_id:c.id,topic:'lunaorbit-lab-events',mode:'consume',consumer_group:'synthetic-lineage-recorded-group',account:'synthetic-downstream-user',client_id:'synthetic-downstream-consumer'}]}
 const created=await page.request.post('/api/v1/information/applications',{headers,data:input});expect(created.status()).toBe(201);const app=await created.json()
 const errors:string[]=[];page.on('pageerror',e=>errors.push(e.message))
 try{
  const url='/topic-lineage?'+new URLSearchParams({cluster_id:a.id,topic:'lunaorbit-lab-events',depth:'2',direction:'both'})
  const response=await page.request.get('/api/v1'+url);expect(response.status()).toBe(200);const graph=await response.json() as Lineage
  const appNode=graph.nodes.find(n=>n.application_id===app.id)!;expect(appNode).toBeDefined();expect(appNode.evidence).toBe('manual')
  const producer=graph.edges.find(e=>e.source===appNode.id&&e.target===graph.root_id)!;expect(producer.evidence).toBe('manual')
  const group=graph.nodes.find(n=>n.kind==='consumer_group'&&n.group==='synthetic-lineage-recorded-group')!;expect(group).toBeDefined()
  expect(graph.edges.find(e=>e.source===group.id&&e.target===appNode.id)).toMatchObject({evidence:'manual',account:'synthetic-downstream-user',client_id:'synthetic-downstream-consumer'})
  await page.goto(url);await expect(page.getByRole('heading',{name:'Topic 全链路',exact:true})).toBeVisible()
  await expect.poll(()=>page.locator('.x6-node').count()).toBeGreaterThanOrEqual(6)
  await page.screenshot({path:'test-results/topic-lineage-default-real.png',fullPage:true})
  await page.getByText('节点与关系列表',{exact:true}).click()
  await page.locator(`[data-lineage-node="${appNode.id}"]`).click();await expect(page.getByLabel('链路详情',{exact:true})).toContainText('人工登记')
  await page.screenshot({path:'test-results/topic-lineage-real.png',fullPage:true})
  await page.setViewportSize({width:390,height:844});await expect.poll(()=>page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true)
  await page.screenshot({path:'test-results/topic-lineage-mobile-real.png',fullPage:true})
  expect(errors).toEqual([])
 }finally{expect((await page.request.delete('/api/v1/information/applications/'+app.id+'?revision='+app.revision,{headers})).status()).toBe(204)}
 const after=await (await page.request.get('/api/v1/topic-lineage?'+new URLSearchParams({cluster_id:a.id,topic:'lunaorbit-lab-events',depth:'2',direction:'both'}))).json() as Lineage
 expect(after.nodes.some(n=>n.application_id===app.id)).toBe(false)
})

test('Topic detail enters lineage and upstream query preserves the selected root',async({page})=>{
 const clusters=(await (await page.request.get('/api/v1/kafka/clusters?page_size=100')).json()).items as {id:string;display_name:string}[]
 const a=clusters.find(c=>c.display_name==='实验源集群')!,c=clusters.find(c=>c.display_name==='实验归档集群 C')!
 await page.goto('/kafka/clusters/'+a.id+'/topics/lunaorbit-lab-events')
 await page.getByRole('link',{name:'查看链路',exact:true}).click();await expect(page).toHaveURL(/\/topic-lineage\?/)
 await expect(page.getByLabel('Topic',{exact:true})).toHaveValue('lunaorbit-lab-events')
 await expect.poll(()=>page.locator('.x6-node').count()).toBeGreaterThanOrEqual(6)
 await page.goto('/topic-lineage?'+new URLSearchParams({cluster_id:c.id,topic:'lunaorbit-lab-events',depth:'2',direction:'upstream'}))
 await expect(page.getByLabel('Topic',{exact:true})).toHaveValue('lunaorbit-lab-events')
 await expect.poll(()=>page.locator('.x6-node').count()).toBeGreaterThanOrEqual(5)
 await expect(page.getByRole('link',{name:'身份与权限',exact:true})).toHaveCount(0)
 await page.screenshot({path:'test-results/topic-lineage-upstream-real.png',fullPage:true})
})


test('application identity form persists bindings and exposes account and location in lineage',async({page})=>{
 const clusters=(await (await page.request.get('/api/v1/kafka/clusters?page_size=100')).json()).items as {id:string;display_name:string}[]
 const a=clusters.find(c=>c.display_name==='实验源集群')!
 await page.goto('/applications');await page.getByRole('button',{name:'新增应用',exact:true}).click()
 const dialog=page.getByRole('dialog');await dialog.getByLabel('应用名称',{exact:true}).fill('合成账号Topic绑定验证')
 await dialog.getByLabel('部署位置',{exact:true}).fill('实验机房 / 订单服务')
 await dialog.getByRole('button',{name:'添加关联',exact:true}).click()
 await dialog.getByLabel('关联集群').selectOption(a.id);await dialog.getByLabel('Topic',{exact:true}).fill('lunaorbit-lab-events')
 await dialog.getByLabel('Kafka 账号',{exact:true}).fill('synthetic-order-user')
 await dialog.getByLabel('Client ID',{exact:true}).fill('synthetic-order-producer')
 await expect(dialog.locator('input[type=password]')).toHaveCount(0)
 const saving=page.waitForResponse(r=>r.request().method()==='POST'&&new URL(r.url()).pathname==='/api/v1/information/applications')
 await dialog.getByRole('button',{name:'保存应用',exact:true}).click();const response=await saving;expect(response.status()).toBe(201)
 let app=await response.json()
 try{
  const row=page.locator(`[data-information-application="${app.id}"]`)
  await expect(row).toContainText('synthetic-order-user');await expect(row).toContainText('实验机房 / 订单服务')
  await page.reload();await row.getByRole('button',{name:'编辑',exact:true}).click()
  await expect(dialog.getByLabel('Kafka 账号',{exact:true})).toHaveValue('synthetic-order-user')
  await expect(dialog.getByLabel('Client ID',{exact:true})).toHaveValue('synthetic-order-producer')
  await dialog.getByLabel('部署位置',{exact:true}).fill('实验机房 / 订单服务 v2')
  const editing=page.waitForResponse(r=>r.request().method()==='PATCH'&&new URL(r.url()).pathname.endsWith('/'+app.id))
  await dialog.getByRole('button',{name:'保存应用',exact:true}).click();const updated=await editing;expect(updated.status()).toBe(200);app=await updated.json()
  const params=new URLSearchParams({cluster_id:a.id,topic:'lunaorbit-lab-events',depth:'2',direction:'both'})
  const graph=await (await page.request.get('/api/v1/topic-lineage?'+params)).json()
  const node=graph.nodes.find((n:LNode)=>n.application_id===app.id)
  const edge=graph.edges.find((e:LEdge)=>e.source===node.id&&e.target===graph.root_id)
  expect(node.location).toBe('实验机房 / 订单服务 v2');expect(edge).toMatchObject({account:'synthetic-order-user',client_id:'synthetic-order-producer',evidence:'manual'})
  await page.goto('/topic-lineage?'+params);await page.getByText('节点与关系列表',{exact:true}).click()
  await page.locator(`[data-lineage-edge="${edge.id}"]`).click()
  await expect(page.getByLabel('链路详情',{exact:true})).toContainText('synthetic-order-user')
  await expect(page.getByLabel('链路详情',{exact:true})).toContainText('synthetic-order-producer')
  await page.locator(`[data-lineage-node="${node.id}"]`).click()
  await expect(page.getByLabel('链路详情',{exact:true})).toContainText('实验机房 / 订单服务 v2')
  await expect.poll(()=>page.locator('.x6-node').count()).toBeGreaterThanOrEqual(7)
  await expect(page.getByText('正在整理链路…',{exact:true})).toHaveCount(0)
  await page.getByRole('button',{name:'适应画布',exact:true}).click()
  await page.screenshot({path:'test-results/application-identity-lineage-real.png',fullPage:true})
 }finally{expect((await page.request.delete('/api/v1/information/applications/'+app.id+'?revision='+app.revision,{headers})).status()).toBe(204)}
})
