import { expect,test } from '@playwright/test'

test.skip(process.env.LUNAORBIT_INFORMATION_LAB_E2E!=='1','Requires the explicit dedicated information-mode lab')
test.use({baseURL:'http://127.0.0.1:8088',trace:'off',video:'off'})
const headers={Origin:'http://127.0.0.1:8088'}

test('anonymous visitors see the same lab inventory without login or management navigation',async({page,browser})=>{
 const authRequests:string[]=[];page.on('request',r=>{if(/\/api\/v1\/(?:session|dev\/session|auth\/)/.test(r.url()))authRequests.push(r.url())})
 await page.goto('/topology');await expect(page.locator('.x6-node')).toHaveCount(8)
 const runtime=await (await page.request.get('/api/runtime')).json();expect(runtime.information_mode).toBe(true);expect(runtime.write_enabled).toBe(false)
 for(const name of ['身份与权限','平台访问控制','变更管理','会话管理'])await expect(page.getByRole('link',{name,exact:true})).toHaveCount(0)
 await expect(page.getByRole('link',{name:'应用信息',exact:true})).toBeVisible()
 const one=await (await page.request.get('/api/v1/kafka/clusters?page_size=100')).json();expect(one.items).toHaveLength(4)
 const context=await browser.newContext();const other=await context.newPage()
 try{
  await other.goto('/topology');await expect(other.locator('.x6-node')).toHaveCount(8)
  const two=await (await other.request.get('/api/v1/kafka/clusters?page_size=100')).json();expect(two.items.map((r:{id:string})=>r.id).sort()).toEqual(one.items.map((r:{id:string})=>r.id).sort())
  expect(await context.cookies()).toEqual([])
 }finally{await context.close()}
 expect(authRequests).toEqual([])
 await page.screenshot({path:'test-results/information-topology-real.png',fullPage:true})
})

test('shared application information persists and rejects stale edits and account fields',async({page,browser})=>{
 await page.goto('/applications');await expect(page.getByRole('heading',{name:'应用信息',exact:true})).toBeVisible()
 const clusters=await (await page.request.get('/api/v1/kafka/clusters?page_size=100')).json()
 const input={name:'合成共享记录验证',owner:'实验验证',environment:'测试',description:'只登记消息关系',relations:[{cluster_id:clusters.items[0].id,topic:'synthetic-information-events',mode:'produce'}]}
 const created=await page.request.post('/api/v1/information/applications',{headers,data:input});expect(created.status()).toBe(201);let record=await created.json()
 const context=await browser.newContext();const other=await context.newPage()
 try{
  await other.goto('/applications');await expect(other.getByText(input.name,{exact:true})).toBeVisible()
  const updated=await other.request.patch('/api/v1/information/applications/'+record.id,{headers,data:{...input,description:'共享编辑已保存',revision:record.revision}});expect(updated.status()).toBe(200)
  const stale=await page.request.patch('/api/v1/information/applications/'+record.id,{headers,data:{...input,revision:record.revision}});expect(stale.status()).toBe(409);record=await updated.json()
  const invalid=await page.request.post('/api/v1/information/applications',{headers,data:{...input,principal:'User:synthetic',password:'synthetic-invalid-field'}});expect(invalid.status()).toBe(400)
  await page.reload();await expect(page.getByText(input.name,{exact:true})).toBeVisible()
  await page.screenshot({path:'test-results/information-applications-real.png',fullPage:true})
 }finally{await context.close();expect((await page.request.delete('/api/v1/information/applications/'+record.id+'?revision='+record.revision,{headers})).status()).toBe(204)}
})

test('removed authentication, credentials, authorization and infrastructure mutation endpoints stay unavailable',async({request})=>{
 const runtime=await (await request.get('/api/runtime')).json();expect(runtime.information_mode).toBe(true)
 const clusters=await (await request.get('/api/v1/kafka/clusters?page_size=100')).json(),id=clusters.items[0].id
 const base='/api/v1/kafka/clusters/'+id+'/workspace'
 for(const path of ['/api/v1/session',base+'/accounts',base+'/acls',base+'/teams',base+'/requests',base+'/resource-operations','/api/v1/auth/me']){
  const response=await request.get(path,{headers:{Cookie:'lunaorbit_dev_session=synthetic-old-admin',Authorization:'synthetic-forged'}});expect([403,404,405],path).toContain(response.status())
 }
 for(const path of ['/api/v1/auth/login','/api/v1/dev/session',base+'/plans',base+'/resource-plans',base+'/accounts',base+'/requests']){
  const response=await request.post(path,{headers,data:{}});expect([403,404,405],path).toContain(response.status())
 }
 const crossSite=await request.post('/api/v1/information/applications',{headers:{Origin:'https://synthetic.invalid','Sec-Fetch-Site':'cross-site'},data:{name:'must-not-create',relations:[]}});expect(crossSite.status()).toBe(403)
})

test('personal topology layout is browser local while shared inventory stays unchanged',async({page,browser})=>{
 const viewWrites:string[]=[];page.on('request',r=>{if(r.method()==='PUT'&&r.url().includes('/view'))viewWrites.push(r.url())})
 await page.goto('/topology');await expect(page.locator('.x6-node')).toHaveCount(8)
 const node=page.locator('.x6-node').first();await node.scrollIntoViewIfNeeded();const id=await node.getAttribute('data-cell-id'),before=await node.getAttribute('transform'),box=await node.boundingBox()
 await page.mouse.move(box!.x+box!.width/2,box!.y+box!.height/2);await page.mouse.down();await page.mouse.move(box!.x+box!.width/2+30,box!.y+box!.height/2+30,{steps:10});await page.mouse.up()
 await page.locator('.topology-more summary').click();await expect(page.locator('[data-save]')).toBeEnabled();await page.locator('[data-save]').click()
 const moved=await node.getAttribute('transform');expect(moved).not.toBe(before)
 await page.reload();await expect(page.locator('.x6-node')).toHaveCount(8);await expect(page.locator(`[data-cell-id="${id}"]`)).toHaveAttribute('transform',moved!)
 const context=await browser.newContext({viewport:{width:1500,height:1050}});const other=await context.newPage()
 try{await other.goto('/topology');await expect(other.locator('.x6-node')).toHaveCount(8);await expect(other.locator(`[data-cell-id="${id}"]`)).toHaveAttribute('transform',before!)}finally{await context.close()}
 expect(viewWrites).toEqual([])
})

test('actual application form records account identity without password or permission fields',async({page})=>{
 await page.goto('/applications');await page.getByRole('button',{name:'新增应用',exact:true}).click()
 const dialog=page.getByRole('dialog');await dialog.getByLabel('应用名称',{exact:true}).fill('合成实际表单验证');await dialog.getByLabel('负责人',{exact:true}).fill('实验验证');await dialog.getByLabel('环境',{exact:true}).fill('测试');await dialog.getByLabel('说明',{exact:true}).fill('实际表单保存并清理')
 await dialog.getByRole('button',{name:'添加关联',exact:true}).click()
 const clusters=await (await page.request.get('/api/v1/kafka/clusters?page_size=100')).json()
 await dialog.getByLabel('关联集群').selectOption(clusters.items[0].id);await dialog.getByLabel('Topic',{exact:true}).fill('synthetic-information-events')
 await dialog.getByLabel('关系').selectOption('consume');await dialog.getByLabel('消费组',{exact:true}).fill('synthetic-information-consumers')
 expect(await dialog.getByLabel(/密码|权限/).count()).toBe(0)
 const savedResponse=page.waitForResponse(r=>r.request().method()==='POST'&&new URL(r.url()).pathname==='/api/v1/information/applications')
 await dialog.getByRole('button',{name:'保存应用',exact:true}).click();const response=await savedResponse;expect(response.status()).toBe(201);const record=await response.json()
 try{
  await expect(dialog).toHaveCount(0);await expect(page.locator(`[data-information-application="${record.id}"]`)).toContainText('synthetic-information-consumers');await expect(page.locator(`[data-information-application="${record.id}"]`)).toContainText('人工登记')
  await page.reload();await expect(page.locator(`[data-information-application="${record.id}"]`)).toBeVisible()
  expect(record.updated_at).toBeTruthy()
 }finally{expect((await page.request.delete('/api/v1/information/applications/'+record.id+'?revision='+record.revision,{headers})).status()).toBe(204)}
})

test('legacy governance pages redirect and real cluster resources remain readable',async({page})=>{
 await page.goto('/security');await expect(page).toHaveURL(/\/applications$/)
 const clusters=await (await page.request.get('/api/v1/kafka/clusters?page_size=100')).json(),id=clusters.items[0].id
 await page.goto('/kafka/clusters/'+id+'/details?tab=applications')
 await expect(page.getByRole('heading',{name:clusters.items[0].display_name,exact:true})).toBeVisible()
 for(const name of ['应用与权限','团队授权','操作记录'])await expect(page.getByRole('button',{name,exact:true})).toHaveCount(0)
 const meta=await (await page.request.get('/api/v1/kafka/clusters/'+id+'/workspace/meta')).json();expect(meta.write_enabled).toBe(false)
 const topicsResponse=await page.request.get('/api/v1/kafka/clusters/'+id+'/topics?page_size=100');expect(topicsResponse.status()).toBe(200)
 const topics=await topicsResponse.json();expect(topics.items.length).toBeGreaterThan(0)
 await page.goto('/kafka/clusters/'+id+'/topics/'+encodeURIComponent(topics.items[0].name))
 await expect(page.getByRole('heading',{name:topics.items[0].name,exact:true})).toBeVisible()
 await expect(page.getByRole('button',{name:'创建 Topic',exact:true})).toHaveCount(0)
 await page.screenshot({path:'test-results/information-topic-readonly-real.png',fullPage:true})
})
