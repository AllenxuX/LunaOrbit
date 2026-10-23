import { expect,test } from '@playwright/test'
import { buildPresentation } from '../src/topology/presentation'
import type { Snapshot,View } from '../src/api/topology'

// Browser-only synthetic graph: no Kafka, Kubernetes or MM2 writes.
function fixture():Snapshot {
 const names=['业务源 A','交易上游 D','业务目标 B','归档中心 C','分析集群 E','报表集群 F']
 const nodes=names.map((label,i)=>({id:'k'+i,kind:'kafka' as const,label,replication_factor:null,rf_status:'unknown'}))
 const workloads=[{id:'w0',kind:'workload' as const,label:'同步服务一',replication_factor:null,rf_status:'unknown'},{id:'w1',kind:'workload' as const,label:'同步服务二',replication_factor:null,rf_status:'unknown'}]
 const routes=[[0,2,0],[0,3,1],[1,2,1],[2,3,1],[2,4,0],[4,5,0],[3,5,1],[2,0,1],[0,2,1]]
 const links=routes.map(([a,b,w],i)=>({id:'l'+i,namespace:'synthetic',workload_uid:'w'+w,workload_name:workloads[w!]!.label,source_alias:names[a!]!,target_alias:names[b!]!,topics_raw:'events'+i+'.*',policy:'DefaultReplicationPolicy',enabled:true,confidence:'high',declared:true,observed:true,verified:false,stale:false,state:'unverified',observed_at:'2026-10-22T00:00:00Z',source:{alias:names[a!]!,status:'confirmed',method:'explicit',confidence:'high',registration_id:'r'+a,candidate_ids:[]},target:{alias:names[b!]!,status:'confirmed',method:'explicit',confidence:'high',registration_id:'r'+b,candidate_ids:[]},runtime:{generation:1,observed_generation:1,replicas:1,ready_replicas:1,pod_candidates:1,pod_association:'labels'},evidence:[],notes:[]}))
 const edges=routes.flatMap(([a,b,w],i)=>[{id:'in'+i,source:'k'+a,target:'w'+w,link_id:'l'+i,relation:'declared_input',stale:false,evidence:[]},{id:'out'+i,source:'w'+w,target:'k'+b,link_id:'l'+i,relation:'declared_output',stale:false,evidence:[]}])
 return {scope_id:'synthetic-dense',synthetic:true,observed_at:'2026-10-22T00:00:00Z',attempted_at:'2026-10-22T00:00:00Z',stale:false,nodes:[...nodes,...workloads],links,edges,issues:[]}
}

test('dense relationship view separates reverse paths, aggregates tasks and preserves mode layouts',async({page})=>{
 const errors:string[]=[];page.on('pageerror',e=>errors.push(e.message))
 const snapshot=fixture()
 let view:View={revision:0,positions:{},filters:{room:'',environment:''},layers:{kafka:true,workloads:true}}
 await page.route(/^http:\/\/[^/]+\/api\/(runtime$|v1\/)/,async route=>{
  const path=new URL(route.request().url()).pathname
  let body:unknown
  if(path==='/api/runtime')body={mode:'mock',synthetic:true,write_enabled:false,capabilities:{discovery:false,kafka_reads:false,acl_reads:false,changes:false,observability:false}}
  else if(path.endsWith('/session'))body={user_id:'synthetic-owner',mode:'mock'}
  else if(path.endsWith('/scopes'))body={items:[{id:snapshot.scope_id,synthetic:true}]}
  else if(path.endsWith('/kafka/clusters'))body={items:snapshot.nodes.filter(n=>n.kind==='kafka').map((n,i)=>({id:'r'+i,display_name:n.label,room:'实验',environment:'测试',bootstrap_servers:['synthetic.invalid:9092']})),total:6}
  else if(path.endsWith('/snapshot'))body={snapshot,total:snapshot.links.length,page:1,page_size:100}
  else if(path.endsWith('/view')){if(route.request().method()==='PUT')view={...route.request().postDataJSON(),revision:view.revision+1};body=view}
  else throw new Error('Unexpected synthetic API: '+path)
  await route.fulfill({json:body})
 })
 await page.goto('/topology')
 await expect(page.locator('.x6-node')).toHaveCount(15)
 await expect(page.locator('.x6-edge')).toHaveCount(18)
 await expect(page.locator('[data-cell-id=k2]')).toHaveCount(1)
 await page.locator('[data-path-source]').selectOption('k0')
 await page.locator('[data-path-target]').selectOption('k3')
 const indirect=await page.locator('[data-path-choice] option').allTextContents()
 expect(indirect.some(text=>text.includes('业务源 A')&&text.includes('业务目标 B')&&text.includes('归档中心 C'))).toBe(true)
 const choices=await page.locator('[data-path-choice] option').evaluateAll(options=>options.map(o=>({value:(o as HTMLOptionElement).value,text:o.textContent??''})))
 const viaB=choices.find(o=>o.text.includes('业务目标 B')&&o.text.includes('归档中心 C')&&JSON.parse(o.value).length===2)!
 expect(viaB).toBeDefined()
 await page.locator('[data-path-choice]').selectOption(viaB.value)
 await expect(page.locator('[data-path-task]')).toHaveCount(2)
 await expect(page.locator('.x6-node')).toHaveCount(5)
 await expect(page.locator('.x6-edge')).toHaveCount(4)
 await page.screenshot({path:'test-results/continuous-mm2-path.png',fullPage:true})
 await page.locator('[data-path-task]').last().click()
 await expect(page.locator('[data-link-summary]')).toContainText('events3.*')
 await page.getByRole('button',{name:'关闭链路详情'}).click()
 await page.locator('[data-clear-path]').click()
 await page.locator('[data-mode=instances]').click()
 await expect(page.locator('[data-mm2-instance]')).toHaveCount(2)
 await expect(page.locator('[data-mm2-task]')).toHaveCount(6)
 await page.locator('[data-expand-tasks]').click()
 await expect(page.locator('[data-mm2-task]')).toHaveCount(9)
 const mm2path=page.locator('[data-mm2-task=l3]')
 await expect(mm2path.locator('[data-source]')).toContainText('业务目标 B')
 await expect(mm2path.locator('[data-target]')).toContainText('归档中心 C')
 await expect(mm2path.locator('svg')).toHaveCount(2)
 await mm2path.locator('[data-mm2-select]').click()
 await expect(page.locator('[data-link-summary]')).toContainText('events3.*')
 await page.screenshot({path:'test-results/mm2-topology-dense.png',fullPage:true})
 await page.getByRole('button',{name:'关闭链路详情'}).click()
 await page.setViewportSize({width:1150,height:850})
 await page.screenshot({path:'test-results/mm2-topology-narrow.png',fullPage:true})
 await page.setViewportSize({width:390,height:844})
 await expect.poll(()=>page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true)
 await page.setViewportSize({width:1500,height:1050})
 await page.locator('[data-mode=relations]').click()
 await expect(page.locator('.x6-node')).toHaveCount(6)
 await expect(page.locator('.x6-edge')).toHaveCount(8)
 await expect(page.locator('.x6-edge-label')).toHaveCount(1)
 await expect(page.locator('.x6-edge-label')).toContainText('2 条链路')
 const host=page.locator('.topology-graph-host');await host.scrollIntoViewIfNeeded()
 await expect(async()=>{
  const bounds=await host.boundingBox();expect(bounds).not.toBeNull()
  for(const n of await page.locator('.x6-node').all()){
   const b=await n.boundingBox();expect(b).not.toBeNull()
   expect(b!.x).toBeGreaterThanOrEqual(bounds!.x);expect(b!.x+b!.width).toBeLessThanOrEqual(bounds!.x+bounds!.width)
   expect(b!.y).toBeGreaterThanOrEqual(bounds!.y);expect(b!.y+b!.height).toBeLessThanOrEqual(bounds!.y+bounds!.height)
  }
 }).toPass()
 await page.screenshot({path:'test-results/topology-dense-relations.png',fullPage:true})
 const aggregate=page.locator('.x6-edge').filter({has:page.locator('.x6-edge-label', {hasText:'2 条链路'})})
 await aggregate.locator('.x6-edge-label').click()
 await expect(page.locator('[data-route-choice]')).toHaveCount(2)
 await expect(page.locator('[data-link-summary]')).toHaveCount(0)
 await page.locator('[data-route-choice=l8]').click()
 await expect(page.locator('[data-link-summary]')).toContainText('events8.*')
 await page.getByRole('button',{name:'关闭链路详情'}).click()
 await page.locator('[data-cell-id=k2]').click()
 await expect(page.locator('[data-focus-direction]')).toBeVisible()
 await page.locator('[data-focus-direction]').selectOption('downstream')
 await expect(page.locator('.evidence-panel')).toHaveCount(0)
 await page.locator('[data-clear-focus]').click()
 await page.locator('[data-mode=tasks]').click()
 await expect(page.locator('.x6-node')).toHaveCount(0)
 await expect(page.locator('.mm2-topology')).toHaveCount(0)
 await expect(page.locator('[data-task-row]')).toHaveCount(9)
 await page.screenshot({path:'test-results/topology-dense-deployments.png',fullPage:true})
 await page.locator('[data-mode=relations]').click()
 await expect(page.locator('.x6-edge')).toHaveCount(8)
 const node=page.locator('[data-cell-id=k3]'),before=await node.getAttribute('transform'),box=await node.boundingBox()
 await page.mouse.move(box!.x+30,box!.y+20);await page.mouse.down();await page.mouse.move(box!.x+70,box!.y+60,{steps:8});await page.mouse.up()
 await page.locator('.topology-more summary').click()
 await expect(page.locator('[data-save]')).toBeEnabled();await page.locator('[data-save]').click()
 await expect(page.getByRole('status')).toHaveText('个人布局已保存')
 expect(view.positions['relations:k3']).toBeDefined()
 expect(await node.getAttribute('transform')).not.toBe(before)
 await page.reload();await page.locator('[data-mode=relations]').click();await expect(page.locator('.x6-node')).toHaveCount(6)
 await page.locator('.topology-more summary').click()
 await page.getByRole('button',{name:'整理布局'}).click()
 await page.locator('.topology-more summary').click()
 await page.setViewportSize({width:1150,height:850})
 await expect(async()=>{
  const bounds=await host.boundingBox()
  for(const n of await page.locator('.x6-node').all()){
   const b=await n.boundingBox();expect(b!.x).toBeGreaterThanOrEqual(bounds!.x);expect(b!.x+b!.width).toBeLessThanOrEqual(bounds!.x+bounds!.width)
  }
 }).toPass()
 await page.screenshot({path:'test-results/topology-narrow-relations.png',fullPage:true})
 await page.locator('[data-cluster-search]').fill('业务目标')
 await page.locator('[data-search-node=k2]').click()
 await expect(page.locator('[data-focus-direction]')).toBeVisible()
 await page.locator('.topology-graph-host svg').first().click({position:{x:12,y:12}})
 await expect(page.locator('[data-focus-direction]')).toHaveCount(0)
 await page.getByRole('button',{name:'全屏',exact:true}).click()
 await expect(page.getByRole('button',{name:'退出全屏'})).toBeVisible()
 await page.getByRole('button',{name:'退出全屏'}).click()
 await page.setViewportSize({width:390,height:844})
 await expect.poll(()=>page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true)
 expect(errors).toEqual([])
})





test('real X6 keeps unrelated routes and viewport stable during repeated drags',async({page})=>{
 const errors:string[]=[];page.on('pageerror',e=>errors.push(e.message))
 const snapshot=fixture()
 let view:View={revision:0,positions:{},filters:{room:'',environment:''},layers:{kafka:true,workloads:true}}
 await page.route(/^http:\/\/[^/]+\/api\/(runtime$|v1\/)/,async route=>{
  const path=new URL(route.request().url()).pathname
  let body:unknown
  if(path==='/api/runtime')body={mode:'mock',synthetic:true,write_enabled:false,capabilities:{discovery:false,kafka_reads:false,acl_reads:false,changes:false,observability:false}}
  else if(path.endsWith('/session'))body={user_id:'synthetic-owner',mode:'mock'}
  else if(path.endsWith('/scopes'))body={items:[{id:snapshot.scope_id,synthetic:true}]}
  else if(path.endsWith('/kafka/clusters'))body={items:snapshot.nodes.filter(n=>n.kind==='kafka').map((n,i)=>({id:'r'+i,display_name:n.label,room:'实验',environment:'测试',bootstrap_servers:['synthetic.invalid:9092']})),total:6}
  else if(path.endsWith('/snapshot'))body={snapshot,total:snapshot.links.length,page:1,page_size:100}
  else if(path.endsWith('/view')){if(route.request().method()==='PUT')view={...route.request().postDataJSON(),revision:view.revision+1};body=view}
  else throw new Error('Unexpected synthetic API: '+path)
  await route.fulfill({json:body})
 })

 await page.goto('/topology');await expect(page.locator('.x6-node')).toHaveCount(15)
 await expect(page.locator('.layout-notice')).toHaveCount(0)
 const graph=page.locator('.topology-graph-host'),node=page.locator('[data-cell-id=k0]')
 await graph.scrollIntoViewIfNeeded()
 const read=()=>page.locator('.x6-edge').evaluateAll(items=>items.map(e=>({id:e.getAttribute('data-cell-id'),source:e.getAttribute('data-source-cell'),target:e.getAttribute('data-target-cell'),path:e.querySelector('path:last-child')?.getAttribute('d')})))
 await node.evaluate(el=>{el.setAttribute('data-drag-identity','original')})
 const viewport=await page.locator('.x6-graph-svg-viewport').getAttribute('transform')
 for(const delta of [10,30,100,-100,-30,-10]){
  const before=await read(),bounds=await node.boundingBox();expect(bounds).not.toBeNull()
  await page.mouse.move(bounds!.x+bounds!.width/2,bounds!.y+bounds!.height/2);await page.mouse.down();await page.mouse.move(bounds!.x+bounds!.width/2+delta,bounds!.y+bounds!.height/2+10,{steps:12});await page.mouse.up()
  await expect(page.locator('.dirty-dot')).toHaveCount(1)
  expect(await node.getAttribute('data-drag-identity')).toBe('original')
  expect(await page.locator('.x6-graph-svg-viewport').getAttribute('transform')).toBe(viewport)
  const after=await read()
  // Only incident or genuinely obstructed edges may change; far terminal E→F remains exact.
  const unrelatedIds=new Set(buildPresentation(snapshot,'chain').edges.filter(e=>e.source==='k4'||e.source==='task:l5'||e.target==='k5').map(e=>e.id))
  const unrelated=before.filter(e=>e.id&&unrelatedIds.has(e.id));expect(unrelated.length).toBeGreaterThan(0)
  for(const edge of unrelated)expect(after.find(a=>a.id===edge.id)?.path).toBe(edge.path)
  expect(after.every(e=>!!e.path&&!e.path.includes('NaN'))).toBe(true)
  expect(await page.locator('.layout-notice').allTextContents()).not.toEqual(expect.arrayContaining([expect.stringMatching(/无法|不可用/)]))
  await expect.poll(()=>graph.evaluate(host=>{
   const boxes=[...host.querySelectorAll('.x6-node')].map(n=>n.getBoundingClientRect())
   let count=0
   for(const edge of host.querySelectorAll('.x6-edge')){const path=edge.querySelector('path:last-child') as SVGPathElement|null;if(!path)continue;const length=path.getTotalLength(),matrix=path.getScreenCTM();if(!matrix)continue;for(let d=0;d<=length;d+=3){const p=path.getPointAtLength(d).matrixTransform(matrix);if(boxes.some(b=>p.x>b.left+3&&p.x<b.right-3&&p.y>b.top+3&&p.y<b.bottom-3))count++}}
   return count
  })).toBe(0)
 }
 await page.screenshot({path:'test-results/stable-routing-drag.png',fullPage:true})
 await page.locator('.topology-more summary').click();await page.locator('[data-save]').click();await expect(page.getByRole('status')).toHaveText('个人布局已保存')
 const transform=await node.getAttribute('transform');await page.reload();await expect(page.locator('.x6-node')).toHaveCount(15)
 await expect(page.locator('[data-cell-id=k0]')).toHaveAttribute('transform',transform!)
 expect(errors).toEqual([])
})
