import { expect, test } from '@playwright/test'
test('Topic lineage renders read-only evidence with real worker layout',async({page})=>{
 const errors:string[]=[];page.on('pageerror',error=>errors.push(error.message))
 const requests:string[]=[]
 await page.route('**/api/**',async route=>{const url=new URL(route.request().url());if(!url.pathname.startsWith('/api/'))return route.continue();requests.push(`${route.request().method()} ${url.pathname}`);if(url.pathname==='/api/runtime')return route.fulfill({json:{mode:'information',synthetic:false,write_enabled:false,information_mode:true,workspace_id:'shared',capabilities:{discovery:false,kafka_reads:true,acl_reads:false,changes:false,observability:false}}});if(url.pathname==='/api/v1/kafka/clusters')return route.fulfill({json:{items:[{id:'east',display_name:'East Kafka'}],total:1}});if(url.pathname.endsWith('/topics'))return route.fulfill({json:{items:[{name:'orders'}],total:1}});if(url.pathname==='/api/v1/topic-lineage')return route.fulfill({json:{root_id:'a',observed_at:'2026-10-23T08:00:00Z',depth:2,truncated:false,issues:[],nodes:[{id:'a',kind:'topic',label:'orders',topic:'orders',cluster_id:'east',cluster_name:'East Kafka',exists:true},{id:'m',kind:'mm2',label:'Mirror east-west',evidence:'configured'},{id:'b',kind:'topic',label:'east.orders',topic:'east.orders',cluster_id:'west',cluster_name:'West Kafka',exists:true}],edges:[{id:'input',source:'a',target:'m',kind:'sync',evidence:'configured'},{id:'output',source:'m',target:'b',kind:'sync',evidence:'configured'}]}});return route.fulfill({json:{items:[],total:0}})})
 await page.goto('/topic-lineage?cluster_id=east&topic=orders')
 await expect(page.getByRole('heading',{name:'Topic 全链路',exact:true})).toBeVisible()
 await expect(page.locator('.x6-node')).toHaveCount(3)
 await expect(page.locator('.x6-edge')).toHaveCount(2)
 await expect(page.getByRole('img',{name:'Topic 全链路画布'})).toHaveAttribute('aria-busy','false')
 for(const width of [1500,1150]){await page.setViewportSize({width,height:1000});await page.screenshot({path: 'test-results/topic-lineage-'+width+'.png',fullPage:true})}
 await page.locator('.x6-node').filter({hasText:'east.orders'}).click()
 await expect(page.getByLabel('链路详情',{exact:true})).toContainText('不代表复制已验证')
 await page.getByRole('button',{name:'适应画布'}).click()
 await page.getByLabel('方向',{exact:true}).selectOption('upstream')
 await page.getByLabel('深度',{exact:true}).selectOption('4')
 await page.getByRole('button',{name:'查看链路',exact:true}).click()
 await expect(page).toHaveURL(/depth=4&direction=upstream/)
 expect(errors).toEqual([])
 expect(requests.every(r=>r.startsWith('GET '))).toBe(true)
 expect(requests.some(r=>/session|auth|acl/.test(r))).toBe(false)
})
