import { mount, flushPromises } from '@vue/test-utils'
import { expect, it, vi } from 'vitest'
import Form from './InformationApplicationForm.vue'
import type { Cluster } from '../api/registry'
const save=vi.hoisted(()=>vi.fn().mockResolvedValue({}))
vi.mock('../api/information',()=>({saveInformationApplication:save}))
it('edits account, client ID and deployment location without dropping old relations',async()=>{
 const item={id:'app',revision:1,name:'订单服务',owner:'业务',environment:'测试',description:'',location:'旧位置',relations:[{cluster_id:'a',topic:'orders',mode:'produce' as const,account:'old-user',client_id:'old-client'}]}
 const wrapper=mount(Form,{props:{item,clusters:[{id:'a',display_name:'A'} as Cluster]}})
 const byLabel=(name:string)=>wrapper.findAll('label').find(l=>l.text()===name)!.find('input')
 expect(byLabel('部署位置').element.value).toBe('旧位置')
 await byLabel('部署位置').setValue('新位置')
 await byLabel('Kafka 账号').setValue('synthetic-user')
 await byLabel('Client ID').setValue('order-producer')
 await wrapper.find('form').trigger('submit.prevent');await flushPromises()
 expect(save).toHaveBeenCalledWith(expect.objectContaining({location:'新位置',relations:[expect.objectContaining({account:'synthetic-user',client_id:'order-producer'})]}),item)
 expect(item.location).toBe('旧位置');expect(item.relations[0].account).toBe('old-user')
 expect(wrapper.find('input[type=password]').exists()).toBe(false)
})
