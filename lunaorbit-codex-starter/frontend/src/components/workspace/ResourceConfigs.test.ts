import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import ResourceConfigs from './ResourceConfigs.vue'

afterEach(()=>vi.unstubAllGlobals())
const meta={synthetic:true,write_enabled:false,approval_required:true,permissions:{read:true,topic_manage:false,applications_manage:false,acl_manage:false,audit_read:false,team_manage:false}}
const retentionEntry={name:'log.retention.ms',value:null as string|null,default_value:null,source:'STATIC_BROKER_CONFIG',sensitive:false,read_only:false,type:'LONG',synonyms:[]}
it('explains the actual minute or hour retention setting without replacing the absent millisecond value',async()=>{
 let minutes:string|null=null
 vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({entries:[retentionEntry,{...retentionEntry,name:'log.retention.minutes',value:minutes},{...retentionEntry,name:'log.retention.hours',value:'1'}]}))))
 const w=mount(ResourceConfigs,{props:{clusterId:'lab',resource:{type:'BROKER',name:'1'},meta,actorId:'viewer',showOperations:false}})
 await flushPromises()
 expect(w.get('.config-current-value').text()).toBe('未设置，使用 log.retention.hours（1 小时）')
 minutes='30';await w.get('.config-heading button').trigger('click');await flushPromises()
 expect(w.get('.config-current-value').text()).toBe('未设置，使用 log.retention.minutes（30 分钟）')
 w.unmount()
})
it('does not infer retention from missing, sensitive or cluster catalogue values',async()=>{
 let entries=[retentionEntry,{...retentionEntry,name:'log.retention.hours',value:'synthetic-sensitive',sensitive:true}]
 vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({entries}))))
 const w=mount(ResourceConfigs,{props:{clusterId:'lab',resource:{type:'BROKER',name:'1'},meta,actorId:'viewer',showOperations:false}})
 await flushPromises();expect(w.get('.config-current-value').text()).toBe('Kafka 未返回');expect(w.text()).not.toContain('synthetic-sensitive')
 entries=[{...retentionEntry,source:'NO_CLUSTER_DEFAULT_OVERRIDE'},{...retentionEntry,name:'log.retention.hours',value:'1'}]
 await w.setProps({resource:{type:'BROKER',name:''}});await flushPromises();expect(w.get('.config-current-value').text()).toBe('未设置')
 entries=[{...retentionEntry,value:'60000'},{...retentionEntry,name:'log.retention.hours',value:'1'}]
 await w.setProps({resource:{type:'BROKER',name:'1'}});await flushPromises();expect(w.get('.config-current-value').text()).toBe('1 分钟')
 w.unmount()
})
it('distinguishes an absent cluster override from an unknown effective Broker value',async()=>{
  vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({entries:[{name:'num.partitions',value:null,default_value:'1',source:'NO_CLUSTER_DEFAULT_OVERRIDE',sensitive:false,read_only:false,type:'INT',synonyms:[]}]}))))
  const w=mount(ResourceConfigs,{props:{clusterId:'lab',resource:{type:'BROKER',name:''},meta,actorId:'viewer',showOperations:false}})
  await flushPromises()
  expect(w.get('thead').text()).toContain('集群覆盖值')
  expect(w.get('.config-current-value').text()).toBe('未设置')
  expect(w.text()).toContain('查看具体 Broker 的实际生效配置')
  await w.setProps({resource:{type:'BROKER',name:'1'}});await flushPromises()
  expect(w.get('thead').text()).toContain('当前值')
  expect(w.get('.config-current-value').text()).toBe('未设置')
  w.unmount()
})
it('does not invent a default or render an empty returned value as missing',async()=>{
  vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({entries:[{name:'num.partitions',value:null,default_value:null,source:'UNKNOWN_CONFIG',sensitive:false,read_only:false,type:'INT',synonyms:[]},{name:'cleanup.policy',value:'',default_value:null,source:'STATIC_BROKER_CONFIG',sensitive:false,read_only:false,type:'STRING',synonyms:[]}]}))))
  const w=mount(ResourceConfigs,{props:{clusterId:'lab',resource:{type:'BROKER',name:'1'},meta,actorId:'viewer',showOperations:false}})
  await flushPromises()
  expect(w.findAll('.config-current-value').map(n=>n.text())).toEqual(['空值','Kafka 未返回'])
  expect(w.text()).not.toContain('未知')
  w.unmount()
})
