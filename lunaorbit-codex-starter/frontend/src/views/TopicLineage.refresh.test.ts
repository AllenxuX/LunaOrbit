import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import TopicLineage from './TopicLineage.vue'
vi.mock('../components/TopicLineageCanvas.vue',()=>({default:{name:'TopicLineageCanvas',props:['view','rootId','states'],emits:['selectNode','selectEdge'],template:'<div data-canvas />'}}))

const graph={root_id:'topic:a',observed_at:'',depth:2,truncated:false,issues:[],nodes:[{id:'topic:a',kind:'topic',label:'orders',cluster_id:'a',cluster_name:'A',topic:'orders',exists:true,evidence:'observed'}],edges:[]}
interface Opts {job:'done'|'running';jobError?:string;after:{stale:boolean;issues:{code:string}[]}}
function stub(o:Opts){
 let refreshed=false,release:()=>void=()=>{}
 const gate=new Promise<void>(r=>{release=r})
 vi.stubGlobal('fetch',vi.fn(async(url:string,init?:RequestInit)=>{
  const json=(b:unknown,status=200)=>({ok:status<400,status,json:async()=>b})
  if(url.endsWith('/runs')&&init?.method==='POST'){await gate;refreshed=true;return json({job_id:'j',status:'pending'},202)}
  if(url.endsWith('/runs/latest'))return json({job_id:'j',status:o.job,...(o.jobError?{error_code:o.jobError}:{})})
  if(url.includes('/snapshot'))return json({snapshot:refreshed?{observed_at:'2026-10-26T11:00:00Z',links:[],...o.after}:{stale:true,observed_at:'2026-10-26T10:44:25Z',issues:[],links:[]}})
  if(url.includes('/discovery/scopes'))return json({items:[{id:'k8s-in-cluster-x',synthetic:false}]})
  if(url.includes('/topic-lineage?'))return json(graph)
  if(url.includes('/topics?'))return json({items:[{name:'orders'}],total:1})
  if(url.includes('/kafka/clusters'))return json({items:[{id:'a',display_name:'A'}],total:1})
  return json({})
 }))
 return release
}
async function page(){const router=createRouter({history:createMemoryHistory(),routes:[{path:'/topic-lineage',component:TopicLineage},{path:'/:p(.*)',component:{template:'<div/>'}}]});await router.push('/topic-lineage?cluster_id=a&topic=orders');await router.isReady();const w=mount(TopicLineage,{global:{plugins:[router]}});for(let i=0;i<6;i++)await flushPromises();return w}
const buttons=(w:ReturnType<typeof mount>)=>w.findAll('button').filter(b=>/刷新发现快照|正在刷新/.test(b.text()))
const settle=async()=>{for(let i=0;i<10;i++){await flushPromises();await vi.advanceTimersByTimeAsync(600)}}
beforeEach(()=>{globalThis.localStorage?.clear();vi.useFakeTimers({shouldAdvanceTime:true})})
afterEach(()=>{vi.useRealTimers();vi.unstubAllGlobals()})

describe('刷新发现快照 feedback',()=>{
 it('both buttons share one refreshing state and success shows the new collection time',async()=>{
  const release=stub({job:'done',after:{stale:false,issues:[]}});const w=await page()
  expect(buttons(w).length).toBe(2)
  await buttons(w)[0]!.trigger('click');await flushPromises()
  for(const b of buttons(w)){expect(b.text()).toBe('正在刷新…');expect(b.attributes('disabled')).toBeDefined()}
  release();await settle()
  expect(w.get('[data-refresh-result]').text()).toMatch(/采集完成，配置采集时间 /)
  w.unmount()
 })
 it('completed but untrusted names the reason',async()=>{
  const release=stub({job:'done',after:{stale:true,issues:[{code:'INVALID_RULE'},{code:'INVALID_RULE'}]}});const w=await page()
  await buttons(w)[1]!.trigger('click');release();await settle()
  expect(w.get('[data-refresh-result]').text()).toBe('采集完成，但有 2 条规则无法解析，快照仍不可信')
  w.unmount()
 })
 it('still running after the wait is reported, not silent',async()=>{
  const release=stub({job:'running',after:{stale:true,issues:[]}});const w=await page()
  await buttons(w)[0]!.trigger('click');release();for(let i=0;i<40;i++){await flushPromises();await vi.advanceTimersByTimeAsync(600)}
  expect(w.get('[data-refresh-result]').text()).toBe('刷新已提交，仍在采集中，请稍后查看')
  for(const b of buttons(w))expect(b.attributes('disabled')).toBeUndefined()
  w.unmount()
 })
 it('a failed collection shows its reason',async()=>{
  const release=stub({job:'done',jobError:'KITE_UNAVAILABLE',after:{stale:true,issues:[]}});const w=await page()
  await buttons(w)[0]!.trigger('click');release();await settle()
  expect(w.get('[data-refresh-result]').text()).toBe('采集失败：Kite 暂不可用，请稍后重试')
  w.unmount()
 })
})
