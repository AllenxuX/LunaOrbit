import { afterEach, describe, expect, it, vi } from 'vitest'
import { createLayoutTask, resetWarmLayoutWorkerForTest, warmLayoutWorker } from './layout-client'
import type { LayoutPlan } from './routing-types'

class FakeWorker {
 static instances:FakeWorker[]=[]
 onmessage:((event:{data:unknown})=>void)|null=null
 onerror:((event:{preventDefault:()=>void})=>void)|null=null
 onmessageerror:(()=>void)|null=null
 terminated=false
 posted:unknown
 constructor(){FakeWorker.instances.push(this)}
 postMessage(message:unknown){this.posted=message}
 terminate(){this.terminated=true}
}
const plan:LayoutPlan={positions:{},ports:{},routes:{},nodeWidth:224,nodeHeight:76}
const start=()=>{vi.stubGlobal('Worker',FakeWorker);return createLayoutTask({nodes:[],edges:[]},224)}
afterEach(()=>{vi.unstubAllGlobals();resetWarmLayoutWorkerForTest();vi.useRealTimers();FakeWorker.instances=[]})
describe('layout worker client',()=>{
 it('resolves a worker plan and releases worker with late replies ignored',async()=>{
  const task=start(),worker=FakeWorker.instances[0]!,reply=worker.onmessage!
  expect(worker.posted).toEqual({presentation:{nodes:[],edges:[]},nodeWidth:224,nodeHeight:76})
  reply({data:{ok:true,plan}});expect(await task.result).toEqual(plan);expect(worker.terminated).toBe(true)
  reply({data:{ok:false}});task.cancel();expect(await task.result).toEqual(plan)
 })
 it('rejects cancellation distinctly and releases worker',async()=>{
  const task=start(),result=expect(task.result).rejects.toMatchObject({name:'AbortError'});task.cancel();await result;expect(FakeWorker.instances[0]!.terminated).toBe(true)
 })
 it('allows 30s for slow public downloads before timing out a hung worker',async()=>{
  vi.useFakeTimers();const task=start();let settled=false;void task.result.catch(()=>{}).finally(()=>{settled=true})
  await vi.advanceTimersByTimeAsync(12000);expect(settled).toBe(false);expect(FakeWorker.instances[0]!.terminated).toBe(false)
  const result=expect(task.result).rejects.toThrow(/超时/);await vi.advanceTimersByTimeAsync(18000);await result;expect(FakeWorker.instances[0]!.terminated).toBe(true)
 })
 it.each(['worker','message','response'])('sanitizes %s errors and releases worker',async kind=>{
  const task=start(),worker=FakeWorker.instances[0]!,result=expect(task.result).rejects.toThrow('拓扑布局失败，请重试')
  if(kind==='worker')worker.onerror!({preventDefault:()=>{}})
  else if(kind==='message')worker.onmessageerror!()
  else worker.onmessage!({data:{ok:false,error:'secret'}})
  await result;expect(worker.terminated).toBe(true)
 })
 it('rejects worker construction failures safely',async()=>{
  vi.stubGlobal('Worker',class {constructor(){throw new Error('secret')}})
  await expect(createLayoutTask({nodes:[],edges:[]},224).result).rejects.toThrow('拓扑布局失败，请重试')
 })
 it('warms the layout worker once when the browser is idle so scripts are cached before the topology page',async()=>{
  vi.stubGlobal('Worker',FakeWorker);const idle:(()=>void)[]=[];vi.stubGlobal('requestIdleCallback',(cb:()=>void)=>{idle.push(cb);return idle.length})
  warmLayoutWorker();warmLayoutWorker();expect(FakeWorker.instances).toHaveLength(0)
  idle.forEach(cb=>cb());expect(FakeWorker.instances).toHaveLength(1)
  const posted=FakeWorker.instances[0]!.posted as {presentation:{nodes:unknown[]}};expect(posted.presentation.nodes).toHaveLength(1)
  FakeWorker.instances[0]!.onmessage!({data:{ok:true,plan}});await Promise.resolve();expect(FakeWorker.instances[0]!.terminated).toBe(true)
 })
})
