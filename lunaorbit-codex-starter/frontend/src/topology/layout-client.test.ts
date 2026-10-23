import { afterEach, describe, expect, it, vi } from 'vitest'
import { createLayoutTask } from './layout-client'
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
afterEach(()=>{vi.unstubAllGlobals();vi.useRealTimers();FakeWorker.instances=[]})
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
 it('times out and terminates a hung worker',async()=>{
  vi.useFakeTimers();const task=start(),result=expect(task.result).rejects.toThrow(/超时/);await vi.advanceTimersByTimeAsync(8000);await result;expect(FakeWorker.instances[0]!.terminated).toBe(true)
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
})
