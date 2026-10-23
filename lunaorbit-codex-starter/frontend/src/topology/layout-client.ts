import type { Presentation } from './presentation'
import type { LayoutPlan } from './routing-types'

export interface LayoutRequest {presentation:Presentation;nodeWidth:number;nodeHeight:number}
export type LayoutResponse={ok:true;plan:LayoutPlan}|{ok:false}
export interface LayoutTask {result:Promise<LayoutPlan>;cancel:()=>void}

export function createLayoutTask(presentation:Presentation,nodeWidth:number,nodeHeight=76):LayoutTask {
 let cancel=()=>{}
 const result=new Promise<LayoutPlan>((resolve,reject)=>{
  let worker:Worker|undefined,timeout:ReturnType<typeof globalThis.setTimeout>|undefined,settled=false
  const finish=(plan?:LayoutPlan,error?:Error)=>{
   if(settled)return
   settled=true
   if(timeout!==undefined)globalThis.clearTimeout(timeout)
   if(worker){worker.onmessage=null;worker.onerror=null;worker.onmessageerror=null;worker.terminate()}
   if(error)reject(error);else resolve(plan!)
  }
  const fail=()=>finish(undefined,new Error('拓扑布局失败，请重试'))
  cancel=()=>{const error=new Error('拓扑布局已取消');error.name='AbortError';finish(undefined,error)}
  try {
   // Vite recognizes the native Worker constructor to bundle its module URL.
   worker=new Worker(new URL('./layout.worker.ts',import.meta.url),{type:'module'})
   worker.onmessage=(event:MessageEvent<LayoutResponse>)=>{if(event.data?.ok&&event.data.plan)finish(event.data.plan);else fail()}
   worker.onerror=event=>{event.preventDefault();fail()}
   worker.onmessageerror=fail
   timeout=globalThis.setTimeout(()=>finish(undefined,new Error('拓扑布局超时，请重试')),8000)
   worker.postMessage({presentation,nodeWidth,nodeHeight} satisfies LayoutRequest)
  } catch {fail()}
 })
 // A caller can cancel before attaching its await handler without a rejected-promise leak.
 void result.catch(()=>{})
 return {result,cancel:()=>cancel()}
}
