import type { Presentation } from './presentation'
import type { LayoutPlan } from './routing-types'

export interface LayoutRequest {presentation:Presentation;nodeWidth:number;nodeHeight:number}
export type LayoutResponse={ok:true;plan:LayoutPlan}|{ok:false}
export interface LayoutTask {result:Promise<LayoutPlan>;cancel:()=>void}

export const LAYOUT_TIMEOUT_MS=30000

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
   // Includes worker/ELK script download, which can take 10s+ over the public link.
   timeout=globalThis.setTimeout(()=>finish(undefined,new Error('拓扑布局超时，请重试')),LAYOUT_TIMEOUT_MS)
   worker.postMessage({presentation,nodeWidth,nodeHeight} satisfies LayoutRequest)
  } catch {fail()}
 })
 // A caller can cancel before attaching its await handler without a rejected-promise leak.
 void result.catch(()=>{})
 return {result,cancel:()=>cancel()}
}

let warmed=false
// Runs one tiny layout when the browser is idle so the worker and ELK engine scripts are already
// downloaded (same URLs as real layouts) before the user opens a topology page.
export function warmLayoutWorker(){
 if(warmed)return
 warmed=true
 const idle=(globalThis as {requestIdleCallback?:(cb:()=>void)=>number}).requestIdleCallback??((cb:()=>void)=>globalThis.setTimeout(cb,2000))
 idle(()=>{void createLayoutTask({nodes:[{id:'warm-up',kind:'kafka',label:'',replication_factor:null,rf_status:'unknown'}],edges:[]},224).result.catch(()=>{})})
}
export function resetWarmLayoutWorkerForTest(){warmed=false}
