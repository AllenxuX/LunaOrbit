import engineUrl from 'elkjs/lib/elk-worker.min.js?url'
import { createElkLayout } from './elk-layout'
import type { LayoutRequest, LayoutResponse } from './layout-client'

const workerScope=globalThis as unknown as {
 onmessage:((event:MessageEvent<LayoutRequest>)=>void)|null
 postMessage:(message:LayoutResponse)=>void
}
workerScope.onmessage=event=>{
 const {presentation,nodeWidth,nodeHeight}=event.data
 void createElkLayout(presentation,nodeWidth,nodeHeight,engineUrl).then(
  plan=>workerScope.postMessage({ok:true,plan}),
  ()=>workerScope.postMessage({ok:false}),
 )
}
