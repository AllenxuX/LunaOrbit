<script setup lang="ts">
import { computed,nextTick,onMounted,onBeforeUnmount,ref,watch } from 'vue'
import { Graph,type Cell,type Node,type Edge } from '@antv/x6'
import type { Snapshot,Position } from '../api/topology'
import { nodeIdentity,type RegistrationIdentity } from '../topology/model'
import { buildPresentation,focusPresentation,type PresentationEdge,type TopologyMode } from '../topology/presentation'
import { createLayoutTask,type LayoutTask } from '../topology/layout-client'
import { RoutingSession } from '../topology/stable-routing'
import type { LayoutPlan,RoutingUpdate } from '../topology/routing-types'
const props=withDefaults(defineProps<{snapshot:Snapshot;positions:Record<string,Position>;affectedNodes?:string[];confirmedLinks?:string[];registrations?:RegistrationIdentity[];mode?:TopologyMode;selectedLinkIds?:string[];focusNodeId?:string;focusDirection?:'neighbors'|'upstream'|'downstream';nodeHints?:Record<string,string>;pathNodeIds?:string[]}>(),{mode:'relations',focusDirection:'neighbors',affectedNodes:()=>[],confirmedLinks:()=>[],registrations:()=>[],selectedLinkIds:()=>[],focusNodeId:undefined,nodeHints:()=>({}),pathNodeIds:()=>[]})
const emit=defineEmits<{move:[id:string,position:Position];select:[linkId:string];selectLinks:[ids:string[]];selectNode:[nodeId:string];clear:[];layoutBusy:[busy:boolean];organize:[]}>()
const container=ref<InstanceType<typeof globalThis.HTMLDivElement>>()
const presentation=computed(()=>{
 const full=buildPresentation(props.snapshot,props.mode)
 if(!props.pathNodeIds.length)return full
 const ids=new Set(props.pathNodeIds)
 return {nodes:full.nodes.filter(n=>ids.has(n.id)),edges:full.edges.filter(e=>ids.has(e.source)&&ids.has(e.target))}
})
const height=76
function nodeWidth(){return props.mode==='chain'?180:224}
const loading=ref(false),message=ref('')
let resizeObserver:InstanceType<typeof globalThis.ResizeObserver>|undefined
let graph:Graph|undefined
let hoveredEdge:string|undefined
let fitEpoch=0,layoutEpoch=0,applying=false,previewFrame=0
let task:LayoutTask|undefined,session:RoutingSession|undefined,plan:LayoutPlan|undefined
const previewEdges=new Set<string>()
const nodes=new Map<string,Node>(),edges=new Map<string,Edge>()
const structure=computed(()=>JSON.stringify([props.mode,presentation.value.nodes.map(n=>[n.id,n.kind]).sort((a,b)=>a[0]!.localeCompare(b[0]!)),presentation.value.edges.map(e=>[e.id,e.source,e.target]).sort((a,b)=>a[0]!.localeCompare(b[0]!))]))
function busy(value:boolean){loading.value=value;emit('layoutBusy',value)}
function apply(update:RoutingUpdate){
 applying=true
 try{
  for(const id of update.changedNodeIds){const p=update.positions[id];if(p)nodes.get(id)?.position(p.x,p.y)}
  for(const id of update.changedEdgeIds)edges.get(id)?.setVertices(update.routes[id]!.slice(1,-1))
 }finally{applying=false}
}
function restoreRoutes(){if(session){const routes=session.getRoutes();for(const id of previewEdges)edges.get(id)?.setVertices(routes[id]!.slice(1,-1));previewEdges.clear()}}
function syncPositions(){
 if(!session||!plan||loading.value||props.pathNodeIds.length)return
 try{apply(session.applyPositions({...plan.positions,...props.positions}))}
 catch{message.value='已保存的布局存在重叠或无法布线，请整理布局后保存'}
}
async function arrange(ignoreSaved=true){
 const token=++layoutEpoch
 task?.cancel();globalThis.cancelAnimationFrame(previewFrame);previewEdges.clear();busy(true);message.value=''
 const current=presentation.value
 task=createLayoutTask(JSON.parse(JSON.stringify(current)),nodeWidth(),height)
 try{
  const result=await task.result
  if(token!==layoutEpoch||!graph)return undefined
  plan=result;session=new RoutingSession(current,result)
  if(!ignoreSaved&&!props.pathNodeIds.length){try{session.applyPositions({...result.positions,...props.positions})}catch{message.value='已保存的布局存在重叠或无法布线，请整理布局后保存'}}
  draw();scheduleFit()
  return session.getPositions()
 }catch(error){if(token===layoutEpoch&&(error as Error).name!=='AbortError'){session=undefined;plan=undefined;nodes.clear();edges.clear();graph?.resetCells([]);message.value='自动布局暂不可用，请重试整理布局'};return undefined}
 finally{if(token===layoutEpoch)busy(false)}
}
function scheduleFit(){
 const instance=graph,token=++fitEpoch
 void nextTick(()=>{if(instance&&graph===instance&&token===fitEpoch)instance.zoomToFit({padding:56,maxScale:1})})
}
function color(edge:PresentationEdge){return edge.stale||edge.state==='disabled'?'#929aa6':edge.state==='verified'?'#35744e':edge.state==='unknown'?'#bd8b39':'#758aa5'}
function status(edge:PresentationEdge){return edge.stale?'数据已过期 · 状态未知':edge.state==='disabled'?'同步已禁用':edge.state==='verified'?'链路已验证':edge.state==='unknown'?'状态未知':'链路未验证'}
function name(id:string){const node=presentation.value.nodes.find(n=>n.id===id);return node?.linkId?node.label:nodeIdentity(props.snapshot,id,props.registrations??[])}
function overlays(){
 if(!graph)return
 const focus=props.focusNodeId?focusPresentation(presentation.value,props.focusNodeId,props.focusDirection):undefined
 const selected=new Set(props.selectedLinkIds??[])
 const selectedEdges=presentation.value.edges.filter(e=>e.linkIds.some(id=>selected.has(id)))
 const activeNodes=new Set(selectedEdges.flatMap(e=>[e.source,e.target]))
 const focusing=!!focus||selectedEdges.length>0
 for(const node of graph.getNodes()){
  const kind=node.getData<{kind:string}>()?.kind
  const affected=props.affectedNodes?.includes(node.id)
  const focused=focus?focus.nodeIds.includes(node.id):activeNodes.has(node.id)
  node.attr('body/stroke',affected?'#c35a15':props.focusNodeId===node.id?'#315fc1':kind==='kafka'?'#6386c7':'#8b70ba')
  node.attr('body/strokeWidth',affected?3:props.focusNodeId===node.id?2:1)
  node.attr('body/opacity',focusing&&!focused?0.3:1)
  node.attr('label/opacity',focusing&&!focused?0.4:1)
 }
 for(const cell of graph.getEdges()){
  const edge=cell.getData<PresentationEdge>()
  if(!edge)continue
  const selectedEdge=edge.linkIds.some(id=>selected.has(id))
  const inFocus=focus?.edgeIds.includes(edge.id)??selectedEdge
  const expanded=hoveredEdge===edge.id||props.mode!=='chain'&&(selectedEdge||!!focus&&inFocus)
  const count=edge.linkIds.length>1?`${edge.linkIds.length} 条链路`:''
  const confirmed=edge.linkIds.some(id=>props.confirmedLinks?.includes(id))?' · 业务人工确认（非健康验证）':''
  const text=expanded?`${name(edge.source)} → ${name(edge.target)}\n${[count,status(edge)+confirmed].filter(Boolean).join(' · ')}`:count
  cell.attr('line/stroke',color(edge))
  cell.attr('line/strokeWidth',selectedEdge||hoveredEdge===edge.id?3:2)
  cell.attr('line/opacity',focusing&&!inFocus&&hoveredEdge!==edge.id?0.15:1)
  cell.setLabels(text?[{position:{distance:0.5,offset:-14},attrs:{label:{text,fontSize:12,fill:'#334761'},body:{fill:'#fff',stroke:'#dce4ef',strokeWidth:1,rx:4,ry:4}}}]:[])
 }
}
function port(edge:PresentationEdge,end:'source'|'target'){return `${edge.id}:${end}`}
function draw(){
 if(!graph||!session||!plan)return
 hoveredEdge=undefined
 nodes.clear();edges.clear()
 const positions=session.getPositions(),routes=session.getRoutes()
 const cells:Cell[]=[]
 for(const n of presentation.value.nodes){
  const p=positions[n.id]!
  const related=presentation.value.edges.flatMap(e=>(['source','target'] as const).filter(end=>e[end]===n.id).map(end=>{const anchor=plan!.ports[e.id]![end];return {id:port(e,end),group:anchor.side,args:{x:anchor.side==='right'?nodeWidth():0,y:anchor.offset}}}))
  const cell=graph.createNode({id:n.id,x:p.x,y:p.y,width:nodeWidth(),height,shape:'rect',markup:[{tagName:'rect',selector:'body'},{tagName:'text',selector:'label'},{tagName:'text',selector:'meta'}],
   ports:{groups:{left:{position:'absolute',attrs:{circle:{r:0,magnet:false}}},right:{position:'absolute',attrs:{circle:{r:0,magnet:false}}}},items:related},
   attrs:{body:{fill:n.kind==='kafka'?'#eff6ff':'#f4f0ff',stroke:n.kind==='kafka'?'#6386c7':'#8b70ba',rx:8,ry:8},label:{text:name(n.id),refX:'50%',refY:'60%',textAnchor:'middle',textVerticalAnchor:'middle',fontSize:16,fontWeight:500,fontFamily:'Inter, "Microsoft YaHei", sans-serif',fill:'#25304a',textWrap:{width:nodeWidth()-20,height:38,ellipsis:true}},meta:{text:`${n.kind==='kafka'?'Kafka':n.linkId?'MM2 同步任务':'MM2'}${props.nodeHints?.[n.id]?' · '+props.nodeHints[n.id]:''}`,refX:'50%',refY:'23%',textAnchor:'middle',textVerticalAnchor:'middle',fontSize:11,fill:'#718099'}},data:{kind:n.kind,linkId:n.linkId}})
  nodes.set(n.id,cell);cells.push(cell)
 }
 for(const edge of presentation.value.edges){
  const route=routes[edge.id]!.slice(1,-1)
  const cell=graph.createEdge({id:edge.id,source:{cell:edge.source,port:port(edge,'source')},target:{cell:edge.target,port:port(edge,'target')},connector:{name:'rounded',args:{radius:6}},router:{name:'normal'},vertices:route,
   attrs:{line:{stroke:color(edge),strokeWidth:2,strokeDasharray:edge.state==='disabled'?'2 5':edge.stale||edge.state!=='verified'?'6 4':'0',targetMarker:{name:'classic',size:8}}},data:edge})
  edges.set(edge.id,cell);cells.push(cell)
 }
 graph.resetCells(cells);overlays()
}
onMounted(()=>{
 graph=new Graph({container:container.value!,autoResize:true,grid:{size:20,visible:true},background:{color:'#fbfcff'},panning:true,mousewheel:{enabled:true,modifiers:['ctrl','meta'],minScale:0.2,maxScale:2},
  interacting:{nodeMovable:()=>!loading.value&&!props.pathNodeIds.length,edgeMovable:false,edgeLabelMovable:false,arrowheadMovable:false,vertexMovable:false,vertexAddable:false,vertexDeletable:false,magnetConnectable:false},connecting:{allowBlank:false,allowLoop:false,allowNode:false,allowEdge:false,allowPort:false}})
 graph.on('node:change:position',({node})=>{
  if(applying||loading.value||props.pathNodeIds.length||!session)return
  globalThis.cancelAnimationFrame(previewFrame)
  previewFrame=globalThis.requestAnimationFrame(()=>{if(!session)return;for(const [id,path] of Object.entries(session.previewMove(node.id,node.position()))){previewEdges.add(id);edges.get(id)?.setVertices(path.slice(1,-1))}})
 })
 graph.on('node:moved',({node})=>{
  globalThis.cancelAnimationFrame(previewFrame)
  if(applying||loading.value||props.pathNodeIds.length||!session)return
  try{const update=session.moveNode(node.id,node.position());restoreRoutes();apply(update);const settled=update.positions[node.id]!;applying=true;node.position(settled.x,settled.y);applying=false;message.value=update.adjusted?'节点已移至附近空位，避免重叠':'';if(update.changedNodeIds.includes(node.id))emit('move',node.id,update.positions[node.id]!)}
  catch{applying=true;node.position(session.getPositions()[node.id]!.x,session.getPositions()[node.id]!.y);applying=false;restoreRoutes();message.value='当前位置无法清晰布线，已恢复；可整理布局后重试'}
 })
 graph.on('edge:click',({edge})=>{const ids=edge.getData<PresentationEdge>()?.linkIds??[];if(ids.length===1)emit('select',ids[0]!);else if(ids.length>1)emit('selectLinks',[...ids])})
 graph.on('blank:click',()=>emit('clear'))
 graph.on('node:click',({node})=>{const task=node.getData<{linkId?:string}>()?.linkId;if(task)emit('select',task);else emit('selectNode',node.id)})
 graph.on('edge:mouseenter',({edge})=>{hoveredEdge=edge.id;overlays()})
 graph.on('edge:mouseleave',()=>{hoveredEdge=undefined;overlays()})
 void arrange(false)
 if(globalThis.ResizeObserver){
  let previous=''
  resizeObserver=new globalThis.ResizeObserver(entries=>{
   const rect=entries[0]?.contentRect
   if(!rect||rect.width<=0||rect.height<=0)return
   const size=`${rect.width}:${rect.height}`
   if(size!==previous){previous=size;scheduleFit()}
  })
  resizeObserver.observe(container.value!)
 }
})
watch(structure,()=>{void arrange(false)})
watch(()=>props.positions,syncPositions)
watch([()=>props.snapshot,()=>props.registrations,()=>props.nodeHints],()=>{
 for(const n of presentation.value.nodes){nodes.get(n.id)?.attr('label/text',name(n.id));nodes.get(n.id)?.attr('meta/text',`${n.kind==='kafka'?'Kafka':n.linkId?'MM2 同步任务':'MM2'}${props.nodeHints[n.id]?' · '+props.nodeHints[n.id]:''}`)}
 for(const edge of presentation.value.edges){edges.get(edge.id)?.setData(edge);edges.get(edge.id)?.attr('line/strokeDasharray',edge.state==='disabled'?'2 5':edge.stale||edge.state!=='verified'?'6 4':'0')}
 overlays()
})
watch([()=>props.affectedNodes,()=>props.confirmedLinks,()=>props.selectedLinkIds,()=>props.focusNodeId,()=>props.focusDirection],overlays)
onBeforeUnmount(()=>{++fitEpoch;++layoutEpoch;task?.cancel();globalThis.cancelAnimationFrame(previewFrame);resizeObserver?.disconnect();graph?.dispose();graph=undefined})
defineExpose({arrange,fit:()=>graph?.zoomToFit({padding:56,maxScale:1}),zoomIn:()=>graph?.zoom(0.15),zoomOut:()=>graph?.zoom(-0.15)})
</script>
<template><div class="topology-canvas" role="img" aria-label="同步拓扑；点击集群查看上下游，点击连线查看同步链路"><div ref="container" class="topology-graph-host" /><div v-if="loading||message" class="layout-notice" aria-live="polite">{{ loading?'正在整理布局…':message }}<button v-if="!loading" @click="emit('organize')">重新整理</button></div></div></template>
<style scoped>
.topology-canvas{position:relative}.layout-notice{position:absolute;top:12px;left:12px;right:12px;padding:10px 14px;background:#fff;border:1px solid #dce4ef;border-radius:6px;color:#53647c;font-size:13px;box-shadow:0 2px 8px #23395d0a}.layout-notice button{margin-left:12px;color:#315fc1;border:0;background:none;cursor:pointer}
</style>
