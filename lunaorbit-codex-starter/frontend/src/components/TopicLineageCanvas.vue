<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { Graph, type Cell } from '@antv/x6'
import { createLayoutTask, type LayoutTask } from '../topology/layout-client'
import { labelDistance, middleEllipsis } from '../topology/cluster-presentation'
import { kindLabel, lineagePresentation, type LineageNode } from '../api/topic-lineage'
import type { LineageView, NodeState } from '../topology/lineage-model'
type ViewEdge=LineageView['edges'][number]
const props=withDefaults(defineProps<{view:LineageView;rootId:string;states?:Record<string,NodeState>}>(),{states:()=>({})})
const emit=defineEmits<{selectNode:[node:LineageNode];selectEdge:[edge:ViewEdge]}>()
const W=220,H=72
const host=ref<InstanceType<typeof globalThis.HTMLDivElement>>(),panel=ref<InstanceType<typeof globalThis.HTMLElement>>(),busy=ref(false),error=ref('')
let graph:Graph|undefined,task:LayoutTask|undefined,epoch=0,observer:InstanceType<typeof globalThis.ResizeObserver>|undefined,resizeFrame=0
function fit(){graph?.zoomToFit({padding:42,maxScale:1})}
function readableFit(){fit();if(graph&&graph.zoom()<0.72){graph.zoomTo(0.72);const root=graph.getCellById(props.rootId);if(root)graph.centerCell(root)}}
async function fullscreen(){try{if(globalThis.document.fullscreenElement)await globalThis.document.exitFullscreen();else await panel.value?.requestFullscreen();fit()}catch{error.value='当前浏览器无法进入全屏'}}
function existence(n:LineageNode){return n.kind!=='topic'?kindLabel(n.kind):props.states[n.id]?.text??(n.exists===true?'':'Topic 存在性未确认')}
async function render(){const token=++epoch;task?.cancel();graph?.resetCells([]);busy.value=true;error.value='';task=createLayoutTask(lineagePresentation(props.view),W,H)
 try{const plan=await task.result;if(token!==epoch||!graph)return;const cells:Cell[]=[]
 for(const node of props.view.nodes){const p=plan.positions[node.id];if(!p)continue;const root=node.id===props.rootId,st=props.states[node.id]?.kind,missing=st==='pending'||st==='break'
  const ports=props.view.edges.flatMap(e=>(['source','target'] as const).filter(end=>e[end]===node.id).map(end=>{const anchor=plan.ports[e.id]![end];return{id:`${e.id}:${end}`,group:'fixed',args:{x:anchor.side==='right'?W:0,y:anchor.offset}}}))
  // Cluster and Topic are two equally prominent lines: same size and weight.
  cells.push(graph.createNode({id:node.id,shape:'rect',x:p.x,y:p.y,width:W,height:H,markup:[{tagName:'title',selector:'title'},{tagName:'rect',selector:'body'},{tagName:'text',selector:'cluster'},{tagName:'text',selector:'topic'},{tagName:'text',selector:'tag'}],ports:{groups:{fixed:{position:'absolute',attrs:{circle:{r:0}}}},items:ports},
   attrs:{title:{text:`${node.cluster_name??''} / ${node.topic??node.label}`},body:{fill:root?'#eaf1ff':'#fff',stroke:missing?'#d08a2c':root?'#315fc1':'#8fa3c2',strokeWidth:root||missing?2.4:1.2,strokeDasharray:st==='pending'?'6 4':'0',rx:6,ry:6},
    cluster:{text:middleEllipsis(node.cluster_name??kindLabel(node.kind),24),refX:12,refY:20,textAnchor:'start',fontSize:14,fontWeight:600,fill:'#25304a'},
    topic:{text:middleEllipsis(node.topic??node.label,24),refX:12,refY:42,textAnchor:'start',fontSize:14,fontWeight:600,fill:'#25304a'},
    tag:{text:existence(node),refX:12,refY:62,textAnchor:'start',fontSize:11,fontWeight:st==='source'?700:400,fill:st==='source'?'#1f7a45':missing?'#9a5b12':'#6b778c'}},data:node}))}
 for(const edge of props.view.edges){const route=plan.routes[edge.id];if(!route)continue;const box=plan.labels?.[edge.id]
  cells.push(graph.createEdge({id:edge.id,source:{cell:edge.source,port:`${edge.id}:source`},target:{cell:edge.target,port:`${edge.id}:target`},vertices:route.slice(1,-1),connector:{name:'rounded',args:{radius:5}},
   attrs:{line:{stroke:edge.evidence==='unknown'?'#ad7829':'#8a94a6',strokeWidth:1.7,strokeDasharray:edge.mm2Id?'0':'6 4',targetMarker:{name:'classic',size:7}}},
   labels:edge.label?[{position:box?{distance:Math.max(1.01,labelDistance(route,box)),offset:0}:{distance:0.5,offset:0},attrs:{label:{text:middleEllipsis(edge.label,28),fontSize:11,fill:'#334761'},body:{fill:'#fff',stroke:'#dce4ef',rx:4,ry:4}}}]:[],data:edge}))}
 graph.resetCells(cells);readableFit()
 }catch(e){if(token===epoch&&(e as Error).name!=='AbortError')error.value='自动布局失败，请重试'}finally{if(token===epoch)busy.value=false}}
onMounted(()=>{graph=new Graph({container:host.value!,autoResize:false,panning:true,mousewheel:{enabled:true,modifiers:['ctrl','meta'],minScale:0.15,maxScale:2},background:{color:'#f8faff'},grid:{visible:true,size:20},interacting:false});graph.on('node:click',({node})=>emit('selectNode',node.getData<LineageNode>()));graph.on('edge:click',({edge})=>emit('selectEdge',edge.getData<ViewEdge>()));void render();if(globalThis.ResizeObserver){observer=new globalThis.ResizeObserver(()=>{globalThis.cancelAnimationFrame(resizeFrame);resizeFrame=globalThis.requestAnimationFrame(()=>{if(graph&&panel.value&&host.value){graph.resize(panel.value.clientWidth-2,host.value.clientHeight);readableFit()}})});observer.observe(panel.value!)}})
watch(()=>props.view,()=>void render())
onBeforeUnmount(()=>{++epoch;task?.cancel();observer?.disconnect();globalThis.cancelAnimationFrame(resizeFrame);graph?.dispose()})
</script>
<template><section ref="panel" class="lineage-canvas"><div class="canvas-tools"><span>只读链路图 · 节点为「集群 / Topic」，连线标签为 MM2 任务与命名策略</span><button aria-label="缩小链路图" @click="graph?.zoom(-0.15)">−</button><button aria-label="放大链路图" @click="graph?.zoom(0.15)">+</button><button @click="fit">适应画布</button><button @click="fullscreen">全屏</button></div><div ref="host" class="graph-host" role="img" aria-label="Topic 链路画布" :aria-busy="busy" /><p v-if="busy" role="status" class="canvas-message">正在整理链路…</p><div v-if="error" role="alert" class="canvas-message">{{ error }} <button @click="render">重试布局</button></div></section></template>
<style scoped>.lineage-canvas{position:relative;min-width:0;border:1px solid #dce5ef;border-radius:8px;overflow:hidden;background:#f8faff}.lineage-canvas:fullscreen{height:100vh}.canvas-tools{display:flex;align-items:center;gap:8px;padding:10px 12px;background:#fff;border-bottom:1px solid #e2e8f0}.canvas-tools span{flex:1;color:#64748b;font-size:12px}.canvas-tools button{padding:5px 10px}.graph-host{height:550px}.lineage-canvas:fullscreen .graph-host{height:calc(100vh - 55px)}.canvas-message{position:absolute;left:20px;bottom:16px;background:#fff;padding:12px;border:1px solid #dce5ef;border-radius:4px}</style>
