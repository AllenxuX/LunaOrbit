<script setup lang="ts">
import { useRuntime } from '../composables/useRuntime'
import { computed,inject,onMounted,onBeforeUnmount,ref,shallowRef,watch,nextTick } from 'vue'
import { routeLocationKey } from 'vue-router'
import TopologyCanvas from '../components/TopologyCanvas.vue'
import MM2Topology from '../components/MM2Topology.vue'
import TopologyTasks from '../components/TopologyTasks.vue'
import ObservabilityPanel from '../components/ObservabilityPanel.vue'
import { ApiError } from '../api/client'
import { ensureSession,registryRequest,type Cluster,type ClusterPage } from '../api/registry'
import { readSnapshot,scopePath,viewPath,type Snapshot,type View,type Position,type Job } from '../api/topology'
import { filterGraph,linkStatus,mappingLabel,mappingName,nodeIdentity,unknown } from '../topology/model'

import { buildPresentation,focusPresentation,tracePaths } from '../topology/presentation'
import { buildClusterPresentation,directNeighbors,presentationFor } from '../topology/cluster-presentation'
import { emptyLocalView, readLocalView, saveLocalView } from '../topology/local-view'

const route=inject(routeLocationKey,undefined)
const { runtime } = useRuntime()
const isLab=computed(()=>['readonly-lab','admin-lab'].includes(runtime.value?.mode??''))
const search=shallowRef(''),fullscreen=shallowRef(false),graphShell=ref<InstanceType<typeof globalThis.HTMLElement>>()
const searchResults=computed(()=>{
 const query=search.value.trim().toLocaleLowerCase()
 if(!query||!visible.value)return []
 return visible.value.nodes.filter(n=>n.kind==='kafka').map(n=>({id:n.id,name:nodeIdentity(visible.value!,n.id,clusters.value)})).filter(n=>n.name.toLocaleLowerCase().includes(query)).slice(0,20)
})
const locationHints=computed(()=>{
 const result:Record<string,string>={}
 for(const link of visible.value?.links??[])for(const [mapping,end] of [[link.source,'source'],[link.target,'target']] as const){
  const row=mapping.status==='confirmed'?clusters.value.find(c=>c.id===mapping.registration_id):undefined
  if(!row)continue
  const hints=[filters.value.room&&(row.room||unknown)!==filters.value.room?'其他机房':'',filters.value.environment&&(row.environment||unknown)!==filters.value.environment?'其他环境':''].filter(Boolean)
  const edge=visible.value?.edges.find(e=>e.link_id===link.id&&e.relation===(end==='source'?'declared_input':'declared_output'))
  if(edge&&hints.length)result[end==='source'?edge.source:edge.target]=hints.join(' · ')
 }
 return result
})
function chooseSearch(id:string){selectNode(id);search.value='';canvas.value?.fit()}
function clearSelection(){selected.value='';routeLinkIds.value=[];selectedNode.value='';pathKey.value=''}
async function toggleFullscreen(){
 try{if(globalThis.document.fullscreenElement)await globalThis.document.exitFullscreen();else await graphShell.value?.requestFullscreen()}
 catch{notice.value='当前浏览器不支持全屏，请使用适应画布'}
}
function onFullscreen(){fullscreen.value=!!globalThis.document.fullscreenElement;void nextTick(()=>canvas.value?.fit())}
const mappingSelections=ref<Record<string,string>>({})
const scopes=ref<{id:string;synthetic:boolean}[]>([]),scope=ref(''),snapshot=ref<Snapshot>(),clusters=ref<Cluster[]>([])
const positions=ref<Record<string,Position>>({}),filters=ref({room:'',environment:''}),layers=ref({kafka:true,workloads:true})
const includeDisabled=ref(false),mode=shallowRef<'mm2'|'relations'|'instances'|'tasks'>(route?.query.view==='relations'?'relations':route?.query.view==='tasks'?'tasks':route?.query.view==='instances'?'instances':'mm2')
const focusDirection=ref<'neighbors'|'upstream'|'downstream'>('neighbors'),routeLinkIds=ref<string[]>([])
// Existing personal-view API stores both mode layouts using distinct position keys.
const savedPositions=ref<Record<string,Position>>({})
const routeChoices=computed(()=>visible.value?.links.filter(link=>routeLinkIds.value.includes(link.id))??[])
const pathSource=shallowRef(''),pathTarget=shallowRef(''),pathKey=shallowRef('')
const graphMode=computed(()=>mode.value==='relations'?'relations':'clusters')
const clusterGraph=computed(()=>visible.value?buildClusterPresentation(visible.value):undefined)
const disabledCount=computed(()=>snapshot.value?filterGraph(snapshot.value,clusters.value,filters.value,{kafka:true,workloads:true},true).links.filter(l=>l.state==='disabled').length:0)
const directCounts=computed(()=>clusterGraph.value&&selectedNode.value?directNeighbors(clusterGraph.value,selectedNode.value):undefined)
const chainGraph=computed(()=>visible.value?buildPresentation(visible.value,'chain'):undefined)
const pathNodes=computed(()=>visible.value?.nodes.filter(n=>n.kind==='kafka').map(n=>({id:n.id,name:nodeIdentity(visible.value!,n.id,clusters.value)}))??[])
const pathResults=computed(()=>chainGraph.value?tracePaths(chainGraph.value,pathSource.value,pathTarget.value):{paths:[],truncated:false})
const chosenPath=computed(()=>pathResults.value.paths.find(p=>p.key===pathKey.value))
const selectedLinkIds=computed(()=>chosenPath.value?.linkIds??(selected.value?[selected.value]:routeLinkIds.value))
function pathName(ids:string[]){return ids.filter(id=>visible.value?.nodes.some(n=>n.kind==='kafka'&&n.id===id)).map(id=>nodeIdentity(visible.value!,id,clusters.value)).join(' → ')}
function choosePath(){selected.value='';selectedNode.value='';routeLinkIds.value=[]}
watch([pathSource,pathTarget],()=>{pathKey.value='';choosePath()})

const focusName=computed(()=>snapshot.value&&selectedNode.value?nodeIdentity(snapshot.value,selectedNode.value,clusters.value):'')
function layoutKey(id:string){return graphMode.value+':'+id}
function restorePositions(){
 if(!visible.value)return
 positions.value=Object.fromEntries(presentationFor(visible.value,graphMode.value,includeDisabled.value).nodes.filter(n=>savedPositions.value[layoutKey(n.id)]).map(n=>[n.id,savedPositions.value[layoutKey(n.id)]!]))
}
function changeMode(value:'mm2'|'relations'|'instances'|'tasks'){mode.value=value;clearSelection();search.value='';if(runtime.value?.information_mode)void loadView(epoch,scope.value);else if(value==='relations'||value==='mm2')restorePositions()}
function selectLink(id:string){if(chosenPath.value&&!chosenPath.value.linkIds.includes(id))pathKey.value='';selected.value=id;routeLinkIds.value=[];selectedNode.value=''}
function selectTask(id:string){selected.value=id;selectedNode.value=''}
function selectLinks(ids:string[]){routeLinkIds.value=[...ids];selected.value='';selectedNode.value=''}
function selectNode(id:string){selectedNode.value=id;selected.value='';routeLinkIds.value=[];pathKey.value=''}

const revision=ref(0),busy=ref(false),ready=ref(false),dirty=ref(false),error=ref(''),notice=ref(''),selected=ref(''),scenario=ref('success')
const canvas=ref<InstanceType<typeof TopologyCanvas>>()
const affectedNodes=ref<string[]>([]),confirmedLinks=ref<string[]>([]),selectedNode=ref('')
let epoch=0,disposed=false
const visible=computed(()=>snapshot.value?filterGraph(snapshot.value,clusters.value,filters.value,{kafka:true,workloads:true},includeDisabled.value):undefined)
watch(pathResults,()=>{if(pathKey.value&&!chosenPath.value)pathKey.value=''})
watch(visible,()=>{
 const graph=visible.value
 if(!graph)return
 const nodes=buildPresentation(graph,'relations').nodes
 if(pathSource.value&&!nodes.some(n=>n.id===pathSource.value))pathSource.value=''
 if(pathTarget.value&&!nodes.some(n=>n.id===pathTarget.value))pathTarget.value=''
 if(selectedNode.value&&!nodes.some(n=>n.id===selectedNode.value))selectedNode.value=''
 if(selected.value&&!graph.links.some(l=>l.id===selected.value))selected.value=''
 routeLinkIds.value=routeLinkIds.value.filter(id=>graph.links.some(l=>l.id===id))
 restorePositions()
})
const focusedLinks=computed(()=>{
 if(!visible.value||!selectedNode.value)return []
 const graph=buildPresentation(visible.value,'relations'),focus=focusPresentation(graph,selectedNode.value,focusDirection.value)
 return graph.edges.filter(e=>focus.edgeIds.includes(e.id)).flatMap(e=>e.linkIds)
})
const pathTasks=computed(()=>chosenPath.value?.linkIds.map(id=>visible.value?.links.find(l=>l.id===id)).filter((link):link is Snapshot['links'][number]=>!!link)??[])
const detail=computed(()=>visible.value?.links.find(l=>l.id===selected.value))
const rooms=computed(()=>[...new Set(clusters.value.map(c=>c.room).filter(Boolean))].sort())
const environments=computed(()=>[...new Set(clusters.value.map(c=>c.environment).filter(Boolean))].sort())
function message(e:unknown) {return e instanceof ApiError ? e.message : '无法完成操作，请稍后重试'}
function clusterName(mapping:Snapshot['links'][number]['source']){return mappingName(mapping,clusters.value)}
function observedTime(value:string){const time=new Date(value);return Number.isNaN(time.getTime())?'未知':time.toLocaleString('zh-CN',{hour12:false})}
function current(id:number){return !disposed&&epoch===id}
async function allClusters() {
 const list:Cluster[]=[]
 for(let page=1;page<=100;page++){
  const p=await registryRequest<ClusterPage>(`/kafka/clusters?page=${page}&page_size=100`)
  list.push(...p.items)
  if(list.length>=p.total)return list
  if(!p.items.length)break
 }
 throw new ApiError('集群列表不完整','INVALID_RESPONSE')
}
async function loadView(id:number,selectedScope:string) {
 const view=runtime.value?.information_mode ? readLocalView(runtime.value.workspace_id!, selectedScope, mode.value) ?? emptyLocalView() : await registryRequest<View>(viewPath(selectedScope))
 if(!current(id))return
 revision.value=view.revision;filters.value=view.filters;layers.value=view.layers
 savedPositions.value={...view.positions};restorePositions();dirty.value=false
}
async function reload() {
 affectedNodes.value=[];confirmedLinks.value=[];selectedNode.value='';routeLinkIds.value=[];pathSource.value='';pathTarget.value='';pathKey.value=''
 const id=++epoch,selectedScope=scope.value
 busy.value=true;error.value='';notice.value='';selected.value='';snapshot.value=undefined;ready.value=false;positions.value={};dirty.value=false
 try {
  const [s,registry]=await Promise.all([readSnapshot(selectedScope),allClusters()])
  if(!current(id))return
  snapshot.value=s;clusters.value=registry;await loadView(id,selectedScope)
  if(current(id))ready.value=true
 }catch(e){if(current(id)&&e instanceof ApiError&&e.status===404&&isLab.value&&runtime.value?.capabilities.discovery===true){await discover();return}if(current(id)){error.value=e instanceof ApiError&&e.status===404?'尚无发现快照，请先运行发现；若接口未启用，请使用 discovery-backend 启动。':message(e)}}
 finally{if(current(id))busy.value=false}
}
onMounted(async()=>{
 globalThis.document.addEventListener('fullscreenchange',onFullscreen)
 try{await ensureSession();if(disposed)return;const data=await registryRequest<{items:{id:string;synthetic:boolean}[]}>('/discovery/scopes');if(disposed)return;scopes.value=data.items;scope.value=data.items.find(item=>item.id===route?.query.scope)?.id??data.items[0]?.id??'';if(scope.value)await reload();else error.value='暂无可用的发现范围'}
 catch(e){if(!disposed)error.value=e instanceof ApiError&&e.status===404?'发现接口未启用，请使用 discovery-backend 启动。':message(e)}
})
onBeforeUnmount(()=>{disposed=true;epoch++;globalThis.document.removeEventListener('fullscreenchange',onFullscreen)})
function move(id:string,p:Position){positions.value={...positions.value,[id]:{x:p.x,y:p.y}};savedPositions.value[layoutKey(id)]={x:p.x,y:p.y};dirty.value=true}
const layoutBusy=ref(false)
async function reset(){const modeAtStart=graphMode.value,scopeAtStart=scope.value;const next=await canvas.value?.arrange();if(!next||modeAtStart!==graphMode.value||scopeAtStart!==scope.value)return;positions.value=next;for(const [id,p] of Object.entries(next))savedPositions.value[layoutKey(id)]=p;dirty.value=true}
async function save(){
 if(runtime.value?.information_mode){
  const saved=saveLocalView(runtime.value.workspace_id!,scope.value,mode.value,{revision:0,positions:savedPositions.value,filters:filters.value,layers:layers.value})
  if(saved){dirty.value=false;notice.value='个人布局已保存到此浏览器'}else error.value='浏览器无法保存布局，请检查存储空间或隐私设置。'
  return
 }
 const id=epoch,selectedScope=scope.value;busy.value=true;error.value='';notice.value=''
 const submitted=JSON.stringify({positions:savedPositions.value,filters:filters.value,layers:layers.value})
 try{
  const view=await registryRequest<View>(viewPath(selectedScope),'PUT',{revision:revision.value,positions:savedPositions.value,filters:filters.value,layers:layers.value})
  if(!current(id))return;revision.value=view.revision;dirty.value=submitted!==JSON.stringify({positions:savedPositions.value,filters:filters.value,layers:layers.value});notice.value=dirty.value?'先前布局已保存，仍有后续改动未保存':'个人布局已保存'
 }catch(e){if(current(id))error.value=e instanceof ApiError&&e.status===409?'布局已被其他页面修改；本次改动仍保留，请重新加载后调整再保存。':message(e)}
 finally{if(current(id))busy.value=false}
}
async function discover(){
 if(runtime.value?.capabilities.discovery !== true)return
 const id=++epoch,selectedScope=scope.value;busy.value=true;error.value='';notice.value=runtime.value?.mode === 'mock'?'正在读取合成资源…':'正在读取资源…'
 try{
  await registryRequest(scopePath(selectedScope)+'/runs','POST',{scenario:scenario.value})
  for(let attempt=0;attempt<30&&current(id);attempt++){
   const job=await registryRequest<Job>(scopePath(selectedScope)+'/runs/latest')
   if(!current(id))return
   if(job.status==='done'){
    // Refresh graph without discarding unsaved personal coordinates/filters.
    const s=await readSnapshot(selectedScope);const registry=await allClusters();if(!current(id))return
    snapshot.value=s;clusters.value=registry;restorePositions()
    if(!ready.value)await loadView(id,selectedScope)
    if(!current(id))return;ready.value=true
    notice.value=job.error_code?'发现未完整成功；请检查过期标记和采集问题。':runtime.value?.mode === 'mock'?'合成资源发现完成；链路健康仍未验证':'资源发现完成；链路健康仍未验证';return
   }
   await new Promise(resolve=>globalThis.setTimeout(resolve,300))
  }
  if(current(id))notice.value='任务仍在运行，请稍后重新加载'
 }catch(e){if(current(id))error.value=message(e)}finally{if(current(id))busy.value=false}
}
async function confirmMapping(alias:string){
 const link=detail.value,registrationId=mappingSelections.value[alias]
 if(!link||!registrationId||busy.value)return
 const id=epoch,selectedScope=scope.value;busy.value=true;error.value=''
 try{
  await registryRequest(scopePath(selectedScope)+'/bindings','PUT',{namespace:link.namespace,workload_uid:link.workload_uid,alias,registration_id:registrationId})
  if(current(id))await discover()
 }catch(e){if(current(id))error.value=message(e)}finally{if(current(id))busy.value=false}
}</script>
<template>
  <section class="topology registry">
    <header class="page-header"><div class="topology-heading"><h1>同步拓扑</h1><span v-if="snapshot" class="topology-updated">{{ snapshot.synthetic?'合成数据 · ':'' }}更新于 {{ observedTime(snapshot.observed_at) }}{{ snapshot.stale?' · 数据过期':'' }}</span></div><span v-if="isLab" class="status-pill">实验环境</span></header>
    <p v-if="error" role="alert" class="error">{{ error }}</p><p v-if="notice" role="status">{{ notice }}</p>
    <div class="filter-bar topology-toolbar panel">
      <div class="view-switch" aria-label="拓扑视图"><button data-mode="mm2" :aria-pressed="mode==='mm2'" @click="changeMode('mm2')">MM2 拓扑</button><button data-mode="relations" :aria-pressed="mode==='relations'" @click="changeMode('relations')">同步关系</button><button data-mode="instances" :aria-pressed="mode==='instances'" @click="changeMode('instances')">按实例查看</button><button data-mode="tasks" :aria-pressed="mode==='tasks'" @click="changeMode('tasks')">同步任务</button></div>
      <div v-if="mode!=='tasks'" class="cluster-search"><input v-model="search" data-cluster-search aria-label="搜索集群" placeholder="搜索集群" autocomplete="off" @keydown.esc="search=''" /><ul v-if="search.trim()" class="search-results"><li v-for="node in searchResults" :key="node.id"><button :data-search-node="node.id" @click="chooseSearch(node.id)">{{ node.name }}</button></li><li v-if="!searchResults.length" class="search-empty">当前筛选下没有匹配集群</li></ul></div>
      <label v-if="scopes.length>1">范围 <select v-model="scope" :disabled="busy" @change="reload"><option v-for="s in scopes" :key="s.id" :value="s.id">{{ s.id }}</option></select></label>
      <label>机房 <select v-model="filters.room" aria-label="机房筛选" @change="dirty=true;selected='' "><option value="">全部</option><option :value="unknown">未知 / 待确认</option><option v-for="r in rooms" :key="r" :value="r">{{ r }}</option></select></label>
      <label>环境 <select v-model="filters.environment" aria-label="环境筛选" @change="dirty=true;selected='' "><option value="">全部</option><option :value="unknown">未知 / 待确认</option><option v-for="e in environments" :key="e" :value="e">{{ e }}</option></select></label>
      <button class="button-primary toolbar-refresh" data-discover :disabled="busy||!scope||runtime?.capabilities.discovery!==true" @click="discover">{{ busy?'刷新中…':'刷新' }}</button>
      <details class="topology-more"><summary>更多<span v-if="dirty" class="dirty-dot" aria-label="有未保存改动"></span></summary><div class="more-menu"><label><input v-model="includeDisabled" data-show-disabled type="checkbox"> {{ mode==='mm2'?`显示禁用链路（${disabledCount}）`:'显示禁用关系' }}</label><button v-if="mode==='relations'||mode==='mm2'" :disabled="busy||layoutBusy||!!chosenPath" @click="reset">整理布局</button><button data-save :disabled="busy||!dirty" @click="save">保存个人布局</button><button :disabled="busy||!scope" @click="reload">重新加载视图{{ dirty?'（丢弃未保存改动）':'' }}</button><span class="helper-text">{{ dirty?'有未保存改动':`布局版本 ${revision}` }}</span><label v-if="runtime?.mode==='mock'">Mock 场景 <select v-model="scenario" :disabled="busy"><option value="success">成功采集</option><option value="partial_failure">部分失败</option><option value="unavailable">服务不可用</option></select></label></div></details>
    </div>
    <template v-if="ready&&snapshot&&visible">
      <div v-if="mode==='relations'||mode==='mm2'" ref="graphShell" class="topology-workspace panel">
        <div class="graph-topline"><div class="topology-legend" aria-label="链路状态图例"><span class="legend-verified">已验证</span><span class="legend-unverified">未验证</span><span class="legend-unknown">状态未知</span><span class="legend-stale">过期 / 禁用</span></div><span class="helper-text">{{ visible.nodes.filter(n=>n.kind==='kafka').length }} 个集群 · {{ mode==='mm2'?`${clusterGraph?.edges.length??0} 条启用链路`:`${visible.links.length} 个任务` }}</span></div>
        <div v-if="selectedNode" class="focus-controls"><strong>{{ focusName }}</strong><span v-if="mode==='mm2'&&directCounts" data-direct-counts>直接上游 {{ directCounts.upstream }} · 直接下游 {{ directCounts.downstream }}</span><select v-model="focusDirection" data-focus-direction aria-label="聚焦范围"><option value="neighbors">直接关联</option><option value="upstream">全部上游</option><option value="downstream">全部下游</option></select><button data-clear-focus @click="clearSelection">显示全部</button></div>
        <div v-if="mode==='mm2'" class="path-controls" aria-label="配置路径追踪"><span title="仅表示启用任务的集群连接关系，Topic 是否连续匹配需逐段确认">配置路径</span><select v-model="pathSource" data-path-source aria-label="路径起点"><option value="">选择起点</option><option v-for="node in pathNodes" :key="node.id" :value="node.id">{{ node.name }}</option></select><span>→</span><select v-model="pathTarget" data-path-target aria-label="路径终点"><option value="">选择终点</option><option v-for="node in pathNodes" :key="node.id" :value="node.id">{{ node.name }}</option></select><select v-if="pathResults.paths.length" v-model="pathKey" data-path-choice aria-label="选择配置路径" @change="choosePath"><option value="">选择路径（{{ pathResults.paths.length }}）</option><option v-for="(path,index) in pathResults.paths" :key="path.key" :value="path.key">{{ index+1 }}. {{ pathName(path.nodeIds) }} · {{ path.linkIds.length }} 段任务</option></select><span v-else-if="pathSource&&pathTarget" class="helper-text">{{ pathSource===pathTarget?'请选择不同的起点和终点':'未找到启用的配置路径' }}</span><button v-if="chosenPath" data-clear-path @click="clearSelection">返回全图</button><span v-if="pathResults.truncated" class="helper-text">路径结果已截断，请缩小范围</span></div>
        <TopologyCanvas ref="canvas" :snapshot="visible" :positions="positions" :mode="graphMode" :show-disabled="includeDisabled" :path-node-ids="chosenPath?.nodeIds" :selected-link-ids="selectedLinkIds" :focus-node-id="selectedNode" :focus-direction="focusDirection" :registrations="clusters" :node-hints="locationHints" :affected-nodes="affectedNodes" :confirmed-links="confirmedLinks" @organize="reset" @layout-busy="layoutBusy=$event" @move="move" @select="selectLink" @select-links="selectLinks" @select-node="selectNode" @clear="clearSelection" />
        <p v-if="!visible.nodes.length" class="empty-graph">当前筛选下没有同步关系</p>
        <div class="graph-controls" aria-label="画布操作"><button aria-label="放大" @click="canvas?.zoomIn()">＋</button><button aria-label="缩小" @click="canvas?.zoomOut()">－</button><button @click="canvas?.fit()">适应画布</button><button @click="toggleFullscreen">{{ fullscreen?'退出全屏':'全屏' }}</button></div>
      </div>
      <section v-if="mode==='instances'" class="mm2-view">
        <div class="mm2-view-heading"><span>源 Kafka → MM2 同步任务 → 目标 Kafka</span><button v-if="selected||selectedNode" data-clear-task @click="clearSelection">显示全部路径</button></div>
        <div v-if="selectedNode" class="focus-controls"><strong>{{ focusName }}</strong><select v-model="focusDirection" data-focus-direction aria-label="聚焦范围"><option value="neighbors">直接关联</option><option value="upstream">全部上游</option><option value="downstream">全部下游</option></select><button data-clear-focus @click="clearSelection">显示全部</button></div>
        <MM2Topology :key="scope" :snapshot="visible" :registrations="clusters" :selected-id="selected" :focus-active="!!selectedNode" :focused-link-ids="focusedLinks" :node-hints="locationHints" @select="selectLink" @select-node="selectNode" />
      </section>
      <component :is="mode==='tasks'?'section':'details'" class="panel task-panel" :open="mode==='relations'" aria-label="同步任务">
        <component :is="mode==='tasks'?'div':'summary'" class="panel-header"><h2>{{ routeChoices.length?'所选关系的同步任务':'同步任务' }}</h2><div class="task-panel-actions"><span class="helper-text">{{ routeChoices.length||visible.links.length }} 个任务</span><button v-if="routeChoices.length" @click.stop.prevent="clearSelection">显示全部任务</button></div></component>
        <TopologyTasks :links="routeChoices.length?routeChoices:visible.links" :registrations="clusters" :scope="scope" :stale="snapshot.stale" :selected-id="selected" :selection-mode="routeChoices.length>0" @select="selectTask" />
      </component>
      <section v-if="chosenPath" class="panel traced-tasks" aria-label="路径任务"><div class="panel-header"><h2>{{ pathName(chosenPath.nodeIds) }}</h2><span class="helper-text">{{ chosenPath.linkIds.length }} 段配置任务</span></div><ol><li v-for="(link,index) in pathTasks" :key="link.id"><button :data-path-task="link.id" @click="selectLink(link.id)">{{ index+1 }}. {{ link.workload_name }} · {{ clusterName(link.source) }} → {{ clusterName(link.target) }}</button></li></ol></section>
      <section v-if="detail" class="evidence-panel panel" aria-label="链路详情">
        <button class="close-detail" aria-label="关闭链路详情" @click="selected='';routeLinkIds=[]">关闭</button>

        <template v-if="detail">
          <h2>同步链路</h2><router-link :to="`/topology/${encodeURIComponent(scope)}/links/${encodeURIComponent(detail.id)}`">打开链路详情</router-link><p class="uncertain">{{ snapshot.stale?'数据已过期 · 状态未知':linkStatus(detail) }}</p>
          <section data-link-summary class="link-summary">
            <div v-for="(m,i) in [detail.source,detail.target]" :key="i" class="endpoint-summary"><span>{{ i===0?'源集群':'目标集群' }}</span><router-link v-if="m.registration_id" :to="`/kafka/clusters/${m.registration_id}/details`">{{ clusterName(m) }}</router-link><strong v-else>{{ clusterName(m) }}</strong></div>
            <div><h3>Topic 匹配规则</h3><code class="topic-rule">{{ detail.topics_raw||'尚未获取' }}</code></div>
            <div><h3>运行概况</h3><dl class="runtime-summary"><dt>MM2 实例</dt><dd>{{ detail.workload_name }}</dd><dt>就绪副本</dt><dd>{{ detail.runtime.ready_replicas }} / {{ detail.runtime.replicas }}</dd><dt>最近发现</dt><dd>{{ observedTime(detail.observed_at) }}</dd></dl></div>
          </section>
          <details class="link-options"><summary>调整集群关联</summary><div v-for="(m,i) in [detail.source,detail.target]" :key="i" class="mapping-card"><label>{{ i===0?'源集群':'目标集群' }}<select v-model="mappingSelections[m.alias]" :disabled="busy"><option value="">{{ clusterName(m) }}</option><option v-for="row in clusters" :key="row.id" :value="row.id">{{ row.display_name }}{{ row.room ? ' · '+row.room : '' }}{{ row.environment ? ' · '+row.environment : '' }}</option></select></label><button :disabled="busy||!mappingSelections[m.alias]" @click="confirmMapping(m.alias)">确认映射并重新发现</button></div></details>
          <details data-diagnostics class="link-options"><summary>诊断信息</summary><p>配置声明 {{ detail.declared?'已发现':'未发现' }} · 运行资源 {{ detail.observed?'已发现':'未发现' }} · 同步健康 {{ detail.verified?'已验证':'未验证' }}</p><p>源关联：{{ mappingLabel(detail.source) }}；目标关联：{{ mappingLabel(detail.target) }}</p><p>MM2 配置别名：{{ detail.source_alias }} → {{ detail.target_alias }}</p><p>Topic 命名策略：{{ detail.policy||'未知' }} · RF 未知</p><p>Deployment：{{ detail.namespace }} / {{ detail.workload_name }} · UID {{ detail.workload_uid }}</p><p>generation {{ detail.runtime.observed_generation }} / {{ detail.runtime.generation }} · 候选 Pod {{ detail.runtime.pod_candidates }}</p><article v-for="(e,i) in detail.evidence" :key="i" class="mapping-card"><strong>{{ e.configmap_name }} / {{ e.config_key }}</strong><p>UID {{ e.configmap_uid }} · RV {{ e.resource_version }}</p><p>指纹 {{ e.fingerprint }}</p></article><p v-for="note in detail.notes" :key="note" class="hint">{{ note }}</p></details>
        </template><template v-else><h2>链路详情</h2><p class="hint">选择一条同步链路，查看源、目标和同步 Topic 规则。</p></template>
      </section>
      <ObservabilityPanel v-if="!runtime?.information_mode && runtime?.capabilities.observability === true" :scope-id="scope" :snapshot="snapshot" :selected-node-id="selectedNode" @affected="affectedNodes=$event" @confirmed="confirmedLinks=$event" />
      <details v-if="snapshot.issues?.length"><summary>采集问题（{{ snapshot.issues.length }}）</summary><p v-for="(issue,i) in snapshot.issues" :key="i">{{ issue.code }} · {{ issue.namespace }} {{ issue.workload_uid }}</p></details>
    </template>
  </section>
</template>




<style scoped>
.path-controls{display:flex;align-items:center;gap:10px;padding:12px 18px;background:#f8fafd;border-bottom:1px solid #e9edf4;flex-wrap:wrap;font-size:12px;color:#64748b}.path-controls select{max-width:300px;min-width:130px}.traced-tasks{margin-bottom:16px}.traced-tasks ol{list-style:none;padding:0 16px 16px;margin:0;display:flex;gap:12px;flex-wrap:wrap}.traced-tasks button{text-align:left;font-size:12px;overflow-wrap:anywhere}.topology-heading{display:flex;align-items:baseline;gap:20px;flex-wrap:wrap}.topology-updated{font-size:12px;color:#718099}.topology-toolbar{gap:12px;padding:14px 16px;position:relative;z-index:3;margin-bottom:16px}.topology-toolbar label{font-size:12px;color:#63738a;display:flex;align-items:center;gap:6px}.topology-toolbar select{max-width:150px}.toolbar-refresh{margin-left:auto}.view-switch{display:inline-flex;border:1px solid #d7e0ed;border-radius:6px;overflow:hidden;flex-shrink:0}.view-switch button{border:0;border-radius:0;margin:0;white-space:nowrap}.view-switch button[aria-pressed="true"]{background:#e8f0ff;color:#2357b3;font-weight:600}.cluster-search{position:relative}.cluster-search input{width:160px}.search-results{position:absolute;top:100%;left:0;min-width:260px;max-width:340px;max-height:260px;overflow:auto;padding:6px;margin:4px 0;list-style:none;border:1px solid #dce4ef;background:#fff;border-radius:6px;box-shadow:0 8px 24px #19365516;z-index:8}.search-results button{width:100%;text-align:left;border:0;overflow-wrap:anywhere}.search-empty{padding:12px;font-size:12px;color:#718099}.topology-more{position:relative}.topology-more>summary{cursor:pointer;color:#52657d;list-style:none;padding:8px;display:flex;align-items:center;gap:6px;font-size:13px}.dirty-dot{width:6px;height:6px;border-radius:50%;background:#d69a35}.more-menu{position:absolute;right:0;top:100%;min-width:235px;background:#fff;border:1px solid #dce4ef;border-radius:6px;padding:12px;display:grid;gap:8px;box-shadow:0 8px 24px #19365516;z-index:9}.more-menu button{text-align:left}.more-menu label{padding:8px 0}
.topology-workspace{display:block;position:relative;overflow:hidden;padding:0;margin-bottom:16px}.graph-topline{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:14px 18px;border-bottom:1px solid #e9edf4;flex-wrap:wrap}.topology-legend{display:flex;gap:18px;flex-wrap:wrap;font-size:12px;color:#64748b}.topology-legend span{display:flex;align-items:center;gap:7px}.topology-legend span:before{content:'';width:20px;border-top:2px solid #35744e}.topology-legend .legend-unverified:before{border-color:#758aa5;border-top-style:dashed}.topology-legend .legend-unknown:before{border-color:#bd8b39;border-top-style:dashed}.topology-legend .legend-stale:before{border-color:#929aa6;border-top-style:dotted}.focus-controls{display:flex;align-items:center;gap:12px;padding:10px 18px;background:#f1f6ff;font-size:12px;flex-wrap:wrap}.graph-controls{position:absolute;right:16px;bottom:16px;display:flex;gap:1px;background:#fff;border:1px solid #dce4ef;border-radius:6px;overflow:hidden;box-shadow:0 3px 12px #1936550d}.graph-controls button{border:0;border-radius:0;padding:8px 12px;font-size:12px}.topology-workspace :deep(.topology-canvas){height:clamp(380px,55vh,620px);border:0;border-radius:0}.topology-workspace:fullscreen{background:#fff;display:flex;flex-direction:column}.topology-workspace:fullscreen :deep(.topology-canvas){height:auto;flex:1}.empty-graph{position:absolute;top:45%;left:0;width:100%;text-align:center;color:#718099;pointer-events:none}.task-panel{overflow:hidden;margin-bottom:16px;min-width:0}.task-panel-actions{display:flex;align-items:center;gap:12px}.mm2-view{min-width:0}.mm2-view-heading{display:flex;align-items:center;justify-content:space-between;margin:0 0 12px;color:#718099;font-size:12px;gap:12px}.mm2-view .focus-controls{margin-bottom:12px;border-radius:6px}.task-panel>summary{cursor:pointer}.evidence-panel{height:auto;max-height:none;overflow:visible;padding:20px;margin-bottom:16px;font-size:12px;min-width:0}.close-detail{float:right}.link-summary{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:20px;align-items:start;margin:16px 0}.endpoint-summary{display:flex;flex-direction:column;gap:8px}.endpoint-summary>span,.runtime-summary dt{color:#718099}.endpoint-summary a,.endpoint-summary strong{color:#1e3a5f;font-size:14px}.link-summary h3{margin:0}.topic-rule{display:block;padding:10px;background:#f4f7fb;border-radius:4px;overflow-wrap:anywhere}.runtime-summary{display:grid;grid-template-columns:76px minmax(0,1fr);gap:10px;margin:0}.runtime-summary dd{margin:0;overflow-wrap:anywhere}.link-options{border-top:1px solid #e8edf3;padding:14px 0;font-size:12px}.link-options summary{cursor:pointer;color:#64748b}
@media(max-width:1000px){.topology-toolbar{gap:8px}.cluster-search input{width:140px}.link-summary{grid-template-columns:repeat(2,minmax(0,1fr))}.graph-topline{padding:12px}.topology-legend{gap:12px}.topology-workspace :deep(.topology-canvas){height:440px}}
@media(max-width:550px){.topology-toolbar{padding:12px}.topology-toolbar select{max-width:110px}.cluster-search{flex:1}.cluster-search input{width:100%;min-width:100px}.view-switch{width:100%}.view-switch button{flex:1}.link-summary{grid-template-columns:1fr}.graph-controls{right:8px;bottom:8px}.graph-controls button{padding:8px}.topology-workspace :deep(.topology-canvas){height:380px}.topology-legend{gap:10px;font-size:11px}.search-results{min-width:220px;max-width:calc(100vw - 70px)}}
</style>
