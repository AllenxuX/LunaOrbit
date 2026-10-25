<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { registryRequest, type ClusterPage } from '../api/registry'
import { kafkaList, type Topic } from '../api/kafkaops'
import { readLineage, observedTime, evidenceLabel, kindLabel, resourcePath, type Direction, type Lineage, type LineageNode } from '../api/topic-lineage'
import { cacheKey, classifyLineage, collapseLineage, conditionsChanged, existenceLabel, mergeDirections, pickDisplayGraph, type Conditions, type LineageView, type Reason, type RuleLink } from '../topology/lineage-model'
import TopicLineageCanvas from '../components/TopicLineageCanvas.vue'
type ViewEdge=LineageView['edges'][number]
interface Discovery {stale:boolean;observedAt:string;links:RuleLink[]}
const route=useRoute(),router=useRouter()
const cluster=ref(''),topic=ref(''),direction=ref<Direction>('both'),depth=ref(2),showConsumers=ref(false)
const clusters=shallowRef<ClusterPage['items']>([]),topics=ref<string[]>([]),topicState=ref<'unknown'|'loading'|'exists'|'missing'|'unreadable'>('unknown'),catalogError=ref('')
const data=shallowRef<Lineage>(),cached=shallowRef<Lineage>(),discovery=shallowRef<Discovery>(),applied=shallowRef<Conditions>()
const selectedNode=shallowRef<LineageNode>(),selectedEdge=shallowRef<ViewEdge>(),busy=ref(false),error=ref(''),notFound=ref(false),refreshing=ref(false)
let request:InstanceType<typeof globalThis.AbortController>|undefined,epoch=0,catalogEpoch=0,timer:ReturnType<typeof globalThis.setTimeout>|undefined
const form=computed<Conditions>(()=>({cluster:cluster.value,topic:topic.value.trim(),direction:direction.value,depth:depth.value}))
const changed=computed(()=>conditionsChanged(applied.value,form.value))
const display=computed(()=>discovery.value?pickDisplayGraph({current:data.value,cached:cached.value,snapshotStale:discovery.value.stale}):{graph:data.value,banner:'',notice:''})
const view=computed(()=>display.value.graph?collapseLineage(display.value.graph,{showConsumers:showConsumers.value}):undefined)
const reasons=computed<Reason[]>(()=>data.value&&applied.value&&discovery.value?classifyLineage({...applied.value,result:data.value,links:discovery.value.links,snapshotStale:discovery.value.stale}):[])
const rootNode=computed(()=>display.value.graph?.nodes.find(n=>n.id===display.value.graph!.root_id))
const nodeById=(id:string)=>display.value.graph?.nodes.find(n=>n.id===id)
const nodeName=(id:string)=>{const n=nodeById(id);return n?`${n.cluster_name??'集群未知'} / ${n.topic??n.label}`:'资源未知'}
const selectedPath=computed(()=>selectedNode.value?resourcePath(selectedNode.value):undefined)
const canQuery=computed(()=>!!cluster.value&&!!topic.value.trim()&&topicState.value!=='missing'&&!busy.value)
const edgeFreshness=computed(()=>discovery.value?.stale||display.value.graph!==data.value?'数据已过期':'当前')
function existence(n:LineageNode){const g=display.value.graph;return g?existenceLabel(g,n):'Topic 存在性未确认'}
const tags=computed(()=>{const g=display.value.graph;return g?Object.fromEntries(g.nodes.filter(n=>n.kind==='topic').map(n=>[n.id,existenceLabel(g,n)])):{}})
async function loadTopics(){
 const token=++catalogEpoch;topics.value=[];catalogError.value='';if(!cluster.value){topicState.value='unknown';return}
 topicState.value='loading'
 try{const names:string[]=[];for(let page=1;page<=100;page++){const result=await kafkaList<Topic>(cluster.value,'topics',{page,pageSize:100});if(token!==catalogEpoch)return;names.push(...result.items.map(n=>n.name));if(names.length>=result.total||!result.items.length)break}
  topics.value=[...new Set(names)].sort();topicState.value='unknown';checkTopic()}
 catch{if(token===catalogEpoch){topicState.value='unreadable';catalogError.value='Topic 列表暂不可读取，无法确认 Topic 是否存在'}}
}
function checkTopic(){if(topicState.value==='unreadable'||topicState.value==='loading')return;topicState.value=!topic.value.trim()?'unknown':topics.value.includes(topic.value.trim())?'exists':'missing'}
async function readDiscovery():Promise<Discovery>{
 const scopes=await registryRequest<{items:{id:string;synthetic:boolean}[]}>('/discovery/scopes')
 const result:Discovery={stale:false,observedAt:'',links:[]}
 for(const s of scopes.items.filter(x=>!x.synthetic)){
  try{const r=await registryRequest<{snapshot:{stale:boolean;observed_at:string;links:RuleLink[]}}>(`/discovery/scopes/${encodeURIComponent(s.id)}/snapshot?page_size=100`);result.stale||=r.snapshot.stale;result.observedAt=r.snapshot.observed_at;result.links.push(...(r.snapshot.links??[]))}
  catch{result.stale=true}
 }
 return result
}
function readCache(c:Conditions){try{const raw=globalThis.localStorage?.getItem(cacheKey(c));return raw?JSON.parse(raw) as Lineage:undefined}catch{return undefined}}
function writeCache(c:Conditions,g:Lineage){try{globalThis.localStorage?.setItem(cacheKey(c),JSON.stringify(g))}catch{/* storage unavailable: history is best effort */}}
async function query(){if(!canQuery.value)return;const previous=route.fullPath;await router.push({path:'/topic-lineage',query:{cluster_id:cluster.value,topic:topic.value.trim(),depth:String(depth.value),direction:direction.value}});if(previous===route.fullPath)void load()}
async function load(){
 globalThis.clearTimeout(timer);request?.abort();const token=++epoch
 selectedNode.value=undefined;selectedEdge.value=undefined;error.value='';notFound.value=false
 if(!cluster.value||!topic.value.trim()){busy.value=false;data.value=undefined;applied.value=undefined;return}
 const c={...form.value};busy.value=true;request=new globalThis.AbortController();timer=globalThis.setTimeout(()=>request?.abort(),20000)
 try{
  // 上下游 = two directed traversals merged; the backend's mixed both-BFS would leak sibling branches.
  const signal=request.signal,trace=c.direction==='both'?Promise.all([readLineage(c.cluster,c.topic,c.depth,'upstream',signal),readLineage(c.cluster,c.topic,c.depth,'downstream',signal)]).then(([u,d])=>mergeDirections(u,d)):readLineage(c.cluster,c.topic,c.depth,c.direction,signal)
  const [result,disc]=await Promise.all([trace,readDiscovery().catch(():Discovery=>({stale:true,observedAt:'',links:[]}))])
  if(token!==epoch)return
  discovery.value=disc;applied.value=c;data.value=result
  if(!disc.stale&&result.edges.length)writeCache(c,result)
  cached.value=disc.stale?readCache(c):undefined
 }catch(e){
  if(token!==epoch)return
  applied.value=c;data.value=undefined
  if((e as {status?:number}).status===404)notFound.value=true
  else error.value=(e as Error).name==='AbortError'?'查询超时或已取消，请重试':`本次查询失败：${(e as Error).message}`
 }finally{if(token===epoch){busy.value=false;globalThis.clearTimeout(timer)}}
}
async function refreshDiscovery(){
 refreshing.value=true
 try{for(const s of (await registryRequest<{items:{id:string;synthetic:boolean}[]}>('/discovery/scopes')).items.filter(x=>!x.synthetic))await registryRequest(`/discovery/scopes/${encodeURIComponent(s.id)}/runs`,'POST',{})
  for(let i=0;i<20;i++){await new Promise(r=>globalThis.setTimeout(r,500));const d=await readDiscovery();if(!d.stale)break}
  await load()}catch{error.value='刷新发现快照失败，请稍后重试'}finally{refreshing.value=false}
}
function act(a:Reason['actions'][number]){if(a.action==='refresh')void refreshDiscovery();else if(a.action==='deeper'&&depth.value<4){depth.value++;void query()}else void load()}
function syncQuery(){cluster.value=String(route.query.cluster_id??'');topic.value=String(route.query.topic??'');const d=Number(route.query.depth??2);depth.value=[1,2,3,4].includes(d)?d:2;direction.value=['upstream','downstream','both'].includes(String(route.query.direction))?route.query.direction as Direction:'both';void load()}
function asRoot(n:LineageNode){if(n.cluster_id)void router.push({path:'/topic-lineage',query:{cluster_id:n.cluster_id,topic:n.topic??n.label,depth:String(depth.value),direction:direction.value}})}
watch(()=>route.fullPath,syncQuery,{immediate:true})
watch(cluster,()=>void loadTopics(),{immediate:true})
watch(topic,checkTopic)
onMounted(async()=>{try{const result=await registryRequest<ClusterPage>('/kafka/clusters?page=1&page_size=100');clusters.value=result.items}catch{catalogError.value='集群列表加载失败，请刷新重试'}})
onBeforeUnmount(()=>{++epoch;++catalogEpoch;request?.abort();globalThis.clearTimeout(timer)})
</script>
<template>
  <section class="lineage-page registry">
    <header class="page-header"><div><h1>Topic 链路</h1><p>数据视角：从一个 Topic 追踪跨集群的同步配置。连线是配置关系，不代表消息已到达。</p></div><router-link to="/applications">登记应用关系</router-link></header>
    <form class="lineage-query panel" @submit.prevent="query">
      <label>集群<select v-model="cluster" aria-label="集群" required><option value="" disabled>选择集群</option><option v-for="item in clusters" :key="item.id" :value="item.id">{{ item.display_name }}</option></select></label>
      <label class="topic-input">Topic<input v-model="topic" aria-label="Topic" list="lineage-topics" placeholder="从所选集群的 Topic 中搜索" autocomplete="off" required><datalist id="lineage-topics"><option v-for="name in topics" :key="name" :value="name" /></datalist></label>
      <label>方向<select v-model="direction" aria-label="方向"><option value="both">上下游</option><option value="upstream">上游</option><option value="downstream">下游</option></select></label>
      <label>深度（跨集群跳数）<select v-model.number="depth" aria-label="深度"><option v-for="value in 4" :key="value" :value="value">{{ value }} 跳</option></select></label>
      <label class="toggle"><input v-model="showConsumers" data-show-consumers type="checkbox"> 显示消费组与应用登记</label>
      <button class="primary" type="submit" :disabled="!canQuery">{{ busy?'查询中…':changed?'应用条件并查询':'查看链路' }}</button>
    </form>
    <p v-if="topicState==='missing'" data-topic-check class="topic-check" role="status">该 Topic 在所选集群不存在，请从下拉列表中选择该集群实际存在的 Topic。</p>
    <p v-else-if="catalogError" class="topic-check" role="status">{{ catalogError }}</p>
    <p v-if="changed" data-stale-conditions class="conditions-changed" role="status">条件已修改，结果尚未更新。下图仍对应：{{ nodeName(display.graph?.root_id??'') }} · {{ applied?.direction==='both'?'上下游':applied?.direction==='upstream'?'上游':'下游' }} · {{ applied?.depth }} 跳</p>
    <p v-if="error" role="alert">{{ error }} <button @click="load">重试查询</button></p><p v-if="busy" role="status">正在读取 Topic 关系…</p>
    <div v-if="notFound" class="panel lineage-empty" data-not-found role="status"><strong>该 Topic 在所选集群不存在</strong><p>请从所选集群实际存在的 Topic 中重新选择，或返回 Topic 列表确认名称。</p><router-link v-if="cluster" :to="`/kafka/clusters/${encodeURIComponent(cluster)}/details`">查看集群 Topic 列表</router-link></div>
    <template v-else-if="applied&&(data||display.banner)">
      <div v-if="display.banner" data-stale-banner class="stale-banner" role="status"><strong>{{ display.banner }}</strong><span>{{ display.graph?'当前保留最近一次配置图，不能据此判断当前连通或消息到达。':'' }}</span><button :disabled="refreshing" @click="refreshDiscovery">{{ refreshing?'正在刷新…':'刷新发现快照' }}</button></div>
      <p v-if="display.notice" class="panel lineage-empty" data-no-history>{{ display.notice }}</p>
      <dl data-lineage-times class="lineage-times"><dt>查询时间</dt><dd>{{ observedTime(data?.observed_at) }}</dd><dt>配置采集时间</dt><dd>{{ observedTime(discovery?.observedAt) }}{{ discovery?.stale?'（已过期）':'' }}</dd><dt>Topic 元数据时间</dt><dd>{{ observedTime(rootNode?.observed_at) }}</dd></dl>
      <ul v-if="reasons.length" class="lineage-reasons"><li v-for="r in reasons" :key="r.kind" :data-reason="r.kind"><strong>{{ r.title }}</strong><p v-for="f in r.facts" :key="f">{{ f }}</p><div class="reason-actions"><template v-for="a in r.actions" :key="a.label"><router-link v-if="a.to" :to="a.to">{{ a.label }}</router-link><button v-else type="button" @click="act(a)">{{ a.label }}</button></template></div></li></ul>
      <div v-if="view" class="lineage-workspace" :class="{'has-selection': selectedNode || selectedEdge}">
        <TopicLineageCanvas :view="view" :root-id="display.graph!.root_id" :tags="tags" @select-node="selectedNode=$event;selectedEdge=undefined" @select-edge="selectedEdge=$event;selectedNode=undefined" />
        <aside v-if="selectedNode || selectedEdge" class="lineage-detail panel" aria-label="链路详情">
          <button class="detail-close" aria-label="关闭链路详情" @click="selectedNode=undefined;selectedEdge=undefined">关闭</button>
          <template v-if="selectedNode"><h2>{{ kindLabel(selectedNode.kind) }}</h2><h3>{{ selectedNode.cluster_name }} / {{ selectedNode.topic??selectedNode.label }}</h3><dl><template v-if="selectedNode.kind==='topic'"><dt>Topic 存在性</dt><dd>{{ existence(selectedNode) }}</dd></template><template v-if="selectedNode.owner"><dt>负责人</dt><dd>{{ selectedNode.owner }}</dd></template><dt>Topic 元数据时间</dt><dd>{{ observedTime(selectedNode.observed_at) }}</dd></dl><router-link v-if="selectedPath" :to="selectedPath">查看资源详情</router-link><button v-if="selectedNode.kind==='topic' && selectedNode.cluster_id" @click="asRoot(selectedNode)">以此 Topic 为根</button></template>
          <template v-else-if="selectedEdge"><h2>关系详情</h2><h3>{{ nodeName(selectedEdge.source) }} → {{ nodeName(selectedEdge.target) }}</h3><dl data-edge-facts><dt>证据类型</dt><dd data-edge-evidence>{{ evidenceLabel(selectedEdge.evidence) }}</dd><dt>运行状态</dt><dd>状态未知</dd><dt>新鲜度</dt><dd data-edge-freshness>{{ edgeFreshness }}</dd><template v-if="selectedEdge.linkId"><dt>MM2 任务</dt><dd><router-link data-edge-mm2 :to="`/topology/${encodeURIComponent(selectedEdge.scope_id??'readonly-lab')}/links/${encodeURIComponent(selectedEdge.linkId)}`">{{ selectedEdge.label }}</router-link> · <router-link to="/topology?view=mm2">在 MM2 拓扑查看</router-link></dd></template><template v-if="selectedEdge.policy"><dt>命名策略</dt><dd>{{ selectedEdge.policy }}</dd></template><template v-if="selectedEdge.rule"><dt>匹配规则</dt><dd>{{ selectedEdge.rule }}</dd></template><template v-if="selectedEdge.account"><dt>Kafka 账号</dt><dd>{{ selectedEdge.account }}</dd></template><dt>配置采集时间</dt><dd>{{ observedTime(selectedEdge.observed_at) }}</dd></dl></template>
        </aside>
      </div>
      <details v-if="view" class="lineage-accessible"><summary>节点与关系列表</summary><div><button v-for="node in view.nodes" :key="node.id" :data-lineage-node="node.id" @click="selectedNode=node;selectedEdge=undefined">{{ kindLabel(node.kind) }}：{{ node.cluster_name }} / {{ node.topic??node.label }}</button></div><div><button v-for="edge in view.edges" :key="edge.id" :data-lineage-edge="edge.id" @click="selectedEdge=edge;selectedNode=undefined">{{ nodeName(edge.source) }} → {{ nodeName(edge.target) }}{{ edge.label?'：'+edge.label:'' }}</button></div></details>
    </template>
    <div v-else-if="!busy && !error" class="panel lineage-empty">选择集群和该集群中的 Topic，查看其跨集群同步关系。</div>
  </section>
</template>
<style scoped>.page-header h1{margin-bottom:8px}.page-header p,.lineage-detail p{color:#64748b;font-size:13px}.lineage-query{display:flex;align-items:end;gap:14px;padding:18px;margin-bottom:12px;flex-wrap:wrap}.lineage-query label{display:flex;flex-direction:column;gap:8px;font-size:12px;color:#526174}.lineage-query label.toggle{flex-direction:row;align-items:center;height:38px}.lineage-query select,.lineage-query input:not([type=checkbox]){height:38px;border:1px solid #ccd7e5;border-radius:5px;padding:0 10px;background:#fff;color:#25304a;min-width:100px}.lineage-query .topic-input{flex:1;min-width:200px}.lineage-query button{height:38px}.topic-check{color:#9a5b12;font-size:13px}.conditions-changed{background:#eef4ff;color:#2a4f9a;padding:10px 12px;border-radius:6px;font-size:13px}.stale-banner{display:flex;gap:14px;align-items:center;flex-wrap:wrap;background:#fff1e0;border:1px solid #e3a65a;color:#7a4a10;padding:12px 14px;border-radius:6px;margin:12px 0;font-size:13px}.lineage-times{display:grid;grid-template-columns:repeat(3,auto 1fr);gap:6px 10px;font-size:12px;color:#64748b;margin:14px 0}.lineage-times dd{margin:0;color:#25304a}.lineage-reasons{list-style:none;padding:0;margin:0 0 14px;display:grid;gap:10px}.lineage-reasons li{background:#fff;border:1px solid #e3c99a;border-left:4px solid #d08a2c;border-radius:6px;padding:12px 14px;font-size:13px}.lineage-reasons p{margin:6px 0;color:#53647c}.reason-actions{display:flex;gap:12px;flex-wrap:wrap}.lineage-workspace{display:grid;grid-template-columns:minmax(0,1fr);gap:16px}.lineage-workspace.has-selection{grid-template-columns:minmax(0,1fr) 280px}.detail-close{float:right}.lineage-detail{padding:20px;overflow-wrap:anywhere}.lineage-detail h2{font-size:14px;margin:0 0 18px}.lineage-detail h3{font-size:15px}.lineage-detail dt{font-size:12px;color:#64748b;margin-top:14px}.lineage-detail dd{font-size:13px;margin:6px 0}.lineage-detail button{margin-top:14px}.lineage-empty{padding:22px;color:#64748b;font-size:13px}.lineage-accessible{margin-top:16px;font-size:12px;color:#64748b}.lineage-accessible summary{cursor:pointer}.lineage-accessible button{margin:8px 8px 0 0;font-size:12px}@media(max-width:1000px){.lineage-workspace,.lineage-workspace.has-selection{grid-template-columns:1fr}.lineage-query label{flex:1}.lineage-times{grid-template-columns:auto 1fr}}
</style>
