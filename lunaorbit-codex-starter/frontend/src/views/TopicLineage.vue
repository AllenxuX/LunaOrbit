<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { registryRequest, type ClusterPage } from '../api/registry'
import { kafkaList, type Topic } from '../api/kafkaops'
import { readLineage, observedTime, evidenceLabel, kindLabel, resourcePath, type Direction, type Lineage, type LineageNode } from '../api/topic-lineage'
import { cacheKey, classifyCandidate, syncCoverage, classifyLineage, collapseLineage, conditionsChanged, unregisteredBreaks, pickDisplayGraph, resolveSources, type NodeState, type Conditions, type LineageView, type Reason, type RuleLink } from '../topology/lineage-model'
import { readCatalog, readTopics as readCatalogTopics, readable, withCatalogEnds, type CatalogEndpoint } from '../api/catalog'
import { refreshMessage, type RefreshMessage, type RefreshSnapshot } from '../topology/refresh-result'
import TopicLineageCanvas from '../components/TopicLineageCanvas.vue'
import { fullName } from '../topology/scopes'
import { policyLabel, topicMatchLabel } from '../topology/policy'
type ViewEdge=LineageView['edges'][number]
interface Discovery {stale:boolean;observedAt:string;links:RuleLink[]}
const route=useRoute(),router=useRouter()
const cluster=ref(''),topic=ref(''),direction=ref<Direction>('both'),depth=ref(2),showConsumers=ref(false)
const clusters=shallowRef<ClusterPage['items']>([]),topics=ref<string[]>([]),topicState=ref<'unknown'|'loading'|'exists'|'missing'|'unreadable'>('unknown'),catalogError=ref('')
const catalog=shallowRef<CatalogEndpoint[]>([])
const reasonText:Record<string,string>={timeout:'连接超时',refused:'连接被拒绝',blocked:'出站未允许',tls:'TLS 失败',unresolvable:'地址无法解析',auth:'认证失败',authorization:'授权不足'}
function aliasOption(r:CatalogEndpoint){const addr=(r.resolved[0]??r.addresses[0]??'地址未解析');const name=r.aliases.join(' / ')||r.display_name||'—';if(r.source==='manual')return `${r.display_name||name} · 手工补充 · ${addr}`;const st=readable(r)?'可读':r.access==='not_connected'?'未连接':`不可读（${reasonText[r.access_reason??'']??(r.access==='address_unresolvable'?'地址无法解析':'凭据未开启或不可读')}）`;return `${name} · ${addr} · ${st}`}
const selectedRow=computed(()=>catalog.value.find(r=>r.lineage_id===cluster.value))
const configOnly=computed(()=>!!selectedRow.value&&selectedRow.value.read_via==='auto'&&!readable(selectedRow.value))
const data=shallowRef<Lineage>(),cached=shallowRef<Lineage>(),discovery=shallowRef<Discovery>(),applied=shallowRef<Conditions>()
const selectedNode=shallowRef<LineageNode>(),selectedEdge=shallowRef<ViewEdge>(),busy=ref(false),error=ref(''),notFound=ref(false),refreshing=ref(false)
let request:InstanceType<typeof globalThis.AbortController>|undefined,epoch=0,catalogEpoch=0,timer:ReturnType<typeof globalThis.setTimeout>|undefined
const form=computed<Conditions>(()=>({cluster:cluster.value,topic:topic.value.trim(),direction:direction.value,depth:depth.value}))
const changed=computed(()=>conditionsChanged(applied.value,form.value))
const display=computed(()=>discovery.value?pickDisplayGraph({current:data.value,cached:cached.value,snapshotStale:discovery.value.stale}):{graph:data.value,banner:'',notice:''})
const unparsedIssues=computed(()=>(data.value?.issues??[]).filter(i=>i.code==='FILTER_UNKNOWN'||i.code==='SEMANTICS_UNKNOWN'))
// Several K8s scopes: MM2 labels carry "K8s / namespace / name" so same names never look alike.
const ruleById=computed(()=>new Map((discovery.value?.links??[]).map(l=>[l.id,l])))
const multiScope=computed(()=>new Set((discovery.value?.links??[]).filter(l=>l.k8s_cluster).map(l=>`${l.k8s_cluster}/${l.namespace}`)).size>1)
function scopedLabel(e:ViewEdge){const r=e.linkId?ruleById.value.get(e.linkId):undefined;return r&&multiScope.value&&r.k8s_cluster&&e.label?e.label.replace(/^.*? · /,fullName({k8s_cluster:r.k8s_cluster,namespace:r.namespace??'',workload_name:r.workload_name})+' · '):e.label}
const mm2Path=(linkId:string)=>{const scope=ruleById.value.get(linkId)?.scope_id;return scope?`/topology/${encodeURIComponent(scope)}/links/${encodeURIComponent(linkId)}`:'/topology?view=mm2'}
const view=computed(()=>{if(!display.value.graph)return undefined;const v=collapseLineage(display.value.graph,{showConsumers:showConsumers.value}),ids=new Set(unparsedIssues.value.map(i=>i.link_id));return {...v,edges:v.edges.map(e=>{const label=scopedLabel(e);return e.linkId&&ids.has(e.linkId)?{...e,label:`${label} · 待确认`}:{...e,label}})}})
const showAllTopics=ref(false)
const ruleLinks=computed(()=>discovery.value?withCatalogEnds(discovery.value.links,catalog.value):undefined)
const candidates=computed(()=>topics.value.map(name=>{const links=ruleLinks.value;if(!links)return {name,kind:'syncing',label:'同步配置覆盖 —'};const c=classifyCandidate(cluster.value,name,links);if(c.kind!=='syncing')return {name,kind:c.kind,label:c.tag};const n=syncCoverage(cluster.value,name,links);return {name,kind:c.kind,label:n===undefined?'同步配置覆盖 —':n===0?'无下游同步配置':`同步配置覆盖 ${n} 个集群`}}))
const shownCandidates=computed(()=>candidates.value.filter(c=>showAllTopics.value||c.kind==='syncing'))
const noCandidate=computed(()=>!configOnly.value&&!showAllTopics.value&&!!cluster.value&&!!topic.value.trim()&&topicState.value!=='loading'&&!shownCandidates.value.some(c=>c.name.includes(topic.value.trim())))
const reasons=computed<Reason[]>(()=>data.value&&applied.value&&discovery.value?classifyLineage({...applied.value,result:data.value,links:ruleLinks.value!,snapshotStale:discovery.value.stale}):[])
const rootNode=computed(()=>display.value.graph?.nodes.find(n=>n.id===display.value.graph!.root_id))
const nodeById=(id:string)=>display.value.graph?.nodes.find(n=>n.id===id)
const nodeName=(id:string)=>{const n=nodeById(id);return n?`${n.cluster_name??'集群未知'} / ${n.topic??n.label}`:'资源未知'}
const selectedPath=computed(()=>{const n=selectedNode.value;if(!n)return undefined;const row=catalog.value.find(r=>r.lineage_id===n.cluster_id);if(row&&n.kind==='topic'&&row.read_via==='auto')return `/endpoints/${encodeURIComponent(row.id)}/topics/${encodeURIComponent(n.topic??n.label)}`;return resourcePath(n)})
const canQuery=computed(()=>!!cluster.value&&!!topic.value.trim()&&topicState.value!=='missing'&&!busy.value)
const edgeFreshness=computed(()=>discovery.value?.stale||display.value.graph!==data.value?'数据已过期':'当前')
const states=shallowRef<Record<string,NodeState>>({}),summary=ref('')
const canvasStates=computed(()=>{const pending=new Set(unparsedIssues.value.map(i=>i.cluster_id).filter(Boolean)),out:Record<string,NodeState>={...states.value};for(const n of data.value?.nodes??[])if(n.cluster_id&&pending.has(n.cluster_id)&&out[n.id])out[n.id]={...out[n.id]!,text:out[n.id]!.text?`${out[n.id]!.text} · 待确认`:'待确认'};const breaks=data.value&&discovery.value?unregisteredBreaks(data.value,ruleLinks.value!):[];for(const n of data.value?.nodes??[])for(const b of breaks)if(n.kind==='topic'&&n.cluster_id===b.clusterId){const prev=out[n.id]??{kind:'exists' as const,text:''},note=`下一跳 ${b.alias} 为自动发现端点，尚未合并登记信息`;if(!prev.text.includes(note))out[n.id]={...prev,text:prev.text?`${prev.text} · ${note}`:note}};return out})
function existence(n:LineageNode){if(n.existence_note&&n.exists==null)return n.existence_note;const st=states.value[n.id];return !st?(n.exists===true?'已存在':'Topic 存在性未确认'):st.kind==='exists'?'已存在':st.text}
async function loadTopics(){
 const token=++catalogEpoch;topics.value=[];catalogError.value='';if(!cluster.value){topicState.value='unknown';return}
 topicState.value='loading'
 const row=selectedRow.value
 if(row&&row.read_via==='auto'){
  if(!readable(row)){topicState.value='unreadable';catalogError.value='无法枚举 Topic；仅沿配置关系查询，Topic 是否存在无法确认';return}
  try{const names:string[]=[];for(let page=1;page<=50;page++){const r=await readCatalogTopics(row.id,'',page);if(token!==catalogEpoch)return;names.push(...r.items.map(t=>t.name));if(names.length>=r.total||!r.items.length)break}
   topics.value=[...new Set(names)].sort();topicState.value='unknown';checkTopic()}
  catch{if(token===catalogEpoch){topicState.value='unreadable';catalogError.value='Topic 列表暂不可读取，无法确认 Topic 是否存在'}}
  return
 }
 try{const names:string[]=[];for(let page=1;page<=100;page++){const result=await kafkaList<Topic>(cluster.value,'topics',{page,pageSize:100});if(token!==catalogEpoch)return;names.push(...result.items.map(n=>n.name));if(names.length>=result.total||!result.items.length)break}
  topics.value=[...new Set(names)].sort();topicState.value='unknown';checkTopic()}
 catch{if(token===catalogEpoch){topicState.value='unreadable';catalogError.value='Topic 列表暂不可读取，无法确认 Topic 是否存在'}}
}
function checkTopic(){if(topicState.value==='unreadable'||topicState.value==='loading')return;topicState.value=!topic.value.trim()?'unknown':topics.value.includes(topic.value.trim())?'exists':'missing'}
async function readDiscovery():Promise<Discovery>{
 const scopes=await registryRequest<{items:{id:string;synthetic:boolean;k8s_cluster?:string;removed?:boolean}[]}>('/discovery/scopes')
 const result:Discovery={stale:false,observedAt:'',links:[]}
 for(const s of scopes.items.filter(x=>!x.synthetic)){
  try{const r=await registryRequest<{snapshot:{stale:boolean;observed_at:string;links:RuleLink[]}}>(`/discovery/scopes/${encodeURIComponent(s.id)}/snapshot?page_size=100`);result.stale||=r.snapshot.stale;result.observedAt=r.snapshot.observed_at;result.links.push(...(r.snapshot.links??[]).filter(l=>!(l as {removed?:boolean}).removed).map(l=>({...l,scope_id:s.id,k8s_cluster:l.k8s_cluster??s.k8s_cluster})))}
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
  // Always read upstream (root 源头/断点 needs it); read downstream unless upstream-only. Never use the backend's mixed both-BFS.
  const signal=request.signal,trace=Promise.all([readLineage(c.cluster,c.topic,c.depth,'upstream',signal),c.direction==='upstream'?undefined:readLineage(c.cluster,c.topic,c.depth,'downstream',signal)]).then(([up,down])=>resolveSources({direction:c.direction,up,down}))
  const [resolved,disc]=await Promise.all([trace,readDiscovery().catch(():Discovery=>({stale:true,observedAt:'',links:[]}))])
  if(token!==epoch)return
  discovery.value=disc;applied.value=c
  if(resolved.rootMissing){data.value=undefined;notFound.value=true;states.value={};summary.value='';return}
  const result=resolved.graph;data.value=result;states.value=resolved.states;summary.value=resolved.summary
  if(!disc.stale&&result.edges.length)writeCache(c,result)
  cached.value=disc.stale?readCache(c):undefined
 }catch(e){
  if(token!==epoch)return
  applied.value=c;data.value=undefined
  if((e as {status?:number}).status===404)notFound.value=true
  else error.value=(e as Error).name==='AbortError'?'查询超时或已取消，请重试':`本次查询失败：${(e as Error).message}`
 }finally{if(token===epoch){busy.value=false;globalThis.clearTimeout(timer)}}
}
const refreshResult=shallowRef<RefreshMessage>()
async function refreshDiscovery(){
 if(refreshing.value)return
 refreshing.value=true;refreshResult.value=undefined
 try{
  const scopes=(await registryRequest<{items:{id:string;synthetic:boolean;removed?:boolean}[]}>('/discovery/scopes')).items.filter(x=>!x.synthetic&&!x.removed)
  const results:string[]=[]
  for(const s of scopes){
   const path=`/discovery/scopes/${encodeURIComponent(s.id)}`
   try{await registryRequest(`${path}/runs`,'POST',{})}catch(e){results.push((e as {code?:string}).code??'REQUEST_FAILED');continue}
   let outcome='PENDING'
   for(let i=0;i<30;i++){const job=await registryRequest<{status:string;error_code?:string}>(`${path}/runs/latest`);if(job.status==='done'){outcome=job.error_code??'';break};await new Promise(r=>globalThis.setTimeout(r,500))}
   results.push(outcome)
  }
  const snaps:RefreshSnapshot[]=[]
  for(const s of scopes){try{snaps.push((await registryRequest<{snapshot:RefreshSnapshot}>(`/discovery/scopes/${encodeURIComponent(s.id)}/snapshot?page_size=100`)).snapshot)}catch{/* reported by the result below */}}
  refreshResult.value=refreshMessage(results,snaps)
  discovery.value=await readDiscovery()
  await load()
 }catch{refreshResult.value={kind:'failed',text:'采集失败：刷新请求未成功，请稍后重试'}}finally{refreshing.value=false}
}
function act(a:Reason['actions'][number]){if(a.action==='refresh')void refreshDiscovery();else if(a.action==='deeper'&&depth.value<4){depth.value++;void query()}else void load()}
function syncQuery(){cluster.value=String(route.query.cluster_id??'');topic.value=String(route.query.topic??'');const d=Number(route.query.depth??2);depth.value=[1,2,3,4].includes(d)?d:2;direction.value=['upstream','downstream','both'].includes(String(route.query.direction))?route.query.direction as Direction:'both';void load()}
function asRoot(n:LineageNode){if(n.cluster_id)void router.push({path:'/topic-lineage',query:{cluster_id:n.cluster_id,topic:n.topic??n.label,depth:String(depth.value),direction:direction.value}})}
watch(()=>route.fullPath,syncQuery,{immediate:true})
watch(cluster,(next,prev)=>{if(catalog.value.length&&prev!==undefined&&prev!==next&&prev!=='')topic.value='';void loadTopics()},{immediate:true})
watch(topic,checkTopic)
onMounted(async()=>{void readCatalog().then(rows=>{catalog.value=rows;if(rows.length&&cluster.value)void loadTopics()}).catch(()=>{});void readDiscovery().then(d=>{if(!discovery.value)discovery.value=d}).catch(()=>{});try{const result=await registryRequest<ClusterPage>('/kafka/clusters?page=1&page_size=100');clusters.value=result.items}catch{catalogError.value='集群列表加载失败，请刷新重试'}})
onBeforeUnmount(()=>{++epoch;++catalogEpoch;request?.abort();globalThis.clearTimeout(timer)})
</script>
<template>
  <section class="lineage-page registry">
    <form class="lineage-bar" data-lineage-bar @submit.prevent="query">
      <h1>Topic 链路</h1>

      <label v-if="catalog.length">集群别名<select v-model="cluster" aria-label="集群别名" required><option value="" disabled>请选择别名</option><option v-for="r in catalog" :key="r.id" :value="r.lineage_id||r.registration_id">{{ aliasOption(r) }}</option></select></label>
      <label v-else>集群<select v-model="cluster" aria-label="集群" required><option value="" disabled>选择集群</option><option v-for="item in clusters" :key="item.id" :value="item.id">{{ item.display_name }}</option></select></label>
      <label class="topic-input">Topic<input v-model="topic" aria-label="Topic" list="lineage-topics" :disabled="catalog.length>0&&!cluster" :placeholder="catalog.length&&!cluster?'先选择别名':configOnly?'配置推导 / 手工输入 Topic':'从所选集群的 Topic 中搜索'" autocomplete="off" required><datalist id="lineage-topics"><option v-for="c in shownCandidates" :key="c.name" :value="c.name" :label="c.label" /></datalist></label>




      <button class="primary" type="submit" :disabled="!canQuery">{{ busy?'查询中…':changed?'应用条件并查询':'查看链路' }}</button>
      <details class="lineage-advanced" data-advanced>
        <summary>高级</summary><div class="adv-menu">
          <label>查询方向<select v-model="direction" aria-label="方向"><option value="both">上下游追踪</option><option value="upstream">向上游追踪</option><option value="downstream">向下游追踪</option></select></label>
          <label>最大深度（跨集群跳数）<select v-model.number="depth" aria-label="深度"><option v-for="value in 4" :key="value" :value="value">{{ value }} 跳</option></select></label>
          <label class="toggle"><input v-model="showAllTopics" data-show-all-topics type="checkbox"> 显示全部 Topic</label>
          <label class="toggle"><input v-model="showConsumers" data-show-consumers type="checkbox"> 显示消费组与应用登记</label>
        </div>
      </details>
    </form>

    <p v-if="noCandidate" data-no-candidate class="topic-check" role="status">未找到参与同步的 Topic。可能是 MM2 内部 Topic 或被规则排除，打开'显示全部 Topic'查看 <button type="button" @click="showAllTopics=true">打开</button></p>
    <p v-if="topicState==='missing'" data-topic-check class="topic-check" role="status">该 Topic 在所选集群不存在，请从下拉列表中选择该集群实际存在的 Topic。</p>
    <p v-else-if="catalogError" class="topic-check" role="status">{{ catalogError }}</p>
    <p v-if="changed" data-stale-conditions class="conditions-changed" role="status">条件已修改，结果尚未更新。下图仍对应：{{ nodeName(display.graph?.root_id??'') }} · {{ applied?.direction==='both'?'上下游':applied?.direction==='upstream'?'上游':'下游' }} · {{ applied?.depth }} 跳</p>
    <p v-if="error" role="alert">{{ error }} <button @click="load">重试查询</button></p><p v-if="busy" role="status">正在读取 Topic 关系…</p>
    <div v-if="notFound" class="panel lineage-empty" data-not-found role="status"><strong>该集群未读取到此 Topic</strong><p>可能原因：该集群确实没有此 Topic；名称不完全一致（大小写、前缀或后缀）；集群连接不可用。</p><button type="button" data-reinput @click="topic='';notFound=false">重新输入 Topic</button> <router-link v-if="cluster" :to="`/kafka/clusters/${encodeURIComponent(cluster)}/details`">查看集群 Topic 列表</router-link></div>
    <template v-else-if="applied&&(data||display.banner)">
      <div v-if="display.banner" data-stale-banner class="stale-banner" role="status"><strong>{{ display.banner }}</strong><span>{{ display.graph?'当前保留最近一次配置图，不能据此判断当前连通或消息到达。':'' }}</span><button :disabled="refreshing" @click="refreshDiscovery">{{ refreshing?'正在刷新…':'刷新发现快照' }}</button></div>
      <p v-if="refreshResult" data-refresh-result :class="['refresh-result',refreshResult.kind]" role="status">{{ refreshResult.text }}</p>
      <p v-if="display.notice" class="panel lineage-empty" data-no-history>{{ display.notice }}</p>


      <ul v-if="reasons.length" class="lineage-reasons"><li v-for="r in reasons" :key="r.kind" :data-reason="r.kind"><strong>{{ r.title }}</strong><p v-for="f in r.facts" :key="f">{{ f }}</p><div v-for="g in r.groups??[]" :key="g.task" data-unparsed-task class="unparsed-task"><strong>{{ g.task }}</strong><span>{{ g.where }}</span><span>{{ g.impact }}</span><router-link :to="g.to">在 MM2 拓扑查看</router-link></div><p v-if="r.more">另有 {{ r.more }} 条同类提示</p><div class="reason-actions"><template v-for="a in r.actions" :key="a.label"><router-link v-if="a.to" :to="a.to">{{ a.label }}</router-link><button v-else type="button" :disabled="a.action==='refresh'&&refreshing" @click="act(a)">{{ a.action==='refresh'&&refreshing?'正在刷新…':a.label }}</button></template></div></li></ul>

      <div v-if="view" class="lineage-workspace" :class="{'has-selection': selectedNode || selectedEdge}">
        <TopicLineageCanvas :view="view" :root-id="display.graph!.root_id" :states="display.graph===data?canvasStates:{}" @select-node="selectedNode=$event;selectedEdge=undefined" @select-edge="selectedEdge=$event;selectedNode=undefined" />
        <aside v-if="selectedNode || selectedEdge" class="lineage-detail panel" aria-label="链路详情">
          <button class="detail-close" aria-label="关闭链路详情" @click="selectedNode=undefined;selectedEdge=undefined">关闭</button>
          <template v-if="selectedNode"><h2>{{ kindLabel(selectedNode.kind) }}</h2><h3>{{ selectedNode.cluster_name }} / {{ selectedNode.topic??selectedNode.label }}</h3><dl><template v-if="selectedNode.kind==='topic'"><dt>Topic 存在性</dt><dd>{{ existence(selectedNode) }}</dd></template><template v-if="selectedNode.owner"><dt>负责人</dt><dd>{{ selectedNode.owner }}</dd></template><dt>Topic 元数据时间</dt><dd>{{ observedTime(selectedNode.observed_at) }}</dd></dl><router-link v-if="selectedPath" :to="selectedPath">查看资源详情</router-link><button v-if="selectedNode.kind==='topic' && selectedNode.cluster_id" @click="asRoot(selectedNode)">以此 Topic 为根</button></template>
          <template v-else-if="selectedEdge"><h2>关系详情</h2><h3>{{ nodeName(selectedEdge.source) }} → {{ nodeName(selectedEdge.target) }}</h3><dl data-edge-facts><dt>证据类型</dt><dd data-edge-evidence>{{ evidenceLabel(selectedEdge.evidence) }}</dd><dt>运行状态</dt><dd>状态未知</dd><dt>新鲜度</dt><dd data-edge-freshness>{{ edgeFreshness }}</dd><template v-if="selectedEdge.linkId"><dt>MM2 任务</dt><dd><router-link data-edge-mm2 :to="mm2Path(selectedEdge.linkId)">{{ selectedEdge.label }}</router-link> · <router-link to="/topology?view=mm2">在 MM2 拓扑查看</router-link></dd></template><template v-if="selectedEdge.policy"><dt>命名策略</dt><dd data-policy-label>{{ policyLabel(selectedEdge.policy) }}<br><code class="long-name">{{ selectedEdge.policy }}</code></dd></template><template v-if="selectedEdge.rule"><dt>匹配规则</dt><dd>{{ topicMatchLabel(selectedEdge.rule) }}<br><code class="long-name">{{ selectedEdge.rule }}</code></dd></template><template v-if="selectedEdge.account"><dt>Kafka 账号</dt><dd>{{ selectedEdge.account }}</dd></template><dt>配置采集时间</dt><dd>{{ observedTime(selectedEdge.observed_at) }}</dd></dl></template>
        </aside>
      </div>
      <details v-if="view" class="lineage-accessible"><summary>节点与关系列表</summary><div><button v-for="node in view.nodes" :key="node.id" :data-lineage-node="node.id" @click="selectedNode=node;selectedEdge=undefined">{{ kindLabel(node.kind) }}：{{ node.cluster_name }} / {{ node.topic??node.label }}</button></div><div><button v-for="edge in view.edges" :key="edge.id" :data-lineage-edge="edge.id" @click="selectedEdge=edge;selectedNode=undefined">{{ nodeName(edge.source) }} → {{ nodeName(edge.target) }}{{ edge.label?'：'+edge.label:'' }}</button></div></details>
      <details class="lineage-accessible" data-query-info><summary>查询信息</summary><dl data-lineage-times class="lineage-times"><dt>查询时间</dt><dd>{{ observedTime(data?.observed_at) }}</dd><dt>配置采集时间</dt><dd>{{ observedTime(discovery?.observedAt) }}{{ discovery?.stale?'（已过期）':'' }}</dd><dt>Topic 元数据时间</dt><dd>{{ observedTime(rootNode?.observed_at) }}</dd></dl><p v-if="summary && display.graph===data" data-source-summary class="source-summary">{{ summary }}</p></details>
    </template>
    <div v-else-if="!busy && !error" class="panel lineage-empty">选择集群和该集群中的 Topic，查看其跨集群同步关系。</div>
  </section>
</template>
<style scoped>.lineage-bar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin:0 0 10px}.lineage-bar h1{font-size:20px;margin:0 6px 0 0;white-space:nowrap}.lineage-bar>label{display:flex;align-items:center;gap:6px;font-size:13px;color:#526174}.lineage-bar .topic-input{flex:1;min-width:200px}.lineage-bar .topic-input input{width:100%}.lineage-advanced{position:relative}.lineage-advanced>summary{list-style:none;cursor:pointer;padding:7px 12px;border:1px solid #d5deea;border-radius:6px;background:#fff;font-size:13px}.adv-menu{position:absolute;right:0;top:calc(100% + 6px);z-index:25;background:#fff;border:1px solid #dce4ef;border-radius:6px;padding:12px;display:grid;gap:10px;min-width:260px;box-shadow:0 8px 24px #19365522}.adv-menu label{display:flex;gap:8px;align-items:center;font-size:13px}@media(max-width:760px){.lineage-bar h1{width:100%}.lineage-bar .topic-input{min-width:0}.adv-menu{position:fixed;left:8px;right:8px;top:auto}}.query-summary{padding:16px 18px;margin:12px 0}.query-summary h2{font-size:15px;margin:0 0 8px}.query-summary p,.query-summary small{font-size:12px;color:#475569;line-height:1.7}.long-name{overflow-wrap:anywhere}.page-header h1{margin-bottom:8px}.page-header p,.lineage-detail p{color:#64748b;font-size:13px}.lineage-query{display:flex;align-items:end;gap:14px;padding:18px;margin-bottom:12px;flex-wrap:wrap}.lineage-query label{display:flex;flex-direction:column;gap:8px;font-size:12px;color:#526174}.lineage-query label.toggle{flex-direction:row;align-items:center;height:38px}.lineage-query select,.lineage-query input:not([type=checkbox]){height:38px;border:1px solid #ccd7e5;border-radius:5px;padding:0 10px;background:#fff;color:#25304a;min-width:100px}.lineage-query .topic-input{flex:1;min-width:200px}.lineage-query button{height:38px}.source-summary{font-size:14px;font-weight:600;color:#1f5f3a;margin:0 0 12px}.candidate-note{font-size:12px;color:#64748b;margin:0 0 8px}.unparsed-task{display:flex;gap:10px;flex-wrap:wrap;margin:6px 0;color:#53647c}.topic-check{color:#9a5b12;font-size:13px}.conditions-changed{background:#eef4ff;color:#2a4f9a;padding:10px 12px;border-radius:6px;font-size:13px}.stale-banner{display:flex;gap:14px;align-items:center;flex-wrap:wrap;background:#fff1e0;border:1px solid #e3a65a;color:#7a4a10;padding:12px 14px;border-radius:6px;margin:12px 0;font-size:13px}.lineage-times{display:grid;grid-template-columns:repeat(3,auto 1fr);gap:6px 10px;font-size:12px;color:#64748b;margin:14px 0}.lineage-times dd{margin:0;color:#25304a}.lineage-reasons{list-style:none;padding:0;margin:0 0 14px;display:grid;gap:10px}.lineage-reasons li{background:#fff;border:1px solid #e3c99a;border-left:4px solid #d08a2c;border-radius:6px;padding:12px 14px;font-size:13px}.lineage-reasons p{margin:6px 0;color:#53647c}.reason-actions{display:flex;gap:12px;flex-wrap:wrap}.lineage-workspace{display:grid;grid-template-columns:minmax(0,1fr);gap:16px}.lineage-workspace.has-selection{grid-template-columns:minmax(0,1fr) 280px}.detail-close{float:right}.lineage-detail{padding:20px;overflow-wrap:anywhere}.lineage-detail h2{font-size:14px;margin:0 0 18px}.lineage-detail h3{font-size:15px}.lineage-detail dt{font-size:12px;color:#64748b;margin-top:14px}.lineage-detail dd{font-size:13px;margin:6px 0}.lineage-detail button{margin-top:14px}.lineage-empty{padding:22px;color:#64748b;font-size:13px}.lineage-accessible{margin-top:16px;font-size:12px;color:#64748b}.lineage-accessible summary{cursor:pointer}.lineage-accessible button{margin:8px 8px 0 0;font-size:12px}@media(max-width:1000px){.lineage-workspace,.lineage-workspace.has-selection{grid-template-columns:1fr}.lineage-query label{flex:1}.lineage-times{grid-template-columns:auto 1fr}}
</style>
