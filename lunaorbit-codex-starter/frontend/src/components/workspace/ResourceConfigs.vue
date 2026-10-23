<script setup lang="ts">
import { computed, onUnmounted, shallowRef, watch } from 'vue'
import { ElDrawer } from 'element-plus'
import { resourceConfigs, previewResource, submitResource, type ConfigSnapshot, type ConfigEntry, type Resource, type ResourcePlan } from '../../api/resource-management'
import { workspaceError, type WorkspaceMeta } from '../../api/workspace'
import { configLabel, configSource, configValue, configIsCommon, configOrder, configDescription } from './config-labels'
import ResourceOperations from './ResourceOperations.vue'
import { useRuntime } from '../../composables/useRuntime'
const { runtime } = useRuntime()
const informationMode = computed(() => runtime.value?.information_mode === true)
const props=withDefaults(defineProps<{clusterId:string;resource:Resource;meta:WorkspaceMeta;actorId:string;showOperations?:boolean;readOnly?:boolean}>(),{showOperations:true})
const emit=defineEmits<{changed:[];submitted:[]}>()
const snapshot=shallowRef<ConfigSnapshot>(),query=shallowRef(''),source=shallowRef(''),editableOnly=shallowRef(false),editing=shallowRef<ConfigEntry>(),value=shallowRef(''),mode=shallowRef<'SET'|'DELETE'>('SET'),plan=shallowRef<ResourcePlan>(),confirmed=shallowRef(false),busy=shallowRef(false),error=shallowRef(''),refreshVersion=shallowRef(0),submitted=shallowRef(false)
let epoch=0,disposed=false,draftEpoch=0
const entries=computed(()=>(snapshot.value?.entries??[]).filter(e=>(!commonOnly.value||!hasCommon.value||configIsCommon(e.name))&&(e.name+' '+configLabel(e.name)).toLowerCase().includes(query.value.toLowerCase())&&(!source.value||e.source===source.value)&&(!editableOnly.value||!e.read_only&&!e.sensitive)).sort((a,b)=>configOrder(a.name)-configOrder(b.name)||a.name.localeCompare(b.name)))
const commonOnly=shallowRef(true)
const hasCommon=computed(()=>snapshot.value?.entries.some(e=>configIsCommon(e.name)))
const sources=computed(()=>[...new Set(snapshot.value?.entries.map(e=>e.source)??[])])
const clusterDefaults=computed(()=>props.resource.type==='BROKER'&&!props.resource.name)
const canEdit=computed(()=>props.meta.permissions.topic_manage&&props.meta.write_enabled&&!props.readOnly)
const previewRows=computed(()=>plan.value?.input.changes?.map(change=>({change,before:plan.value?.before?.entries.find(e=>e.name===change.name),after:plan.value?.after?.entries.find(e=>e.name===change.name)}))??[])
function displayed(entry:ConfigEntry|undefined,catalogue:ConfigEntry[]|undefined=snapshot.value?.entries){
 if(entry?.sensitive)return '已隐藏'
 if(entry?.source==='NO_CLUSTER_DEFAULT_OVERRIDE'&&entry.value==null)return '未设置'
 if(props.resource.type==='BROKER'&&props.resource.name&&entry?.name==='log.retention.ms'&&entry.value==null){
  const fallback=retentionFallback(catalogue??[])
  if(fallback)return fallback
 }
 return returnedValue(entry?.name??'',entry?.value)
}
function retentionFallback(catalogue:ConfigEntry[]):string|undefined{
 for(const [name,unit] of [['log.retention.minutes','分钟'],['log.retention.hours','小时']] as const){
  const entry=catalogue.find(e=>e.name===name)
  if(entry?.sensitive)return undefined
  if(entry?.value==null)continue
  if(!/^(?:-1|\d+)$/.test(entry.value))return undefined
  const value=entry.value==='-1'?'不限制（-1）':`${entry.value} ${unit}`
  return `未设置，使用 ${name}（${value}）`
 }
}
function returnedValue(name:string,value:string|null|undefined){return value==null?'Kafka 未返回':value===''?'空值':configValue(name,value)}
function resetPlan(){++draftEpoch;plan.value=undefined;confirmed.value=false;submitted.value=false}
function close(){if(busy.value)return;editing.value=undefined;resetPlan()}
async function load(){const token=++epoch;snapshot.value=undefined;editing.value=undefined;resetPlan();busy.value=true;error.value='';try{const result=await resourceConfigs(props.clusterId,props.resource);if(token===epoch&&!disposed)snapshot.value=result}catch(e){if(token===epoch&&!disposed)error.value=workspaceError(e)}finally{if(token===epoch&&!disposed)busy.value=false}}
function edit(entry:ConfigEntry){if(!canEdit.value||entry.read_only||entry.sensitive)return;editing.value=entry;value.value=entry.value??'';mode.value='SET';error.value='';resetPlan()}
async function preview(){if(!editing.value||busy.value)return;const token=epoch;busy.value=true;error.value='';resetPlan();const draft=draftEpoch;try{const result=await previewResource(props.clusterId,{kind:'configs',resource:{...props.resource},changes:[mode.value==='DELETE'?{name:editing.value.name,operation:'DELETE'}:{name:editing.value.name,operation:'SET',value:value.value}]});if(token===epoch&&!disposed&&draft===draftEpoch)plan.value=result}catch(e){if(token===epoch&&!disposed)error.value=workspaceError(e)}finally{if(token===epoch&&!disposed)busy.value=false}}
async function submit(){if(!plan.value||!confirmed.value||busy.value||submitted.value)return;const token=epoch;busy.value=true;submitted.value=true;error.value='';try{await submitResource(props.clusterId,plan.value,globalThis.crypto.randomUUID());if(token===epoch&&!disposed){plan.value=undefined;editing.value=undefined;refreshVersion.value++;emit('changed');emit('submitted')}}catch(e){if(token===epoch&&!disposed)error.value=workspaceError(e)}finally{if(token===epoch&&!disposed)busy.value=false}}
watch([value,mode],resetPlan)
watch(()=>[props.clusterId,props.resource.type,props.resource.name],()=>{query.value='';source.value='';commonOnly.value=true;void load()},{immediate:true,flush:'sync'})
onUnmounted(()=>{disposed=true;++epoch})
</script>
<template>
  <section class="resource-configs panel">
    <header class="config-heading"><h2>{{ clusterDefaults?'集群默认配置':resource.type==='BROKER'?'Broker 配置':'Topic 配置' }}</h2><button :disabled="busy" @click="load">刷新配置</button></header>
    <p v-if="error&&!editing" role="alert" class="config-notice">{{ error }}</p><p v-if="busy&&!editing" role="status" class="config-notice">加载中…</p>
    <p v-if="resource.type==='BROKER'&&!resource.name" class="config-notice">这里仅显示统一设置的集群覆盖值。未设置时，各 Broker 使用自己的配置；查看具体 Broker 的实际生效配置。</p>
    <div v-if="hasCommon" class="config-view-switch" role="group" aria-label="配置范围"><button :aria-pressed="commonOnly" @click="commonOnly=true">常用配置</button><button :aria-pressed="!commonOnly" @click="commonOnly=false">全部配置（{{ snapshot?.entries.length }}）</button></div>
    <div class="config-filters"><label><span class="sr-only">筛选配置</span><input v-model="query" aria-label="筛选配置" placeholder="搜索配置名称或参数"></label><label><span class="sr-only">来源</span><select v-model="source" aria-label="来源"><option value="">全部来源</option><option v-for="s in sources" :key="s" :value="s">{{ configSource(s) }}</option></select></label><label v-if="!informationMode" class="editable-filter"><input v-model="editableOnly" type="checkbox">仅可修改</label></div>
    <div class="table-scroll"><table class="data-table"><thead><tr><th>配置项</th><th>{{ clusterDefaults?'集群覆盖值':'当前值' }}</th><th>来源</th><th v-if="!informationMode">操作</th></tr></thead><tbody><tr v-for="entry in entries" :key="entry.name"><td><details class="config-information"><summary><span>{{ configLabel(entry.name) }}</span><small v-if="configLabel(entry.name)!==entry.name">{{ entry.name }}</small></summary><div class="config-documentation"><p>{{ configDescription(entry.name) }}</p><dl><dt>默认值</dt><dd>{{ entry.sensitive?'已隐藏':returnedValue(entry.name,entry.default_value) }}</dd><dt>来源</dt><dd>{{ configSource(entry.source) }}</dd><dt>类型</dt><dd>{{ entry.type||'未知' }}</dd></dl><ul v-if="!entry.sensitive"><li v-for="(s,i) in entry.synonyms" :key="i">{{ configSource(s.source) }}：{{ configValue(s.name,s.value) }}</li></ul></div></details></td><td><span class="config-current-value">{{ displayed(entry) }}</span><span v-if="entry.read_only" class="readonly-label">只读</span></td><td><span class="config-source">{{ configSource(entry.source) }}</span></td><td v-if="!informationMode"><button class="config-edit" :data-edit-config="entry.name" :disabled="!canEdit||entry.read_only||entry.sensitive||busy" @click="edit(entry)">修改</button></td></tr></tbody></table></div>
    <p v-if="snapshot&&!entries.length" class="config-notice">没有匹配的配置</p>
    <ElDrawer v-if="!informationMode" :model-value="!!editing" :title="editing?configLabel(editing.name):'修改配置'" size="min(520px, 100vw)" :close-on-click-modal="!busy" :close-on-press-escape="!busy" :show-close="!busy" destroy-on-close @close="close">
      <template v-if="editing">
        <p class="parameter-name">{{ editing.name }}</p><p v-if="error" role="alert">{{ error }}</p><dl class="current-config"><dt>{{ clusterDefaults?'集群覆盖值':'当前值' }}</dt><dd>{{ displayed(editing) }}</dd><dt>来源</dt><dd>{{ configSource(editing.source) }}</dd></dl>
        <form class="config-editor" @submit.prevent="preview"><label>操作<select v-model="mode" :disabled="busy"><option value="SET">设置覆盖值</option><option value="DELETE">恢复继承</option></select></label><label v-if="mode==='SET'">新值 <span class="value-unit">{{ editing.name.endsWith('.ms')?'（毫秒）':editing.name.endsWith('.bytes')?'（字节）':'' }}</span><input v-model="value" aria-label="新配置值" :disabled="busy"></label><p v-else class="inherit-note">删除当前覆盖，由 Kafka 继承配置。请核对预览中的值。</p><button class="button-primary" :disabled="busy||submitted">预览差异并校验</button></form>
        <section v-if="plan" class="config-preview"><h3>变更预览</h3><div v-for="row in previewRows" :key="row.change.name" class="config-diff"><div><span>修改前</span><strong>{{ displayed(row.before,plan.before?.entries??[]) }}</strong><small>{{ configSource(row.before?.source??'UNKNOWN_CONFIG') }}</small></div><span aria-hidden="true">→</span><div><span>修改后</span><strong>{{ displayed(row.after,plan.after?.entries??[]) }}</strong><small>{{ configSource(row.after?.source??'UNKNOWN_CONFIG') }}</small></div></div><label class="confirm-config"><input v-model="confirmed" type="checkbox">我已核对目标资源和配置差异</label><button class="button-primary" :disabled="busy||!confirmed||submitted" @click="submit">提交变更申请</button></section>
      </template>
    </ElDrawer>
    <ResourceOperations v-if="showOperations && !informationMode" :cluster-id="clusterId" :resource="resource" :meta="meta" :actor-id="actorId" :refresh-version="refreshVersion" @changed="emit('changed')" />
  </section>
</template>
<style scoped>
.resource-configs{min-width:0;overflow:hidden}.config-heading{display:flex;align-items:center;justify-content:space-between;padding:18px 20px}.config-view-switch{display:flex;gap:0;margin:0 20px 18px}.config-view-switch button{border-radius:0;font-size:12px}.config-view-switch button:first-child{border-radius:4px 0 0 4px}.config-view-switch button:last-child{border-radius:0 4px 4px 0;border-left:0}.config-view-switch button[aria-pressed=true]{background:#eff6ff;border-color:#93b4ee;color:#2563eb}.config-heading h2{font-size:16px;margin:0}.config-filters{display:flex;align-items:center;gap:12px;flex-wrap:wrap;padding:0 20px 18px}.config-filters>label:first-child{flex:1;max-width:360px}.config-filters input:not([type=checkbox]){width:100%}.editable-filter{display:flex;align-items:center;gap:6px;font-size:13px;white-space:nowrap;color:#64748b}.config-information summary{cursor:pointer;max-width:380px;overflow-wrap:anywhere}.config-information small{display:block;font-size:11px;color:#64748b;margin:5px 0 0 14px}.config-documentation{max-width:440px;font-size:12px;color:#64748b;line-height:1.6;padding:8px 14px}.config-documentation dl{display:grid;grid-template-columns:60px 1fr;gap:6px}.config-documentation dd{margin:0}.config-current-value{overflow-wrap:anywhere}.config-source{font-size:12px;color:#64748b}.readonly-label{display:inline-block;margin-left:8px;color:#64748b;background:#f1f5f9;padding:2px 6px;border-radius:3px;font-size:11px}.config-edit{border:0;background:none;color:#2563eb;padding:5px 0}.config-edit:disabled{color:#94a3b8}.config-notice{margin:0;padding:0 20px 16px;color:#64748b;font-size:13px}.table-scroll{overflow:auto}.data-table{width:100%}.parameter-name{font-size:12px;color:#64748b;overflow-wrap:anywhere;margin:0 0 22px}.current-config{display:grid;grid-template-columns:64px 1fr;font-size:13px;gap:12px;padding-bottom:22px;border-bottom:1px solid #e5ebf3}.current-config dt{color:#64748b}.current-config dd{margin:0}.config-editor{display:grid;gap:20px;margin-top:22px}.config-editor label{display:grid;gap:8px;font-size:13px}.config-editor button,.config-preview button{justify-self:start}.value-unit{font-size:12px;color:#64748b}.inherit-note{font-size:13px;color:#64748b;line-height:1.7;margin:0}.config-preview{margin-top:24px;padding-top:20px;border-top:1px solid #e5ebf3;display:grid;gap:18px}.config-preview h3{font-size:15px;margin:0}.config-diff{display:grid;grid-template-columns:1fr 20px 1fr;gap:10px;align-items:center;background:#f8fafc;padding:16px;border-radius:5px}.config-diff>div{display:grid;gap:8px;overflow-wrap:anywhere}.config-diff span,.config-diff small{font-size:12px;color:#64748b}.config-diff strong{font-size:15px;font-weight:500}.confirm-config{display:flex;align-items:center;gap:8px;font-size:13px}.sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%)}@media(max-width:640px){.data-table{min-width:0;table-layout:fixed}.data-table th:first-child{width:50%}.data-table th:last-child{width:20%}.config-filters{gap:10px}.config-filters>label:first-child{max-width:none;flex-basis:100%}.data-table th:nth-child(3),.data-table td:nth-child(3){display:none}.data-table th,.data-table td{padding:12px 8px;min-width:0}.data-table td:first-child{width:50%}.config-information summary{min-width:0;max-width:160px}.config-information small{overflow-wrap:anywhere}.config-current-value{font-size:12px}}
</style>
