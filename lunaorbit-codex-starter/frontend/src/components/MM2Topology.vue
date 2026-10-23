<script setup lang="ts">
import { computed,ref,watch } from 'vue'
import type { Link,Snapshot } from '../api/topology'
import { linkStatus,mappingName,type RegistrationIdentity } from '../topology/model'
import { deploymentGroups } from '../topology/deployments'

const props=defineProps<{snapshot:Snapshot;registrations:RegistrationIdentity[];selectedId?:string;focusActive?:boolean;focusedLinkIds?:string[];nodeHints?:Record<string,string>}>()
const emit=defineEmits<{select:[id:string];selectNode:[id:string];clear:[]}>()
const collapsed=ref(new Set<string>()),expanded=ref(new Set<string>())
const groups=computed(()=>deploymentGroups(props.snapshot.links,props.snapshot.stale))
function toggle(set:Set<string>,id:string){if(set.has(id))set.delete(id);else set.add(id)}
watch([groups,()=>props.selectedId,()=>props.focusedLinkIds],()=>{
 for(const group of groups.value){
  const matches=(l:Link)=>l.id===props.selectedId||!!props.focusActive&&!!props.focusedLinkIds?.includes(l.id)
  if(group.links.some(matches))collapsed.value.delete(group.id)
  if(group.links.slice(3).some(matches))expanded.value.add(group.id)
 }
},{immediate:true})
function isExpanded(id:string){return expanded.value.has(id)}
function paths(id:string,links:Link[]){return isExpanded(id)?links:links.slice(0,3)}
function node(link:Link,end:'source'|'target'){
 const edge=props.snapshot.edges.find(e=>e.link_id===link.id&&e.relation===(end==='source'?'declared_input':'declared_output'))
 return edge?(end==='source'?edge.source:edge.target):undefined
}
function hint(link:Link,end:'source'|'target'){const id=node(link,end);return id?props.nodeHints?.[id]:''}
function selectNode(link:Link,end:'source'|'target'){const id=node(link,end);if(id)emit('selectNode',id)}
function status(link:Link){return props.snapshot.stale?'数据已过期 · 状态未知':linkStatus(link)}
function state(link:Link){return props.snapshot.stale||link.stale?'stale':link.state==='disabled'?'disabled':link.state==='unknown'?'unknown':link.verified?'verified':'unverified'}
function dimmed(link:Link){return props.focusActive?!props.focusedLinkIds?.includes(link.id):!!props.selectedId&&props.selectedId!==link.id}
</script>

<template>
  <section class="mm2-topology" aria-label="MM2 拓扑">
    <p v-if="!groups.length" class="empty-mm2">当前筛选下没有 MM2 同步任务</p>
    <article v-for="group in groups" :key="group.id" :data-mm2-instance="group.id" class="mm2-instance">
      <header class="instance-header"><button data-toggle-instance class="instance-toggle" :aria-expanded="!collapsed.has(group.id)" @click="toggle(collapsed,group.id)"><span class="collapse-icon">{{ collapsed.has(group.id)?'▸':'▾' }}</span><span class="mm2-tag">MM2</span><strong>{{ group.name }}</strong><small>{{ group.namespace }}</small></button><div class="instance-meta"><span>{{ group.links.length }} 个任务</span><span>{{ group.runtime }}</span></div></header>
      <div v-if="!collapsed.has(group.id)" class="paths-scroll">
        <div class="path-headings" aria-hidden="true"><span>源 Kafka</span><span></span><span>MM2 同步任务</span><span></span><span>目标 Kafka</span></div>
        <ul class="path-list">
          <li v-for="link in paths(group.id,group.links)" :key="link.id" :data-mm2-task="link.id" class="mm2-path" :class="['path-'+state(link),{'path-selected':selectedId===link.id,'path-dimmed':dimmed(link)}]">
            <button data-source class="endpoint" :title="mappingName(link.source,registrations)" :disabled="!node(link,'source')" @click="selectNode(link,'source')"><strong>{{ mappingName(link.source,registrations) }}</strong><small v-if="hint(link,'source')">{{ hint(link,'source') }}</small></button>
            <svg class="path-arrow" viewBox="0 0 60 20" aria-hidden="true"><path d="M0 10 H56 M50 5 L56 10 L50 15" /></svg>
            <button data-mm2-select class="sync-task" :aria-pressed="selectedId===link.id" @click="emit('select',link.id)"><strong>同步任务</strong><code :title="link.topics_raw">{{ link.topics_raw||'尚未获取 Topic 规则' }}</code><small>{{ status(link) }}</small></button>
            <svg class="path-arrow" viewBox="0 0 60 20" aria-hidden="true"><path d="M0 10 H56 M50 5 L56 10 L50 15" /></svg>
            <button data-target class="endpoint" :title="mappingName(link.target,registrations)" :disabled="!node(link,'target')" @click="selectNode(link,'target')"><strong>{{ mappingName(link.target,registrations) }}</strong><small v-if="hint(link,'target')">{{ hint(link,'target') }}</small></button>
          </li>
        </ul>
      </div>
      <button v-if="!collapsed.has(group.id)&&group.links.length>3" data-expand-tasks class="expand-tasks" @click="toggle(expanded,group.id)">{{ isExpanded(group.id)?'收起任务':`展开其余 ${group.links.length-3} 个任务` }}</button>
    </article>
  </section>
</template>

<style scoped>
.mm2-topology{min-width:0}.mm2-instance{border:1px solid #dce4ef;border-radius:8px;overflow:hidden;background:#fff;margin-bottom:16px}.instance-header{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:14px 18px;background:#f8f9fd;border-bottom:1px solid #e6eaf2;flex-wrap:wrap}.instance-toggle{border:0;background:none;display:flex;align-items:center;gap:10px;text-align:left;padding:0;min-width:0;cursor:pointer}.instance-toggle strong{font-size:15px;color:#283b58;overflow-wrap:anywhere}.instance-toggle small{font-size:11px;color:#718099;overflow-wrap:anywhere}.mm2-tag{font-size:10px;font-weight:600;letter-spacing:.3px;color:#7753b5;background:#eee7fb;padding:4px 7px;border-radius:4px}.collapse-icon{color:#718099}.instance-meta{display:flex;gap:20px;font-size:12px;color:#64748b}.paths-scroll{overflow-x:auto;padding:18px 22px 8px}.path-headings,.mm2-path{display:grid;grid-template-columns:minmax(150px,1fr) 60px minmax(190px,1.1fr) 60px minmax(150px,1fr);align-items:center;min-width:610px}.path-headings{text-align:center;font-size:11px;color:#718099;margin-bottom:6px}.path-list{list-style:none;padding:0;margin:0}.mm2-path{padding:14px 0;--path-color:#758aa5;--path-dash:6 4;transition:opacity .15s}.endpoint,.sync-task{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:7px;min-width:0;min-height:78px;padding:12px;border-radius:7px;cursor:pointer;margin:0}.endpoint{background:#f1f6ff;border:1px solid #91acd7;color:#263c5d}.endpoint strong{font-size:14px;font-weight:500;line-height:1.5;overflow-wrap:anywhere}.endpoint small{font-size:11px;color:#718099}.sync-task{background:#f7f3ff;border:1px solid #b8a3d9;color:#5c4285}.sync-task strong{font-size:12px;font-weight:500}.sync-task code{display:block;max-width:100%;font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.sync-task small{font-size:11px;color:#718099}.path-arrow{width:100%;height:20px;fill:none;stroke:var(--path-color);stroke-width:1.8;stroke-dasharray:var(--path-dash);overflow:visible}.path-arrow path{vector-effect:non-scaling-stroke}.path-verified{--path-color:#35744e;--path-dash:none}.path-unknown{--path-color:#bd8b39}.path-stale,.path-disabled{--path-color:#929aa6}.path-disabled{--path-dash:2 5}.path-selected .sync-task{border:2px solid #7952b6;background:#eee6fb}.path-selected .path-arrow{stroke-width:2.8}.path-dimmed{opacity:.35}.path-dimmed:hover,.path-dimmed:focus-within{opacity:1}.expand-tasks{display:block;width:100%;border:0;border-top:1px solid #edf0f6;border-radius:0;padding:12px;background:#fbfcfe;color:#526b93;font-size:12px;cursor:pointer}.empty-mm2{padding:40px;text-align:center;color:#718099}.endpoint:focus-visible,.sync-task:focus-visible,.instance-toggle:focus-visible{outline:2px solid #315fc1;outline-offset:3px}
@media(max-width:650px){.instance-header{padding:12px}.instance-toggle{flex-wrap:wrap;gap:7px}.instance-meta{gap:14px}.paths-scroll{padding:12px 14px}.endpoint strong{font-size:13px}}
@media(prefers-reduced-motion:reduce){.mm2-path{transition:none}}
</style>
