<script setup lang="ts">
import { computed } from 'vue'
import { deploymentGroups } from '../topology/deployments'
import type { Link } from '../api/topology'
import { linkStatus,mappingName,type RegistrationIdentity } from '../topology/model'

const props=defineProps<{links:Link[];registrations:RegistrationIdentity[];scope:string;grouped?:boolean;selectedId?:string;selectionMode?:boolean;stale?:boolean}>()
const emit=defineEmits<{select:[id:string]}>()
const groups=computed(()=>props.grouped?deploymentGroups(props.links,props.stale):[{id:'all',name:'',namespace:'',links:props.links,runtime:''}])
function policy(value:string){return value==='DefaultReplicationPolicy'?'保留源集群前缀':value==='IdentityReplicationPolicy'?'保持 Topic 原名':value||'未知'}
function name(mapping:Link['source']){return mappingName(mapping,props.registrations)}
</script>

<template>
  <div class="topology-tasks">
    <component :is="grouped?'details':'div'" v-for="group in groups" :key="group.id" :data-deployment="grouped?group.id:undefined" :class="{'deployment-group':grouped}" open>
      <summary v-if="grouped" class="deployment-heading"><span><strong>{{ group.name }}</strong><small>{{ group.namespace }}</small></span><span class="deployment-meta">{{ group.links.length }} 个任务<span>{{ group.runtime }}</span></span></summary>
      <div class="task-scroll">
        <table class="task-table">
          <thead><tr><th>源集群</th><th>目标集群</th><th v-if="!grouped">MM2 实例</th><th>Topic 匹配规则</th><th>命名策略</th><th>同步状态</th><th class="task-action">操作</th></tr></thead>
          <tbody><tr v-for="link in group.links" :key="link.id" :data-task-row="link.id" :class="{'task-selected':selectedId===link.id}"><td>{{ name(link.source) }}</td><td>{{ name(link.target) }}</td><td v-if="!grouped">{{ link.workload_name }}</td><td><code>{{ link.topics_raw||'尚未获取' }}</code></td><td>{{ policy(link.policy) }}</td><td>{{ stale?'数据已过期 · 状态未知':linkStatus(link) }}</td><td class="task-actions"><button :data-link="link.id" :data-route-choice="selectionMode?link.id:undefined" :aria-pressed="selectedId===link.id" @click="emit('select',link.id)">查看</button><router-link :to="`/topology/${encodeURIComponent(scope)}/links/${encodeURIComponent(link.id)}`" :data-link-detail="link.id">详情</router-link></td></tr></tbody>
        </table>
      </div>
    </component>
    <p v-if="!links.length" class="task-empty">当前筛选下没有同步任务</p>
  </div>
</template>

<style scoped>
.deployment-group{border:1px solid #dce4ef;border-radius:8px;background:#fff;overflow:hidden;margin-bottom:16px}.deployment-heading{display:flex;align-items:center;justify-content:space-between;padding:18px 20px;cursor:pointer;background:#f8fafd;gap:16px}.deployment-heading>span:first-child{display:flex;align-items:baseline;gap:12px}.deployment-heading strong{font-size:15px;color:#253956}.deployment-heading small{color:#718099}.deployment-meta{display:flex;gap:24px;font-size:12px;color:#52657d}.task-scroll{overflow:auto}.task-table{width:100%;border-collapse:collapse;text-align:left;font-size:12px}.task-table th{padding:12px 16px;background:#f8fafd;color:#66758c;font-weight:500;white-space:nowrap}.task-table td{padding:14px 16px;border-top:1px solid #e9edf4;color:#35465f;min-width:100px;overflow-wrap:anywhere}.task-table code{white-space:pre-wrap;word-break:break-word;font-size:12px}.task-table .task-action,.task-table .task-actions{min-width:104px;white-space:nowrap}.task-actions button{padding:0;border:0;background:none;color:#295db6;margin-right:12px;cursor:pointer}.task-actions a{color:#295db6}.task-selected{background:#edf4ff}.task-empty{padding:24px;color:#718099;text-align:center}
@media(max-width:650px){.deployment-heading{align-items:flex-start}.deployment-heading>span:first-child,.deployment-meta{flex-direction:column;gap:6px}.task-table td,.task-table th{padding:12px}}
</style>
