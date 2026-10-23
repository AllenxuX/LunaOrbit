<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ensureSession, registryRequest, probeStatus, type Cluster } from '../api/registry'
import { workspaceMeta, workspaceError, type PlanInput, type WorkspaceMeta } from '../api/workspace'
import WorkspaceResources from '../components/workspace/WorkspaceResources.vue'
import BrokerManagement from '../components/workspace/BrokerManagement.vue'
import ResourceOperations from '../components/workspace/ResourceOperations.vue'
import WorkspaceAccess from '../components/workspace/WorkspaceAccess.vue'
import WorkspacePlanDialog from '../components/workspace/WorkspacePlanDialog.vue'
import WorkspaceOperations from '../components/workspace/WorkspaceOperations.vue'
import WorkspaceTeams from '../components/workspace/WorkspaceTeams.vue'
import WorkspaceLinks from '../components/workspace/WorkspaceLinks.vue'
import { useRuntime } from '../composables/useRuntime'

const props = defineProps<{ id: string }>()
const route = useRoute(), router = useRouter()
const cluster = ref<Cluster>(), meta = ref<WorkspaceMeta>(), actorId = ref(''), busy = ref(false), error = ref(''), input = ref<PlanInput | null>(null), refreshVersion = ref(0)
const { runtime } = useRuntime()
const informationMode = computed(() => runtime.value?.information_mode === true)
const tabs = computed(() => [{ id: 'overview', label: '概览' }, { id: 'topics', label: 'Topics' }, { id:'brokers',label:'Broker 与配置' }, { id: 'groups', label: '消费组' }, ...(!informationMode.value ? [{ id: 'applications', label: '应用与权限' }] : []), { id: 'links', label: '同步链路' }, ...(!informationMode.value ? [{ id: 'operations', label: '操作记录' }] : [])])
const tab = computed(() => !informationMode.value && route.query.tab === 'teams' ? 'teams' : tabs.value.some(item => item.id === route.query.tab) ? String(route.query.tab) : 'overview')
const resourceSection = computed(() => tab.value === 'topics' ? 'topics' : tab.value === 'groups' ? 'groups' : 'overview')
let epoch = 0
async function load() {
  const token = ++epoch; busy.value = true; error.value = ''; cluster.value = undefined; meta.value = undefined; input.value = null
  try {
    const session = await ensureSession(); if (token !== epoch) return
    const [registration, metadata] = await Promise.all([registryRequest<Cluster>(`/kafka/clusters/${encodeURIComponent(props.id)}`), workspaceMeta(props.id)])
    if (token === epoch) { cluster.value = registration; meta.value = metadata; actorId.value = session.user_id }
  } catch (e) { if (token === epoch) error.value = workspaceError(e) } finally { if (token === epoch) busy.value = false }
}
function selectTab(value: string) { input.value = null; void router.replace({ query: { ...route.query, tab: value } }) }
function complete() { ++refreshVersion.value }
watch(() => props.id, load, { immediate: true })
onBeforeUnmount(() => { ++epoch })
</script>
<template>
  <section class="kafka-workspace registry">
    <header class="page-header"><div><div class="workspace-back"><router-link to="/kafka/clusters">Kafka 集群</router-link><span>/</span><span>工作台</span></div><h1>{{ cluster?.display_name || 'Kafka 工作台' }}</h1><p>{{ cluster ? [cluster.room,cluster.environment].filter(Boolean).join(' · ') || '集群资源与应用授权' : '正在连接工作空间' }}</p></div><div class="workspace-header-actions"><span v-if="meta" class="status-pill" :class="meta.synthetic ? 'status-pill--warning' : 'status-pill--muted'">{{ meta.synthetic ? '演示数据' : meta.write_enabled ? '管理操作已启用' : '只读访问' }}</span><button :disabled="busy" @click="load">刷新</button></div></header>
    <p v-if="busy" role="status">正在读取集群工作台…</p><p v-if="error" role="alert" class="error">{{ error }}</p>
    <template v-if="cluster && meta">
      <div class="cluster-context"><span>{{ probeStatus(cluster.status,meta.synthetic) }}</span><span>{{ cluster.security_protocol }}</span><span v-if="cluster.security_protocol==='SASL_PLAINTEXT'" class="transport-warning">认证流量未加密，建议使用 SASL_SSL</span></div>
      <nav class="workspace-tabs" aria-label="集群工作台"><button v-for="item in tabs" :key="item.id" :class="{active:tab===item.id}" :aria-current="tab===item.id ? 'page' : undefined" @click="selectTab(item.id)">{{ item.label }}</button></nav>
      <WorkspaceResources v-if="['overview','topics','groups'].includes(tab)" :key="`${id}:${resourceSection}:${refreshVersion}`" :cluster-id="id" :section="resourceSection" :meta="meta" :actor-id="actorId" :can-manage-topics="!informationMode && meta.permissions.topic_manage && meta.write_enabled" @intent="input=$event" />
      <BrokerManagement v-else-if="tab==='brokers'" :key="id" :cluster-id="id" :meta="meta" :actor-id="actorId" />
      <WorkspaceAccess v-else-if="tab==='applications'" :key="id" :cluster-id="id" :meta="meta" :actor-id="actorId" :refresh-version="refreshVersion" @intent="input=$event" />
      <WorkspaceLinks v-else-if="tab==='links'" :key="id" :cluster-id="id" />
      <WorkspaceOperations v-else-if="tab==='operations'" :key="id" :cluster-id="id" :meta="meta" :actor-id="actorId" :refresh-version="refreshVersion" @completed="complete" />
      <ResourceOperations v-if="tab==='operations'" :cluster-id="id" :meta="meta" :actor-id="actorId" />
      <section v-else-if="tab==='teams'" class="legacy-team-access"><p class="panel">团队授权现属于平台访问控制。<router-link :to="{path:'/access-control',query:{cluster:id}}">前往平台访问控制</router-link></p><WorkspaceTeams :key="id" :cluster-id="id" :can-manage="meta.permissions.team_manage" /></section>
      <WorkspacePlanDialog v-if="!informationMode" :key="id" :cluster-id="id" :input="input" :meta="meta" :actor-id="actorId" @close="input=null" @completed="complete" />
    </template>
  </section>
</template>
<style scoped>
.workspace-back { display:flex; align-items:center; gap:9px; color:#64748b; font-size:12px; margin-bottom:9px; }.workspace-header-actions { display:flex; align-items:center; flex-wrap:wrap; gap:12px; }.cluster-context { display:flex; gap:20px; flex-wrap:wrap; font-size:12px; color:#64748b; margin-bottom:18px; }.transport-warning { color:#9b6225; }.workspace-tabs { display:flex; gap:6px; overflow-x:auto; background:#fff; border:1px solid #e5ebf3; border-radius:6px; margin-bottom:20px; padding:0 14px; }.workspace-tabs button { border:0; background:transparent; border-bottom:3px solid transparent; border-radius:0; padding:15px 18px; white-space:nowrap; color:#64748b; }.workspace-tabs button.active { color:#1d4ed8; border-bottom-color:#2563eb; font-weight:600; }.workspace-tabs button:hover { background:#f8fafc; }@media(max-width:640px){.workspace-tabs{padding:0 6px;}.workspace-tabs button{padding:13px 11px;}.workspace-header-actions{margin-top:12px;}}
</style>
