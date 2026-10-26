<script setup lang="ts">
import { useRuntime } from '../composables/useRuntime'
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { ApiError } from '../api/client'
import { ensureSession, registryRequest, probeMessage, probeStatus, capabilityStatus, type Cluster, type ClusterPage } from '../api/registry'
import ClusterForm from '../components/ClusterForm.vue'

const { runtime } = useRuntime()

const isMock = computed(() => runtime.value?.mode === 'mock')
const props = defineProps<{ focusId?: string; prefill?: { display_name: string; bootstrap_servers: string[] } }>()
const focused = () => props.focusId && /^[a-f0-9]{32}$/.test(props.focusId) ? props.focusId : ''

const items = ref<Cluster[]>([]), total = ref(0), page = ref(1), query = ref('')
const ready = ref(false), busy = ref(false), error = ref(''), notice = ref(''), persistent = ref(false)
const formOpen = ref(false), editing = ref<Cluster>(), deleting = ref<Cluster>()
const activeId = ref(globalThis.sessionStorage.getItem('lunaorbit.activeClusterId') ?? '')
const probing = ref(''), scenario = ref('success')
let disposed = false
let pollTimer: ReturnType<typeof globalThis.setTimeout> | undefined
function fail(e: unknown) { error.value = e instanceof ApiError ? e.message : '操作失败，请稍后重试' }
async function load() {
  error.value = ''; busy.value = true
  try {
    if (focused()) {
      const cluster = await registryRequest<Cluster>(`/kafka/clusters/${focused()}`)
      items.value = [cluster]; total.value = 1; page.value = 1; return
    }
    const data = await registryRequest<ClusterPage>(`/kafka/clusters?page=${page.value}&page_size=20&query=${encodeURIComponent(query.value)}`)
    items.value = data.items; total.value = data.total
  } catch (e) { fail(e) } finally { busy.value = false }
}
watch(() => props.focusId, () => { if (ready.value) void load() })
onMounted(async () => {
  try { const session = await ensureSession(); persistent.value = session.persistent; ready.value = true; await load(); if (props.prefill) openForm() }
  catch (e) { fail(e) }
})
onUnmounted(() => { disposed = true; globalThis.clearTimeout(pollTimer) })
function switchCluster(cluster: Cluster) { activeId.value = cluster.id; globalThis.sessionStorage.setItem('lunaorbit.activeClusterId', cluster.id) }
function openForm(cluster?: Cluster) { editing.value = cluster; formOpen.value = true }
async function saved() { formOpen.value = false; editing.value = undefined; notice.value = isMock.value ? '集群配置已保存；连接结果以模拟探测为准' : '集群配置已保存；连接结果以实际探测为准'; await load() }
async function toggle(cluster: Cluster) {
  busy.value = true; error.value = ''
  try { await registryRequest(`/kafka/clusters/${encodeURIComponent(cluster.id)}`, 'PATCH', { revision: cluster.revision, enabled: !cluster.enabled }); await load() }
  catch (e) { fail(e) } finally { busy.value = false }
}
async function remove() {
  if (!deleting.value) return
  busy.value = true; error.value = ''
  const cluster = deleting.value
  try {
    await registryRequest(`/kafka/clusters/${encodeURIComponent(cluster.id)}`, 'DELETE', { revision: cluster.revision })
    if (activeId.value === cluster.id) { activeId.value = ''; globalThis.sessionStorage.removeItem('lunaorbit.activeClusterId') }
    deleting.value = undefined; await load()
  } catch (e) { fail(e) } finally { busy.value = false }
}
async function probe(cluster: Cluster) {
  if (!runtime.value || runtime.value.mode === 'disabled') return
  probing.value = cluster.id; error.value = ''; notice.value = isMock.value ? 'Mock 模拟探测排队中' : '连接探测排队中'
  const path = `/kafka/clusters/${encodeURIComponent(cluster.id)}/probe`
  try {
    await registryRequest(path, 'POST', { mock_scenario: scenario.value })
    const poll = async (attempt: number) => {
      if (disposed) return
      try {
        const job = await registryRequest<{ status: string; error_code?: string }>(path)
        if (disposed) return
        if (job.status === 'done') { probing.value = ''; notice.value = job.error_code ? `${isMock.value ? 'Mock：' : ''}${probeMessage(job.error_code, isMock.value)}` : isMock.value ? 'Mock 模拟探测完成；未连接真实 Kafka' : '连接探测完成；请查看各项能力结果'; await load() }
        else if (attempt >= 60) { probing.value = ''; notice.value = '探测任务仍未完成，请刷新查看结果' }
        else pollTimer = globalThis.setTimeout(() => { void poll(attempt + 1) }, 500)
      } catch (e) { probing.value = ''; fail(e) }
    }
    await poll(0)
  } catch (e) { probing.value = ''; fail(e) }
}
</script>

<template>
  <section class="registry">
    <header class="page-header"><div><h1>Kafka 集群管理</h1></div><div class="page-actions"><button :disabled="!ready || busy" @click="load">刷新</button><button v-if="!runtime?.information_mode" class="button-primary" :disabled="!ready || busy" @click="openForm()">新增 Kafka 集群</button></div></header>
    <p v-if="runtime?.information_mode" class="helper-text" data-auto-note>集群由 MM2 配置自动发现；这里只保留历史登记的只读查看，新增、编辑、启停、删除和连接测试入口已隐藏，数据未删除。</p>
    <p v-if="isMock" class="mock-banner">Mock 开发演示 · 所有连接测试和能力结果均为模拟，未验证真实 Kafka。{{ persistent ? '集群配置持久化；演示认证信息在服务重启后需重新填写。' : '集群配置与密码仅临时保存。' }}</p>
    <p v-if="focused()">当前仅显示关联集群。<a href="/kafka/clusters">查看全部集群</a></p>
    <p v-if="error" role="alert" class="error">{{ error }}</p><p v-if="notice" role="status">{{ notice }}</p>
    <section class="panel">
      <div class="panel-header"><h2>集群</h2><span class="helper-text">共 {{ total }} 个集群</span></div><div class="filter-bar">
        <form v-if="!focused()" @submit.prevent="page = 1; load()"><label>筛选 <input v-model="query" placeholder="名称 / 机房 / 环境"></label><button :disabled="busy || !ready">查询</button></form>
        <details v-if="runtime?.mode === 'mock'" class="demo-settings"><summary>演示设置</summary><label>Mock 场景 <select v-model="scenario"><option v-for="s in ['success','authentication_failed','tls_failed','broker_unreachable','advertised_unreachable','authorization_denied','unsupported_api','timeout','kafka37','kafka392']" :key="s">{{ s }}</option></select></label></details>
      </div>
      <div class="table-scroll">
        <table class="data-table">
          <thead><tr><th>集群 / 环境</th><th>连接入口</th><th>安全</th><th>{{ isMock ? '模拟状态' : '连接状态' }}</th><th>版本 / 能力</th><th>操作</th></tr></thead>
          <tbody>
            <tr v-for="cluster in items" :id="'cluster-'+cluster.id" :key="cluster.id">
              <td><strong>{{ cluster.display_name }}</strong><br>{{ cluster.room }} / {{ cluster.environment }}<br><span class="status-pill" :class="cluster.enabled ? 'status-pill--success' : 'status-pill--muted'">{{ cluster.enabled ? '启用' : '停用' }}</span></td>
              <td>{{ cluster.bootstrap_servers.join(', ') }}</td>
              <td>{{ cluster.security_protocol }} / {{ cluster.sasl_mechanism }}<br>认证信息：{{ cluster.credentials_configured ? '已配置' : '未配置' }}</td>
              <td>{{ isMock ? 'Mock：' : '' }}{{ probeStatus(cluster.status, isMock) }}<br>{{ cluster.last_probe_at || (isMock ? '尚未模拟探测' : '尚未探测') }}<br><span v-if="cluster.last_probe_error_code">{{ probeMessage(cluster.last_probe_error_code, isMock) }}</span></td>
              <td>{{ cluster.version_reported || '未设置' }}（{{ cluster.version_source || 'manual' }}）<br><span v-if="!cluster.capabilities?.length">能力尚未探测</span><details v-else class="capability-details"><summary>能力明细（{{ cluster.capabilities.length }}）</summary><div v-for="cap in cluster.capabilities" :key="cap.feature">{{ isMock ? 'Mock ' : '' }}{{ cap.feature }}：{{ capabilityStatus(cap) }}</div></details></td>
              <td class="actions"><router-link :to="`/kafka/clusters/${cluster.id}/details`">只读详情</router-link><template v-if="!runtime?.information_mode"> · <router-link :to="`/kafka/clusters/${cluster.id}/security`">ACL / 权限</router-link></template><button data-action="switch" :disabled="!cluster.enabled || activeId === cluster.id" @click="switchCluster(cluster)">{{ activeId === cluster.id ? '当前集群' : '切换' }}</button><template v-if="!runtime?.information_mode"><button data-action="edit" :disabled="busy" @click="openForm(cluster)">编辑</button><button :disabled="!!probing || !cluster.enabled || busy || !runtime || runtime.mode === 'disabled'" @click="probe(cluster)">{{ probing === cluster.id ? (isMock ? '模拟探测中…' : '探测中…') : (isMock ? '模拟连接测试' : '连接测试') }}</button><button :disabled="busy" @click="toggle(cluster)">{{ cluster.enabled ? '停用' : '启用' }}</button><button class="button-danger" :disabled="busy" @click="deleting = cluster">删除</button></template></td>
            </tr><tr v-if="!items.length"><td colspan="6">{{ busy ? '加载中…' : '暂无集群' }}</td></tr>
          </tbody>
        </table>
      </div>
      <div class="pagination"><button :disabled="page <= 1 || busy" @click="page--; load()">上一页</button><span>第 {{ page }} 页 · {{ total }} 条</span><button :disabled="page * 20 >= total || busy" @click="page++; load()">下一页</button></div>
    </section>
    <ClusterForm v-if="formOpen" :cluster="editing" :prefill="editing ? undefined : props.prefill" @saved="saved" @cancel="formOpen = false; editing = undefined" />
    <div v-if="deleting" class="modal-backdrop"><section role="dialog" aria-modal="true" aria-labelledby="delete-title" class="registry-dialog"><h2 id="delete-title">移除集群</h2><p>删除添加「{{ deleting.display_name }}」？不会删除 Kafka 数据；添加将从列表移除。</p><button class="button-danger" :disabled="busy" @click="remove">移除集群</button><button :disabled="busy" @click="deleting = undefined">取消</button></section></div>
  </section>
</template>
