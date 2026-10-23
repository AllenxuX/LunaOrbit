<script setup lang="ts">
import { computed, onMounted, onUnmounted, shallowRef } from 'vue'
import AppIcon from '../components/AppIcon.vue'
import { ApiError, getHealth } from '../api/client'
import { ensureSession, probeStatus, registryRequest, type Cluster, type ClusterPage } from '../api/registry'
import { scopePath } from '../api/topology'
import { useRuntime } from '../composables/useRuntime'

const { runtime } = useRuntime()
const loading = shallowRef(true), health = shallowRef<'loading' | 'ready' | 'error'>('loading')
const healthError = shallowRef(''), inventoryError = shallowRef(''), snapshotMessage = shallowRef('尚未读取发现快照')
const clusters = shallowRef<Cluster[]>([]), registeredTotal = shallowRef<number | null>(null), scopeTotal = shallowRef<number | null>(null), linkTotal = shallowRef<number | null>(null)
const observedAt = shallowRef(''), refreshedAt = shallowRef('')
const enabledTotal = computed(() => registeredTotal.value === null ? null : clusters.value.filter(cluster => cluster.enabled).length)
const enabledLabel = computed(() => registeredTotal.value !== null && clusters.value.length < registeredTotal.value ? '本页启用集群' : '启用集群')
const sourceLabel = computed(() => runtime.value?.information_mode ? '内网信息平台' : runtime.value?.mode === 'admin-lab' ? '实验管理环境' : runtime.value?.mode === 'readonly-lab' ? '只读实验环境' : runtime.value?.mode === 'mock' ? 'Mock 合成数据' : runtime.value?.mode === 'disabled' ? '数据源已禁用' : '数据源待确认')
const isMock = computed(() => runtime.value?.mode === 'mock')
let epoch = 0, disposed = false
const current = (token: number) => !disposed && token === epoch
function safeError(error: unknown) { return error instanceof ApiError ? error.message : '数据暂时不可用，请稍后刷新' }
async function checkHealth(token: number) {
  try { await getHealth(); if (current(token)) health.value = 'ready' }
  catch (error) { if (current(token)) { health.value = 'error'; healthError.value = safeError(error) } }
}
async function readInventory(token: number) {
  try {
    await ensureSession(); if (!current(token)) return
    const result = await registryRequest<ClusterPage>('/kafka/clusters?page=1&page_size=5')
    if (!current(token)) return
    if (!Array.isArray(result.items) || !Number.isInteger(result.total) || result.total < 0) throw new ApiError('集群列表格式错误', 'INVALID_RESPONSE')
    clusters.value = result.items; registeredTotal.value = result.total
  } catch (error) { if (current(token)) inventoryError.value = safeError(error); return }
  if (runtime.value?.capabilities.discovery === false) { snapshotMessage.value = '当前运行模式未启用资源发现'; return }
  try {
    const scopes = await registryRequest<{ items: { id: string }[] }>('/discovery/scopes')
    if (!current(token)) return
    if (!Array.isArray(scopes.items)) throw new ApiError('发现范围响应格式错误', 'INVALID_RESPONSE')
    scopeTotal.value = scopes.items.length
    if (!scopes.items.length) { snapshotMessage.value = '暂无可用的发现范围'; return }
    const snapshots = await Promise.all(scopes.items.map(scope => registryRequest<{ total: number; snapshot: { observed_at: string; synthetic: boolean; stale: boolean } }>(`${scopePath(scope.id)}/snapshot?page=1&page_size=1`)))
    if (!current(token)) return
    if (snapshots.some(result => !Number.isInteger(result.total) || result.total < 0 || !result.snapshot)) throw new ApiError('快照响应格式错误', 'INVALID_RESPONSE')
    linkTotal.value = snapshots.reduce((sum, result) => sum + result.total, 0)
    observedAt.value = snapshots.map(result => result.snapshot.observed_at).filter(Boolean).sort()[0] ?? ''
    snapshotMessage.value = snapshots.some(result => result.snapshot.stale) ? '包含过期发现数据，请进入拓扑复核' : snapshots.every(result => result.snapshot.synthetic) ? '合成发现数据，不代表实际链路健康' : '发现数据可用，链路健康仍需验证'
  } catch (error) {
    if (current(token)) snapshotMessage.value = error instanceof ApiError && error.status === 404 ? '尚无发现快照，请进入同步拓扑运行发现' : safeError(error)
  }
}
async function refresh() {
  const token = ++epoch
  loading.value = true; health.value = 'loading'; healthError.value = ''; inventoryError.value = ''; clusters.value = []; registeredTotal.value = null; linkTotal.value = null; scopeTotal.value = null; observedAt.value = ''; snapshotMessage.value = '正在读取发现数据'
  await Promise.all([checkHealth(token), readInventory(token)])
  if (current(token)) { loading.value = false; refreshedAt.value = new Date().toLocaleTimeString('zh-CN', { hour12: false }) }
}
onMounted(refresh)
onUnmounted(() => { disposed = true; ++epoch })
</script>

<template>
  <section class="overview registry">
    <header class="page-header">
      <div><h1>运维总览</h1></div>
      <div class="overview-actions"><span v-if="refreshedAt" class="helper-text">更新于 {{ refreshedAt }}</span><button data-refresh :disabled="loading" @click="refresh"><AppIcon name="refresh" :size="15" />{{ loading ? '刷新中…' : '刷新' }}</button></div>
    </header>
    <div class="inventory-cards">
      <article class="inventory-card"><div class="inventory-card-title"><span>已添加集群</span><AppIcon name="database" /></div><strong data-registered-total>{{ registeredTotal ?? '未知' }}</strong><router-link to="/kafka/clusters">管理 Kafka 集群 <AppIcon name="arrow-right" :size="14" /></router-link></article>
      <article class="inventory-card"><div class="inventory-card-title"><span>{{ enabledLabel }}</span><AppIcon name="check" /></div><strong data-enabled-total>{{ enabledTotal ?? '未知' }}</strong></article>
      <article class="inventory-card"><div class="inventory-card-title"><span>发现同步关系</span><AppIcon name="topology" /></div><strong data-link-total>{{ linkTotal ?? '未知' }}</strong><router-link to="/topology">查看发现数据 <AppIcon name="arrow-right" :size="14" /></router-link></article>
      <article class="inventory-card"><div class="inventory-card-title"><span>发现范围</span><AppIcon name="shield" /></div><strong>{{ scopeTotal ?? '未知' }}</strong></article>
    </div>
    <div class="dashboard-columns">
      <section class="panel inventory-panel">
        <div class="panel-header"><h2>集群</h2><router-link to="/kafka/clusters">查看全部 <AppIcon name="arrow-right" :size="14" /></router-link></div>
        <div v-if="inventoryError" class="inventory-empty"><p role="alert">{{ inventoryError }}</p><button :disabled="loading" @click="refresh">重试读取</button></div>
        <div v-else class="table-scroll"><table class="data-table"><thead><tr><th>集群名称</th><th>机房 / 环境</th><th>启用状态</th><th>连接状态</th><th /></tr></thead><tbody><tr v-for="cluster in clusters" :key="cluster.id"><td><strong>{{ cluster.display_name }}</strong></td><td>{{ cluster.room || '未知' }} / {{ cluster.environment || '未知' }}</td><td><span class="status-pill" :class="cluster.enabled ? 'status-pill--success' : 'status-pill--muted'">{{ cluster.enabled ? '启用' : '停用' }}</span></td><td>{{ probeStatus(cluster.status, isMock) }}</td><td><router-link :to="`/kafka/clusters/${cluster.id}/details`">详情</router-link></td></tr><tr v-if="!clusters.length"><td colspan="5"><div class="inventory-empty"><AppIcon name="database" :size="28" /><p>{{ loading ? '正在读取集群…' : '尚未设置 Kafka 集群' }}</p><router-link v-if="!loading" to="/kafka/clusters">添加第一个集群</router-link></div></td></tr></tbody></table></div>
        <div class="inventory-footer"><span>展示前 {{ clusters.length }} 个集群</span></div>
      </section>
      <aside class="panel source-panel">
        <div class="panel-header"><h2>数据源与采集</h2><AppIcon name="clock" :size="16" /></div>
        <div class="panel-body">
          <div class="source-row"><span>当前数据源</span><strong>{{ sourceLabel }}</strong></div>
          <div class="source-row" role="status" aria-live="polite"><span>平台 API</span><span class="status-pill" :class="health === 'ready' ? 'status-pill--success' : health === 'error' ? 'status-pill--warning' : 'status-pill--muted'">{{ health === 'loading' ? '检查中' : health === 'ready' ? '进程可用' : healthError }}</span></div>
          <div class="collection-status"><h3>发现数据</h3><p>{{ snapshotMessage }}</p><span v-if="observedAt" class="helper-text">最早数据更新时间 {{ observedAt }}</span></div>
          <router-link class="source-link" to="/topology">打开同步拓扑 <AppIcon name="arrow-right" :size="14" /></router-link>
        </div>
      </aside>
    </div>
    <section class="panel entry-panel"><div class="panel-header"><h2>运维工作台</h2></div><div class="workspace-entries"><router-link to="/topology"><AppIcon name="topology" /><div><strong>同步拓扑</strong><span>查看同步链路和 MM2 运行状态</span></div><AppIcon name="arrow-right" :size="16" /></router-link><router-link v-if="runtime?.information_mode" to="/applications"><AppIcon name="database" /><div><strong>应用信息</strong><span>查看业务归属和生产消费登记</span></div><AppIcon name="arrow-right" :size="16" /></router-link><router-link v-if="!runtime?.information_mode" to="/security"><AppIcon name="shield" /><div><strong>身份与权限</strong><span>管理平台权限与审批申请</span></div><AppIcon name="arrow-right" :size="16" /></router-link><router-link v-if="!runtime?.information_mode" to="/changes"><AppIcon name="changes" /><div><strong>变更管理</strong><span>预览规则变更与核对审批记录</span></div><AppIcon name="arrow-right" :size="16" /></router-link></div></section>
  </section>
</template>

<style scoped>
.overview-actions { display:flex; align-items:center; gap:14px; }
.inventory-cards { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:18px; margin-bottom:24px; }
.inventory-card { padding:20px; background:#fff; border:1px solid #e2e8f0; border-radius:6px; }
.inventory-card-title { display:flex; align-items:center; justify-content:space-between; color:#64748b; font-size:13px; }
.inventory-card-title .app-icon { color:#7390b5; }
.inventory-card > strong { display:block; font-size:30px; font-weight:600; line-height:1.3; margin:12px 0; color:#172b48; }
.inventory-card > a, .inventory-card > p { font-size:12px; margin:0; min-height:18px; }
.inventory-card > p { color:#64748b; }
.inventory-card a, .panel-header > a, .source-link { display:inline-flex; align-items:center; gap:7px; }
.dashboard-columns { display:grid; grid-template-columns:minmax(0,1fr) 330px; gap:24px; align-items:start; margin-bottom:24px; }
.inventory-panel { min-width:0; }.table-scroll { overflow-x:auto; }
.inventory-footer { display:flex; flex-wrap:wrap; gap:12px; justify-content:space-between; padding:16px 20px; font-size:12px; color:#65748b; border-top:1px solid #e8edf3; }
.inventory-empty { display:flex; flex-direction:column; align-items:center; justify-content:center; min-height:180px; color:#65748b; gap:12px; }.inventory-empty p { margin:0; }
.source-row { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:18px; font-size:13px; }.source-row > span:first-child { color:#64748b; }.source-row > strong { font-weight:500; }
.source-note { font-size:12px; color:#65748b; line-height:1.7; margin:0 0 20px; }
.collection-status { padding-top:18px; border-top:1px solid #e8edf3; margin-bottom:20px; }.collection-status h3 { font-size:13px; margin:0 0 10px; }.collection-status p { font-size:12px; line-height:1.7; color:#64748b; margin:0 0 8px; }.source-link { font-size:12px; }
.workspace-entries { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); }.workspace-entries > a { display:flex; gap:14px; align-items:center; padding:24px; border-right:1px solid #e8edf3; color:#426389; }.workspace-entries > a:last-child { border-right:0; }.workspace-entries div { flex:1; }.workspace-entries strong { display:block; color:#263b56; font-size:14px; margin-bottom:7px; }.workspace-entries span { display:block; font-size:12px; color:#65748b; line-height:1.6; }
@media(max-width:1200px) { .dashboard-columns { grid-template-columns:minmax(0,1fr); }.inventory-cards { gap:12px; }.workspace-entries > a { padding:18px; } }
@media(max-width:700px) { .inventory-cards { grid-template-columns:repeat(2,minmax(0,1fr)); }.workspace-entries { grid-template-columns:1fr; }.workspace-entries > a { border-right:0; border-bottom:1px solid #e8edf3; }.overview-actions { flex-wrap:wrap; } }
</style>
