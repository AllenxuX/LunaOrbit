<script setup lang="ts">
import { computed, onMounted, onUnmounted, shallowRef } from 'vue'
import AppIcon from '../components/AppIcon.vue'
import ReadActions from '../components/ReadActions.vue'
import { ApiError, getHealth } from '../api/client'
import { ensureSession, probeStatus, registryRequest, type Cluster, type ClusterPage } from '../api/registry'
import { readSnapshot, scopePath, type Job, type ScopeEntry, type ScopeInfo } from '../api/topology'
import { inspect, scopeLabel, type Inspection } from '../topology/inspection'
import { useRuntime } from '../composables/useRuntime'

const { runtime } = useRuntime()
const loading = shallowRef(true), collecting = shallowRef(false), health = shallowRef<'loading' | 'ready' | 'error'>('loading')
const healthError = shallowRef(''), inventoryError = shallowRef(''), snapshotMessage = shallowRef('尚未读取发现快照'), collectNotice = shallowRef('')
const clusters = shallowRef<Cluster[]>([]), registeredTotal = shallowRef<number | null>(null), scopes = shallowRef<ScopeInfo[]>([]), scopeTotal = shallowRef<number | null>(null)
const summary = shallowRef<Inspection | null>(null), kafkaTotal = shallowRef<number | null>(null), refreshedAt = shallowRef('')
const linkTotal = computed(() => summary.value?.relations ?? null)
const enabledTotal = computed(() => registeredTotal.value === null ? null : clusters.value.filter(cluster => cluster.enabled).length)
const enabledLabel = computed(() => registeredTotal.value !== null && clusters.value.length < registeredTotal.value ? '本页启用集群' : '启用集群')
const sourceLabel = computed(() => runtime.value?.information_mode ? '内网信息平台' : runtime.value?.mode === 'admin-lab' ? '实验管理环境' : runtime.value?.mode === 'readonly-lab' ? '只读实验环境' : runtime.value?.mode === 'mock' ? 'Mock 合成数据' : runtime.value?.mode === 'disabled' ? '数据源已禁用' : '数据源待确认')
const isMock = computed(() => runtime.value?.mode === 'mock')
const scopeText = computed(() => scopes.value.filter(s => !s.removed).map(scopeLabel).join('；'))
const observedAt = computed(() => summary.value?.oldestObservedAt ? new Date(summary.value.oldestObservedAt).toLocaleString('zh-CN', { hour12: false }) : '')
const num = (v: number | null | undefined) => v ?? '未知'
let epoch = 0, disposed = false
const current = (token: number) => !disposed && token === epoch
function safeError(error: unknown) { return error instanceof ApiError ? error.message : '数据暂时不可用，请稍后刷新' }
async function checkHealth(token: number) {
  try { await getHealth(); if (current(token)) health.value = 'ready' }
  catch (error) { if (current(token)) { health.value = 'error'; healthError.value = safeError(error) } }
}
async function readDiscovery(token: number) {
  if (runtime.value?.capabilities.discovery === false) { snapshotMessage.value = '当前运行模式未启用资源发现'; return }
  try {
    const result = await registryRequest<{ items: ScopeInfo[] }>('/discovery/scopes')
    if (!current(token)) return
    if (!Array.isArray(result.items)) throw new ApiError('发现范围响应格式错误', 'INVALID_RESPONSE')
    scopes.value = result.items; scopeTotal.value = result.items.filter(s => !s.removed).length
    if (!scopeTotal.value) { snapshotMessage.value = '暂无可用的发现范围'; return }
    const entries: ScopeEntry[] = []
    for (const scope of result.items.filter(s => !s.removed)) {
      try { entries.push({ scope, snapshot: await readSnapshot(scope.id) }) }
      catch (error) { entries.push({ scope, error: error instanceof ApiError && error.status === 404 ? 'NO_SNAPSHOT' : 'READ_FAILED' }) }
      if (!current(token)) return
    }
    summary.value = inspect(entries)
    snapshotMessage.value = !summary.value.readScopes ? (entries.every(e => e.error === 'NO_SNAPSHOT') ? '尚无发现快照，可重新采集' : '发现快照读取失败，数量未知') : summary.value.synthetic ? '合成发现数据，不代表实际链路健康' : '配置与运行资源已读取；同步健康未验证'
  } catch (error) { if (current(token)) snapshotMessage.value = safeError(error) }
  try { const body = await registryRequest<{ items?: unknown[] }>('/discovery/catalog'); if (current(token) && Array.isArray(body.items)) kafkaTotal.value = body.items.length } catch { /* unknown stays null */ }
}
async function readInventory(token: number) {
  try {
    await ensureSession(); if (!current(token)) return
    const result = await registryRequest<ClusterPage>('/kafka/clusters?page=1&page_size=5')
    if (!current(token)) return
    if (!Array.isArray(result.items) || !Number.isInteger(result.total) || result.total < 0) throw new ApiError('集群列表格式错误', 'INVALID_RESPONSE')
    clusters.value = result.items; registeredTotal.value = result.total
  } catch (error) { if (current(token)) inventoryError.value = safeError(error); return }
  await readDiscovery(token)
}
async function refresh() {
  const token = ++epoch
  loading.value = true; health.value = 'loading'; healthError.value = ''; inventoryError.value = ''; clusters.value = []; registeredTotal.value = null; scopeTotal.value = null; summary.value = null; kafkaTotal.value = null; snapshotMessage.value = '正在读取发现数据'
  await Promise.all([checkHealth(token), readInventory(token)])
  if (current(token)) { loading.value = false; refreshedAt.value = new Date().toLocaleTimeString('zh-CN', { hour12: false }) }
}
async function collect() {
  if (collecting.value) return
  collecting.value = true; collectNotice.value = '正在重新采集…'
  const failures: string[] = []
  for (const scope of scopes.value.filter(s => !s.synthetic && !s.removed)) {
    try {
      await registryRequest(`${scopePath(scope.id)}/runs`, 'POST', {})
      let job: Job | undefined
      for (let attempt = 0; attempt < 40 && !disposed; attempt++) { job = await registryRequest<Job>(`${scopePath(scope.id)}/runs/latest`); if (job.status === 'done') break; await new Promise(r => globalThis.setTimeout(r, 500)) }
      if (job?.status !== 'done') failures.push(`${scope.id}：仍在运行`)
      else if (job.error_code) failures.push(`${scope.id}：未完整成功`)
    } catch (error) { failures.push(`${scope.id}：${error instanceof ApiError && error.status === 429 ? '请求过于频繁，请稍后再试' : safeError(error)}`) }
  }
  collecting.value = false
  collectNotice.value = failures.length ? `重新采集结束，部分范围未完成：${failures.join('；')}。已保留上次成功数据。` : '重新采集完成；同步健康仍未验证。'
  await refresh()
}
onMounted(refresh)
onUnmounted(() => { disposed = true; ++epoch })
</script>

<template>
  <section class="overview registry">
    <header class="page-header">
      <div><h1>运维总览</h1><p class="page-lead">发现需要复核的事；关系解释见同步拓扑，单个 Topic 见 Topic 链路。</p></div>
      <div class="overview-actions"><span v-if="refreshedAt" class="helper-text">页面读取于 {{ refreshedAt }}</span></div>
    </header>
    <ReadActions :scope-label="scopeText" :busy="loading || collecting" :can-collect="runtime?.capabilities.discovery === true && scopes.some(s => !s.synthetic && !s.removed)" @reread="refresh" @collect="collect" />
    <p v-if="collectNotice" class="collect-notice" role="status">{{ collectNotice }}</p>
    <div v-if="summary && summary.failedScopes.length" class="partial-notice" data-partial role="status">局部采集失败：{{ summary.failedScopes.map(f => f.scope + (f.namespace ? ' / ' + f.namespace : '')).join('、') }}。{{ summary.failedScopes.every(f => f.hasHistory) ? '保留上次成功数据；' : summary.failedScopes.some(f => f.hasHistory) ? '有旧快照的范围保留上次成功数据，其余无可用数据；' : '无可用快照；' }}该范围运行状态未知，不判为链路故障。</div>
    <section class="panel conclusion" aria-label="巡检结论" data-conclusion>
      <h2>巡检结论</h2>
      <p v-if="loading && !summary" role="status">正在读取发现数据…（首次加载不显示 0）</p>
      <template v-else>
        <p>{{ snapshotMessage }}。未验证不计入故障。</p>
        <div class="strip">
          <span><strong>未知</strong>已证实异常<small>当前无异常证据来源</small></span>
          <span><strong data-unverified-mm2>{{ num(summary?.unverifiedMm2) }}</strong>含未验证关系的 MM2 / 个</span>
          <span><strong data-failed-scopes>{{ summary ? summary.failedScopes.length : '未知' }}</strong>采集失败 / 范围</span>
          <span><strong data-stale-scopes>{{ summary ? summary.staleScopes.length : '未知' }}</strong>数据过期 / 范围</span>
        </div>
        <p class="muted">各指标按各自单位阅读；未验证 MM2、失败范围、过期范围可能交叠，不提供相加总数。<span v-if="summary && summary.unreadScopes">数量仅覆盖已读取的 {{ summary.readScopes }} 个范围<template v-if="summary.uncollectedScopes.length">（{{ summary.uncollectedScopes.length }} 个范围尚未采集）</template>。</span></p>
      </template>
    </section>
    <div class="dashboard-columns">
      <section class="panel" aria-label="待复核与证据缺口">
        <div class="panel-header"><h2>待复核与证据缺口</h2></div>
        <div class="table-scroll">
          <table class="data-table">
            <thead><tr><th>对象与问题</th><th>证据 / 下一步</th><th /></tr></thead><tbody>
              <tr v-for="f in summary?.failedScopes ?? []" :key="f.scope + (f.namespace ?? '')" data-review-failed><td class="long-name">{{ f.scope }}{{ f.namespace ? ' / ' + f.namespace : '' }}<br><small>采集失败</small></td><td>{{ f.reason }}{{ f.observedAt ? ' · 上次成功 ' + new Date(f.observedAt).toLocaleString('zh-CN', { hour12: false }) : '' }}</td><td><router-link to="/topology">查看范围</router-link></td></tr>
              <tr v-for="u in summary?.uncollectedScopes ?? []" :key="'u' + u" data-review-uncollected><td class="long-name">{{ u }}<br><small>尚未采集</small></td><td>该范围还没有发现快照；不是采集失败，数量未知</td><td><router-link to="/topology">去重新采集</router-link></td></tr>
              <tr v-if="summary?.unverifiedMm2"><td>{{ summary.unverifiedMm2 }} 个 MM2 含未验证关系</td><td>配置已读取；不等同于消息已到达</td><td><router-link to="/topology">查看同步关系</router-link></td></tr>
              <tr v-if="summary && !summary.failedScopes.length && !summary.uncollectedScopes.length && !summary.unverifiedMm2"><td colspan="3">当前已读取范围内没有待复核项；这不是“没有故障”的证明。</td></tr>
              <tr v-if="!summary && !loading"><td colspan="3">尚无可显示的发现数据；数量为未知，不显示为 0。</td></tr>
            </tbody>
          </table>
        </div>
      </section>
      <aside class="panel source-panel" aria-label="数据覆盖">
        <div class="panel-header"><h2>数据覆盖</h2><AppIcon name="clock" :size="16" /></div>
        <div class="panel-body">
          <div class="source-row"><span>Kafka 集群（目录合并）</span><strong data-kafka-total>{{ num(kafkaTotal) }} 个</strong></div>
          <div class="source-row"><span>MM2 实例</span><strong>{{ num(summary?.mm2Instances) }} 个</strong></div>
          <div class="source-row"><span>同步关系（MM2 规则）</span><strong data-link-total>{{ linkTotal ?? '未知' }}</strong></div>
          <div class="source-row"><span>有向集群对</span><strong>{{ num(summary?.directedPairs) }} 对</strong></div>
          <div class="source-row"><span>发现范围</span><strong>{{ scopeTotal ?? '未知' }} 个</strong></div>
          <p class="helper-text">最早快照采集时间：{{ observedAt || '未知' }}</p>
          <details class="counting"><summary>统计口径</summary><p>关系按（范围，关系 ID）去重；MM2 按（范围，namespace，Deployment UID）去重；失败范围按（范围，namespace）去重；Kafka 集群按目录合并后计数。全局与 Topic 查询范围不同，不要求数值相等。</p></details>
          <router-link class="source-link" to="/topology">打开同步拓扑 <AppIcon name="arrow-right" :size="14" /></router-link>
        </div>
      </aside>
    </div>
    <details class="panel assets">
      <summary>平台与资产</summary>
      <div class="source-row" role="status" aria-live="polite"><span>平台 API（仅表示进程可用）</span><span class="status-pill" :class="health === 'ready' ? 'status-pill--success' : health === 'error' ? 'status-pill--warning' : 'status-pill--muted'">{{ health === 'loading' ? '检查中' : health === 'ready' ? '进程可用' : healthError }}</span></div>
      <div class="source-row"><span>当前数据源</span><strong>{{ sourceLabel }}</strong></div>
      <div class="source-row"><span>已添加集群</span><strong data-registered-total>{{ registeredTotal ?? '未知' }}</strong></div>
      <div class="source-row"><span>{{ enabledLabel }}</span><strong data-enabled-total>{{ enabledTotal ?? '未知' }}</strong></div>
      <div v-if="inventoryError" class="inventory-empty"><p role="alert">{{ inventoryError }}</p><button :disabled="loading" @click="refresh">重试读取</button></div>
      <div v-else class="table-scroll"><table class="data-table"><thead><tr><th>集群名称</th><th>机房 / 环境</th><th>启用状态</th><th>连接状态</th><th /></tr></thead><tbody><tr v-for="cluster in clusters" :key="cluster.id"><td class="long-name"><strong>{{ cluster.display_name }}</strong></td><td>{{ cluster.room || '未知' }} / {{ cluster.environment || '未知' }}</td><td>{{ cluster.enabled ? '启用' : '停用' }}</td><td>{{ probeStatus(cluster.status, isMock) }}</td><td><router-link :to="`/kafka/clusters/${cluster.id}/details`">详情</router-link></td></tr><tr v-if="!clusters.length"><td colspan="5"><div class="inventory-empty"><p>{{ loading ? '正在读取集群…' : '尚未设置 Kafka 集群' }}</p><router-link v-if="!loading" to="/kafka/clusters">添加第一个集群</router-link></div></td></tr></tbody></table></div>
      <p class="helper-text">展示前 {{ clusters.length }} 个集群 · <router-link to="/kafka/clusters">查看集群目录</router-link><template v-if="runtime?.information_mode"> · <router-link to="/applications">应用信息</router-link></template></p>
    </details>
  </section>
</template>

<style scoped>
.page-lead{color:#64748b;font-size:13px;margin:6px 0 0}.overview-actions{display:flex;align-items:center;gap:14px}
.conclusion{padding:20px;margin-bottom:20px}.conclusion h2{margin:0 0 8px;font-size:16px}.strip{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:12px 0}.strip span{display:flex;flex-direction:column;gap:4px;padding:12px;border:1px solid #e2e8f0;border-radius:6px;font-size:12px;color:#475569}.strip strong{font-size:24px;color:#172b48}.strip small{color:#94a3b8}.muted{color:#64748b;font-size:12px}
.partial-notice,.collect-notice{padding:10px 14px;margin:0 0 14px;border:1px solid #f3d38a;background:#fffbeb;border-radius:6px;font-size:13px}.collect-notice{border-color:#cbd5e1;background:#f8fafc}
.dashboard-columns{display:grid;grid-template-columns:minmax(0,1fr) 330px;gap:24px;align-items:start;margin-bottom:24px}.table-scroll{overflow-x:auto}.long-name{overflow-wrap:anywhere}
.source-row{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px;font-size:13px}.source-row>span:first-child{color:#64748b}.source-row>strong{font-weight:500}
.counting p{font-size:12px;color:#64748b;line-height:1.7}.source-link{display:inline-flex;align-items:center;gap:7px;font-size:12px}
.assets{padding:16px 20px}.assets summary{cursor:pointer;font-weight:600;margin-bottom:12px}.inventory-empty{display:flex;flex-direction:column;align-items:center;gap:10px;min-height:120px;justify-content:center;color:#65748b}
@media(max-width:1200px){.dashboard-columns{grid-template-columns:minmax(0,1fr)}}
@media(max-width:700px){.strip{grid-template-columns:repeat(2,minmax(0,1fr))}}
</style>
