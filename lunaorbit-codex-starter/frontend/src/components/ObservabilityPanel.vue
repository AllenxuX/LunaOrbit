<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import type { Snapshot } from '../api/topology'
import { confirmations, confirmLineage, impact, metrics, observationError, type Confirmation, type Impact, type Metric, type Observation } from '../api/observability'
const props = defineProps<{ scopeId: string; snapshot: Snapshot; selectedNodeId?: string }>()
const emit = defineEmits<{ affected: [nodes: string[]]; confirmed: [links: string[]] }>()
const observation = ref<Observation>(), candidates = ref<Impact>(), lineage = ref<Confirmation[]>([])
const scenario = ref('success'), nodeId = ref(''), linkId = ref(''), topic = ref(''), description = ref(''), explicit = ref(false)
const error = ref(''), impactError = ref(''), notice = ref(''), busy = ref(false), confirming = ref(false)
const now = ref(Date.now())
let expiryTimer: ReturnType<typeof globalThis.setInterval> | undefined
function stale(metric: Metric) {
  const expires = metric.expires_at ? Date.parse(metric.expires_at) : NaN
  return metric.stale || (Number.isFinite(expires) && expires > 0 && now.value >= expires)
}
const observationStale = computed(() => observation.value?.stale || observation.value?.items.some(stale))
let epoch = 0, impactEpoch = 0, disposed = false
const valid = (token: number) => !disposed && token === epoch
const observedTime = (value?: string) => !value || value.startsWith('0001-') || !Number.isFinite(Date.parse(value)) ? '未知' : value
function showConfirmations(items: Confirmation[]) { lineage.value = items; emit('confirmed', [...new Set(items.filter(c => c.active === true && c.kind === 'manual_business_confirmation').map(c => c.link_id))]) }
async function load() {
  const token = ++epoch; ++impactEpoch; busy.value = true; error.value = ''; impactError.value = ''; notice.value = ''; observation.value = undefined; candidates.value = undefined; emit('affected', []); showConfirmations([])
  try {
    const [m, c] = await Promise.allSettled([metrics(props.scopeId, scenario.value), confirmations(props.scopeId)])
    if (!valid(token)) return
    if (m.status === 'fulfilled') observation.value = { ...m.value, items: m.value.items ?? [] }
    else error.value = observationError(m.reason)
    if (c.status === 'fulfilled') showConfirmations(c.value.items ?? [])
    else error.value = observationError(c.reason)
    if (nodeId.value) await selectNode()
  } finally { if (valid(token)) busy.value = false }
}
async function selectNode() {
  const token = epoch, request = ++impactEpoch; candidates.value = undefined; impactError.value = ''; emit('affected', [])
  if (!props.snapshot.nodes.some(n => n.id === nodeId.value)) return
  try {
    const result = await impact(props.scopeId, nodeId.value)
    if (!valid(token) || request !== impactEpoch) return
    candidates.value = result
    const known = new Set(props.snapshot.nodes.map(n => n.id))
    emit('affected', [...new Set((result.items ?? []).filter(i => i.state === 'candidate' && known.has(i.node_id)).map(i => i.node_id))])
  } catch (e) { if (valid(token) && request === impactEpoch) impactError.value = observationError(e) }
}
async function confirm() {
  if (!explicit.value || !props.snapshot.links.some(l => l.id === linkId.value) || !topic.value.trim()) return
  const token = epoch; confirming.value = true; error.value = ''; notice.value = ''
  try {
    const result = await confirmLineage(props.scopeId, { link_id: linkId.value, topic: topic.value.trim(), description: description.value.trim() })
    if (!valid(token)) return
    showConfirmations([result, ...lineage.value.filter(c => c.id !== result.id)])
    notice.value = '已记录业务人工确认；未升级链路 verified，也不代表复制健康'; explicit.value = false; topic.value = ''; description.value = ''
  } catch (e) { if (valid(token)) error.value = observationError(e) }
  finally { if (valid(token)) confirming.value = false }
}
watch(() => [props.scopeId, props.snapshot] as const, () => { nodeId.value = ''; linkId.value = props.snapshot.links[0]?.id ?? ''; explicit.value = false; topic.value = ''; description.value = ''; confirming.value = false; void load() }, { immediate: true })
watch(() => props.selectedNodeId, id => { nodeId.value = id ?? ''; void selectNode() })
onMounted(() => { now.value = Date.now(); expiryTimer = globalThis.setInterval(() => { now.value = Date.now() }, 1000) })
onUnmounted(() => { disposed = true; ++epoch; ++impactEpoch; globalThis.clearInterval(expiryTimer) })
</script>

<template>
  <section class="observability-panel" aria-label="查询与潜在影响">
    <h2>查询与潜在影响</h2>
    <span class="status-pill status-pill--warning">演示指标</span>
    <label>指标 Mock 场景 <select v-model="scenario" :disabled="busy" @change="load"><option>success</option><option>stale</option><option>unavailable</option><option>no_data</option></select></label><button :disabled="busy" @click="load">刷新查询</button>
    <p v-if="busy">正在读取查询…</p><p v-if="error" role="alert">{{ error }}</p><p v-if="notice" role="status">{{ notice }}</p>
    <p v-if="observation">{{ observation.synthetic ? 'Mock 合成查询，未验证真实指标' : '指标查询' }} · {{ observationStale ? '已过期 · 状态未知' : '各项指标状态见下表' }} · {{ observationStale ? 'unknown' : observation.state }}</p>
    <table><thead><tr><th>关系 / 指标</th><th>数值 / 单位</th><th>来源</th><th>采样 / 查询时间</th><th>数据状态</th></tr></thead><tbody><tr v-for="(m, i) in observation?.items" :key="i"><td>{{ m.link_id }} / {{ m.metric }}</td><td>{{ m.value == null ? '未知' : m.value }} {{ m.unit }}</td><td>{{ m.source || '未知' }} {{ m.synthetic ? 'Mock' : '' }}</td><td>{{ observedTime(m.sampled_at) }} / {{ observedTime(m.observed_at) }}</td><td>{{ stale(m) ? '已过期 · 状态未知' : m.state }} {{ m.value == null ? '暂无数据' : '' }}</td></tr></tbody></table>
    <p v-if="observation && !observation.items.length">没有可用指标，状态未知。</p>
    <h3>可能受影响的资源</h3>
    <label>起点节点 <select v-model="nodeId" data-impact-node @change="selectNode"><option value="">选择节点</option><option v-for="node in snapshot.nodes" :key="node.id" :value="node.id">{{ node.label }}</option></select></label>
    <details><summary>影响范围说明</summary><p>高亮为可能受影响的资源，不代表确定故障影响。</p></details>
    <p v-if="impactError" role="alert">{{ impactError }}</p>
    <ul><li v-for="(candidate, i) in candidates?.items" :key="i">{{ candidate.node_id }} · {{ candidate.link_id }} · 可能受影响的资源 {{ candidates?.synthetic ? 'Mock' : '' }}</li></ul>
    <h3>业务关系确认</h3>
    <form data-lineage-form @submit.prevent="confirm">
      <label>发现关系 <select v-model="linkId"><option v-for="link in snapshot.links" :key="link.id" :value="link.id">{{ link.source_alias }} → {{ link.target_alias }}</option></select></label>
      <label>业务 Topic <input v-model="topic" name="business_topic" maxlength="249" required></label><label>确认说明 <input v-model="description" name="business_description" maxlength="256"></label>
      <label><input v-model="explicit" data-explicit-confirm type="checkbox">我明确确认该业务血缘，理解其不等于复制健康</label>
      <button :disabled="confirming || !explicit || !linkId || !topic.trim()">记录人工业务确认</button>
    </form>
    <ul data-confirmations><li v-for="confirmation in lineage" :key="confirmation.id">业务人工确认 · {{ confirmation.active ? '当前有效' : '历史记录，当前关系未确认有效' }} · {{ confirmation.link_id }} · {{ confirmation.topic }} · {{ confirmation.confirmed_by }} · {{ confirmation.confirmed_at }} · {{ confirmation.description }}（链路 verified 保持原发现数据）</li></ul>
  </section>
</template>

<style scoped>
.observability-panel { margin-block: 24px; padding: 16px; border: 1px solid #d8dfeb; border-radius: 10px; } table { width: 100%; border-collapse: collapse; } th, td { text-align: left; border-bottom: 1px solid #ddd; padding: 8px; } label { display: inline-block; margin: 6px; } [role="alert"] { color: #b42318; }
</style>
