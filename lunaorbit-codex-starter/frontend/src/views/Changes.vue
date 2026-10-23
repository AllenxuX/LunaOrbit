<script setup lang="ts">
import { useRuntime } from '../composables/useRuntime'
import { onMounted, onUnmounted, ref, watch } from 'vue'
import { ensureSession, registryRequest } from '../api/registry'
import type { AccessSession } from '../api/access'
import { approveChange, changeDetails, changeReason, changeStatus, changeTargets, changesError, createChange, executeChange, listChanges, type Change, type ChangeTarget } from '../api/changes'

const { runtime } = useRuntime()
const session = ref<AccessSession>(), targets = ref<ChangeTarget[]>([]), items = ref<Change[]>([]), total = ref(0), page = ref(1)
const targetId = ref(''), topicRule = ref(''), preview = ref<Change>(), detail = ref<Change>(), busy = ref(false), error = ref(''), scenario = ref('success')
let epoch = 0, detailEpoch = 0, disposed = false
const valid = (token: number) => !disposed && token === epoch
const target = () => targets.value.find(t => t.id === targetId.value)
const mayApprove = (change: Change) => change.can_approve === true && change.status === 'pending_approval' && (change.applicant !== session.value?.user_id || runtime.value?.mode === 'admin-lab')
const mayExecute = (change: Change) => (change.synthetic === true || runtime.value?.mode === 'admin-lab' && runtime.value.write_enabled) && change.can_execute === true && change.status === 'approved_not_executed'
const unknown = (value: number | boolean | null | undefined) => value == null ? '未知' : typeof value === 'boolean' ? value ? '是' : '否' : value
async function load(initial = false) {
  if (runtime.value?.capabilities.changes !== true) return
  const token = ++epoch; ++detailEpoch; busy.value = true; error.value = ''; detail.value = undefined; preview.value = undefined; items.value = []
  try {
    if (initial) { await ensureSession(); if (!valid(token)) return; session.value = await registryRequest<AccessSession>('/session'); if (!valid(token)) return }
    const [allowed, changes] = await Promise.all([changeTargets(), listChanges(page.value)])
    if (!valid(token)) return
    targets.value = allowed.items; items.value = changes.items; total.value = changes.total
    if (!target()) targetId.value = allowed.items[0]?.id ?? ''
  } catch (e) { if (valid(token)) { error.value = changesError(e); targets.value = [] } }
  finally { if (valid(token)) busy.value = false }
}
async function submit(previewOnly: boolean) {
  const selected = target()
  if (!selected || (!previewOnly && !preview.value)) return
  ++detailEpoch
  const token = epoch, input = { target_id: selected.id, topic_rule: topicRule.value, expected_resource_version: selected.resource_version }
  busy.value = true; error.value = ''
  try {
    const result = await createChange(input, previewOnly); if (!valid(token)) return
    if (previewOnly) { if (targetId.value === input.target_id && topicRule.value === input.topic_rule) preview.value = result }
    else { items.value = [result, ...items.value.filter(c => c.id !== result.id)]; detail.value = result; preview.value = undefined; total.value++ }
  } catch (e) { if (valid(token)) error.value = changesError(e) }
  finally { if (valid(token)) busy.value = false }
}
async function inspect(change: Change) {
  const token = epoch, request = ++detailEpoch; error.value = ''; detail.value = undefined
  try { const result = await changeDetails(change.id); if (valid(token) && request === detailEpoch) detail.value = result }
  catch (e) { if (valid(token) && request === detailEpoch) error.value = changesError(e) }
}
async function transition(change: Change, execute: boolean) {
  if (execute ? !mayExecute(change) : !mayApprove(change)) return
  ++detailEpoch
  const token = epoch; busy.value = true; error.value = ''
  try { const result = execute ? await executeChange(change, scenario.value) : await approveChange(change); if (valid(token)) { items.value = items.value.map(c => c.id === result.id ? result : c); detail.value = result } }
  catch (e) { if (valid(token)) error.value = changesError(e) }
  finally { if (valid(token)) busy.value = false }
}
watch([targetId, topicRule], () => { preview.value = undefined })
onMounted(() => { void load(true) })
onUnmounted(() => { disposed = true; ++epoch; ++detailEpoch })
</script>

<template>
  <section class="changes-view registry">
    <header class="page-header"><div><h1>MM2 变更</h1></div><button :disabled="busy" @click="load()">刷新</button></header>
    <p v-if="runtime?.mode === 'mock'">生产写入已禁用。此页面仅演示合成 Mock 目标、审批与模拟执行，未连接真实生产资源。</p>
    <p v-else-if="runtime?.mode === 'admin-lab'" role="status">实验环境可提交 MM2 变更。</p><p v-else role="status">当前运行模式未启用变更模拟。生产写入已禁用。</p>
    <p v-if="busy">处理中…</p><p v-if="error" role="alert">{{ error }}</p>
    <section v-if="runtime?.capabilities.changes === true" class="panel">
      <div class="panel-header"><h2>白名单目标与单个 Topic 规则</h2><span class="status-pill status-pill--muted">生产写入禁用</span></div>
      <form data-change-form @submit.prevent="submit(true)">
        <label>目标 <select v-model="targetId" name="target_id" :disabled="busy"><option v-for="t in targets" :key="t.id" :value="t.id">{{ t.name }} · {{ t.synthetic ? 'Mock' : '目标' }}</option></select></label>
        <label>Topic 规则 <input v-model="topicRule" name="topic_rule" required maxlength="256" :disabled="busy"></label>
        <p v-if="target()">目标 UID：{{ target()!.workload_uid }} · resourceVersion：{{ target()!.resource_version }}</p>
        <button class="button-primary" :disabled="busy || !target() || !topicRule">脱敏预览</button><button type="button" data-submit :disabled="busy || !preview" @click="submit(false)">提交待审批变更</button>
      </form>
      <section v-if="preview" data-preview class="nested-detail"><h3>脱敏预览</h3><p>{{ preview.target_id }} · Topic 规则 {{ preview.topic_rule }} · resourceVersion {{ preview.expected_resource_version }} · {{ changeStatus(preview.status) }}</p></section>
    </section><section v-if="runtime?.capabilities.changes !== false" class="panel">
      <div class="panel-header"><h2>变更与独立审批</h2><span class="helper-text">共 {{ total }} 项变更</span></div>
      <details v-if="runtime?.mode === 'mock'" class="demo-settings"><summary>演示设置</summary><label>合成执行场景 <select v-model="scenario" :disabled="busy"><option>success</option><option>conflict</option><option>rollout_failed</option><option>metrics_missing</option></select></label></details>
      <div class="table-scroll"><table class="data-table"><thead><tr><th>目标 / Topic 规则</th><th>状态</th><th>申请 / 审批人</th><th>操作</th></tr></thead><tbody><tr v-for="change in items" :key="change.id"><td>{{ change.target_id }} / {{ change.topic_rule }}</td><td>{{ change.synthetic ? 'Mock 合成' : '实验实际资源' }} · {{ changeStatus(change.status) }}</td><td>{{ change.applicant }} / {{ change.approver || '未审批' }}</td><td><button @click="inspect(change)">详情</button><button data-approve :disabled="busy || !mayApprove(change)" @click="transition(change, false)">{{ runtime?.mode === 'admin-lab' ? '管理员批准' : '独立审批' }}</button><button data-execute :disabled="busy || !mayExecute(change)" @click="transition(change, true)">{{ change.synthetic ? '模拟执行' : '执行实验变更' }}</button></td></tr></tbody></table></div>
      <div class="pagination"><button :disabled="busy || page <= 1" @click="page--; load()">上一页</button><span>{{ page }} / 共 {{ total }} 项</span><button :disabled="busy || page * 20 >= total" @click="page++; load()">下一页</button></div>
    </section>
    <section v-if="detail" data-detail class="panel">
      <h2>变更详情 · {{ changeStatus(detail.status) }}</h2>
      <p>{{ detail.synthetic ? '模拟执行结果' : '实验执行结果 · 同步状态需单独验证' }}</p>
      <p>目标 {{ detail.target_id }} · UID {{ detail.workload_uid || targets.find(t => t.id === detail!.target_id)?.workload_uid || '未知' }} · expected resourceVersion {{ detail.expected_resource_version }} · 当前 resourceVersion {{ detail.current_resource_version || '未知' }} · 修订 {{ detail.revision }}</p>
      <p>Topic 规则：{{ detail.topic_rule }}</p><p>{{ changeReason(detail.reason) }}</p>
      <h3>检查结果</h3><p>generation：{{ unknown(detail.evidence?.generation) }} · observedGeneration：{{ unknown(detail.evidence?.observed_generation) }} · ready：{{ unknown(detail.evidence?.ready) }} · 复制健康：{{ unknown(detail.evidence?.replication_healthy) }}</p>
    </section>
  </section>
</template>

<style scoped>
.page-actions, .pagination { display:flex; align-items:center; flex-wrap:wrap; gap:8px; }
.pagination { padding:14px 20px; border-top:1px solid #e8edf3; color:#64748b; font-size:12px; }
.table-scroll { overflow-x:auto; }
.panel > h2, .panel > h3 { padding:18px 20px 0; margin:0; }
.panel > p, .panel > ul, .panel > form, .panel > details { margin:16px 20px; }
.panel > form { display:flex; flex-wrap:wrap; align-items:end; gap:14px; }
.panel > form label { display:flex; flex-direction:column; gap:6px; font-size:12px; color:#526176; }
.nested-detail { padding:16px 20px; background:#f8fafc; border-top:1px solid #e3e8ef; }
.nested-detail h3 { font-size:14px; margin:0 0 12px; }
@media(max-width:640px) { .panel > form label { width:100%; } }
</style>
