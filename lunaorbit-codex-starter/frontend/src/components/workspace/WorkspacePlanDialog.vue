<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { previewPlan, submitPlan, approveRequest, executeRequest, requestKind, requestState, workspaceError, type PlanInput, type Plan, type WorkspaceRequest, type WorkspaceMeta } from '../../api/workspace'
import PlanDiff from './PlanDiff.vue'

const props = defineProps<{ clusterId: string; input: PlanInput | null; meta: WorkspaceMeta; actorId: string }>()
const emit = defineEmits<{ close: []; completed: [request: WorkspaceRequest] }>()
const draft = ref<PlanInput>({ kind: 'topic_create', topic: '', partitions: 1, replication_factor: 1, retention_ms: 86400000 })
const plan = ref<Plan>(), request = ref<WorkspaceRequest>(), busy = ref(false), error = ref(''), confirmed = ref(false)
let epoch = 0
watch(() => [props.input, props.clusterId] as const, () => { ++epoch; draft.value = { partitions: 1, replication_factor: 1, retention_ms: 86400000, ...props.input, kind: props.input?.kind ?? 'topic_create' }; plan.value = undefined; request.value = undefined; error.value = ''; confirmed.value = false; busy.value = false }, { immediate: true })
watch(draft, () => { plan.value = undefined; confirmed.value = false }, { deep: true })
const isTopic = computed(() => draft.value.kind.startsWith('topic_'))
const mayApprove = computed(() => request.value?.status === 'pending_approval' && request.value.created_by !== props.actorId)
const terminal = computed(() => request.value && !['pending_approval', 'approved', 'executing'].includes(request.value.status))
function setOpen(value: boolean) { if (!value && !busy.value) emit('close') }
async function preview() {
  const token = ++epoch, submitted = JSON.stringify(draft.value); busy.value = true; error.value = ''
  try { const result = await previewPlan(props.clusterId, draft.value); if (epoch === token && submitted === JSON.stringify(draft.value)) plan.value = result }
  catch (e) { if (token === epoch) error.value = workspaceError(e) } finally { if (token === epoch) busy.value = false }
}
async function transition(action: 'submit' | 'approve' | 'execute') {
  if (!confirmed.value || !plan.value || busy.value) return
  const token = ++epoch; busy.value = true; error.value = ''
  try {
    const result = action === 'submit' ? await submitPlan(props.clusterId, plan.value) : action === 'approve' && request.value ? await approveRequest(props.clusterId, request.value) : request.value ? await executeRequest(props.clusterId, request.value) : undefined
    if (token !== epoch || !result) return
    request.value = result
    if (!['pending_approval', 'approved', 'executing'].includes(result.status)) emit('completed', result)
  } catch (e) { if (token === epoch) error.value = workspaceError(e) } finally { if (token === epoch) busy.value = false }
}
</script>
<template>
  <el-dialog :model-value="input !== null" :title="requestKind(draft.kind)" width="min(760px, 94vw)" :close-on-click-modal="!busy" :show-close="!busy" @update:model-value="setOpen">
    <p class="helper-text">{{ meta.synthetic ? '当前执行合成测试流程。' : '操作将作用于当前 Kafka 集群。' }}提交前查看差异，执行后检查结果。</p>
    <form v-if="isTopic" class="operation-form" @submit.prevent="preview">
      <label>Topic 名称<input v-model="draft.topic" name="topic_name" required maxlength="249" :disabled="busy || !!request || draft.kind !== 'topic_create'"></label>
      <label v-if="draft.kind !== 'topic_retention'">{{ draft.kind === 'topic_expand' ? '扩容后的总分区数' : '分区数' }}<input v-model.number="draft.partitions" name="partitions" type="number" min="1" max="100000" required :disabled="busy || !!request"></label>
      <label v-if="draft.kind === 'topic_create'">副本数<input v-model.number="draft.replication_factor" name="replication_factor" type="number" min="1" max="32767" required :disabled="busy || !!request"></label>
      <label v-if="draft.kind !== 'topic_expand'">保留时间（毫秒）<input v-model.number="draft.retention_ms" name="retention_ms" type="number" min="1" max="31536000000" required :disabled="busy || !!request"></label>
      <p v-if="draft.kind === 'topic_expand'" class="scope-note">增加分区不可缩回，可能改变消息键到分区的分配；请先确认应用兼容。</p>
      <button v-if="!request" class="button-primary" :disabled="busy">预览变更</button>
    </form>
    <button v-else-if="!plan && !request" class="button-primary" :disabled="busy" @click="preview">生成授权差异</button>
    <PlanDiff v-if="plan" :plan="plan" />
    <p v-if="error" role="alert" class="error">{{ error }}</p>
    <label v-if="plan && !terminal" class="confirm-check"><input v-model="confirmed" type="checkbox" :disabled="busy">我已确认上述资源范围和变更内容</label>
    <section v-if="request" data-operation-result class="operation-result"><strong>{{ requestState(request.status) }}</strong><p v-if="request.status === 'pending_approval'">等待审批人批准，可在操作记录中继续处理。</p><ul><li v-for="(result,index) in request.results" :key="index">{{ result.rule?.resource_name ?? draft.topic }} · {{ result.state }} {{ result.code || '' }}</li></ul><p v-if="request.status === 'executed'">已重新查询 Kafka 并核对变更结果。</p><p v-else-if="terminal">请逐项检查结果；部分成功或结果未知时不会自动回退。</p></section>
    <template #footer><button :disabled="busy" @click="emit('close')">{{ terminal ? '关闭' : '取消' }}</button><button v-if="plan && !request" data-workspace-submit class="button-primary" :disabled="busy || !confirmed" @click="transition('submit')">{{ meta.approval_required ? '提交审批' : '提交变更' }}</button><button v-if="mayApprove" :disabled="busy || !confirmed" @click="transition('approve')">批准变更</button><button v-if="request?.status === 'approved'" data-workspace-execute class="button-primary" :disabled="busy || !confirmed || !meta.write_enabled" @click="transition('execute')">执行</button></template>
  </el-dialog>
</template>
<style scoped>
.operation-form { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:16px; }.operation-form label { display:flex; flex-direction:column; gap:7px; font-size:12px; }.scope-note { grid-column:1/-1; color:#956328; font-size:12px; margin:0; }.confirm-check { display:flex; align-items:center; gap:8px; margin:16px 0; }.operation-result { border:1px solid #dbe5f1; padding:16px; border-radius:6px; margin-top:16px; }.operation-result p { font-size:12px; }.operation-result ul { padding-left:18px; font-size:12px; }.operation-result li { margin:8px 0; }@media(max-width:600px){.operation-form{grid-template-columns:1fr;}}
</style>
