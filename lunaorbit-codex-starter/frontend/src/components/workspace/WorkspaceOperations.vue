<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from 'vue'
import { workspaceRequests, workspaceAudit, approveRequest, executeRequest, workspaceError, requestKind, requestState, type WorkspaceRequest, type WorkspaceAudit, type WorkspaceMeta } from '../../api/workspace'
import PlanDiff from './PlanDiff.vue'
const props = defineProps<{ clusterId: string; meta: WorkspaceMeta; actorId: string; refreshVersion: number }>()
const emit = defineEmits<{ completed: [] }>()
const rows = ref<WorkspaceRequest[]>([]), audit = ref<WorkspaceAudit[]>([]), page = ref(1), total = ref(0), busy = ref(false), error = ref(''), selected = ref<WorkspaceRequest>(), confirmExecute = ref(false)
let epoch = 0
function mayManage(row: WorkspaceRequest) { return row.plan.input.kind.startsWith('topic_') ? props.meta.permissions.topic_manage : props.meta.permissions.acl_manage }
async function load() {
  const token = ++epoch; busy.value = true; error.value = ''
  try { const data = await workspaceRequests(props.clusterId, page.value); if (token !== epoch) return; rows.value = data.items; total.value = data.total; if (props.meta.permissions.audit_read) { const events = await workspaceAudit(props.clusterId, page.value); if (token === epoch) audit.value = events.items } }
  catch (e) { if (token === epoch) error.value = workspaceError(e) } finally { if (token === epoch) busy.value = false }
}
function inspect(row: WorkspaceRequest) { selected.value = row; confirmExecute.value = false }
async function transition(execute: boolean) {
  const row = selected.value
  if (!row || !mayManage(row) || busy.value || execute && !confirmExecute.value) return
  const token = epoch; busy.value = true; error.value = ''
  try { const result = execute ? await executeRequest(props.clusterId, row) : await approveRequest(props.clusterId, row); if (token === epoch) { selected.value = result; rows.value = rows.value.map(item => item.id === result.id ? result : item); confirmExecute.value = false; emit('completed') } }
  catch (e) { if (token === epoch) error.value = workspaceError(e) } finally { if (token === epoch) busy.value = false }
}
watch(() => props.clusterId, () => { selected.value = undefined; audit.value = []; page.value = 1; void load() }, { immediate: true })
watch(() => props.refreshVersion, () => { void load() })
onBeforeUnmount(() => { ++epoch })
const time = (value: string) => new Date(value).toLocaleString('zh-CN', { hour12: false })
const auditAction = (value: string) => ({ 'application.save': '保存应用', 'application.delete': '删除应用资料', 'request.submit': '提交变更', 'request.approve': '批准变更', 'request.execute.start': '开始执行', 'request.execute.finish': '保存验证结果' })[value] ?? value
</script>
<template>
  <section class="workspace-operations">
    <div class="panel"><div class="panel-header"><h2>操作记录</h2><button :disabled="busy" @click="load">刷新</button></div><p v-if="error" role="alert" class="error inline-message">{{ error }}</p><div class="table-scroll"><table class="data-table"><thead><tr><th>操作</th><th>目标</th><th>状态</th><th>提交时间</th><th>操作</th></tr></thead><tbody><tr v-for="row in rows" :key="row.id"><td>{{ requestKind(row.plan.input.kind) }}</td><td>{{ row.plan.input.topic || row.plan.input.topics?.map(topic=>topic.name).join(', ') || '应用权限' }}</td><td>{{ requestState(row.status) }}</td><td>{{ time(row.created_at) }}</td><td><button @click="inspect(row)">查看差异与结果</button></td></tr><tr v-if="!rows.length"><td colspan="5" class="empty-state">{{ busy ? '正在读取…' : '暂无操作记录' }}</td></tr></tbody></table></div><div class="pagination"><button :disabled="busy||page<=1" @click="page--;load()">上一页</button><span>第 {{ page }} 页 · 共 {{ total }} 项</span><button :disabled="busy||page*20>=total" @click="page++;load()">下一页</button></div></div>
    <section v-if="selected" class="panel operation-detail"><h2>{{ requestKind(selected.plan.input.kind) }} · {{ requestState(selected.status) }}</h2><PlanDiff :plan="selected.plan" /><p>提交人：{{ selected.created_by }} · {{ selected.approved_by ? '已记录审批人' : '尚未批准' }}</p><ul><li v-for="(result,index) in selected.results" :key="index">{{ result.rule?.resource_name || selected.plan.input.topic }} · {{ result.state }} {{ result.code || '' }}</li></ul><div class="operation-actions"><button v-if="selected.status==='pending_approval' && mayManage(selected)" :disabled="busy || selected.created_by===actorId" @click="transition(false)">审批通过</button><template v-if="selected.status==='approved' && mayManage(selected)"><label><input v-model="confirmExecute" type="checkbox">我已核对变更范围，确认执行</label><button class="button-primary" :disabled="busy || !confirmExecute || !meta.write_enabled" @click="transition(true)">执行</button></template></div></section>
    <details v-if="meta.permissions.audit_read" class="panel audit-panel"><summary>审计事件</summary><table class="data-table"><thead><tr><th>时间</th><th>操作者</th><th>动作</th></tr></thead><tbody><tr v-for="(entry,index) in audit" :key="index"><td>{{ time(entry.at) }}</td><td>{{ entry.actor }}</td><td>{{ auditAction(entry.action) }}</td></tr></tbody></table></details>
  </section>
</template>
<style scoped>
.workspace-operations { display:grid; gap:20px; }h2 { margin:0; }.inline-message { padding:12px 20px; }.pagination { display:flex; flex-wrap:wrap; align-items:center; gap:10px; padding:14px 20px; color:#64748b; font-size:12px; border-top:1px solid #e8edf3; }.operation-detail { padding:20px; }.operation-actions { display:flex; flex-wrap:wrap; align-items:center; gap:12px; }.operation-detail p,.operation-detail li { font-size:12px; color:#64748b; }.audit-panel { padding:16px 20px; }.audit-panel summary { cursor:pointer; }.empty-state { padding:28px; text-align:center; color:#64748b; }
</style>
