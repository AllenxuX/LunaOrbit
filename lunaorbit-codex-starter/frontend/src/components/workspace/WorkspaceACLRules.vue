<script setup lang="ts">
import { computed, ref, watch, onBeforeUnmount } from 'vue'
import { aclCapabilities, allACLRules, customACLIntent, type ACLCapabilities } from '../../api/acl-rules'
import { workspaceError, type PlanInput, type WorkspaceMeta } from '../../api/workspace'
import type { ACLRule } from '../../api/access'

const props = defineProps<{ clusterId: string; meta: WorkspaceMeta; refreshVersion: number }>()
const emit = defineEmits<{ intent: [input: PlanInput] }>()
const rows = ref<ACLRule[]>([]), selected = ref<ACLRule[]>([]), total = ref(0), page = ref(1), principal = ref(''), busy = ref(false), error = ref('')
const capabilities = ref<ACLCapabilities>({ resources: {}, pattern_types: [], permissions: [] })
const form = ref<ACLRule>({ principal: '', host: '*', resource_type: 'TOPIC', resource_name: '', pattern_type: 'LITERAL', operation: 'READ', permission: 'ALLOW' })
const mayWrite = computed(() => props.meta.permissions.acl_manage && props.meta.write_enabled)
const operations = computed(() => capabilities.value.resources[form.value.resource_type] ?? [])
let epoch = 0
async function load() {
  const token = ++epoch; busy.value = true; error.value = ''; rows.value = []; selected.value = []
  try {
    const [rules, caps] = await Promise.all([allACLRules(props.clusterId, principal.value, page.value), aclCapabilities(props.clusterId)])
    if (token === epoch) { rows.value = rules.items; total.value = rules.total; capabilities.value = caps }
  } catch (e) { if (token === epoch) error.value = workspaceError(e) } finally { if (token === epoch) busy.value = false }
}
function add() {
  if (!mayWrite.value || !operations.value.includes(form.value.operation)) return
  emit('intent', customACLIntent(form.value.principal, [{ ...form.value }], []))
}
function remove() {
  if (!mayWrite.value || !selected.value.length) return
  const identity = selected.value[0]!.principal
  if (selected.value.some(r => r.principal !== identity)) { error.value = '每次请选择同一个账号的权限规则'; return }
  emit('intent', customACLIntent(identity, [], selected.value.map(r => ({ ...r }))))
}
watch(() => form.value.resource_type, type => {
  form.value.operation = capabilities.value.resources[type]?.[0] ?? ''
  if (type === 'CLUSTER') { form.value.resource_name = 'kafka-cluster'; form.value.pattern_type = 'LITERAL' }
})
watch(() => props.clusterId, () => { page.value = 1; principal.value = ''; form.value.principal = ''; form.value.resource_name = ''; void load() }, { immediate: true })
watch(() => props.refreshVersion, () => { void load() })
onBeforeUnmount(() => { ++epoch })
</script>
<template>
  <section class="panel acl-rules-workspace">
    <div class="panel-header"><div><h2>全部权限规则</h2><p class="helper-text">查看账号的 Kafka 权限，包括未关联应用的账号。</p></div><button :disabled="busy" @click="load">刷新规则</button></div>
    <form class="filter-bar" @submit.prevent="page=1;load()"><label>账号<input v-model="principal" name="acl_filter" maxlength="256" placeholder="留空查询全部账号"></label><button :disabled="busy">查询</button></form>
    <p v-if="error" role="alert" class="error inline-message">{{ error }}</p>
    <div class="table-scroll">
      <table class="data-table">
        <thead><tr><th v-if="mayWrite">选择</th><th>账号 / 主机</th><th>资源</th><th>匹配</th><th>操作</th><th>效果</th></tr></thead><tbody>
          <tr v-for="(rule,index) in rows" :key="index"><td v-if="mayWrite"><input v-model="selected" type="checkbox" :value="rule" :aria-label="`选择 ${rule.principal} ${rule.resource_name} ${rule.operation} ${rule.permission}`"></td><td>{{ rule.principal }}<small>{{ rule.host }}</small></td><td>{{ rule.resource_type }} · {{ rule.resource_name }}</td><td>{{ rule.pattern_type }}</td><td>{{ rule.operation }}</td><td>{{ rule.permission }}</td></tr>
          <tr v-if="!rows.length"><td :colspan="mayWrite?6:5" class="empty-state">{{ busy ? '正在读取权限规则…' : error ? '规则查询未完成' : '当前范围没有规则' }}</td></tr>
        </tbody>
      </table>
    </div>
    <div class="pagination"><button :disabled="busy || page<=1" @click="page--;load()">上一页</button><span>第 {{ page }} 页 · 共 {{ total }} 条</span><button :disabled="busy || page*20>=total" @click="page++;load()">下一页</button><button v-if="mayWrite" data-revoke-native :disabled="busy || !selected.length" @click="remove">预览删除所选规则</button></div>
    <form v-if="mayWrite" class="native-acl-form" data-add-native @submit.prevent="add">
      <h3>添加权限规则</h3><p class="helper-text">通配符 * 覆盖所有资源；DENY 可能覆盖已有允许规则。提交前将显示差异并进入审批。</p>
      <div class="intent-fields">
        <label>账号<input v-model="form.principal" name="acl_principal" required maxlength="256" placeholder="User:业务账号"></label><label>主机<input v-model="form.host" name="acl_host" required maxlength="256"></label>
        <label>资源类型<select v-model="form.resource_type" name="acl_type"><option v-for="(_,type) in capabilities.resources" :key="type" :value="type">{{ type }}</option></select></label>
        <label>资源名称<input v-model="form.resource_name" name="acl_resource" required maxlength="249" :readonly="form.resource_type==='CLUSTER'"></label>
        <label>匹配方式<select v-model="form.pattern_type" name="acl_pattern" :disabled="form.resource_type==='CLUSTER'"><option v-for="pattern in capabilities.pattern_types" :key="pattern">{{ pattern }}</option></select></label>
        <label>操作<select v-model="form.operation" name="acl_operation"><option v-for="op in operations" :key="op">{{ op }}</option></select></label>
        <label>效果<select v-model="form.permission" name="acl_permission"><option v-for="permission in capabilities.permissions" :key="permission">{{ permission }}</option></select></label>
      </div><button class="button-primary" :disabled="busy || !operations.length">预览添加规则</button>
    </form>
  </section>
</template>
<style scoped>
.native-acl-form { border-top: 1px solid var(--border-color, #dbe2ea); padding-top: 1rem; margin-top: 1rem; }
small { display: block; color: var(--text-secondary, #64748b); }
</style>
