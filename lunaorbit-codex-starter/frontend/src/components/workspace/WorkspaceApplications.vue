<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { applications, saveApplication, removeApplication, workspaceACLs, workspaceError, type Application, type PlanInput, type ResourcePattern, type WorkspaceMeta } from '../../api/workspace'
import type { ACLRule } from '../../api/access'

const props = defineProps<{ clusterId: string; meta: WorkspaceMeta; refreshVersion: number; applicationId?:string }>()
const emit = defineEmits<{ intent: [input: PlanInput]; accounts:[application:Application] }>()
const rows = ref<Application[]>([]), total = ref(0), page = ref(1), query = ref(''), busy = ref(false), error = ref(''), notice = ref('')
const editorOpen = ref(false), editing = ref<Application>(), form = ref({ name: '', principal: '', owner: '', description: '' }), removing = ref<Application>()
const selected = ref<Application>(), acls = ref<ACLRule[]>([]), aclTotal = ref(0), aclPage = ref(1), aclBusy = ref(false), aclError = ref(''), selectedRules = ref<ACLRule[]>([])
const mode = ref<'produce' | 'consume'>('produce'), topicText = ref(''), topicPattern = ref<ResourcePattern['pattern_type']>('LITERAL'), groupName = ref(''), groupPattern = ref<ResourcePattern['pattern_type']>('LITERAL'), granting = ref(false)
let epoch = 0, aclEpoch = 0
const complex = computed(() => acls.value.some(rule => rule.permission === 'DENY' || rule.resource_name === '*' || rule.operation === 'ALL'))
const mayGrant = computed(() => props.meta.permissions.acl_manage && props.meta.write_enabled)
async function load() {
  const token = ++epoch; busy.value = true; error.value = ''; notice.value = ''
  try {
    let data = await applications(props.clusterId, page.value, query.value)
    if(props.applicationId){
      let next=1
      while(!data.items.some(app=>app.id===props.applicationId)&&data.items.length&&next*(data.page_size||20)<data.total){
        data=await applications(props.clusterId,++next);if(token!==epoch)return
      }
      if(token!==epoch)return
      const app=data.items.find(app=>app.id===props.applicationId)
      rows.value=app?[app]:[];total.value=rows.value.length
      if(app)void readACLs(app);else error.value='应用不存在或已移除，请返回应用列表'
    }else if(token===epoch){rows.value=data.items;total.value=data.total}
  }
  catch (e) { if (token === epoch) error.value = workspaceError(e) } finally { if (token === epoch) busy.value = false }
}
function edit(app?: Application) { editing.value = app; form.value = app ? { name: app.name, principal: app.principal, owner: app.owner, description: app.description } : { name: '', principal: '', owner: '', description: '' }; editorOpen.value = true; error.value = '' }
async function save() {
  const token = epoch; busy.value = true; error.value = ''
  try { await saveApplication(props.clusterId, form.value, editing.value); if (token === epoch) { editorOpen.value = false; await load() } }
  catch (e) { if (token === epoch) error.value = workspaceError(e) } finally { if (token === epoch) busy.value = false }
}
async function remove() {
  if (!removing.value) return
  const token = epoch; busy.value = true; error.value = ''
  try { await removeApplication(props.clusterId, removing.value); if (token === epoch) { removing.value = undefined; selected.value = undefined; await load() } }
  catch (e) { if (token === epoch) error.value = workspaceError(e) } finally { if (token === epoch) busy.value = false }
}
async function readACLs(app = selected.value, reset = true) {
  if (!app) return
  selected.value = app; if (reset) aclPage.value = 1
  const token = ++aclEpoch; aclBusy.value = true; aclError.value = ''; acls.value = []; selectedRules.value = []
  try { const data = await workspaceACLs(props.clusterId, app.principal, aclPage.value); if (token === aclEpoch) { acls.value = data.items; aclTotal.value = data.total } }
  catch (e) { if (token === aclEpoch) aclError.value = workspaceError(e) } finally { if (token === aclEpoch) aclBusy.value = false }
}
function select(app: Application, grant = false) { granting.value = grant; topicText.value = ''; groupName.value = ''; void readACLs(app) }
function grant() {
  if (!selected.value || !mayGrant.value) return
  const names = [...new Set(topicText.value.split(/[\n,]/).map(name => name.trim()).filter(Boolean))]
  if (!names.length || names.length > 20 || mode.value === 'consume' && !groupName.value.trim()) { aclError.value = '请选择 Topic；消费授权还必须指定消费组，单次最多20个 Topic'; return }
  emit('intent', { kind: 'grant', application_id: selected.value.id, mode: mode.value, topics: names.map(name => ({ name, pattern_type: topicPattern.value })), ...(mode.value === 'consume' ? { group: { name: groupName.value.trim(), pattern_type: groupPattern.value } } : {}) })
}
function revoke() { if (selected.value && mayGrant.value && selectedRules.value.length) emit('intent', { kind: 'revoke', application_id: selected.value.id, selected_rules: selectedRules.value }) }
watch(() => [props.clusterId,props.applicationId], () => { ++aclEpoch; rows.value = []; selected.value = undefined; editorOpen.value = false; removing.value = undefined; query.value = ''; page.value = 1; void load() }, { immediate: true,flush:'sync' })
watch(() => props.refreshVersion, () => { void load(); if (selected.value) void readACLs(selected.value) })
onBeforeUnmount(()=>{++epoch;++aclEpoch})
</script>
<template>
  <section class="applications-workspace">
    <div class="panel">
      <div class="panel-header"><div><h2>应用账号</h2><span class="helper-text">管理应用、负责人和关联账号</span></div><button v-if="!applicationId&&meta.permissions.applications_manage" data-new-application class="button-primary" :disabled="busy" @click="edit()">添加应用</button></div>
      <form v-if="!applicationId" class="filter-bar" @submit.prevent="page=1;load()"><label>应用名称<input v-model="query" placeholder="搜索应用或负责人" maxlength="128"></label><button :disabled="busy">查询</button></form>
      <p v-if="error" role="alert" class="error inline-message">{{ error }}</p><p v-if="notice" role="status">{{ notice }}</p>
      <div class="table-scroll"><table class="data-table"><thead><tr><th>应用 / 用途</th><th>负责人</th><th>Kafka 账号</th><th>操作</th></tr></thead><tbody><tr v-for="app in rows" :key="app.id"><td><router-link v-if="!applicationId" :to="`/kafka/clusters/${encodeURIComponent(clusterId)}/applications/${encodeURIComponent(app.id)}`">{{ app.name }}</router-link><strong v-else>{{ app.name }}</strong><small>{{ app.description || '未填写用途' }}</small></td><td>{{ app.owner }}</td><td>{{ app.principal }}</td><td class="row-actions"><button v-if="meta.permissions.account_manage" data-application-accounts @click="emit('accounts',app)">查看账号</button><button @click="select(app)">查看权限</button><button v-if="mayGrant" @click="select(app,true)">授权</button><button v-if="meta.permissions.applications_manage" @click="edit(app)">编辑</button><button v-if="meta.permissions.applications_manage" @click="removing=app">移除应用</button></td></tr><tr v-if="!rows.length"><td colspan="4" class="empty-state">{{ busy ? '正在读取应用…' : '尚未设置应用。先添加应用，再为其授权。' }}</td></tr></tbody></table></div>
      <div v-if="!applicationId" class="pagination"><button :disabled="busy || page<=1" @click="page--;load()">上一页</button><span>第 {{ page }} 页 · 共 {{ total }} 个应用</span><button :disabled="busy || page*20>=total" @click="page++;load()">下一页</button></div>
    </div>
    <section v-if="removing" class="panel confirmation"><p>删除“{{ removing.name }}”的应用资料？已有授权记录的应用需要先处理权限，不能直接删除。</p><button @click="removing=undefined">取消</button><button :disabled="busy" @click="remove">确认移除应用</button></section>
    <section v-if="selected" class="panel application-access">
      <div class="panel-header"><div><h2>{{ selected.name }} · Kafka 权限</h2><span class="helper-text">{{ selected.principal }}</span></div><button :disabled="aclBusy" @click="readACLs()">刷新权限</button></div>
      <p v-if="aclError" role="alert" class="error inline-message">{{ aclError }}</p>
      <form v-if="granting && mayGrant" data-intent-grant class="grant-form" @submit.prevent="grant">
        <h3>这个应用需要做什么？</h3>
        <div class="intent-options"><label><input v-model="mode" type="radio" value="produce">生产消息</label><label><input v-model="mode" type="radio" value="consume">消费消息</label></div>
        <div class="intent-fields"><label>Topic 名称或前缀<textarea v-model="topicText" name="grant_topics" required rows="2" placeholder="例如 orders；多个名称用换行或逗号分隔" maxlength="5000" /></label><label>Topic 匹配方式<select v-model="topicPattern"><option value="LITERAL">精确名称</option><option value="PREFIXED">前缀范围</option></select></label><label v-if="mode==='consume'">消费组<input v-model="groupName" name="grant_group" required maxlength="249" placeholder="例如 reporting"></label><label v-if="mode==='consume'">消费组匹配方式<select v-model="groupPattern"><option value="LITERAL">精确名称</option><option value="PREFIXED">前缀范围</option></select></label></div>
        <p class="helper-text">{{ mode==='produce' ? '生成 Topic 写入与描述权限；不会自动授予创建 Topic 或集群级权限。' : '生成 Topic 读取、描述及指定消费组权限。' }}</p><button data-preview-grant class="button-primary">预览授权差异</button>
      </form>
      <div class="permission-summary"><h3>当前 Broker ACL</h3><p class="helper-text">这是当前查询到的规则，不等同于对所有认证、通配符及 DENY 组合的完整有效权限判定。</p><p v-if="complex" class="complex-note">包含 DENY、ALL 或通配符规则，请结合高级规则检查最终访问范围。</p></div>
      <div class="table-scroll"><table class="data-table"><thead><tr><th v-if="mayGrant">选择</th><th>资源</th><th>范围</th><th>操作</th><th>效果</th></tr></thead><tbody><tr v-for="(rule,index) in acls" :key="index"><td v-if="mayGrant"><input v-model="selectedRules" :value="rule" type="checkbox" :aria-label="`选择 ${rule.resource_name} ${rule.operation} ${rule.permission}`"></td><td>{{ rule.resource_type==='GROUP' ? '消费组' : rule.resource_type==='TOPIC' ? 'Topic' : rule.resource_type }} · {{ rule.resource_name }}</td><td>{{ rule.pattern_type==='PREFIXED' ? '前缀' : '精确' }}</td><td>{{ rule.operation }}</td><td>{{ rule.permission==='DENY' ? '拒绝' : '允许' }}</td></tr><tr v-if="!acls.length"><td :colspan="mayGrant?5:4" class="empty-state">{{ aclBusy ? '正在查询实际 ACL…' : aclError ? '当前无法确认 Kafka ACL' : '该账号暂无权限规则' }}</td></tr></tbody></table></div>
      <div class="pagination"><button v-if="mayGrant" :disabled="!selectedRules.length || aclBusy" @click="revoke">预览撤销选中权限</button><button :disabled="aclBusy||aclPage<=1" @click="aclPage--;readACLs(selected,false)">上一页权限</button><span>共 {{ aclTotal }} 条规则</span><button :disabled="aclBusy||aclPage*100>=aclTotal" @click="aclPage++;readACLs(selected,false)">下一页权限</button></div>
      <details class="advanced-rules"><summary>高级规则详情</summary><div v-for="(rule,index) in acls" :key="index" class="raw-rule">{{ rule.principal }} · {{ rule.host }} · {{ rule.resource_type }} / {{ rule.resource_name }} · {{ rule.pattern_type }} · {{ rule.operation }} / {{ rule.permission }}</div></details>
    </section>
    <el-dialog v-model="editorOpen" :title="editing ? '编辑应用' : '添加应用'" width="min(580px, 94vw)" :close-on-click-modal="!busy">
      <p class="helper-text">关联现有 Kafka 账号不会创建用户或修改密码。格式：User:用户名。</p>
      <form data-application-form class="application-form" @submit.prevent="save"><label>应用名称<input v-model="form.name" name="app_name" required maxlength="128"></label><label>负责人 / 团队<input v-model="form.owner" name="app_owner" required maxlength="128"></label><label>Kafka 账号<input v-model="form.principal" name="app_principal" required maxlength="256" placeholder="User:orders-service" :disabled="!!editing"></label><label>用途说明<textarea v-model="form.description" name="app_description" rows="3" maxlength="2048" /></label><p v-if="error" role="alert" class="error">{{ error }}</p><button data-save-application class="button-primary" :disabled="busy || !meta.permissions.applications_manage">保存应用</button></form>
    </el-dialog>
  </section>
</template>
<style scoped>
.applications-workspace { display:grid; gap:20px; }.panel-header { gap:14px; }h2 { margin:0 0 5px; }.data-table small { display:block; font-size:12px; color:#64748b; margin-top:5px; }.row-actions { white-space:nowrap; }.row-actions button { margin-right:5px; }.inline-message,.confirmation { padding:16px 20px; }.pagination { display:flex; align-items:center; flex-wrap:wrap; gap:10px; padding:14px 20px; border-top:1px solid #e8edf3; color:#64748b; font-size:12px; }.grant-form { padding:18px 20px; background:#f8fafc; border-top:1px solid #e8edf3; border-bottom:1px solid #e8edf3; }.grant-form h3 { margin:0 0 14px; }.intent-options { display:flex; gap:24px; margin-bottom:18px; }.intent-options label { display:flex; gap:7px; align-items:center; }.intent-fields { display:grid; grid-template-columns:2fr 1fr; gap:14px; }.intent-fields label,.application-form label { display:flex; flex-direction:column; gap:7px; font-size:12px; color:#526176; }.application-form { display:grid; gap:16px; }.permission-summary { padding:14px 20px; }.permission-summary h3 { margin:0; }.complex-note { color:#98642e; font-size:12px; }.advanced-rules { padding:16px 20px; font-size:12px; color:#64748b; }.advanced-rules summary { cursor:pointer; }.raw-rule { padding:10px 0; overflow-wrap:anywhere; }.empty-state { text-align:center; padding:28px; color:#64748b; }@media(max-width:640px){.intent-fields{grid-template-columns:1fr;}.panel-header{flex-wrap:wrap;}}
</style>
