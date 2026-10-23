<script setup lang="ts">
import { useRuntime } from '../composables/useRuntime'
import { computed, onUnmounted, ref, watch } from 'vue'
import { ensureSession, registryRequest, type Cluster } from '../api/registry'
import { kafkaPath, scenarios, reasonLabel, type Page } from '../api/kafkaops'
import { accessError, approvalInbox, approveACL, executeACL, authSession, listACLRequests, listACLs, login, logout, requestACL, type ACLRequest, type ACLRule, type AccessSession } from '../api/access'

const { runtime } = useRuntime()
const aclAvailable = computed(() => runtime.value?.capabilities.acl_reads === true)
const props = defineProps<{ id?: string }>()
const session = ref<AccessSession>(), cluster = ref<Cluster>(), username = ref(''), password = ref('')
const error = ref(''), notice = ref(''), busy = ref(false), loginBusy = ref(false), query = ref(''), page = ref(1), scenario = ref('success')
const acls = ref<Page<ACLRule>>(), requests = ref<ACLRequest[]>([]), preview = ref<ACLRequest>()
const inbox = ref<ACLRequest[]>([]), inboxError = ref('')
const inboxPage = ref(1), inboxTotal = ref(0)
const rule = ref<ACLRule>({ principal: '', host: '*', resource_type: 'TOPIC', resource_name: '', pattern_type: 'LITERAL', operation: 'READ', permission: 'ALLOW' })
let epoch = 0, disposed = false, authEpoch = 0
const valid = (token: number) => !disposed && token === epoch
const aclReason = (reason?: string) => reason === 'Kafka authorizer and explicit Kafka principal mapping have not been verified' ? 'Kafka 权限检查与账号关联尚未验证' : reasonLabel(reason)
function requestStatus(status: string) { return status === 'preview' ? '预览，尚未提交' : status === 'pending_approval' ? (runtime.value?.mode === 'admin-lab' ? 'pending_approval · 等待管理员批准，未执行' : 'pending_approval · 等待审批，未执行') : status === 'approved_not_executed' ? 'approved_not_executed · 已批准，未执行 Kafka ACL 写入' : status === 'executed' ? 'Kafka ACL 已执行' : status === 'execution_failed' ? '实际执行失败，请检查 Broker authorizer 与权限' : status === 'executing' ? '实际执行中，结果待确认' : '状态未知，未确认执行' }
async function loadLists(token: number) {
  if (!props.id || runtime.value?.capabilities.acl_reads === false) return
  const registered = await registryRequest<Cluster>(kafkaPath(props.id)); if (!valid(token)) return
  cluster.value = registered
  const results = await Promise.allSettled([listACLs(props.id, page.value, query.value, scenario.value), listACLRequests(props.id)])
  if (!valid(token)) return
  const [aclResult, requestResult] = results
  if (aclResult.status === 'fulfilled') acls.value = aclResult.value
  else error.value = accessError(aclResult.reason)
  if (requestResult.status === 'fulfilled') requests.value = requestResult.value.items
  else error.value = accessError(requestResult.reason)
}
async function load(initial = false) {
  const token = ++epoch; busy.value = true; error.value = ''; acls.value = undefined; cluster.value = undefined; requests.value = []; inbox.value = []; inboxTotal.value = 0; inboxError.value = ''; preview.value = undefined
  try {
    if (initial) { await ensureSession(); if (!valid(token)) return; session.value = await registryRequest<AccessSession>('/session'); if (!valid(token)) return }
    if (!aclAvailable.value) return
    const result = await Promise.allSettled([loadLists(token), approvalInbox(inboxPage.value)])
    if (!valid(token)) return
    if (result[0].status === 'rejected') error.value = accessError(result[0].reason)
    if (result[1].status === 'fulfilled') { inbox.value = result[1].value.items ?? []; inboxTotal.value = result[1].value.total ?? 0; inboxError.value = '' }
    else { inbox.value = []; inboxTotal.value = 0; inboxError.value = accessError(result[1].reason) }
  } catch (e) { if (valid(token)) error.value = accessError(e) }
  finally { if (valid(token)) busy.value = false }
}
async function signIn() {
  ++epoch; busy.value = false
  const token = ++authEpoch; loginBusy.value = true; error.value = ''; notice.value = ''
  try {
    await login(username.value, password.value)
    if (disposed || token !== authEpoch) return
    session.value = await authSession(); if (disposed || token !== authEpoch) return
    notice.value = '平台身份已更新；Kafka 账号仍由集群连接配置决定'
    inboxPage.value = 1
    await load()
  } catch (e) { if (!disposed && token === authEpoch) error.value = accessError(e) }
  finally { password.value = ''; if (!disposed && token === authEpoch) loginBusy.value = false }
}
async function signOut() {
  ++authEpoch; ++epoch; busy.value = false; loginBusy.value = true; password.value = ''; error.value = ''
  try { await logout(); if (!disposed) { session.value = undefined; acls.value = undefined; requests.value = []; inbox.value = []; inboxPage.value = 1; inboxTotal.value = 0; preview.value = undefined; cluster.value = undefined; notice.value = '已退出，请填写登录信息' } }
  catch (e) { if (!disposed) error.value = accessError(e) }
  finally { if (!disposed) loginBusy.value = false }
}
async function submit(previewOnly: boolean) {
  if (!aclAvailable.value || !props.id || (!previewOnly && !preview.value)) return
  const submittedRule = JSON.stringify(rule.value)
  const token = epoch; busy.value = true; error.value = ''; notice.value = ''
  try {
    const result = await requestACL(props.id, rule.value, previewOnly); if (!valid(token)) return
    if (previewOnly) { if (submittedRule === JSON.stringify(rule.value)) preview.value = result }
    else { preview.value = undefined; requests.value = [result, ...requests.value.filter(r => r.id !== result.id)]; notice.value = requestStatus(result.status) }
  } catch (e) { if (valid(token)) error.value = accessError(e) }
  finally { if (valid(token)) busy.value = false }
}
async function approve(request: ACLRequest) {
  if (!aclAvailable.value || request.can_approve !== true || (request.applicant === session.value?.user_id && runtime.value?.mode !== 'admin-lab') || request.status !== 'pending_approval') return
  const token = epoch; busy.value = true; error.value = ''
  try { const updated = await approveACL(request); if (valid(token)) { requests.value = requests.value.map(r => r.id === updated.id ? updated : r); inbox.value = inbox.value.map(r => r.id === updated.id ? updated : r); notice.value = requestStatus(updated.status) } }
  catch (e) { if (valid(token)) error.value = accessError(e) }
  finally { if (valid(token)) busy.value = false }
}
async function execute(request: ACLRequest) {
  if (runtime.value?.mode !== 'admin-lab' || !runtime.value.write_enabled || request.can_execute !== true) return
  const token = epoch; busy.value = true; error.value = ''
  try { const updated = await executeACL(request); if (valid(token)) { requests.value = requests.value.map(r => r.id === updated.id ? updated : r); notice.value = updated.status === 'executed' ? '实验 ACL 已实际执行' : '执行未确认，请检查记录' } }
  catch (e) { if (valid(token)) { error.value = accessError(e); await loadLists(token) } }
  finally { if (valid(token)) busy.value = false }
}
function filter() { page.value = 1; void load() }
watch(() => props.id, () => { query.value = ''; page.value = 1; inboxPage.value = 1; rule.value.principal = ''; rule.value.resource_name = ''; void load(true) }, { immediate: true })
watch(rule, () => { preview.value = undefined }, { deep: true })
onUnmounted(() => { disposed = true; ++epoch; ++authEpoch; password.value = '' })
</script>

<template>
  <section class="security-view registry">
    <header class="page-header"><div><h1>身份与 Kafka ACL</h1></div><span class="status-pill status-pill--muted">{{ runtime?.mode === 'admin-lab' && runtime.write_enabled ? '实验 ACL 管理' : 'ACL 写入禁用' }}</span></header><section class="panel">
      <div class="panel-header"><h2>平台身份</h2><span class="helper-text">LDAP 登录与 Kafka 集群认证分别授权</span></div>
      <p v-if="session">当前平台身份：{{ session.user_id }} · {{ session.mode === 'mock' ? 'Mock 开发模式，未验证真实 LDAP' : session.mode }} · 角色 {{ session.role || session.roles?.join(', ') || '由服务端限定' }}</p>
      <p v-if="!session || session.mode === 'mock' || session.ldap_configured === false">LDAP 尚未配置或验证。</p>
      <form data-login @submit.prevent="signIn">
        <label>用户名 <input v-model="username" name="username" autocomplete="username" required :disabled="loginBusy"></label>
        <label>密码 <input v-model="password" name="password" type="password" autocomplete="current-password" required :disabled="loginBusy"></label>
        <button :disabled="loginBusy" class="button-primary">登录</button><button type="button" :disabled="loginBusy" @click="signOut">退出</button>
      </form>
    </section>
    <p v-if="error" role="alert">{{ error }}</p><p v-if="notice" role="status">{{ notice }}</p><p v-if="busy">正在读取…</p>
    <section v-if="aclAvailable" class="panel">
      <div class="panel-header"><h2>待审批事项</h2><span class="helper-text">{{ inboxTotal }} 项授权申请</span></div>
      <p>仅显示服务端授权范围内的他人申请；批准后仍不执行 Kafka ACL 写入。</p>
      <p v-if="inboxError">{{ inboxError }}</p>
      <ul data-inbox class="approval-list"><li v-for="request in inbox" :key="request.id">{{ request.source_registration_id }} · {{ request.rule.principal }} · {{ request.rule.resource_name }} · {{ requestStatus(request.status) }} · 申请人 {{ request.applicant }} <button data-acl-approve :disabled="busy || request.can_approve !== true || (request.applicant === session?.user_id && runtime?.mode !== 'admin-lab') || request.status !== 'pending_approval'" @click="approve(request)">审批此申请</button></li></ul>
      <div class="pagination"><button data-inbox-prev :disabled="busy || inboxPage <= 1" @click="inboxPage--; load()">上一页审批</button><span data-inbox-total>{{ inboxPage }} / 共 {{ inboxTotal }} 项授权申请</span><button data-inbox-next :disabled="busy || inboxPage * 20 >= inboxTotal" @click="inboxPage++; load()">下一页审批</button></div>
    </section>
    <p v-if="!aclAvailable" role="status">当前运行模式未启用 Kafka ACL 查询与审批；平台身份登录仍可使用。</p>
    <p v-if="aclAvailable && !id">从集群管理进入具体集群 ID 的安全页面可读取 ACL。{{ runtime?.mode === 'admin-lab' ? '可管理当前所有者集群的实验 ACL。' : '所有申请仅预览和审批，不执行 Kafka 写入。' }}</p>
    <template v-if="aclAvailable && id">
      <section class="panel">
        <div class="panel-header"><h2>{{ cluster?.display_name || '集群' }} · ACL 只读查询</h2><button :disabled="busy" @click="load()">刷新</button></div>
        <form @submit.prevent="filter"><label>ACL 过滤 <input v-model="query" maxlength="128"></label><button :disabled="busy">查询</button></form>
        <details v-if="runtime?.mode === 'mock'" class="demo-settings"><summary>演示设置</summary><label>Mock 场景 <select v-model="scenario" :disabled="busy" @change="filter"><option v-for="s in scenarios" :key="s">{{ s }}</option></select></label></details>
        <p v-if="acls">{{ acls.synthetic ? 'Mock 合成 ACL，未验证真实 Kafka' : 'Kafka ACL 查询结果' }} · {{ acls.state === 'pending' ? 'pending · 状态待确认' : acls.state }} · {{ acls.observed_at || '查询时间未知' }} {{ acls.stale ? '已过期' : '' }} {{ aclReason(acls.reason) }}</p>
        <div class="table-scroll"><table class="data-table"><thead><tr><th>账号</th><th>主机</th><th>资源类型 / 名称</th><th>匹配方式</th><th>操作</th><th>权限</th><th>添加来源</th></tr></thead><tbody><tr v-for="(acl, i) in acls?.items" :key="i"><td>{{ acl.principal }}</td><td>{{ acl.host }}</td><td>{{ acl.resource_type }} / {{ acl.resource_name }}</td><td>{{ acl.pattern_type }}</td><td>{{ acl.operation }}</td><td>{{ acl.permission }}</td><td>{{ acl.source_registration_id }}</td></tr></tbody></table></div>
        <div class="pagination"><button :disabled="busy || page <= 1" @click="page--; load()">上一页</button><span>{{ page }} / 共 {{ acls?.total ?? '未知' }} 项</span><button :disabled="busy || !acls || page * 20 >= acls.total" @click="page++; load()">下一页</button></div>
      </section>
      <section class="panel">
        <div class="panel-header"><h2>ACL 申请预览</h2><span class="status-pill status-pill--muted">仅预览与审批</span></div><p>提交只创建待审批申请；批准后仍为未执行。{{ runtime?.mode === 'admin-lab' ? '实验管理员可执行已批准申请，实际结果取决于 Broker authorizer。' : '此页面不提供 Kafka ACL 执行操作。' }}</p>
        <form data-acl-request @submit.prevent="submit(true)">
          <label>Kafka 账号 <input v-model="rule.principal" name="principal" required maxlength="256"></label>
          <label>Host <input v-model="rule.host" name="host" required maxlength="256"></label>
          <label>资源类型 <select v-model="rule.resource_type"><option>TOPIC</option><option>GROUP</option><option>CLUSTER</option><option>TRANSACTIONAL_ID</option></select></label>
          <label>资源名称 <input v-model="rule.resource_name" name="resource_name" required maxlength="256"></label>
          <label>Pattern <select v-model="rule.pattern_type"><option>LITERAL</option><option>PREFIXED</option></select></label>
          <label>Operation <select v-model="rule.operation"><option>READ</option><option>WRITE</option><option>DESCRIBE</option><option>CREATE</option><option>DELETE</option><option>ALTER</option><option>ALL</option></select></label>
          <label>Permission <select v-model="rule.permission"><option>ALLOW</option><option>DENY</option></select></label>
          <button class="button-primary" :disabled="busy">预览申请</button><button type="button" data-submit-request :disabled="busy || !preview" @click="submit(false)">提交待审批申请</button>
        </form>
        <p v-if="preview" data-preview>{{ requestStatus(preview.status) }} · {{ preview.rule.principal }} · {{ preview.rule.resource_type }}/{{ preview.rule.resource_name }} · {{ preview.rule.operation }} {{ preview.rule.permission }}</p>
      </section><section class="panel">
        <div class="panel-header"><h2>审批记录</h2><span class="helper-text">审批人授权</span></div><p>审批默认未配置。仅服务端明确授权的审批人可审批；{{ runtime?.mode === 'admin-lab' ? '本隔离实验允许管理员批准自己的申请。' : '申请人不能自批。' }}</p>
        <ul class="approval-list"><li v-for="request in requests" :key="request.id">{{ request.rule.principal }} · {{ request.rule.resource_name }} · {{ requestStatus(request.status) }} · 申请人 {{ request.applicant }} <button data-acl-approve :disabled="busy || request.can_approve !== true || (request.applicant === session?.user_id && runtime?.mode !== 'admin-lab') || request.status !== 'pending_approval'" @click="approve(request)">{{ runtime?.mode === 'admin-lab' ? '管理员批准' : '独立审批' }}</button><button v-if="runtime?.mode === 'admin-lab' && runtime.write_enabled" data-acl-execute :disabled="busy || request.can_execute !== true" @click="execute(request)">执行实验 ACL</button></li></ul>
      </section>
    </template>
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
.approval-list { padding:0; list-style:none; }
.approval-list li { display:flex; flex-wrap:wrap; align-items:center; gap:10px; padding:14px 0; border-bottom:1px solid #e8edf3; font-size:13px; }
.approval-list li button { margin-left:auto; }
@media(max-width:640px) { .panel > form label { width:100%; } }
</style>
