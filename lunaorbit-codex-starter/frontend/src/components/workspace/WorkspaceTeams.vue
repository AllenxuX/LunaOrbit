<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, shallowRef, watch } from 'vue'
import { registryRequest } from '../../api/registry'
import { ApiError } from '../../api/client'
import { workspaceError } from '../../api/workspace'
interface GrantInput { subject_type: 'user' | 'group'; subject_id: string; role: string; topic_prefix: string; group_prefix: string }
interface Grant extends GrantInput { id: string; revision: number; revoked_at?: string }
const props = defineProps<{ clusterId: string; canManage: boolean }>()
const rows = ref<Grant[]>([])
const loading = shallowRef(false), busy = shallowRef(false), error = shallowRef(''), notice = shallowRef('')
const draft = reactive<GrantInput>({ subject_type: 'user', subject_id: '', role: 'viewer', topic_prefix: '', group_prefix: '' })
const preview = ref<GrantInput>(), revoke = ref<Grant>()
const roles: Record<string, string> = { viewer: '只读查看', operator: 'Topic 与应用运维', 'security-admin': '安全授权管理', admin: '集群管理员' }
let epoch = 0, mutationEpoch = 0
const active = computed(() => rows.value.filter(row => !row.revoked_at))
const path = (id: string) => `/kafka/clusters/${encodeURIComponent(id)}/team-grants`
async function load() {
 const token = ++epoch, id = props.clusterId
 loading.value = true; rows.value = []; error.value = ''; preview.value = undefined; revoke.value = undefined
 try { const page = await registryRequest<{ items: Grant[] }>(path(id)); if (token === epoch) rows.value = page.items }
 catch (e) { if (token === epoch) error.value = workspaceError(e) }
 finally { if (token === epoch) loading.value = false }
}
watch(() => props.clusterId, () => {
 mutationEpoch++; busy.value = false; notice.value = '';Object.assign(draft,{subject_type:'user',subject_id:'',role:'viewer',topic_prefix:'',group_prefix:''});void load()
}, { immediate: true })
watch(() => props.canManage, () => { preview.value = undefined; revoke.value = undefined })
onBeforeUnmount(() => { epoch++ })
function prepare() { if (!props.canManage || busy.value || !draft.subject_id.trim()) return; preview.value = { ...draft, subject_id: draft.subject_id.trim() }; revoke.value = undefined; error.value = ''; notice.value = '' }
async function grant() {
 if (!props.canManage || busy.value || !preview.value) return
 const mutation = ++mutationEpoch, token = epoch, id = props.clusterId, input = { ...preview.value }; busy.value = true; error.value = ''
 try { await registryRequest<Grant>(path(id), 'POST', input); if (token !== epoch) return; preview.value = undefined; notice.value = '团队授权已保存'; await load() }
 catch (e) { if (token === epoch) { error.value = e instanceof ApiError && e.status === 409 ? '相同对象、角色和范围的授权已存在，请刷新列表后查看或撤销' : workspaceError(e); preview.value = undefined } }
 finally { if (mutation === mutationEpoch) busy.value = false }
}
async function confirmRevoke() {
 if (!props.canManage || busy.value || !revoke.value) return
 const mutation = ++mutationEpoch, token = epoch, id = props.clusterId, target = { ...revoke.value }; busy.value = true; error.value = ''
 try { await registryRequest(`${path(id)}/${encodeURIComponent(target.id)}`, 'DELETE', { revision: target.revision }); if (token !== epoch) return; revoke.value = undefined; notice.value = '授权已撤销，后续请求立即重新校验'; await load() }
 catch (e) { if (token === epoch) { error.value = workspaceError(e); revoke.value = undefined } }
 finally { if (mutation === mutationEpoch) busy.value = false }
}
</script>

<template>
  <section class="teams" aria-label="团队授权">
    <div class="heading"><div><h3>团队授权</h3><p>将当前集群共享给可信用户或 LDAP 组，角色仅在此集群生效。</p></div><button :disabled="loading || busy" @click="load">刷新</button></div>
    <p v-if="error" role="alert">{{ error }}</p><p v-if="notice" role="status">{{ notice }}</p>
    <p v-if="loading" role="status">正在读取团队授权…</p>
    <div v-else class="grant-table">
      <table>
        <thead><tr><th>授权对象</th><th>集群角色</th><th>Topic 范围</th><th>消费组范围</th><th v-if="canManage">操作</th></tr></thead><tbody>
          <tr v-for="row in active" :key="row.id"><td>{{ row.subject_type === 'group' ? 'LDAP 组' : '用户' }}：{{ row.subject_id }}</td><td>{{ roles[row.role] ?? row.role }}</td><td>{{ row.topic_prefix ? `前缀 ${row.topic_prefix}` : '全部 Topic' }}</td><td>{{ row.group_prefix ? `前缀 ${row.group_prefix}` : '全部消费组' }}</td><td v-if="canManage"><button data-revoke :disabled="busy" @click="revoke = row; preview = undefined">撤销</button></td></tr>
          <tr v-if="!active.length && !error"><td :colspan="canManage ? 5 : 4">暂无团队授权，集群所有者仍可访问。</td></tr>
        </tbody>
      </table>
    </div>
    <form v-if="canManage" class="grant-form" @submit.prevent="prepare">
      <h4>新增共享授权</h4>
      <p class="help">填写身份服务提供的可信主体标识。用户标识和 LDAP 组标识均不可由显示名称推断；这里不会更改登录用户的组成员关系。</p>
      <div class="fields">
        <label>对象类型<select v-model="draft.subject_type" :disabled="busy"><option value="user">用户</option><option value="group">可信 LDAP 组</option></select></label>
        <label>{{ draft.subject_type === 'group' ? 'LDAP 组标识' : '可信用户主体标识' }}<input v-model="draft.subject_id" data-subject required maxlength="512" :disabled="busy" /></label>
        <label>角色<select v-model="draft.role" :disabled="busy"><option v-for="(label, key) in roles" :key="key" :value="key">{{ label }}</option></select></label>
        <label>Topic 前缀<input v-model="draft.topic_prefix" data-topic-prefix maxlength="249" :disabled="busy" placeholder="例如 orders." /></label>
        <label>消费组前缀<input v-model="draft.group_prefix" data-group-prefix maxlength="249" :disabled="busy" placeholder="例如 orders-" /></label>
      </div>
      <p class="scope-note">前缀按原样匹配，不使用 * 通配符。留空表示该类资源覆盖整个集群，请在预览中确认。</p>
      <button type="submit" :disabled="busy || loading || !draft.subject_id.trim()">预览授权</button>
    </form>
    <div v-if="preview && canManage" class="confirmation" role="region" aria-label="授权预览">
      <h4>确认共享范围</h4><p>{{ preview.subject_type === 'group' ? 'LDAP 组' : '用户' }}：{{ preview.subject_id }} · {{ roles[preview.role] }}</p>
      <p>Topic：{{ preview.topic_prefix ? `以 ${preview.topic_prefix} 开头` : '全部 Topic（整个集群）' }}；消费组：{{ preview.group_prefix ? `以 ${preview.group_prefix} 开头` : '全部消费组（整个集群）' }}</p>
      <p>此授权只管理平台访问，不会创建 Kafka 账号或修改 Broker ACL。</p>
      <button data-confirm-grant :disabled="busy" @click="grant">确认授权</button><button :disabled="busy" @click="preview = undefined">取消</button>
    </div>
    <div v-if="revoke && canManage" class="confirmation" role="region" aria-label="撤销确认"><h4>确认撤销授权</h4><p>撤销 {{ revoke.subject_id }} 的{{ roles[revoke.role] }}授权。其他有效授权仍会保留。</p><button data-confirm-revoke :disabled="busy" @click="confirmRevoke">确认撤销</button><button :disabled="busy" @click="revoke = undefined">取消</button></div>
  </section>
</template>

<style scoped>
.teams { display: grid; gap: 18px; }
.heading { display: flex; align-items: center; justify-content: space-between; gap: 16px; }
.heading h3, .grant-form h4, .confirmation h4 { margin: 0 0 8px; }
.heading p, .help, .scope-note { color: var(--text-secondary, #64748b); line-height: 1.6; max-width: 80ch; }
.grant-table { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; text-align: left; }
th, td { padding: 12px; border-bottom: 1px solid var(--border-color, #dce3ed); overflow-wrap: anywhere; }
th { font-weight: 600; color: var(--text-secondary, #64748b); }
.grant-form { border-top: 1px solid var(--border-color, #dce3ed); padding-top: 20px; }
.fields { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 14px; }
.fields label { display: grid; gap: 8px; font-size: 14px; }
input, select { min-width: 0; padding: 9px 10px; border: 1px solid var(--border-color, #cbd5e1); border-radius: 6px; background: var(--surface, white); color: inherit; }
button { cursor: pointer; padding: 8px 13px; border: 1px solid var(--border-color, #cbd5e1); border-radius: 6px; background: var(--surface, white); color: inherit; }
button:disabled { cursor: default; opacity: .5; }
.confirmation { padding: 18px; border: 1px solid #d6a853; border-radius: 8px; background: #fffaee; color: #694b13; }
.confirmation button + button { margin-left: 10px; }
[role="alert"] { color: #b42318; }
</style>
