<script setup lang="ts">
import { useRuntime } from '../composables/useRuntime'
import { computed, onUnmounted, reactive, ref } from 'vue'
import { ApiError } from '../api/client'
import { registryRequest, type Cluster, type Credentials } from '../api/registry'
import { sanitizeAddress } from '../topology/unregistered'

const { runtime } = useRuntime()
const props = defineProps<{ cluster?: Cluster; prefill?: { display_name: string; bootstrap_servers: string[] } }>()
const emit = defineEmits<{ saved: []; cancel: [] }>()
const cluster = props.cluster
const base = reactive({ display_name: cluster?.display_name ?? props.prefill?.display_name ?? '', room: cluster?.room ?? '', environment: cluster?.environment ?? '', description: cluster?.description ?? '', enabled: cluster?.enabled ?? true, version_reported: cluster?.version_reported ?? '' })
const endpoints = ref(cluster?.bootstrap_servers.join('\n') ?? props.prefill?.bootstrap_servers.map(sanitizeAddress).filter(Boolean).join('\n') ?? '')
const credentials = reactive<Credentials>({ security_protocol: cluster?.security_protocol ?? 'SASL_SSL', sasl_mechanism: cluster?.sasl_mechanism || 'PLAIN', username: '', password: '', tls_ca_pem: '', tls_cert_pem: '', tls_key_pem: '', tls_server_name: cluster?.tls_server_name ?? '' })
const sasl = computed(() => credentials.security_protocol.startsWith('SASL'))
const tls = computed(() => credentials.security_protocol.endsWith('SSL'))
const busy = ref(false), error = ref('')
const clearFields = reactive({ username: false, password: false, tls_ca_pem: false, tls_cert_pem: false, tls_key_pem: false })
const secretKeys = ['username', 'password', 'tls_ca_pem', 'tls_cert_pem', 'tls_key_pem'] as const
function clearSecrets() { for (const key of secretKeys) credentials[key] = '' }
onUnmounted(clearSecrets)
function cancel() { clearSecrets(); emit('cancel') }
async function save() {
  error.value = ''
  const servers = endpoints.value.split(/[\n,]+/).map(s => s.trim()).filter(Boolean)
  if (!base.display_name.trim() || !servers.length) { error.value = '请填写显示名称和至少一个 Broker 入口'; return }
  const auth: Credentials = { security_protocol: credentials.security_protocol, sasl_mechanism: sasl.value ? credentials.sasl_mechanism : '', tls_server_name: tls.value ? credentials.tls_server_name : '' }
  for (const key of secretKeys) {
    const relevant = key === 'username' || key === 'password' ? sasl.value : tls.value
    if (relevant && (credentials[key] || clearFields[key])) auth[key] = credentials[key] ?? ''
  }
  if (!cluster && sasl.value && (!auth.username || !auth.password)) { error.value = '新增 SASL 添加需要用户名和密码'; return }
  busy.value = true
  // Secrets leave component state as soon as the request begins, including failed submissions.
  clearSecrets()
  try {
    if (cluster) {
      const updated = await registryRequest<Cluster>(`/kafka/clusters/${encodeURIComponent(cluster.id)}`, 'PATCH', { ...base, bootstrap_servers: servers, revision: cluster.revision })
      await registryRequest(`/kafka/clusters/${encodeURIComponent(cluster.id)}/${runtime.value?.information_mode ? 'connection-credentials' : 'credentials'}`, 'PUT', { ...auth, revision: updated.revision })
    } else await registryRequest('/kafka/clusters', 'POST', { ...base, bootstrap_servers: servers, ...auth })
    emit('saved')
  } catch (e) {
    error.value = e instanceof ApiError ? e.message : '保存失败，请刷新后重试'
    if (cluster) error.value += '；基础信息可能已保存，请关闭并刷新后再编辑'
  } finally {
    for (const key of secretKeys) delete auth[key]
    busy.value = false
  }
}
</script>

<template>
  <div class="modal-backdrop">
    <section role="dialog" aria-modal="true" aria-labelledby="cluster-form-title" class="registry-dialog">
      <h2 id="cluster-form-title">{{ cluster ? '编辑' : '新增' }} Kafka 集群</h2>
      <p>{{ runtime?.mode === 'mock' ? 'Mock 添加 · 保存后可在列表启动异步模拟连接测试，也可保留未验证状态。' : '集群 · 保存后可启动异步连接测试；认证信息按集群独立配置。' }}</p>
      <p v-if="error" role="alert" class="error">{{ error }}</p>
      <form @submit.prevent="save">
        <fieldset :disabled="busy">
          <legend>基础信息</legend>
          <label>显示名称 <input v-model="base.display_name" required maxlength="120"></label>
          <label>机房 <input v-model="base.room" maxlength="120"></label><label>环境 <input v-model="base.environment" maxlength="120"></label>
          <label>说明 <textarea v-model="base.description" maxlength="2000" /></label>
          <label>人工添加版本 <input v-model="base.version_reported" placeholder="可选；不推断实际能力" maxlength="64"></label>
          <label><input v-model="base.enabled" type="checkbox">启用集群</label>
        </fieldset>
        <fieldset :disabled="busy"><legend>连接端点</legend><label>Bootstrap servers（逗号或换行分隔）<textarea v-model="endpoints" required placeholder="broker.example.test:9093" /></label><p>入口仍受后端网络允许列表与访问范围限制。</p></fieldset>
        <fieldset :disabled="busy">
          <legend>认证安全</legend>
          <label>security.protocol <select v-model="credentials.security_protocol"><option>SASL_SSL</option><option>SSL</option><option>SASL_PLAINTEXT</option><option>PLAINTEXT</option></select></label>
          <p v-if="credentials.security_protocol === 'SASL_PLAINTEXT'" role="alert" class="risk">风险：SASL_PLAINTEXT 不加密网络流量，认证信息及数据可能暴露。推荐 SASL_SSL。</p>
          <p v-if="credentials.security_protocol === 'PLAINTEXT'" role="alert" class="risk">风险：PLAINTEXT 无认证且不加密，推荐使用 TLS。</p>
          <template v-if="sasl">
            <label>SASL mechanism <select v-model="credentials.sasl_mechanism"><option>PLAIN</option><option>SCRAM-SHA-256</option><option>SCRAM-SHA-512</option></select></label>
            <label>用户名（只写）<input v-model="credentials.username" name="username" autocomplete="off"></label>
            <label>密码（只写）<input v-model="credentials.password" name="password" type="password" autocomplete="new-password"></label>
          </template>
          <template v-if="tls">
            <label>TLS ServerName <input v-model="credentials.tls_server_name" autocomplete="off"></label>
            <label>CA PEM（只写）<textarea v-model="credentials.tls_ca_pem" autocomplete="off" /></label>
            <label>客户端证书 PEM（只写）<textarea v-model="credentials.tls_cert_pem" autocomplete="off" /></label>
            <label>客户端私钥 PEM（只写）<textarea v-model="credentials.tls_key_pem" autocomplete="off" /></label>
            <p>验证 TLS 证书；客户端证书和私钥需成对提供。</p>
          </template>
          <p v-if="cluster">编辑不读取原始秘密，留空保留已有值。认证信息 {{ cluster.credentials_configured ? '已配置' : '未配置' }}；CA {{ cluster.tls_ca_configured ? '已配置' : '未配置' }}；客户端证书 {{ cluster.tls_client_configured ? '已配置' : '未配置' }}。提交或取消即清除输入。</p>
          <template v-if="cluster"><label v-for="key in secretKeys" :key="key"><input v-model="clearFields[key]" type="checkbox">明确清除 {{ key }}（勾选并留空）</label></template>
        </fieldset>
        <div class="toolbar"><button class="button-primary" type="submit" :disabled="busy">{{ busy ? '保存中…' : '保存添加' }}</button><button type="button" data-action="cancel" :disabled="busy" @click="cancel">取消</button></div>
      </form>
    </section>
  </div>
</template>
