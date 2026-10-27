<script setup lang="ts">
import { computed, onBeforeUnmount, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ensureSession, registryRequest, type Cluster } from '../api/registry'
import { kafkaTopic, kafkaError, type Topic, type Markers } from '../api/kafkaops'
import { workspaceMeta, type WorkspaceMeta, type PlanInput } from '../api/workspace'
import { resourceConfigs, type ConfigEntry } from '../api/resource-management'
import ResourceConfigs from '../components/workspace/ResourceConfigs.vue'
import ResourceOperations from '../components/workspace/ResourceOperations.vue'
import TopicLifecycle from '../components/workspace/TopicLifecycle.vue'
import WorkspacePlanDialog from '../components/workspace/WorkspacePlanDialog.vue'
import { useRuntime } from '../composables/useRuntime'

const props = defineProps<{ id: string; topicName: string }>()
const route = useRoute(), router = useRouter()
const cluster = shallowRef<Cluster>(), meta = shallowRef<WorkspaceMeta>(), topic = shallowRef<Topic>(), evidence = shallowRef<Markers>()
const actorId = shallowRef(''), busy = shallowRef(false), error = shallowRef(''), input = shallowRef<PlanInput|null>(null)
const { runtime } = useRuntime()
const informationMode = computed(() => runtime.value?.information_mode === true)
const tabs = computed(() => [{ id: 'partitions', label: '分区' }, { id: 'configs', label: '配置' }, ...(!informationMode.value ? [{ id: 'replicas', label: '副本调整' }, { id: 'operations', label: '操作记录' }] : [])])
const tab = computed(() => !informationMode.value && route.query.tab === 'delete' ? 'delete' : tabs.value.some(t => t.id === route.query.tab) ? String(route.query.tab) : 'partitions')
const resource = computed(() => ({ type: 'TOPIC' as const, name: props.topicName }))
const back = computed(() => ({ path: `/kafka/clusters/${encodeURIComponent(props.id)}/details`, query: { tab: 'topics' } }))
const canManage = computed(() => !informationMode.value && !!meta.value?.permissions.topic_manage && !!meta.value.write_enabled && !topic.value?.internal)
const inSync = computed(() => topic.value?.partitions.filter(p => !p.error_code && p.leader >= 0 && p.replicas.length > 0 && p.replicas.every(id => p.isr.includes(id))).length ?? 0)
const replicaCount = computed(() => topic.value?.rf_min == null ? '未知' : topic.value.rf_min === topic.value.rf_max ? topic.value.rf_min : `${topic.value.rf_min}–${topic.value.rf_max}`)
const configs = shallowRef<ConfigEntry[]>(), configError = shallowRef(''), configAt = shallowRef('')
function entry(name: string) { const e = configs.value?.find(c => c.name === name); return e && !e.sensitive && e.value !== null ? e : undefined }
function duration(ms: string) { const n = Number(ms); if (n === -1) return '无限保留'; if (!Number.isFinite(n) || n < 0) return ms; const d = n / 86400000; return d >= 1 && Number.isInteger(d) ? `${d} 天` : n % 3600000 === 0 ? `${n / 3600000} 小时` : `${n} 毫秒` }
const cleanupText: Record<string, string> = { delete: '按保留规则删除', compact: '按 Key 压缩保留最新值', 'compact,delete': '压缩并按保留规则删除', 'delete,compact': '压缩并按保留规则删除' }
const minIsr = computed(() => { const e = entry('min.insync.replicas'); return e ? `${e.value} 个副本` : '未读取' })
const retention = computed(() => { const e = entry('retention.ms'); return e ? duration(e.value!) : '未读取' })
const cleanup = computed(() => { const e = entry('cleanup.policy'); return e ? cleanupText[e.value!.replace(/\s/g, '')] ?? e.value! : '未读取' })
const rawConfigs = computed(() => ['retention.ms', 'cleanup.policy', 'min.insync.replicas'].map(n => { const e = configs.value?.find(c => c.name === n); return { name: n, value: !e ? '未读取' : e.sensitive ? '（敏感，不展示）' : e.value ?? '未读取', source: e?.source ?? '—' } }))
async function loadConfigs(token: number) {
  configs.value = undefined; configError.value = ''
  try { const r = await resourceConfigs(props.id, { type: 'TOPIC', name: props.topicName }); if (token === epoch) { configs.value = r.entries; configAt.value = new Date().toLocaleString('zh-CN', { hour12: false }) } }
  catch (e) { if (token === epoch) configError.value = kafkaError(e) }
}
let epoch = 0, detailEpoch = 0
async function load() {
  const token = ++epoch
  ++detailEpoch
  busy.value = true; error.value = ''; cluster.value = undefined; meta.value = undefined; topic.value = undefined; evidence.value = undefined; input.value = null
  try {
    const session = await ensureSession()
    if (token !== epoch) return
    const [registration, permissions, result] = await Promise.all([
      registryRequest<Cluster>(`/kafka/clusters/${encodeURIComponent(props.id)}`), workspaceMeta(props.id), kafkaTopic(props.id, props.topicName, 'success'),
    ])
    if (token !== epoch) return
    cluster.value = registration; meta.value = permissions; topic.value = result.item; evidence.value = result; actorId.value = session.user_id
    void loadConfigs(token)
  } catch (e) { if (token === epoch) error.value = kafkaError(e) }
  finally { if (token === epoch) busy.value = false }
}
function brokerPath(brokerId:number){return `/kafka/clusters/${encodeURIComponent(props.id)}/brokers/${brokerId}`}
function selectTab(value: string) { input.value = null; void router.push({ query: { tab: value } }) }
async function refreshTopic() {
  const token = epoch, request = ++detailEpoch
  try {
    const result = await kafkaTopic(props.id, props.topicName, 'success')
    if (token === epoch && request === detailEpoch) { topic.value = result.item; evidence.value = result }
  } catch (e) { if (token === epoch && request === detailEpoch) error.value = kafkaError(e) }
}
watch(() => [props.id, props.topicName], load, { immediate: true, flush: 'sync' })
onBeforeUnmount(() => { ++epoch })
</script>

<template>
  <section class="topic-page registry">
    <nav class="topic-breadcrumb" aria-label="Topic 路径"><router-link to="/kafka/clusters">Kafka 集群</router-link><span>/</span><router-link :to="back">{{ cluster?.display_name || '集群' }}</router-link><span>/</span><router-link :to="back">Topics</router-link></nav>
    <header class="page-header"><div class="topic-title"><h1>{{ topicName }}</h1><span v-if="topic" class="status-pill status-pill--muted">{{ topic.internal ? '内部 Topic' : '业务 Topic' }}</span><span v-if="meta?.synthetic" class="status-pill status-pill--warning">演示数据</span></div><div class="topic-actions"><router-link v-if="informationMode" :to="{path: '/topic-lineage', query: {cluster_id: id, topic: topicName}}">查看链路</router-link><button :disabled="busy" @click="load">刷新</button><details v-if="canManage" class="topic-more"><summary>更多操作</summary><button class="danger-action" data-delete-topic @click="selectTab('delete')">删除 Topic</button></details></div></header>
    <p v-if="busy" role="status">加载中…</p><p v-if="error" role="alert">{{ error }}</p>
    <template v-if="topic && meta">
      <p class="topic-scope">对象范围：{{ cluster?.display_name || id }} / 此 Topic · 元数据采集时间 {{ evidence?.observed_at || '未知' }}{{ evidence?.stale ? '（已过期）' : '' }} · 只读</p>
      <dl class="topic-summary" data-topic-summary><div><dt>分区数</dt><dd>{{ topic.partitions.length }} 个分区</dd></div><div><dt>各分区副本数 RF</dt><dd>{{ replicaCount === '未知' ? '未知' : `每分区 ${replicaCount} 个副本` }}</dd></div><div><dt>副本同步完整的分区</dt><dd>{{ inSync }} <span>/ {{ topic.partitions.length }}</span></dd></div><div><dt>最小同步副本数</dt><dd data-min-isr>{{ minIsr }}</dd></div><div><dt>消息保留时间</dt><dd data-retention>{{ retention }}</dd></div><div><dt>清理方式</dt><dd data-cleanup>{{ cleanup }}</dd></div></dl>
      <p v-if="configError" class="config-partial" data-config-partial role="status">配置子查询失败：{{ configError }}。保留已读取的分区元数据；配置值显示“未读取”，不填默认值。</p>
      <p class="helper-text">字段单位不同，不汇总相加；RF 来自各分区元数据，不由集群默认值推断。</p>
      <details class="topic-diagnostics" data-config-raw><summary>技术详情：原始配置、来源与查询诊断</summary><p v-for="c in rawConfigs" :key="c.name" class="long-name">{{ c.name }} = {{ c.value }} <small>（来源 {{ c.source }}）</small></p><p>配置查询时间：{{ configError ? '本次读取失败' : configAt || '读取中' }}</p></details>
      <nav class="topic-tabs" aria-label="Topic 详情"><button v-for="item in tabs" :key="item.id" :class="{active:tab===item.id}" :aria-current="tab===item.id?'page':undefined" @click="selectTab(item.id)">{{ item.label }}</button></nav>
      <section v-if="tab==='partitions'" class="panel topic-partitions"><header class="panel-heading"><h2>分区</h2><button v-if="canManage" data-expand-topic @click="input={kind:'topic_expand',topic:topicName,partitions:topic.partitions.length+1}">增加分区</button></header><div class="table-scroll"><table class="data-table"><thead><tr><th>Partition ID</th><th>Leader Broker ID</th><th>Replica Broker IDs</th><th>ISR Broker IDs</th><th>状态</th></tr></thead><tbody><tr v-for="partition in topic.partitions" :key="partition.partition"><td>{{ partition.partition }}</td><td><router-link v-if="partition.leader>=0" :to="brokerPath(partition.leader)">{{ partition.leader }}</router-link><span v-else>未知</span></td><td><template v-for="(broker,index) in partition.replicas" :key="index"><span v-if="index">, </span><router-link v-if="broker>=0" :to="brokerPath(broker)">{{ broker }}</router-link><span v-else>未知</span></template><span v-if="!partition.replicas.length">未知</span></td><td><template v-for="(broker,index) in partition.isr" :key="index"><span v-if="index">, </span><router-link v-if="broker>=0" :to="brokerPath(broker)">{{ broker }}</router-link><span v-else>未知</span></template><span v-if="!partition.isr.length">未知</span></td><td><span class="status-pill" :class="partition.error_code || partition.leader<0 ? 'status-pill--warning' : partition.replicas.length>0 && partition.replicas.every(id=>partition.isr.includes(id)) ? 'status-pill--success' : 'status-pill--warning'">{{ partition.error_code || partition.leader<0 ? '查询异常' : partition.replicas.length>0 && partition.replicas.every(id=>partition.isr.includes(id)) ? '副本已同步' : '副本未同步完整' }}</span></td></tr></tbody></table></div></section>
      <ResourceConfigs v-else-if="tab==='configs'" :cluster-id="id" :resource="resource" :meta="meta" :actor-id="actorId" :show-operations="false" :read-only="informationMode || topic.internal" @submitted="selectTab('operations')" />
      <TopicLifecycle v-else-if="tab==='replicas' || tab==='delete'" :key="tab" :cluster-id="id" :topic="topic" :meta="meta" :actor-id="actorId" :show-operations="false" :action="tab==='delete'?'topic_delete':undefined" :hide-delete="tab==='replicas'" @submitted="selectTab('operations')" />
      <ResourceOperations v-else-if="tab==='operations'" :cluster-id="id" :resource="resource" :meta="meta" :actor-id="actorId" />
      <details v-if="evidence" class="topic-diagnostics"><summary>查询信息</summary><p>{{ evidence.synthetic?'演示数据':'Kafka 实际查询' }} · {{ evidence.observed_at || '时间未知' }} · {{ evidence.stale?'已过期':'本次查询' }}</p></details>
      <WorkspacePlanDialog v-if="!informationMode" :cluster-id="id" :input="input" :meta="meta" :actor-id="actorId" @close="input=null" @completed="refreshTopic" />
    </template>
  </section>
</template>

<style scoped>
.topic-breadcrumb,.topic-actions,.topic-title{display:flex;align-items:center;gap:12px;flex-wrap:wrap}.topic-breadcrumb{font-size:13px;color:#64748b;margin-bottom:18px}.topic-title{min-width:0}.topic-title h1{overflow-wrap:anywhere;font-size:24px;margin:0}.topic-summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));row-gap:16px;padding:22px 0;margin:0 0 20px;border-block:1px solid #dce5ef}.topic-summary>div{padding:0 24px;border-right:1px solid #dce5ef}.topic-summary>div:first-child{padding-left:0}.topic-summary>div:last-child{border:0}.topic-summary dt{color:#64748b;font-size:13px}.topic-summary dd{font-size:24px;font-weight:600;font-variant-numeric:tabular-nums;margin:8px 0 0}.topic-summary dd span{font-size:14px;color:#64748b;font-weight:400}.topic-tabs{display:flex;gap:24px;border-bottom:1px solid #dce5ef;margin-bottom:20px;overflow:auto}.topic-tabs button{border:0;border-radius:0;border-bottom:2px solid transparent;background:none;padding:12px 2px;white-space:nowrap;color:#64748b}.topic-tabs button.active{color:#2563eb;border-bottom-color:#2563eb;font-weight:600}.panel-heading{display:flex;justify-content:space-between;align-items:center;padding:18px 20px}.panel-heading h2{font-size:16px;margin:0}.topic-diagnostics{font-size:12px;color:#64748b;margin-top:20px}.topic-diagnostics summary,.topic-more summary{cursor:pointer}.topic-more{position:relative}.topic-more summary{border:1px solid #dce5ef;border-radius:5px;padding:9px 12px;background:white;font-size:13px}.topic-more button{position:absolute;right:0;top:42px;z-index:3;white-space:nowrap;background:white;box-shadow:0 4px 16px #24365014}.danger-action{color:#dc2626}.table-scroll{overflow:auto}@media(max-width:640px){.topic-summary>div{padding:0 10px}.topic-summary dt{font-size:12px}.topic-summary dd{font-size:20px}.page-header{gap:16px}.topic-title h1{font-size:21px}}
.topic-scope{font-size:12px;color:#64748b;margin:0 0 8px}.config-partial{padding:8px 12px;border:1px solid #f3d38a;background:#fffbeb;border-radius:6px;font-size:12px}.long-name{overflow-wrap:anywhere}
</style>
