<script setup lang="ts">
import { useRuntime } from '../composables/useRuntime'
import { onUnmounted, ref, watch } from 'vue'
import { ensureSession, registryRequest, type Cluster } from '../api/registry'
import { kafkaError, kafkaList, kafkaPath, kafkaTopic, reasonLabel, scenarios, type Broker, type Group, type Markers, type Offset, type Page, type Topic } from '../api/kafkaops'

const { runtime } = useRuntime()
const props = defineProps<{ id: string }>()
const cluster = ref<Cluster>(), error = ref(''), busy = ref(false), scenario = ref('success')
const brokers = ref<Page<Broker>>(), topics = ref<Page<Topic>>(), groups = ref<Page<Group>>(), offsets = ref<Page<Offset>>()
const topic = ref<({ item: Topic } & Markers)>(), selectedGroup = ref('')
const query = ref(''), page = ref(1), groupPage = ref(1), brokerPage = ref(1), offsetPage = ref(1)
let epoch = 0, detailEpoch = 0, groupEpoch = 0, disposed = false
const unknown = (value: number | null | undefined) => value ?? '未知'
const valid = (token: number) => !disposed && token === epoch
async function load() {
  if (runtime.value?.capabilities.kafka_reads === false) return
  const token = ++epoch; ++detailEpoch; ++groupEpoch
  busy.value = true; error.value = ''; cluster.value = undefined; brokers.value = undefined; topics.value = undefined; groups.value = undefined; offsets.value = undefined; topic.value = undefined; selectedGroup.value = ''
  try {
    await ensureSession(); if (!valid(token)) return
    const registered = await registryRequest<Cluster>(kafkaPath(props.id)); if (!valid(token)) return
    cluster.value = registered
    const common = { query: query.value, scenario: scenario.value }
    const results = await Promise.allSettled([
      kafkaList<Broker>(props.id, 'brokers', { ...common, page: brokerPage.value }),
      kafkaList<Topic>(props.id, 'topics', { ...common, page: page.value }),
      kafkaList<Group>(props.id, 'groups', { ...common, page: groupPage.value }),
    ])
    if (!valid(token)) return
    const [b, t, g] = results
    if (b.status === 'fulfilled') brokers.value = b.value
    if (t.status === 'fulfilled') topics.value = t.value
    if (g.status === 'fulfilled') groups.value = g.value
    for (const result of results) if (result.status === 'rejected') error.value = kafkaError(result.reason)
  } catch (e) { if (valid(token)) error.value = kafkaError(e) }
  finally { if (valid(token)) busy.value = false }
}
async function showTopic(name: string) {
  const token = epoch, request = ++detailEpoch; topic.value = undefined; error.value = ''
  try { const data = await kafkaTopic(props.id, name, scenario.value); if (valid(token) && request === detailEpoch) topic.value = data }
  catch (e) { if (valid(token) && request === detailEpoch) error.value = kafkaError(e) }
}
async function showGroup(id: string, reset = true) {
  if (reset) offsetPage.value = 1
  const token = epoch, request = ++groupEpoch; selectedGroup.value = id; offsets.value = undefined; error.value = ''
  try { const data = await kafkaList<Offset>(props.id, `groups/${encodeURIComponent(id)}/offsets`, { page: offsetPage.value, scenario: scenario.value }); if (valid(token) && request === groupEpoch) offsets.value = data }
  catch (e) { if (valid(token) && request === groupEpoch) error.value = kafkaError(e) }
}
function filter() { page.value = 1; groupPage.value = 1; brokerPage.value = 1; void load() }
watch(() => props.id, () => { query.value = ''; page.value = 1; groupPage.value = 1; brokerPage.value = 1; void load() }, { immediate: true })
onUnmounted(() => { disposed = true; ++epoch })
</script>

<template>
  <section class="kafka-details registry">
    <header class="page-header"><div><h1>Kafka 只读详情 · {{ cluster?.display_name ?? '集群' }}</h1></div><button :disabled="busy" @click="load">刷新</button></header>
    <p v-if="runtime?.capabilities.kafka_reads === false" role="status">当前运行模式未启用 Kafka 只读查询。</p>
    <div class="resource-summary"><div><span>Broker</span><strong>{{ brokers?.total ?? '—' }}</strong></div><div><span>Topics</span><strong>{{ topics?.total ?? '—' }}</strong></div><div><span>Consumer Groups</span><strong>{{ groups?.total ?? '—' }}</strong></div></div>
    <p v-if="cluster?.security_protocol === 'SASL_PLAINTEXT'" role="status">SASL_PLAINTEXT 不加密传输，建议使用 SASL_SSL。</p>
    <section class="panel">
      <div class="filter-bar">
        <form @submit.prevent="filter"><label>资源过滤 <input v-model="query" maxlength="128" aria-label="资源过滤"></label> <button :disabled="busy">查询</button></form>
        <details v-if="runtime?.mode === 'mock'" class="demo-settings"><summary>演示设置</summary><label>Mock 场景 <select v-model="scenario" :disabled="busy" @change="filter"><option v-for="s in scenarios" :key="s">{{ s }}</option></select></label></details>
      </div>
    </section>
    <p v-if="busy" role="status">正在读取…</p><p v-if="error" role="alert">{{ error }}</p>
    <section class="panel query-evidence">
      <details>
        <summary>查询信息</summary><div v-for="(data, index) in [brokers, topics, groups, topic, offsets]" :key="index" :class="{ 'evidence-meta': data }">
          <p v-if="data">{{ ['Broker', 'Topics', 'Consumer Groups', 'Topic 详情', 'Offsets'][index] }}：{{ data.synthetic ? 'Mock 合成数据，未验证真实 Kafka' : 'Kafka 实际数据' }} · {{ data.observed_at || '查询时间未知' }} · {{ data.stale ? '已过期' : '未标记过期' }} · {{ data.state }} {{ reasonLabel(data.reason) }}</p>
          <ul v-if="data?.capabilities?.length"><li v-for="c in data.capabilities" :key="c.feature">{{ c.feature }}：{{ c.supported ? '支持' : '不支持 / 未知' }} {{ !c.supported ? reasonLabel(c.reason) || '实际 API 未确认支持' : '' }}</li></ul>
        </div>
      </details>
    </section>
    <section class="panel">
      <h2>Broker</h2><div class="table-scroll"><table class="data-table"><thead><tr><th>ID</th><th>主机</th><th>Port</th></tr></thead><tbody><tr v-for="b in brokers?.items" :key="b.id"><td>{{ b.id }}</td><td>{{ b.host }}</td><td>{{ b.port }}</td></tr></tbody></table></div>
      <div class="pagination"><button :disabled="busy || brokerPage <= 1" @click="brokerPage--; load()">上一页 Broker</button><span>{{ brokerPage }} / 共 {{ brokers?.total ?? '未知' }} 项</span><button :disabled="busy || !brokers || brokerPage * 20 >= brokers.total" @click="brokerPage++; load()">下一页 Broker</button></div>
    </section>
    <section class="panel">
      <h2>Topics</h2><div class="table-scroll"><table class="data-table"><thead><tr><th>Topic</th><th>Internal</th><th>RF 范围</th></tr></thead><tbody><tr v-for="t in topics?.items" :key="t.name"><td><button :data-topic="t.name" @click="showTopic(t.name)">{{ t.name }}</button></td><td>{{ t.internal ? '是' : '否' }}</td><td>{{ unknown(t.rf_min) }} – {{ unknown(t.rf_max) }}</td></tr></tbody></table></div>
      <div class="pagination"><button :disabled="busy || page <= 1" @click="page--; load()">上一页 Topic</button><span>{{ page }} / 共 {{ topics?.total ?? '未知' }} 项</span><button :disabled="busy || !topics || page * 20 >= topics.total" @click="page++; load()">下一页 Topic</button></div>
      <div v-if="topic" class="nested-detail"><h3>{{ topic.item.name }} 分区 · RF {{ unknown(topic.item.rf_min) }} – {{ unknown(topic.item.rf_max) }}</h3><div class="table-scroll"><table class="data-table"><thead><tr><th>分区</th><th>Leader</th><th>Replicas</th><th>ISR</th><th>状态</th></tr></thead><tbody><tr v-for="p in topic.item.partitions" :key="p.partition"><td>{{ p.partition }}</td><td>{{ p.leader < 0 ? '未知' : p.leader }}</td><td>{{ p.replicas?.join(', ') || '未知' }}</td><td>{{ p.isr?.join(', ') || '未知' }}</td><td>{{ p.error_code ? reasonLabel(p.error_code) : '已获取' }}</td></tr></tbody></table></div></div>
    </section>
    <section class="panel">
      <h2>Consumer Groups</h2><div class="table-scroll"><table class="data-table"><thead><tr><th>Group</th><th>State</th><th>Protocol</th></tr></thead><tbody><tr v-for="g in groups?.items" :key="g.id"><td><button :data-group="g.id" @click="showGroup(g.id)">{{ g.id }}</button></td><td>{{ g.state || '未知' }}</td><td>{{ g.protocol_type || '未知' }}</td></tr></tbody></table></div>
      <div class="pagination"><button :disabled="busy || groupPage <= 1" @click="groupPage--; load()">上一页 Group</button><span>{{ groupPage }} / 共 {{ groups?.total ?? '未知' }} 项</span><button :disabled="busy || !groups || groupPage * 20 >= groups.total" @click="groupPage++; load()">下一页 Group</button></div>
      <div v-if="selectedGroup" data-offsets class="nested-detail">
        <h3>{{ selectedGroup }} offsets / lag</h3><p>未知值不等于 0，lag 仅代表本次查询。</p><div class="table-scroll"><table class="data-table"><thead><tr><th>Topic / Partition</th><th>Committed</th><th>Log end</th><th>Lag</th><th>状态</th></tr></thead><tbody><tr v-for="o in offsets?.items" :key="`${o.topic}:${o.partition}`"><td>{{ o.topic }} / {{ o.partition }}</td><td>{{ unknown(o.committed) }}</td><td>{{ unknown(o.log_end) }}</td><td>{{ unknown(o.lag) }}</td><td>{{ reasonLabel(o.error_code) }}</td></tr></tbody></table></div>
        <button :disabled="offsetPage <= 1" @click="offsetPage--; showGroup(selectedGroup, false)">上一页 Offset</button><span>{{ offsetPage }} / 共 {{ offsets?.total ?? '未知' }} 项</span><button :disabled="!offsets || offsetPage * 20 >= offsets.total" @click="offsetPage++; showGroup(selectedGroup, false)">下一页 Offset</button>
      </div>
    </section>
  </section>
</template>

<style scoped>
.page-actions, .pagination { display:flex; align-items:center; flex-wrap:wrap; gap:8px; }
.pagination { padding:14px 20px; border-top:1px solid #e8edf3; color:#64748b; font-size:12px; }
.resource-summary { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:16px; margin-bottom:20px; }
.query-evidence { padding:14px 20px; }
.resource-summary > div { background:#fff; border:1px solid #e3e8ef; border-radius:6px; padding:16px 20px; }
.resource-summary span { display:block; color:#64748b; font-size:12px; }
.resource-summary strong { display:block; font-size:24px; margin-top:6px; color:#182c47; }
.table-scroll { overflow-x:auto; }
.panel > h2, .panel > h3 { padding:18px 20px 0; margin:0; }
.panel > p, .panel > ul, .panel > form, .panel > details { margin:16px 20px; }
.panel > form { display:flex; flex-wrap:wrap; align-items:end; gap:14px; }
.panel > form label { display:flex; flex-direction:column; gap:6px; font-size:12px; color:#526176; }
.evidence-meta { padding:12px 16px; border-left:3px solid #cbd5e1; background:#f8fafc; margin:8px 0; font-size:12px; color:#64748b; }
.evidence-meta p { margin:0; }.evidence-meta ul { margin:8px 0 0; }
.nested-detail { padding:16px 20px; background:#f8fafc; border-top:1px solid #e3e8ef; }
.nested-detail h3 { font-size:14px; margin:0 0 12px; }
@media(max-width:640px) { .resource-summary { gap:8px; }.resource-summary > div { padding:12px; }.panel > form label { width:100%; } }
</style>
