<script setup lang="ts">
import { computed, onUnmounted, shallowRef, watch } from 'vue'
import { kafkaList, kafkaError, reasonLabel, type Broker, type Topic, type Group, type Page } from '../../api/kafkaops'
import type { WorkspaceMeta } from '../../api/workspace'
const props = defineProps<{ clusterId: string; section: 'overview' | 'topics' | 'groups'; canManageTopics: boolean; meta?:WorkspaceMeta;actorId?:string }>()
const emit = defineEmits<{ intent: [input: { kind: 'topic_create' | 'topic_expand' | 'topic_retention'; topic?: string; partitions?: number; replication_factor?: number; retention_ms?: number }] }>()
const brokers = shallowRef<Page<Broker>>(), topics = shallowRef<Page<Topic>>(), groups = shallowRef<Page<Group>>()
const query = shallowRef(''), page = shallowRef(1)
const busy = shallowRef(false), error = shallowRef('')
let epoch = 0, disposed = false
const valid = (token: number) => !disposed && token === epoch
const unknown = (value: number | null | undefined) => value ?? '未知'
const current = computed(() => props.section === 'topics' ? topics.value : props.section === 'groups' ? groups.value : brokers.value)
const evidence = computed(() => [brokers.value, topics.value, groups.value].filter((x): x is Page<Broker> | Page<Topic> | Page<Group> => !!x))
async function load() {
  const token = ++epoch
  busy.value = true; error.value = ''; brokers.value = undefined; topics.value = undefined; groups.value = undefined;
  const id = props.clusterId, section = props.section, options = { page: page.value, query: query.value }
  try {
    if (section === 'overview') {
      const results = await Promise.allSettled([kafkaList<Broker>(id, 'brokers', options), kafkaList<Topic>(id, 'topics', { pageSize: 1 }), kafkaList<Group>(id, 'groups', { pageSize: 1 })])
      if (!valid(token)) return
      const [b, t, g] = results
      if (b.status === 'fulfilled') brokers.value = b.value
      if (t.status === 'fulfilled') topics.value = t.value
      if (g.status === 'fulfilled') groups.value = g.value
      for (const r of results) if (r.status === 'rejected') error.value = kafkaError(r.reason)
    } else if (section === 'topics') {
      const result = await kafkaList<Topic>(id, 'topics', options); if (valid(token)) topics.value = result
    } else {
      const result = await kafkaList<Group>(id, 'groups', options); if (valid(token)) groups.value = result
    }
  } catch (e) { if (valid(token)) error.value = kafkaError(e) }
  finally { if (valid(token)) busy.value = false }
}
function filter() { page.value = 1; void load() }
watch(() => [props.clusterId, props.section], () => { query.value = ''; page.value = 1; void load() }, { immediate: true, flush: 'sync' })
onUnmounted(() => { disposed = true; ++epoch })
</script>

<template>
  <section class="workspace-resources">
    <header class="resource-heading"><div><h2>{{ section === 'overview' ? '集群资源概览' : section === 'topics' ? 'Topics' : '消费组' }}</h2></div><div class="resource-actions"><button :disabled="busy" @click="load">刷新</button><button v-if="section === 'topics' && canManageTopics" class="primary" data-create-topic @click="emit('intent', { kind: 'topic_create' })">创建 Topic</button></div></header>
    <div v-if="section === 'overview'" class="resource-counts"><div><span>Broker</span><strong>{{ brokers?.total ?? '未知' }}</strong></div><div><span>Topics</span><strong>{{ topics?.total ?? '未知' }}</strong></div><div><span>消费组</span><strong>{{ groups?.total ?? '未知' }}</strong></div></div>
    <form v-if="section !== 'overview'" class="resource-filter" @submit.prevent="filter"><label>{{ section === 'topics' ? '搜索 Topic' : '搜索消费组' }} <input v-model="query" maxlength="128" :aria-label="section === 'topics' ? '搜索 Topic' : '搜索消费组'" placeholder="输入名称"></label><button :disabled="busy">查询</button></form>
    <p v-if="busy" role="status">正在读取资源…</p><p v-if="error" role="alert">{{ error }}</p>
    <div class="table-scroll">
      <table v-if="section === 'overview'" class="data-table"><thead><tr><th>Broker ID</th><th>主机</th><th>Port</th></tr></thead><tbody><tr v-for="b in brokers?.items" :key="b.id"><td><router-link class="resource-link" :data-broker="b.id" :to="`/kafka/clusters/${encodeURIComponent(clusterId)}/brokers/${b.id}`">{{ b.id }}</router-link></td><td>{{ b.host }}</td><td>{{ b.port }}</td></tr></tbody></table>
      <table v-else-if="section === 'topics'" class="data-table"><thead><tr><th>Topic</th><th>分区</th><th>副本数范围</th><th>类型</th><th>链路</th></tr></thead><tbody><tr v-for="t in topics?.items" :key="t.name"><td><router-link class="resource-link" :data-topic="t.name" :to="`/kafka/clusters/${encodeURIComponent(clusterId)}/topics/${encodeURIComponent(t.name)}`">{{ t.name }}</router-link></td><td>{{ t.partitions?.length || '未知' }}</td><td>{{ unknown(t.rf_min) }} – {{ unknown(t.rf_max) }}</td><td>{{ t.internal ? '内部' : '业务' }}</td><td><router-link :data-topic-lineage="t.name" :to="{path:'/topic-lineage',query:{cluster_id:clusterId,topic:t.name}}">查看链路</router-link></td></tr></tbody></table>
      <table v-else class="data-table"><thead><tr><th>消费组</th><th>状态</th><th>协议</th></tr></thead><tbody><tr v-for="g in groups?.items" :key="g.id"><td><router-link class="resource-link" :data-group="g.id" :to="`/kafka/clusters/${encodeURIComponent(clusterId)}/groups/${encodeURIComponent(g.id)}`">{{ g.id }}</router-link></td><td>{{ g.state || '未知' }}</td><td>{{ g.protocol_type || '未知' }}</td></tr></tbody></table>
    </div>
    <p v-if="!busy && current && current.items.length === 0" class="empty-state">没有匹配的资源</p>
    <div class="pagination"><button :disabled="busy || page <= 1" @click="page--; load()">上一页</button><span>第 {{ page }} 页 · 共 {{ current?.total ?? '未知' }} 项</span><button :disabled="busy || !current || page * 20 >= current.total" @click="page++; load()">下一页</button></div>

    <details v-if="evidence.length" class="query-evidence"><summary>查询信息</summary><div v-for="(data, index) in evidence" :key="index"><p>{{ data.synthetic ? 'Mock 合成数据，未验证真实 Kafka' : 'Kafka 实际查询' }} · {{ data.observed_at || '查询时间未知' }} · {{ data.stale ? '已过期' : '未标记过期' }} · {{ data.state }} {{ reasonLabel(data.reason) }}</p><ul v-if="data.capabilities?.length"><li v-for="c in data.capabilities" :key="c.feature">{{ c.feature }}：{{ c.supported ? '支持' : '不支持 / 未知' }} {{ !c.supported ? reasonLabel(c.reason) : '' }}</li></ul></div></details>
  </section>
</template>

<style scoped>
.workspace-resources { color:#243650; min-width:0; }
.resource-heading { display:flex; justify-content:space-between; align-items:center; gap:16px; padding:20px; }
.resource-heading h2,.resource-heading h3 { margin:0; font-size:18px; font-weight:650; }
.resource-heading p,.resource-detail > p { color:#64748b; font-size:12px; margin:7px 0 0; }
.resource-actions { display:flex; gap:8px; flex-wrap:wrap; }
.resource-counts { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); margin:0 20px 20px; border:1px solid #dce5ef; border-radius:6px; background:#f7faff; }
.resource-counts > div { padding:17px 22px; border-right:1px solid #dce5ef; }.resource-counts > div:last-child { border:0; }
.resource-counts span { display:block; font-size:12px; color:#64748b; }.resource-counts strong { display:block; font-size:26px; margin-top:6px; font-variant-numeric:tabular-nums; color:#1e426c; }
.resource-filter { display:flex; align-items:end; gap:12px; padding:0 20px 18px; }.resource-filter label { display:flex; flex-direction:column; gap:6px; font-size:12px; color:#526176; }
.table-scroll { overflow:auto; }.data-table { width:100%; }.resource-link { border:0; padding:0; background:transparent; color:#2563a6; text-align:left; word-break:break-word; }
.pagination { display:flex; align-items:center; gap:10px; padding:14px 20px; flex-wrap:wrap; color:#64748b; font-size:12px; border-top:1px solid #e5ebf2; }
.resource-detail { border-top:1px solid #dce5ef; background:#f8fafc; padding:0 0 16px; }.resource-detail > h3,.resource-detail > h4,.resource-detail > p { margin-left:20px; margin-right:20px; }.resource-detail > h3 { padding-top:18px; font-size:15px; }
.config-grid { display:grid; grid-template-columns:minmax(150px,220px) 1fr; gap:8px 20px; padding:0 20px; font-size:12px; }.config-grid dt { color:#64748b; }.config-grid dd { margin:0; font-variant-numeric:tabular-nums; }
.query-evidence { padding:16px 20px; border-top:1px solid #e5ebf2; font-size:12px; color:#64748b; }.query-evidence summary { cursor:pointer; }.empty-state { padding:20px; color:#64748b; }.workspace-resources > [role] { margin:12px 20px; }
button:focus-visible,input:focus-visible { outline:2px solid #4f82c8; outline-offset:3px; }
@media(max-width:640px) { .resource-heading { align-items:flex-start; flex-direction:column; }.resource-counts > div { padding:12px; }.resource-counts strong { font-size:22px; }.resource-filter label { width:100%; }.config-grid { grid-template-columns:1fr 1fr; gap:10px; } }
</style>
