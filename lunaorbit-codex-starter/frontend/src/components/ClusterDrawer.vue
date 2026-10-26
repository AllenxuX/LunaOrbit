<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { PAGE_SIZE, accessLabel, errorText, readGroups, readTopic, readTopics, readable, replicaSummary, sourceLabel, type CatalogEndpoint, type TopicDetail, type TopicSummary } from '../api/catalog'

const props = defineProps<{ endpoint: CatalogEndpoint }>()
defineEmits<{ close: [] }>()

const expanded = ref(false), tab = ref<'topics' | 'groups'>('topics')
const query = ref(''), applied = ref(''), page = ref(1), total = ref(0), topics = ref<TopicSummary[]>([])
const topicsState = ref<'idle' | 'loading' | 'ready' | 'error'>('idle'), topicsError = ref('')
const chosen = ref(''), detail = ref<TopicDetail>(), detailError = ref('')
const groups = ref<string[]>(), groupsState = ref<'idle' | 'loading' | 'ready' | 'error'>('idle'), groupsError = ref('')
let epoch = 0

const access = computed(() => accessLabel(props.endpoint))
const addresses = computed(() => props.endpoint.resolved.length ? props.endpoint.resolved : props.endpoint.addresses)
const shown = computed(() => expanded.value ? [...new Set([...addresses.value, ...props.endpoint.addresses])] : addresses.value.slice(0, 2))
const count = computed(() => Math.max(addresses.value.length, props.endpoint.addresses.length))
const pages = computed(() => Math.max(1, Math.ceil(total.value / PAGE_SIZE)))
const summary = computed(() => detail.value ? replicaSummary(detail.value.partitions) : undefined)
const detailRoute = computed(() => props.endpoint.read_via === 'registration' && props.endpoint.registration_id ? `/kafka/clusters/${encodeURIComponent(props.endpoint.registration_id)}/topics/${encodeURIComponent(chosen.value)}` : `/endpoints/${encodeURIComponent(props.endpoint.id)}/topics/${encodeURIComponent(chosen.value)}`)
const checked = computed(() => props.endpoint.checked_at && !props.endpoint.checked_at.startsWith('0001-') ? new Date(props.endpoint.checked_at).toLocaleString('zh-CN', { hour12: false }) : '')

async function loadTopics() {
  const id = ++epoch, ep = props.endpoint.id
  if (!readable(props.endpoint)) { topicsState.value = 'idle'; return }
  topicsState.value = 'loading'
  try {
    const result = await readTopics(ep, applied.value, page.value)
    if (id !== epoch) return
    topics.value = result.items; total.value = result.total; topicsState.value = 'ready'
  } catch (e) { if (id === epoch) { topicsState.value = 'error'; topicsError.value = errorText(e) } }
}
async function choose(name: string) {
  const id = epoch, ep = props.endpoint.id
  chosen.value = name; detail.value = undefined; detailError.value = ''
  try { const d = await readTopic(ep, name); if (id === epoch && chosen.value === name) detail.value = d } catch (e) { if (id === epoch) detailError.value = errorText(e) }
}
function search() { applied.value = query.value.trim(); page.value = 1; void loadTopics() }
function go(delta: number) { page.value = Math.min(pages.value, Math.max(1, page.value + delta)); void loadTopics() }
async function openGroups() {
  tab.value = 'groups'
  if (groupsState.value !== 'idle' || !readable(props.endpoint)) return
  const id = epoch, ep = props.endpoint.id
  groupsState.value = 'loading'
  try { const g = await readGroups(ep); if (id === epoch) { groups.value = g; groupsState.value = 'ready' } } catch (e) { if (id === epoch) { groupsState.value = 'error'; groupsError.value = errorText(e) } }
}
watch(() => props.endpoint.id, () => {
  expanded.value = false; tab.value = 'topics'; query.value = ''; applied.value = ''; page.value = 1; total.value = 0; topics.value = []
  chosen.value = ''; detail.value = undefined; groups.value = undefined; groupsState.value = 'idle'
  void loadTopics()
}, { immediate: true })
</script>

<template>
  <aside class="cluster-drawer" data-cluster-drawer aria-label="集群信息">
    <header class="drawer-head"><h2>{{ endpoint.display_name || endpoint.aliases.join(' / ') || '手工补充' }}</h2><button data-close-drawer @click="$emit('close')">关闭</button></header>
    <p class="drawer-tags"><span class="tag" data-source>{{ sourceLabel(endpoint) }}</span><span v-if="endpoint.ambiguous?.length" class="tag warn">映射歧义 · 待确认</span></p>
    <dl class="drawer-facts">
      <dt>别名</dt><dd>{{ endpoint.aliases.join('、') || '—' }}</dd>
      <dt>地址</dt><dd data-addresses>{{ shown.join(', ') }}<template v-if="!expanded && count > 2"> 等 {{ count }} 个</template><button v-if="!expanded && (count > 2 || endpoint.addresses.length !== addresses.length)" data-expand-addresses class="link-button" @click="expanded = true">展开全部地址</button><span v-if="!endpoint.resolved.length && endpoint.addresses.length" class="muted"> · 地址未解析</span></dd>
      <dt>访问状态</dt><dd data-access :class="['access', access.tone]">{{ access.symbol }} {{ access.text }}</dd>
      <dt>最近成功采集</dt><dd>{{ endpoint.access === 'readable' && checked ? checked : '尚未成功采集' }}</dd>
      <template v-if="endpoint.environment || endpoint.room"><dt>环境 / 机房</dt><dd data-business>{{ [endpoint.environment, endpoint.room].filter(Boolean).join(' · ') }}</dd></template>
      <template v-if="endpoint.description"><dt>说明</dt><dd data-description>{{ endpoint.description }}</dd></template>
    </dl>
    <p v-if="endpoint.credential_source" class="note" data-credential-source>凭据来源：{{ endpoint.credential_source }}</p>
    <div class="tabs" role="tablist"><button role="tab" data-tab-topics :aria-selected="tab === 'topics'" @click="tab = 'topics'">Topic</button><button role="tab" data-tab-groups :aria-selected="tab === 'groups'" @click="openGroups">消费组（展开查询）</button></div>
    <section v-if="tab === 'topics'" data-topics-panel>
      <p v-if="!readable(endpoint)" class="muted">{{ endpoint.read_via === 'registration' ? '自动发现读取失败，可从登记的集群查看。' : '该集群当前不可读，无法列出 Topic。' }}</p>
      <template v-else>
        <form data-topic-search-form class="row" @submit.prevent="search"><input v-model="query" data-topic-search aria-label="搜索 Topic" placeholder="搜索 Topic"><button type="submit">搜索</button></form>
        <p v-if="topicsState === 'loading'" class="muted">正在读取 Topic…</p>
        <p v-else-if="topicsState === 'error'" class="muted" data-topics-error>{{ topicsError }}<button class="link-button" @click="loadTopics">刷新只读数据</button></p>
        <p v-else-if="topicsState === 'ready' && !topics.length" class="muted">没有匹配的 Topic</p>
        <ul class="topic-list"><li v-for="t in topics" :key="t.name"><button data-topic :aria-pressed="chosen === t.name" @click="choose(t.name)">{{ t.name }}</button><span class="muted">{{ t.partitions }} 分区</span></li></ul>
        <div class="row"><button data-prev :disabled="page <= 1" @click="go(-1)">上一页</button><span data-page>{{ page }} / {{ pages }}</span><button data-next :disabled="page >= pages" @click="go(1)">下一页</button></div>
        <div v-if="chosen" class="note">
          <strong>{{ chosen }}</strong>
          <p v-if="summary" data-topic-summary>分区 {{ summary.partitions }} · 副本 {{ summary.replicas }} · {{ summary.isr }}</p>
          <p v-else-if="detailError" class="muted">{{ detailError }}</p>
          <p v-else class="muted">正在读取…</p>
          <RouterLink data-topic-detail-link :to="detailRoute">打开 Topic 详情</RouterLink>
        </div>
      </template>
    </section>
    <section v-else data-groups-panel>
      <p v-if="!readable(endpoint)" class="muted">该集群当前不可读，无法查询消费组。</p>
      <p v-else-if="groupsState === 'loading'" class="muted">正在查询消费组…</p>
      <p v-else-if="groupsState === 'error'" class="muted">{{ groupsError }}</p>
      <p v-else-if="groups && !groups.length" class="muted">没有消费组</p>
      <ul v-else><li v-for="g in groups" :key="g">{{ g }}</li></ul>
    </section>
  </aside>
</template>

<style scoped>
.cluster-drawer{position:fixed;right:0;top:0;height:100vh;width:min(560px,100vw);background:#fff;border-left:1px solid #ccd8e6;box-shadow:-10px 0 25px #10203a12;padding:22px;overflow:auto;z-index:20}
.drawer-head{display:flex;align-items:center;justify-content:space-between}.drawer-head h2{font-size:18px;margin:0}
.drawer-facts{display:grid;grid-template-columns:110px 1fr;gap:6px 12px;margin:12px 0}.drawer-facts dt{color:#607087}
.tag{background:#edf3fa;padding:2px 6px;border-radius:4px;font-size:12px;margin-right:6px}.tag.warn{background:#fff3da;color:#845600}
.access{font-weight:600;border-radius:4px;padding:1px 6px;justify-self:start}.ok{background:#e4f5ed;color:#176344}.warn{background:#fff3da;color:#845600}.bad{background:#ffebeb;color:#a12432}.gray{background:#eef0f3;color:#586477}.lock{background:#e3e6eb;color:#2f3a4a}
.note{background:#eff5ff;border-radius:6px;padding:10px}.muted{color:#607087;font-size:12px}
.tabs{display:flex;gap:8px;margin:12px 0}.tabs button[aria-selected=true]{background:#e9f1ff;border-color:#2865b4}
.row{display:flex;gap:12px;align-items:center;margin:10px 0}.topic-list{list-style:none;padding:0}.topic-list li{display:flex;justify-content:space-between;border-bottom:1px solid #e1e7ef;padding:6px 0}
.topic-list button,.link-button{background:none;border:0;color:#2459a0;cursor:pointer;padding:0 4px}
</style>
