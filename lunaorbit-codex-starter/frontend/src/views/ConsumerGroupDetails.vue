<script setup lang="ts">
import { computed, onBeforeUnmount, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ensureSession, registryRequest, type Cluster } from '../api/registry'
import { kafkaList, kafkaError, reasonLabel, type Group, type Offset, type Page } from '../api/kafkaops'

const props = defineProps<{ id: string; groupName: string }>()
const route = useRoute(), router = useRouter()
const cluster = shallowRef<Cluster>(), group = shallowRef<Group>(), offsets = shallowRef<Page<Offset>>()
const busy = shallowRef(false), error = shallowRef(''), offsetError = shallowRef(''), page = shallowRef(1)
const tab = computed(() => route.query.tab === 'members' ? 'members' : 'offsets')
const back = computed(() => ({ path: `/kafka/clusters/${encodeURIComponent(props.id)}/details`, query: { tab: 'groups' } }))
const unknown = (value: number | null | undefined) => value ?? '未知'
const state = computed(() => ({ Stable: '稳定', Empty: '无在线成员', Dead: '已停止', PreparingRebalance: '准备重新分配', CompletingRebalance: '正在重新分配' }[group.value?.state ?? ''] ?? group.value?.state ?? '未知'))
let epoch = 0, offsetEpoch = 0
async function loadOffsets() {
  const token = epoch, request = ++offsetEpoch
  offsets.value = undefined; offsetError.value = ''
  try {
    const result = await kafkaList<Offset>(props.id, `groups/${encodeURIComponent(props.groupName)}/offsets`, { page: page.value })
    if (token === epoch && request === offsetEpoch) offsets.value = result
  } catch (e) { if (token === epoch && request === offsetEpoch) offsetError.value = kafkaError(e) }
}
async function load() {
  const token = ++epoch; ++offsetEpoch
  busy.value = true; error.value = ''; offsetError.value = ''; group.value = undefined; cluster.value = undefined; offsets.value = undefined; page.value = 1
  const id = props.id, name = props.groupName
  try {
    await ensureSession()
    if (token !== epoch) return
    const [registration, first] = await Promise.all([registryRequest<Cluster>(`/kafka/clusters/${encodeURIComponent(id)}`), kafkaList<Group>(id, 'groups', { query: name.length <= 128 ? name : '', pageSize: 100 })])
    let result = first, found = result.items.find(item => item.id === name), number = 1
    while (!found && number * 100 < result.total) {
      if (token !== epoch) return
      result = await kafkaList<Group>(id, 'groups', { query: name.length <= 128 ? name : '', pageSize: 100, page: ++number })
      found = result.items.find(item => item.id === name)
    }
    if (token !== epoch) return
    cluster.value = registration
    if (!found) { error.value = '消费组不存在或已被删除'; return }
    group.value = found
    await loadOffsets()
  } catch (e) { if (token === epoch) error.value = kafkaError(e) }
  finally { if (token === epoch) busy.value = false }
}
function selectTab(value: string) { void router.push({ query: { tab: value } }) }
function changePage(delta: number) { page.value += delta; void loadOffsets() }
watch(() => [props.id, props.groupName], load, { immediate: true, flush: 'sync' })
onBeforeUnmount(() => { ++epoch; ++offsetEpoch })
</script>

<template>
  <section class="group-page registry">
    <nav class="group-breadcrumb" aria-label="消费组路径"><router-link to="/kafka/clusters">Kafka 集群</router-link><span>/</span><router-link :to="back">{{ cluster?.display_name || '集群' }}</router-link><span>/</span><router-link :to="back">消费组</router-link></nav>
    <header class="page-header"><h1>{{ groupName }}</h1><button :disabled="busy" @click="load">刷新</button></header>
    <p v-if="busy" role="status">加载中…</p><p v-if="error" role="alert">{{ error }}</p>
    <template v-if="group">
      <dl class="group-summary"><div><dt>状态</dt><dd>{{ state || '未知' }}</dd></div><div><dt>协议</dt><dd>{{ group.protocol_type || '未知' }}</dd></div></dl>
      <p v-if="group.error_code" role="alert">{{ reasonLabel(group.error_code) }}</p>
      <nav class="group-tabs" aria-label="消费组详情"><button :class="{active:tab==='offsets'}" :aria-current="tab==='offsets'?'page':undefined" @click="selectTab('offsets')">消费位点</button><button :class="{active:tab==='members'}" :aria-current="tab==='members'?'page':undefined" @click="selectTab('members')">成员</button></nav>
      <section v-if="tab==='members'" class="panel member-unavailable"><h2>成员</h2><p>暂不支持查看成员信息。</p></section>
      <section v-else class="panel">
        <p v-if="offsetError" class="query-message" role="alert">{{ offsetError }}</p>
        <p v-else-if="!offsets" class="query-message" role="status">正在读取消费位点…</p>
        <template v-else>
          <p v-if="offsets.synthetic" class="query-message">演示数据</p>
          <div class="table-scroll"><table class="data-table"><thead><tr><th>Topic</th><th>分区</th><th>已提交位点</th><th>日志末端位点</th><th>积压消息</th><th>查询状态</th></tr></thead><tbody><tr v-for="item in offsets.items" :key="`${item.topic}:${item.partition}`"><td><router-link :to="`/kafka/clusters/${encodeURIComponent(id)}/topics/${encodeURIComponent(item.topic)}`">{{ item.topic }}</router-link></td><td>{{ item.partition }}</td><td>{{ unknown(item.committed) }}</td><td>{{ unknown(item.log_end) }}</td><td>{{ unknown(item.lag) }}</td><td>{{ item.error_code ? reasonLabel(item.error_code) : item.lag==null ? '位点信息不完整' : '已获取' }}</td></tr></tbody></table></div>
          <p v-if="!offsets.items.length" class="query-message">没有已提交的消费位点</p>
          <div class="pagination"><button :disabled="page<=1" @click="changePage(-1)">上一页</button><span>第 {{ page }} 页 · 共 {{ offsets.total }} 项</span><button :disabled="page*20>=offsets.total" @click="changePage(1)">下一页</button></div>
          <details class="query-info"><summary>查询信息</summary><p>{{ offsets.synthetic?'演示数据':'Kafka 实际查询' }} · {{ offsets.observed_at || '时间未知' }} · {{ offsets.stale?'已过期':'本次查询' }}</p></details>
        </template>
      </section>
    </template>
  </section>
</template>

<style scoped>
.group-breadcrumb{display:flex;gap:12px;flex-wrap:wrap;font-size:13px;color:#64748b;margin-bottom:18px}.page-header h1{font-size:24px;margin:0;overflow-wrap:anywhere}.group-summary{display:flex;gap:48px;padding:22px 0;margin:0 0 20px;border-block:1px solid #dce5ef}.group-summary dt{font-size:13px;color:#64748b}.group-summary dd{font-size:20px;font-weight:600;margin:8px 0 0}.group-tabs{display:flex;gap:24px;border-bottom:1px solid #dce5ef;margin-bottom:20px}.group-tabs button{border:0;border-radius:0;border-bottom:2px solid transparent;background:none;padding:12px 2px;color:#64748b}.group-tabs button.active{color:#2563eb;border-bottom-color:#2563eb;font-weight:600}.table-scroll{overflow:auto}.query-message,.query-info,.pagination,.member-unavailable{padding:16px 20px}.query-message,.query-info,.pagination,.member-unavailable p{font-size:13px;color:#64748b}.pagination{display:flex;gap:12px;align-items:center;flex-wrap:wrap;border-top:1px solid #dce5ef}.member-unavailable h2{font-size:16px;margin:0}.query-info summary{cursor:pointer}@media(max-width:640px){.page-header{gap:16px}.page-header h1{font-size:21px}.group-summary{gap:24px}}
</style>
