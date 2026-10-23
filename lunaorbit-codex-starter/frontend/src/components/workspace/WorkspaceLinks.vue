<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from 'vue'
import { registryRequest, type ClusterPage } from '../../api/registry'
import { readSnapshot, type Link } from '../../api/topology'
import { mappingName, linkStatus } from '../../topology/model'
import { workspaceError } from '../../api/workspace'
const props = defineProps<{ clusterId: string }>()
const links = ref<(Link & {scopeId:string})[]>([]), names = ref<ClusterPage['items']>([]), busy = ref(false), error = ref('')
let epoch = 0
async function load() {
  const token = ++epoch,clusterId=props.clusterId; busy.value = true; error.value = ''; links.value = [];names.value=[]
  try {
    const scopes = await registryRequest<{ items: { id: string }[] }>('/discovery/scopes')
    const registry: ClusterPage['items'] = []
    for (let page = 1; page <= 100; page++) { const data = await registryRequest<ClusterPage>(`/kafka/clusters?page=${page}&page_size=100`); registry.push(...data.items); if (registry.length >= data.total) break }
    const found: (Link & {scopeId:string})[] = []
    for (const scope of scopes.items.slice(0, 20)) {
      if(token!==epoch)return
      try { const snapshot = await readSnapshot(scope.id); found.push(...snapshot.links.filter(link => link.state !== 'disabled' && [link.source, link.target].some(mapping => mapping.status==='confirmed'&&mapping.registration_id === clusterId)).map(link=>({...link,scopeId:scope.id}))) }
      catch { /* A missing or inaccessible discovery snapshot is not a claim that no MM2 exists. */ }
    }
    if (token === epoch) { links.value = found; names.value = registry }
  } catch (e) { if (token === epoch) error.value = workspaceError(e) } finally { if (token === epoch) busy.value = false }
}
watch(() => props.clusterId, load, { immediate: true })
onBeforeUnmount(() => { ++epoch })
</script>
<template>
  <section class="panel"><div class="panel-header"><div><h2>相关同步链路</h2><span class="helper-text">仅展示当前账号已发现并确认关联的启用方向</span></div><router-link to="/topology">进入同步拓扑</router-link></div><p v-if="error" role="alert" class="error inline-message">{{ error }}</p><div class="table-scroll"><table class="data-table"><thead><tr><th>源集群</th><th>目标集群</th><th>MM2 实例</th><th>Topic 规则</th><th>状态</th><th>操作</th></tr></thead><tbody><tr v-for="link in links" :key="`${link.scopeId}:${link.id}`"><td><router-link v-if="link.source.status==='confirmed'&&link.source.registration_id" :to="`/kafka/clusters/${encodeURIComponent(link.source.registration_id)}/details`">{{ mappingName(link.source,names) }}</router-link><span v-else>{{ mappingName(link.source,names) }}</span></td><td><router-link v-if="link.target.status==='confirmed'&&link.target.registration_id" :to="`/kafka/clusters/${encodeURIComponent(link.target.registration_id)}/details`">{{ mappingName(link.target,names) }}</router-link><span v-else>{{ mappingName(link.target,names) }}</span></td><td>{{ link.workload_name }}</td><td>{{ link.topics_raw || '未知' }}</td><td>{{ linkStatus(link) }}</td><td><router-link :to="`/topology/${encodeURIComponent(link.scopeId)}/links/${encodeURIComponent(link.id)}`">查看详情</router-link></td></tr><tr v-if="!links.length"><td colspan="6" class="empty-state">{{ busy ? '正在读取发现快照…' : '暂无已确认的链路。可到同步拓扑运行发现或确认关联。' }}</td></tr></tbody></table></div></section>
</template>
<style scoped>
h2 { margin:0 0 6px; }.inline-message { padding:12px 20px; }.empty-state { text-align:center; padding:28px; color:#64748b; }
</style>
