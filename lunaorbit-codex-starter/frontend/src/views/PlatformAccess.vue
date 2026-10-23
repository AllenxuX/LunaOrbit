<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ensureSession, registryRequest, type ClusterPage } from '../api/registry'
import { workspaceError } from '../api/workspace'
import WorkspaceTeams from '../components/workspace/WorkspaceTeams.vue'

const route = useRoute(), router = useRouter()
const clusters = ref<ClusterPage['items']>([]), selectedId = ref(''), busy = ref(true), permissionsBusy = ref(false), canManage = ref(false), error = ref('')
const selected = computed(() => clusters.value.find(cluster => cluster.id === selectedId.value))
let epoch = 0, selectionEpoch = 0
async function choose() {
  const requested = typeof route.query.cluster === 'string' ? route.query.cluster : ''
  const next = clusters.value.find(cluster => cluster.id === requested)?.id ?? clusters.value[0]?.id ?? ''
  if (next === selectedId.value) return
  selectedId.value = next; canManage.value = false; error.value = ''
  const token = ++selectionEpoch; permissionsBusy.value = !!next
  if (!next) return
  try {
    const result = await registryRequest<{ can_manage_team: boolean }>(`/kafka/clusters/${encodeURIComponent(next)}/permissions`)
    if (token === selectionEpoch) canManage.value = result.can_manage_team === true
  } catch (e) { if (token === selectionEpoch) error.value = workspaceError(e) } finally { if (token === selectionEpoch) permissionsBusy.value = false }
}
async function load() {
  const token = ++epoch; ++selectionEpoch; busy.value = true; error.value = ''; selectedId.value = ''; clusters.value = []; canManage.value = false
  try {
    await ensureSession(); if (token !== epoch) return
    const rows: ClusterPage['items'] = []
    for (let page = 1; ; page++) {
      const result = await registryRequest<ClusterPage>(`/kafka/clusters?page=${page}&page_size=100`)
      if (token !== epoch) return
      rows.push(...result.items)
      if (!result.items.length || rows.length >= result.total) break
    }
    clusters.value = rows; void choose()
  } catch (e) { if (token === epoch) error.value = workspaceError(e) } finally { if (token === epoch) busy.value = false }
}
const choice = computed({ get: () => selectedId.value, set: (id: string) => { void router.replace({ query: { ...route.query, cluster: id } }) } })
watch(() => route.query.cluster, () => { void choose() })
onBeforeUnmount(() => { ++epoch; ++selectionEpoch })
void load()
</script>
<template>
  <section class="platform-access">
    <header class="page-header"><div><h1>平台访问控制</h1></div><button :disabled="busy" @click="load">刷新集群</button></header>
    <div class="panel access-selector"><label>选择集群<select v-model="choice" name="access_cluster" :disabled="busy || !clusters.length"><option v-for="cluster in clusters" :key="cluster.id" :value="cluster.id">{{ cluster.display_name }}</option></select></label></div>
    <p v-if="error" role="alert" class="error">{{ error }}</p>
    <p v-if="busy || permissionsBusy" role="status">正在读取集群访问权限…</p>
    <template v-else-if="selected">
      <WorkspaceTeams v-if="canManage" :key="selectedId" :cluster-id="selectedId" :can-manage="canManage" />
      <p v-else-if="!error" class="panel access-message">当前身份没有此集群的团队授权管理权限。</p>
    </template>
    <p v-else-if="!busy && !error" class="panel access-message">暂无可访问集群，请先添加集群或联系管理员授权。</p>
  </section>
</template>
<style scoped>
.platform-access { display: grid; gap: 18px; }.access-selector,.access-message { padding: 20px; }.access-selector label { display: grid; gap: 8px; max-width: 420px; }.access-selector p { margin-bottom: 0; }
</style>
