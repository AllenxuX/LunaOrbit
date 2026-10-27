<script setup lang="ts">
import { onMounted, onBeforeUnmount, shallowRef } from 'vue'
import { ApiError } from '../api/client'
import { registryRequest, type Cluster, type ClusterPage } from '../api/registry'
import { listInformationApplications, deleteInformationApplication, type InformationApplication } from '../api/information'
import InformationApplicationForm from '../components/InformationApplicationForm.vue'
const items = shallowRef<InformationApplication[]>([]), clusters = shallowRef<Cluster[]>([])
const page = shallowRef(1), query = shallowRef(''), total = shallowRef(0), busy = shallowRef(false), error = shallowRef(''), notice = shallowRef('')
const formOpen = shallowRef(false), editing = shallowRef<InformationApplication>(), deleting = shallowRef<InformationApplication>()
let epoch = 0
async function load() {
  const token = ++epoch; busy.value = true; error.value = ''
  try { const result = await listInformationApplications(page.value, query.value); if (token === epoch) { items.value = result.items; total.value = result.total } }
  catch { if (token === epoch) error.value = '应用信息暂时不可用，请重读重试。' }
  finally { if (token === epoch) busy.value = false }
}
async function loadClusters() {
  try {
    const result: Cluster[] = []
    for (let current = 1; current <= 100; current++) {
      const data = await registryRequest<ClusterPage>(`/kafka/clusters?page=${current}&page_size=100`)
      result.push(...data.items)
      if (result.length >= data.total || !data.items.length) break
    }
    clusters.value = result
  } catch { error.value = '集群选项加载失败，请重读重试。' }
}
function open(item?: InformationApplication) { editing.value = item; formOpen.value = true }
async function saved() { formOpen.value = false; editing.value = undefined; notice.value = '应用信息已保存，所有访客共享此记录。'; await load() }
async function remove() {
  if (!deleting.value || busy.value) return
  busy.value = true; error.value = ''
  try { await deleteInformationApplication(deleting.value); deleting.value = undefined; notice.value = '应用信息已删除。'; await load() }
  catch (e) { error.value = e instanceof ApiError && e.status === 409 ? '应用已被其他人更新，请取消并重读后重试。' : '删除失败，请重读后重试。' }
  finally { busy.value = false }
}
function clusterName(id: string) { return clusters.value.find(cluster => cluster.id === id)?.display_name ?? '已移除的集群' }
function updatedTime(value?: string) { if (!value) return '未知'; const date = new Date(value); return Number.isFinite(date.getTime()) ? date.toLocaleString('zh-CN', { hour12: false }) : '未知' }
onMounted(() => { void load(); void loadClusters() })
onBeforeUnmount(() => { ++epoch })
</script>
<template>
  <section class="registry information-applications">
    <header class="page-header"><div><h1>应用信息</h1><p>登记业务应用、负责人和 Kafka 生产消费关系。登记关系不等于实际生产消费流量。</p></div><div class="page-actions"><button :disabled="busy" @click="load(); loadClusters()">重读已有数据</button><button class="button-primary" @click="open()">新增应用</button></div></header>
    <p v-if="error" class="error" role="alert">{{ error }}</p><p v-if="notice" role="status">{{ notice }}</p>
    <section class="panel">
      <div class="panel-header"><h2>业务应用</h2><span class="helper-text">共 {{ total }} 个 · 所有访客共享</span></div>
      <form class="filter-bar" @submit.prevent="page = 1; load()"><label>筛选 <input v-model="query" placeholder="名称 / 负责人 / 环境"></label><button :disabled="busy">查询</button></form>
      <div class="table-scroll">
        <table class="data-table">
          <thead><tr><th>应用 / 环境</th><th>负责人 / 部署位置</th><th>生产消费关系</th><th>说明</th><th>更新时间</th><th>操作</th></tr></thead><tbody>
            <tr v-for="item in items" :id="`application-${item.id}`" :key="item.id" :data-information-application="item.id"><td><strong>{{ item.name }}</strong><br><span class="helper-text">{{ item.environment || '环境未登记' }}</span></td><td>{{ item.owner || '待登记' }}<br><span class="helper-text">{{ item.location || '位置未登记' }}</span></td><td><ul v-if="item.relations?.length" class="application-relations"><li v-for="(relation,index) in item.relations" :key="index"><span class="status-pill status-pill--muted">人工登记</span><span v-if="relation.account">账号：{{ relation.account }} · </span><span v-if="relation.client_id">Client ID：{{ relation.client_id }} · </span> {{ clusterName(relation.cluster_id) }} / {{ relation.topic }} · {{ relation.mode === 'produce' ? '生产' : '消费' }}<small v-if="relation.consumer_group"> · {{ relation.consumer_group }}</small></li></ul><span v-else class="helper-text">尚未登记关联</span></td><td>{{ item.description || '—' }}</td><td>{{ updatedTime(item.updated_at) }}</td><td class="actions"><button :disabled="busy" @click="open(item)">编辑</button><button class="button-danger" :disabled="busy" @click="deleting = item">删除</button></td></tr>
            <tr v-if="!items.length"><td colspan="6">{{ busy ? '加载中…' : '暂无应用，点击“新增应用”登记业务信息。' }}</td></tr>
          </tbody>
        </table>
      </div><div class="pagination"><button :disabled="busy || page <= 1" @click="page--; load()">上一页</button><span>第 {{ page }} 页 · {{ total }} 条</span><button :disabled="busy || page * 20 >= total" @click="page++; load()">下一页</button></div>
    </section>
    <InformationApplicationForm v-if="formOpen" :item="editing" :clusters="clusters" @saved="saved" @cancel="formOpen = false" />
    <div v-if="deleting" class="modal-backdrop"><section class="registry-dialog" role="dialog" aria-modal="true" aria-labelledby="delete-application-title"><h2 id="delete-application-title">删除应用信息</h2><p>确认删除「{{ deleting.name }}」及其登记关系？</p><p v-if="error" role="alert">{{ error }}</p><button class="button-danger" :disabled="busy" @click="remove">确认删除</button><button :disabled="busy" @click="deleting = undefined">取消</button></section></div>
  </section>
</template>
<style scoped>.table-scroll{overflow:auto}.application-relations{list-style:none;padding:0;margin:0}.application-relations li+li{margin-top:10px}.information-applications .page-header p{font-size:13px;color:#64748b}.application-relations li{overflow-wrap:anywhere}</style>
