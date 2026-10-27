<script setup lang="ts">
import { computed } from 'vue'
import type { ScopeSummary } from '../topology/scopes'
const props=defineProps<{summary:ScopeSummary;clusters:string[];namespaces:{cluster:string;namespace:string}[];cluster:string;namespace:string;includeRemoved:boolean;observedAt:string;busy:boolean;dismissed:string[]}>()
const emit=defineEmits<{'update:cluster':[string];'update:namespace':[string];'update:includeRemoved':[boolean];refresh:[];retry:[string];dismiss:[string]}>()
const nsOptions=computed(()=>props.namespaces.filter(n=>!props.cluster||n.cluster===props.cluster))
const fresh=computed(()=>props.summary.fresh.filter(c=>!props.dismissed.includes(c)))
function time(v:string){const t=new Date(v);return v&&!Number.isNaN(t.getTime())?t.toLocaleString('zh-CN',{hour12:false}):'未知'}
function pickCluster(v:string){emit('update:cluster',v);if(props.namespace&&!props.namespaces.some(n=>n.namespace===props.namespace&&(!v||n.cluster===v)))emit('update:namespace','')}
</script>
<template>
  <section data-scope-bar class="scope-bar panel" aria-label="发现范围">
    <div class="scope-line">
      <strong data-scope-counts>扫描 {{ summary.clusters }} 个 K8s 集群 / {{ summary.namespaces }} 个命名空间，其中 {{ summary.withMM2 }} 个含 MM2</strong>
      <span v-if="summary.partial" class="scope-partial">部分统计</span>
      <span class="helper-text">最后采集 {{ time(observedAt) }}</span>
      <button class="scope-refresh" :disabled="busy" data-scope-refresh @click="emit('refresh')">{{ busy?'采集中…':'重新采集全部范围' }}</button>
    </div>
    <div class="scope-line">
      <label>K8s 集群 <select data-scope-cluster :value="cluster" @change="pickCluster(($event.target as HTMLSelectElement).value)"><option value="">全部</option><option v-for="c in clusters" :key="c" :value="c">{{ c }}</option></select></label>
      <label>命名空间 <select data-scope-namespace :value="namespace" @change="emit('update:namespace',($event.target as HTMLSelectElement).value)"><option value="">全部</option><option v-for="n in nsOptions" :key="n.cluster+'/'+n.namespace" :value="n.namespace">{{ n.cluster }} / {{ n.namespace }}</option></select></label>
      <label><input data-include-removed type="checkbox" :checked="includeRemoved" @change="emit('update:includeRemoved',($event.target as HTMLInputElement).checked)"> 包含已移除范围</label>
      <span class="helper-text">Kite 只列出当前账号有权限的集群；默认排除 kube-system、kube-public、kube-node-lease。</span>
    </div>
    <p v-for="c in fresh" :key="c" data-scope-new class="scope-note scope-new">发现新增集群 <strong>{{ c }}</strong>，已加入扫描范围，等待首次采集。<button aria-label="关闭提示" @click="emit('dismiss',c)">关闭</button></p>
    <p v-for="f in summary.failures" :key="f.scopeId" :data-scope-failure="f.scopeId" class="scope-note scope-fail">范围采集不完整：<strong>{{ f.cluster }}</strong> {{ f.reason }}。{{ f.hasHistory?`最后成功采集 ${time(f.lastSuccess)}，保留历史配置图，数据已过期，不代表没有关系。`:'尚无可用快照。' }}<button :disabled="busy" @click="emit('retry',f.scopeId)">重试此范围</button></p>
    <template v-if="includeRemoved"><p v-for="r in summary.removed" :key="r.scopeId" :data-scope-removed="r.scopeId" class="scope-note scope-removed"><strong>{{ r.cluster }}</strong> 已移除 · 最后采集 {{ time(r.lastSuccess) }}（历史快照与绑定保留；同名重建无法区分是否为同一集群）</p></template>
    <p class="scope-note">未识别到 MM2 不等于没有 MM2；自动识别可能遗漏，疑似资源见“待确认”。</p>
    <details v-if="summary.suspected.length" data-suspected class="scope-suspected"><summary>待确认（{{ summary.suspected.length }}）· 不进入主图</summary><ul><li v-for="w in summary.suspected" :key="w.workload_uid" :title="w.signals.join('；')"><span class="suspect-badge">疑似</span> {{ w.k8s_cluster }} / {{ w.namespace }} / {{ w.name }} <span class="helper-text">依据：{{ w.signals.join('；') }}</span></li></ul></details>
  </section>
</template>
<style scoped>
.scope-bar{padding:12px 16px;display:grid;gap:8px;font-size:13px}
.scope-line{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.scope-line label{display:flex;align-items:center;gap:6px;font-size:12px;color:#63738a}
.scope-line select{max-width:220px}
.scope-partial{color:#9a5b12;background:#fff6e8;border-radius:4px;padding:2px 6px;font-size:12px}
.scope-note{margin:0;font-size:12px;color:#53647c;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.scope-new{color:#1d4f9a;background:#eef4ff;padding:6px 10px;border-radius:5px}
.scope-fail{color:#8a4b08;background:#fff6e8;padding:6px 10px;border-radius:5px}
.scope-removed{color:#6b778c;background:#f3f5f8;padding:6px 10px;border-radius:5px}
.scope-suspected summary{cursor:pointer;color:#8a4b08}
.scope-suspected ul{margin:6px 0 0;padding-left:16px}
.suspect-badge{color:#b45309;border:1px solid #f0c98a;border-radius:4px;padding:0 4px;font-size:11px}
</style>
