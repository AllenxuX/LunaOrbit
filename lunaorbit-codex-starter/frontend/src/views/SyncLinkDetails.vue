<script setup lang="ts">
import { computed,onBeforeUnmount,shallowRef,watch } from 'vue'
import { ensureSession,registryRequest,type Cluster,type ClusterPage } from '../api/registry'
import { readSnapshot,type Snapshot,type Link,type Mapping } from '../api/topology'
import { linkStatus,mappingName } from '../topology/model'
import { ApiError } from '../api/client'
const props=defineProps<{scopeId:string;linkId:string}>()
const snapshot=shallowRef<Snapshot>(),link=shallowRef<Link>(),clusters=shallowRef<Cluster[]>([]),busy=shallowRef(false),error=shallowRef('')
const back=computed(()=>({path:'/topology',query:{scope:props.scopeId}}))
const sourceName=computed(()=>link.value?mappingName(link.value.source,clusters.value):'源集群')
const targetName=computed(()=>link.value?mappingName(link.value.target,clusters.value):'目标集群')
const title=computed(()=>link.value?`${sourceName.value} → ${targetName.value}`:'同步链路详情')
const status=computed(()=>link.value?linkStatus({...link.value,stale:link.value.stale||!!snapshot.value?.stale}):'')
let epoch=0
function clusterPath(mapping:Mapping){return mapping.status==='confirmed'&&mapping.registration_id?`/kafka/clusters/${encodeURIComponent(mapping.registration_id)}/details`:undefined}
function observedTime(value:string){const date=new Date(value);return Number.isNaN(date.getTime())?'未获取':date.toLocaleString('zh-CN',{hour12:false})}
async function allClusters(token:number){const rows:Cluster[]=[];for(let page=1;page<=100;page++){const result=await registryRequest<ClusterPage>(`/kafka/clusters?page=${page}&page_size=100`);if(token!==epoch)return[];rows.push(...result.items);if(rows.length>=result.total)return rows;if(!result.items.length)break}throw new Error('incomplete-clusters')}
async function load(){
 const token=++epoch;busy.value=true;error.value='';snapshot.value=undefined;link.value=undefined;clusters.value=[]
 try{await ensureSession();if(token!==epoch)return;const[data,registry]=await Promise.all([readSnapshot(props.scopeId),allClusters(token)]);if(token!==epoch)return;const found=data.links.find(item=>item.id===props.linkId);if(!found){error.value='这条链路不在当前发现结果中，请返回同步拓扑刷新';return}snapshot.value=data;link.value=found;clusters.value=registry}
 catch(e){if(token===epoch)error.value=e instanceof ApiError&&e.status===404?'尚未获取同步链路，请返回同步拓扑运行发现':e instanceof ApiError&&e.status===403?'没有查看这条同步链路的权限':'同步链路加载失败，请刷新重试'}finally{if(token===epoch)busy.value=false}
}
watch(()=>[props.scopeId,props.linkId],load,{immediate:true,flush:'sync'})
onBeforeUnmount(()=>{++epoch})
</script>
<template>
  <section class="sync-link-page registry">
    <nav class="sync-breadcrumb" aria-label="同步链路路径"><router-link :to="back">同步拓扑</router-link><span>/</span><span>链路详情</span></nav>
    <header class="page-header"><div><h1>{{ title }}</h1><span v-if="snapshot?.synthetic" class="status-pill status-pill--warning">演示数据</span></div><button :disabled="busy" @click="load">刷新</button></header>
    <p v-if="busy" role="status">加载中…</p><p v-if="error" role="alert">{{ error }}</p>
    <template v-if="link&&!busy&&!error">
      <p class="sync-status">{{ status }}</p>
      <section class="panel sync-endpoints"><div v-for="(mapping,index) in [link.source,link.target]" :key="index"><h2>{{ index===0?'源集群':'目标集群' }}</h2><router-link v-if="clusterPath(mapping)" :to="clusterPath(mapping)!">{{ index===0?sourceName:targetName }}</router-link><strong v-else>{{ index===0?sourceName:targetName }}</strong></div></section>
      <section class="panel sync-topics"><h2>同步 Topic</h2><dl><div><dt>匹配规则</dt><dd><code>{{ link.topics_raw||'尚未获取' }}</code></dd></div><div><dt>命名策略</dt><dd>{{ link.policy||'尚未获取' }}</dd></div></dl><p>这里显示 MM2 配置规则，实际同步 Topic 尚未验证。</p></section>
      <section class="panel sync-runtime"><h2>MM2 运行信息</h2><dl><div><dt>实例</dt><dd>{{ link.workload_name||'尚未获取' }}</dd></div><div><dt>命名空间</dt><dd>{{ link.namespace||'尚未获取' }}</dd></div><div><dt>就绪副本</dt><dd>{{ link.observed?`${link.runtime.ready_replicas} / ${link.runtime.replicas}`:'尚未获取' }}</dd></div><div><dt>最近发现</dt><dd>{{ observedTime(link.observed_at) }}</dd></div><div><dt>配置</dt><dd>{{ link.declared?'已发现':'未发现' }}</dd></div><div><dt>同步验证</dt><dd>{{ link.verified?'已验证':'未验证' }}</dd></div></dl></section>
    </template>
  </section>
</template>
<style scoped>.sync-breadcrumb{display:flex;align-items:center;gap:12px;font-size:13px;color:#64748b;margin-bottom:18px}.page-header h1{font-size:24px;margin:0 0 8px;overflow-wrap:anywhere}.sync-status{font-size:13px;color:#64748b}.sync-endpoints{display:grid;grid-template-columns:1fr 1fr;gap:24px}.sync-endpoints,.sync-topics,.sync-runtime{padding:20px;margin-bottom:18px}.sync-link-page h2{font-size:15px;margin:0 0 16px}.sync-link-page dl{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:22px;margin:0}.sync-link-page dt,.sync-topics p{font-size:12px;color:#64748b}.sync-link-page dd{margin:8px 0 0;overflow-wrap:anywhere}.sync-topics p{margin:18px 0 0}.sync-topics code{white-space:pre-wrap}@media(max-width:640px){.sync-endpoints,.sync-link-page dl{grid-template-columns:1fr}.page-header h1{font-size:21px}}</style>
