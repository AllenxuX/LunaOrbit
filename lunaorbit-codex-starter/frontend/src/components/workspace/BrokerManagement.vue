<script setup lang="ts">
import { onBeforeUnmount, shallowRef, watch } from 'vue'
import { kafkaList, kafkaError, type Broker } from '../../api/kafkaops'
import type { WorkspaceMeta } from '../../api/workspace'
const props=defineProps<{clusterId:string;meta:WorkspaceMeta;actorId:string}>()
const brokers=shallowRef<Broker[]>([]),busy=shallowRef(false),error=shallowRef(''),page=shallowRef(1),total=shallowRef(0)
let epoch=0
function detail(id:string|number){return `/kafka/clusters/${encodeURIComponent(props.clusterId)}/brokers/${encodeURIComponent(id)}`}
async function load(){const token=++epoch;busy.value=true;error.value='';brokers.value=[];try{const data=await kafkaList<Broker>(props.clusterId,'brokers',{page:page.value});if(token===epoch){brokers.value=data.items;total.value=data.total}}catch(e){if(token===epoch)error.value=kafkaError(e)}finally{if(token===epoch)busy.value=false}}
function turn(value:number){page.value=value;void load()}
watch(()=>props.clusterId,()=>{page.value=1;total.value=0;void load()},{immediate:true,flush:'sync'});onBeforeUnmount(()=>{++epoch})
</script>
<template>
  <section class="broker-management panel">
    <header class="broker-heading"><h2>Brokers</h2><div class="broker-actions"><router-link :to="detail('default')">集群默认配置</router-link><button :disabled="busy" @click="load">刷新</button></div></header>
    <p v-if="busy" class="broker-notice" role="status">加载中…</p><p v-if="error" class="broker-notice" role="alert">{{ error }}</p>
    <div v-if="!busy&&!error" class="table-scroll"><table class="data-table"><thead><tr><th>Broker</th><th>地址</th><th>端口</th><th>操作</th></tr></thead><tbody><tr v-for="broker in brokers" :key="broker.id"><td><router-link :to="detail(broker.id)" :data-broker="broker.id">Broker {{ broker.id }}</router-link></td><td>{{ broker.host }}</td><td>{{ broker.port }}</td><td><router-link :to="{path:detail(broker.id),query:{tab:'configs'}}">配置</router-link></td></tr><tr v-if="!brokers.length"><td colspan="4">没有查询到 Broker</td></tr></tbody></table></div>
    <footer v-if="total>20" class="broker-pagination"><button :disabled="busy||page<=1" @click="turn(page-1)">上一页</button><span>第 {{ page }} 页，共 {{ total }} 个 Broker</span><button :disabled="busy||page*20>=total" @click="turn(page+1)">下一页</button></footer>
  </section>
</template>
<style scoped>.broker-heading,.broker-actions,.broker-pagination{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}.broker-heading{padding:18px 20px}.broker-heading h2{font-size:16px;margin:0}.broker-notice,.broker-pagination{padding:16px 20px;margin:0}.table-scroll{overflow:auto}</style>
