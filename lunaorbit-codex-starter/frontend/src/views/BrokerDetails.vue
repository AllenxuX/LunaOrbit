<script setup lang="ts">
import { computed, onBeforeUnmount, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ensureSession, registryRequest, type Cluster } from '../api/registry'
import { kafkaList, kafkaError, type Broker } from '../api/kafkaops'
import { workspaceMeta, type WorkspaceMeta } from '../api/workspace'
import ResourceConfigs from '../components/workspace/ResourceConfigs.vue'
import ResourceOperations from '../components/workspace/ResourceOperations.vue'
import { useRuntime } from '../composables/useRuntime'
const props=defineProps<{id:string;brokerId:string}>()
const route=useRoute(),router=useRouter()
const cluster=shallowRef<Cluster>(),meta=shallowRef<WorkspaceMeta>(),broker=shallowRef<Broker>(),actorId=shallowRef(''),busy=shallowRef(false),error=shallowRef('')
const isDefault=computed(()=>props.brokerId==='default')
const title=computed(()=>isDefault.value?'集群默认配置':`Broker ${props.brokerId}`)
const { runtime } = useRuntime()
const informationMode = computed(() => runtime.value?.information_mode === true)
const tabs=computed(()=>[{id:'overview',label:'概览'},{id:'configs',label:'配置'},...(!informationMode.value?[{id:'operations',label:'操作记录'}]:[])])
const tab=computed(()=>tabs.value.some(item=>item.id===route.query.tab)?String(route.query.tab):isDefault.value?'configs':'overview')
const resource=computed(()=>({type:'BROKER' as const,name:isDefault.value?'':props.brokerId}))
const back=computed(()=>({path:`/kafka/clusters/${encodeURIComponent(props.id)}/details`,query:{tab:'brokers'}}))
let epoch=0
async function findBroker(clusterId:string,brokerId:string,token:number){
  for(let page=1;;page++){
    const result=await kafkaList<Broker>(clusterId,'brokers',{page,pageSize:100})
    if(token!==epoch)return undefined
    const found=result.items.find(item=>String(item.id)===brokerId)
    if(found)return found
    if(!result.items.length||page*result.page_size>=result.total)return undefined
  }
}
async function load(){
  const token=++epoch;busy.value=true;error.value='';cluster.value=undefined;meta.value=undefined;broker.value=undefined;actorId.value=''
  try{
    if(!isDefault.value&&!/^\d+$/.test(props.brokerId))throw new Error('invalid-broker')
    const session=await ensureSession();if(token!==epoch)return
    const [registration,permissions,found]=await Promise.all([registryRequest<Cluster>(`/kafka/clusters/${encodeURIComponent(props.id)}`),workspaceMeta(props.id),isDefault.value?Promise.resolve(undefined):findBroker(props.id,props.brokerId,token)])
    if(token!==epoch)return
    if(!isDefault.value&&!found){error.value='该 Broker 不在当前集群中，请返回 Broker 列表刷新';return}
    cluster.value=registration;meta.value=permissions;broker.value=found;actorId.value=session.user_id
  }catch(e){if(token===epoch)error.value=e instanceof Error&&e.message==='invalid-broker'?'Broker 编号无效':kafkaError(e)}finally{if(token===epoch)busy.value=false}
}
function selectTab(value:string){void router.push({query:{tab:value}})}
watch(()=>[props.id,props.brokerId],load,{immediate:true,flush:'sync'})
onBeforeUnmount(()=>{++epoch})
</script>
<template>
  <section class="broker-page registry">
    <nav class="broker-breadcrumb" aria-label="Broker 路径"><router-link to="/kafka/clusters">Kafka 集群</router-link><span>/</span><router-link :to="back">{{ cluster?.display_name||'集群' }}</router-link><span>/</span><router-link :to="back">Brokers</router-link></nav>
    <header class="page-header"><div class="broker-title"><h1>{{ title }}</h1><span v-if="meta?.synthetic" class="status-pill status-pill--warning">演示数据</span></div><button :disabled="busy" @click="load">刷新</button></header>
    <p v-if="busy" role="status">加载中…</p><p v-if="error" role="alert">{{ error }}</p>
    <template v-if="meta&&!busy&&!error">
      <p v-if="isDefault" class="broker-scope">这里设置整个集群的动态默认配置。查看某台 Broker 的实际生效值，请返回列表选择该 Broker。</p>
      <nav class="broker-tabs" aria-label="Broker 详情"><button v-for="item in tabs" :key="item.id" :aria-current="tab===item.id?'page':undefined" :class="{active:tab===item.id}" @click="selectTab(item.id)">{{ item.label }}</button></nav>
      <section v-if="tab==='overview'" class="panel broker-overview"><h2>{{ isDefault?'配置范围':'基本信息' }}</h2><p v-if="isDefault">对未单独设置这些参数的 Broker 生效。此处不包含各 Broker 的完整配置。</p><dl v-else-if="broker"><div><dt>Broker 编号</dt><dd>{{ broker.id }}</dd></div><div><dt>地址</dt><dd>{{ broker.host }}</dd></div><div><dt>端口</dt><dd>{{ broker.port }}</dd></div></dl><button @click="selectTab('configs')">查看配置</button></section>
      <ResourceConfigs v-else-if="tab==='configs'" :key="`${id}:${brokerId}`" :cluster-id="id" :resource="resource" :meta="meta" :actor-id="actorId" :show-operations="false" :read-only="informationMode" @submitted="selectTab('operations')" />
      <ResourceOperations v-else-if="tab==='operations'" :key="`${id}:${brokerId}`" :cluster-id="id" :resource="resource" :meta="meta" :actor-id="actorId" />
    </template>
  </section>
</template>
<style scoped>.broker-breadcrumb,.broker-title{display:flex;align-items:center;gap:12px;flex-wrap:wrap}.broker-breadcrumb{font-size:13px;color:#64748b;margin-bottom:18px}.broker-title h1{font-size:24px;margin:0}.broker-tabs{display:flex;gap:24px;border-bottom:1px solid #dce5ef;margin:20px 0;overflow:auto}.broker-tabs button{border:0;border-radius:0;border-bottom:2px solid transparent;background:none;padding:12px 2px;white-space:nowrap;color:#64748b}.broker-tabs button.active{color:#2563eb;border-bottom-color:#2563eb;font-weight:600}.broker-overview{padding:20px}.broker-overview h2{font-size:16px;margin:0 0 20px}.broker-overview dl{display:grid;grid-template-columns:1fr 2fr 1fr;gap:20px;margin-bottom:24px}.broker-overview dt,.broker-scope{font-size:13px;color:#64748b;line-height:1.7}.broker-overview dd{margin:8px 0;overflow-wrap:anywhere}@media(max-width:640px){.broker-overview dl{grid-template-columns:1fr}.broker-title h1{font-size:21px}}</style>
