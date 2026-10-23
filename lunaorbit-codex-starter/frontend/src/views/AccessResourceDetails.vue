<script setup lang="ts">
import { computed, onBeforeUnmount, shallowRef, watch } from 'vue'
import { useRoute,useRouter } from 'vue-router'
import { ensureSession,registryRequest,type Cluster } from '../api/registry'
import { workspaceMeta,workspaceError,type WorkspaceMeta,type PlanInput } from '../api/workspace'
import WorkspaceApplications from '../components/workspace/WorkspaceApplications.vue'
import WorkspaceAccounts from '../components/workspace/WorkspaceAccounts.vue'
import WorkspacePlanDialog from '../components/workspace/WorkspacePlanDialog.vue'
const props=defineProps<{id:string;applicationId?:string;accountId?:string}>()
const route=useRoute(),router=useRouter()
const cluster=shallowRef<Cluster>(),meta=shallowRef<WorkspaceMeta>(),actorId=shallowRef(''),busy=shallowRef(false),error=shallowRef(''),input=shallowRef<PlanInput|null>(null),refreshVersion=shallowRef(0)
const back=computed(()=>({path:`/kafka/clusters/${encodeURIComponent(props.id)}/details`,query:{tab:'applications',...(props.accountId?{access:'accounts'}:{})}}))
const accountView=computed(()=>!!props.accountId||route.query.tab==='accounts')
let epoch=0
async function load(){const token=++epoch;busy.value=true;error.value='';meta.value=undefined;cluster.value=undefined;input.value=null;try{const session=await ensureSession();if(token!==epoch)return;const[registration,permissions]=await Promise.all([registryRequest<Cluster>(`/kafka/clusters/${encodeURIComponent(props.id)}`),workspaceMeta(props.id)]);if(token!==epoch)return;cluster.value=registration;meta.value=permissions;actorId.value=session.user_id}catch(e){if(token===epoch)error.value=workspaceError(e)}finally{if(token===epoch)busy.value=false}}
function selectTab(tab:string){input.value=null;void router.push({query:{tab}})}
watch(()=>[props.id,props.applicationId,props.accountId],load,{immediate:true,flush:'sync'})
onBeforeUnmount(()=>{++epoch})
</script>
<template>
  <section class="access-resource-page registry">
    <nav class="access-breadcrumb" aria-label="应用与账号路径"><router-link to="/kafka/clusters">Kafka 集群</router-link><span>/</span><router-link :to="back">{{ cluster?.display_name||'集群' }}</router-link><span>/</span><router-link :to="back">{{ accountId?'账号':'应用与权限' }}</router-link></nav>
    <header class="page-header"><h1>{{ accountId?'账号详情':'应用详情' }}</h1><button :disabled="busy" @click="load">刷新</button></header>
    <p v-if="busy" role="status">加载中…</p><p v-if="error" role="alert">{{ error }}</p>
    <template v-if="meta&&!busy&&!error">
      <nav v-if="applicationId" class="access-tabs" aria-label="应用详情"><button :aria-current="!accountView?'page':undefined" :class="{active:!accountView}" @click="selectTab('permissions')">应用与权限</button><button v-if="meta.permissions.account_manage" :aria-current="accountView?'page':undefined" :class="{active:accountView}" @click="selectTab('accounts')">关联账号</button></nav>
      <WorkspaceAccounts v-if="accountView&&meta.permissions.account_manage" :key="`${id}:${accountId||applicationId}`" :cluster-id="id" :meta="meta" :actor-id="actorId" :refresh-version="refreshVersion" :account-id="accountId" :application-id="applicationId" />
      <p v-else-if="accountView" role="alert">没有查看账号的权限</p>
      <WorkspaceApplications v-else-if="applicationId" :key="`${id}:${applicationId}`" :cluster-id="id" :meta="meta" :refresh-version="refreshVersion" :application-id="applicationId" @intent="input=$event" @accounts="selectTab('accounts')" />
      <WorkspacePlanDialog :cluster-id="id" :input="input" :meta="meta" :actor-id="actorId" @close="input=null" @completed="refreshVersion++" />
    </template>
  </section>
</template>
<style scoped>.access-breadcrumb{display:flex;align-items:center;gap:12px;flex-wrap:wrap;font-size:13px;color:#64748b;margin-bottom:18px}.page-header h1{font-size:24px;margin:0}.access-tabs{display:flex;gap:24px;border-bottom:1px solid #dce5ef;margin:20px 0;overflow:auto}.access-tabs button{border:0;border-radius:0;border-bottom:2px solid transparent;background:none;padding:12px 2px;white-space:nowrap;color:#64748b}.access-tabs button.active{color:#2563eb;border-bottom-color:#2563eb;font-weight:600}</style>
