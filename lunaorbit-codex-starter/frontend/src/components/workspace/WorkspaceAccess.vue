<script setup lang="ts">
import { inject, ref, shallowRef, watch } from 'vue'
import { routeLocationKey, routerKey } from 'vue-router'
import WorkspaceApplications from './WorkspaceApplications.vue'
import WorkspaceAccounts from './WorkspaceAccounts.vue'
import WorkspaceACLRules from './WorkspaceACLRules.vue'
import type { Application, PlanInput, WorkspaceMeta } from '../../api/workspace'
defineProps<{clusterId:string;meta:WorkspaceMeta;actorId:string;refreshVersion:number}>()
const emit=defineEmits<{intent:[input:PlanInput]}>()
const route=inject(routeLocationKey,undefined)
const router=inject(routerKey,undefined)
const section=ref(route?.query.access==='accounts'?'accounts':'applications'),openVersion=ref(0)
const accountApplication=shallowRef<Application>()
function rememberSection(value:string){if(router&&route)void router.push({query:{...route.query,access:value}})}
function viewAccounts(app:Application){section.value='accounts';accountApplication.value=app;openVersion.value=0;rememberSection('accounts')}
function choose(value:string){accountApplication.value=undefined;openVersion.value=0;section.value=value;rememberSection(value)}
function onboard(){accountApplication.value=undefined;section.value='accounts';++openVersion.value;rememberSection('accounts')}
watch(()=>route?.query.access,value=>{if(value==='accounts'||value==='rules'||value==='applications')section.value=value;else section.value='applications'})
</script>
<template>
  <section class="access-workspace">
    <nav class="access-views" aria-label="应用访问管理"><button :aria-pressed="section==='applications'" @click="choose('applications')">应用</button><button v-if="meta.permissions.account_manage" :aria-pressed="section==='accounts'" @click="choose('accounts')">账号</button><button v-if="meta.permissions.acl_manage" :aria-pressed="section==='rules'" @click="choose('rules')">权限规则</button><button v-if="meta.permissions.account_manage" class="button-primary" data-onboard-application @click="onboard">新建应用和账号</button></nav>
    <WorkspaceApplications v-if="section==='applications'" :cluster-id="clusterId" :meta="meta" :refresh-version="refreshVersion" @intent="emit('intent',$event)" @accounts="viewAccounts" />
    <WorkspaceAccounts v-else-if="section==='accounts'" :application-id="accountApplication?.id" :application-name="accountApplication?.name" :open-version="openVersion" :cluster-id="clusterId" :meta="meta" :actor-id="actorId" :refresh-version="refreshVersion" @clear-filter="accountApplication=undefined" />
    <WorkspaceACLRules v-else :cluster-id="clusterId" :meta="meta" :refresh-version="refreshVersion" @intent="emit('intent',$event)" />
  </section>
</template>
<style scoped>
.access-workspace{display:grid;gap:16px}.access-views{display:flex;gap:8px;flex-wrap:wrap}.access-views button[aria-pressed=true]{color:#1d4ed8;border-color:#93b4ee;background:#eff6ff}
</style>
