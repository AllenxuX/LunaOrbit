<script setup lang="ts">
import { onUnmounted, shallowRef, watch } from 'vue'
import { resourceOperations, resourceTransition, resourceState, resourceKind, resourceLabel, type ResourceOperation, type Resource } from '../../api/resource-management'
import { workspaceError, type WorkspaceMeta } from '../../api/workspace'
const props = defineProps<{clusterId:string;meta:WorkspaceMeta;actorId:string;refreshVersion?:number;resource?:Resource}>()
const emit = defineEmits<{changed:[]}>()
const items=shallowRef<ResourceOperation[]>([]),selected=shallowRef<ResourceOperation>(),busy=shallowRef(false),error=shallowRef(''),confirmed=shallowRef(false),page=shallowRef(1),total=shallowRef(0)
let epoch=0,disposed=false
async function load(){const token=++epoch;busy.value=true;error.value='';try{const result=await resourceOperations(props.clusterId,page.value,props.resource);if(token===epoch&&!disposed){items.value=result.items;total.value=result.total;const fresh=result.items.find(x=>x.id===selected.value?.id);if(fresh&&selected.value&&fresh.revision>selected.value.revision){selected.value=fresh;confirmed.value=false}}}catch(e){if(token===epoch&&!disposed)error.value=workspaceError(e)}finally{if(token===epoch&&!disposed)busy.value=false}}
async function transition(action:'approve'|'execute'|'refresh') {const op=selected.value;if(!op||busy.value||action==='execute'&&!confirmed.value)return;const token=epoch;busy.value=true;error.value='';confirmed.value=false;try{const result=await resourceTransition(props.clusterId,op,action);if(token!==epoch||disposed)return;selected.value=result;items.value=items.value.map(x=>x.id===result.id?result:x);emit('changed')}catch(e){if(token===epoch&&!disposed)error.value=workspaceError(e)}finally{if(token===epoch&&!disposed)busy.value=false}}
watch(()=>[props.clusterId,props.resource?.type,props.resource?.name],()=>{selected.value=undefined;confirmed.value=false;items.value=[];page.value=1;void load()},{immediate:true,flush:'sync'})
watch(()=>props.refreshVersion,()=>{void load()})
onUnmounted(()=>{disposed=true;++epoch})
</script>
<template>
  <section class="resource-operations panel">
    <header class="panel-heading"><h3>操作记录</h3><button :disabled="busy" @click="load">刷新记录</button></header><p v-if="error" role="alert">{{ error }}</p><p v-if="busy" role="status">正在读取操作状态…</p>
    <div class="table-scroll"><table class="data-table"><thead><tr><th>资源</th><th>操作</th><th>状态</th><th>详情</th></tr></thead><tbody><tr v-for="op in items" :key="op.id"><td>{{ resourceLabel(op.plan.input.resource) }}</td><td>{{ resourceKind(op.plan.input.kind) }}</td><td>{{ resourceState(op.status) }}</td><td><button @click="selected=op;confirmed=false">查看</button></td></tr></tbody></table></div><p v-if="!busy&&!items.length">暂无资源操作记录。</p>
    <div class="operation-actions"><button :disabled="busy||page<=1" @click="page--;load()">上一页</button><span>第 {{ page }} 页，共 {{ total }} 项</span><button :disabled="busy||page*20>=total" @click="page++;load()">下一页</button></div>
    <section v-if="selected" class="operation-detail">
      <h4>{{ resourceLabel(selected.plan.input.resource) }} · {{ resourceKind(selected.plan.input.kind) }} · {{ resourceState(selected.status) }}</h4><details class="operation-metadata"><summary>执行信息</summary><p>提交人：{{ selected.created_by }}；审批人：{{ selected.approved_by||'尚未审批' }}</p></details><p v-if="['submitting','observing','unknown'].includes(selected.status)">等待 Kafka 完成，可刷新状态。</p><ul><li v-for="c in selected.plan.input.changes" :key="c.name">{{ c.name }}：{{ c.operation==='DELETE'?'删除覆盖，恢复继承':`设置为 ${c.value}` }}</li><li v-for="a in selected.plan.input.assignments" :key="a.partition">分区 {{ a.partition }}：{{ a.replicas.join(', ') }}</li></ul>
      <div class="operation-actions"><button v-if="selected.status==='pending_approval'&&meta.permissions.topic_manage" :disabled="busy||selected.created_by===actorId" @click="transition('approve')">审批通过</button><template v-if="selected.status==='approved'&&meta.permissions.topic_manage"><label><input v-model="confirmed" type="checkbox">我已核对范围，确认执行</label><button :disabled="busy||!confirmed||!meta.write_enabled" @click="transition('execute')">执行变更</button></template><button :disabled="busy" @click="transition('refresh')">检查结果</button></div>
    </section>
  </section>
</template>
<style scoped>
.resource-operations{padding:20px;display:grid;gap:14px}.panel-heading,.operation-actions{display:flex;align-items:center;flex-wrap:wrap;gap:12px}.panel-heading{justify-content:space-between}.operation-detail{border-top:1px solid #e8edf3;padding-top:14px}.operation-detail p{color:#64748b;font-size:13px}h3,h4{margin:0}
</style>
