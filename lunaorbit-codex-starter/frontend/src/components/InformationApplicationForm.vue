<script setup lang="ts">
import { ref, shallowRef } from 'vue'
import { ApiError } from '../api/client'
import { saveInformationApplication, type ApplicationDraft, type InformationApplication } from '../api/information'
import type { Cluster } from '../api/registry'
const props = defineProps<{ item?: InformationApplication; clusters: Cluster[] }>()
const emit = defineEmits<{ saved: []; cancel: [] }>()
const draft = ref<ApplicationDraft>(props.item ? { name: props.item.name, owner: props.item.owner, environment: props.item.environment, description: props.item.description, location:props.item.location??'', relations: props.item.relations.map(relation => ({ ...relation })) } : { name: '', owner: '', environment: '', description: '', location:'', relations: [] })
const busy = shallowRef(false), error = shallowRef('')
async function save() {
  if (busy.value) return
  busy.value = true; error.value = ''
  try {
    const { name, owner, environment, description, location, relations } = draft.value
    await saveInformationApplication({ name: name.trim(), owner, environment, description, location, relations }, props.item)
    emit('saved')
  } catch (e) { error.value = e instanceof ApiError && e.status === 409 ? '应用已被其他人更新，请取消并刷新后重新编辑。' : '保存失败，请检查填写内容后重试。' }
  finally { busy.value = false }
}
</script>
<template>
  <div class="modal-backdrop" @keydown.esc="!busy && emit('cancel')">
    <section class="registry-dialog application-dialog" role="dialog" aria-modal="true" aria-labelledby="application-form-title">
      <h2 id="application-form-title">{{ item ? '编辑应用' : '新增应用' }}</h2>
      <p class="helper-text">人工登记业务归属和生产消费关系，关联不代表实际流量已验证。</p>
      <p v-if="error" role="alert" class="error">{{ error }}</p>
      <form @submit.prevent="save">
        <div class="application-fields">
          <label>应用名称<input v-model="draft.name" required maxlength="128" autofocus :disabled="busy"></label>
          <label>负责人<input v-model="draft.owner" maxlength="128" :disabled="busy"></label>
          <label>环境<input v-model="draft.environment" maxlength="128" :disabled="busy"></label>
          <label>部署位置<input v-model="draft.location" maxlength="512" placeholder="机房 / 主机 / 服务地址" :disabled="busy"></label>
          <label class="application-description">说明<textarea v-model="draft.description" maxlength="4096" rows="3" :disabled="busy" /></label>
        </div>
        <div class="relation-heading"><h3>生产 / 消费关联</h3><button type="button" :disabled="busy || draft.relations.length >= 200 || !clusters.length" @click="draft.relations.push({ cluster_id: '', topic: '', mode: 'produce' })">添加关联</button></div>
        <p v-if="!clusters.length" class="helper-text">暂无可用集群，可先保存应用，再添加集群关联。</p>
        <fieldset v-for="(relation,index) in draft.relations" :key="index" class="application-relation" :disabled="busy">
          <legend>关联 {{ index + 1 }} · 人工登记</legend>
          <label>关联集群<select v-model="relation.cluster_id" required><option value="" disabled>选择集群</option><option v-for="cluster in clusters" :key="cluster.id" :value="cluster.id">{{ cluster.display_name }}</option></select></label>
          <label>Topic<input v-model="relation.topic" required maxlength="249" placeholder="填写 Topic 名称"></label>
          <label>关系<select v-model="relation.mode"><option value="produce">生产</option><option value="consume">消费</option></select></label>
          <label>Kafka 账号<input v-model="relation.account" maxlength="256" placeholder="用户名或 Principal，可选"></label>
          <label>Client ID<input v-model="relation.client_id" maxlength="256" placeholder="客户端标识，可选"></label>
          <label v-if="relation.mode === 'consume'">消费组<input v-model="relation.consumer_group" maxlength="249" placeholder="可选"></label>
          <button type="button" @click="draft.relations.splice(index, 1)">移除关联</button>
        </fieldset>
        <div class="application-form-actions"><button class="button-primary" :disabled="busy">{{ busy ? '保存中…' : '保存应用' }}</button><button type="button" :disabled="busy" @click="emit('cancel')">取消</button></div>
      </form>
    </section>
  </div>
</template>
<style scoped>
.application-dialog{max-width:760px;width:calc(100vw - 40px);max-height:90vh;overflow:auto}.application-fields{display:grid;grid-template-columns:1fr 1fr;gap:16px}.application-fields label,.application-relation label{display:grid;gap:7px;font-size:13px}.application-fields input,.application-fields textarea,.application-relation input,.application-relation select{width:100%;box-sizing:border-box}.application-description{grid-column:1/-1}.relation-heading,.application-form-actions{display:flex;align-items:center;gap:12px;margin-top:22px}.relation-heading{justify-content:space-between}.relation-heading h3{font-size:15px}.application-relation{display:grid;grid-template-columns:1fr 1fr;gap:12px;border:1px solid #dce5ef;margin:16px 0;padding:16px}.application-relation legend{font-size:12px;color:#64748b}.application-relation button{justify-self:start}@media(max-width:600px){.application-fields,.application-relation{grid-template-columns:1fr}}
</style>
