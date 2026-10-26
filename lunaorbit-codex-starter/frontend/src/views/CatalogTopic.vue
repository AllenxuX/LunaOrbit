<script setup lang="ts">
import { computed, shallowRef, watch } from 'vue'
import { errorText, readTopic, replicaSummary, type TopicDetail } from '../api/catalog'

const props = defineProps<{ id: string; topicName: string }>()
const detail = shallowRef<TopicDetail>(), error = shallowRef(''), busy = shallowRef(false)
const summary = computed(() => detail.value ? replicaSummary(detail.value.partitions) : undefined)
let epoch = 0
watch(() => [props.id, props.topicName], async () => {
  const token = ++epoch
  busy.value = true; error.value = ''; detail.value = undefined
  try { const d = await readTopic(props.id, props.topicName); if (token === epoch) detail.value = d }
  catch (e) { if (token === epoch) error.value = errorText(e) }
  finally { if (token === epoch) busy.value = false }
}, { immediate: true })
</script>

<template>
  <section class="registry catalog-topic">
    <RouterLink data-back to="/topology">返回拓扑</RouterLink>
    <h1>{{ topicName }}</h1>
    <p class="muted">只读 · 数据来自自动发现的集群</p>
    <p v-if="busy" class="muted">正在读取…</p>
    <p v-else-if="error" class="panel" role="status">{{ error }}</p>
    <template v-else-if="detail && summary">
      <p>分区 {{ summary.partitions }} · 副本 {{ summary.replicas }} · {{ summary.isr }}</p>
      <table class="panel">
        <thead><tr><th>分区</th><th>Leader</th><th>副本数</th><th>ISR</th></tr></thead>
        <tbody><tr v-for="p in detail.partitions" :key="p.partition" data-partition><td>{{ p.partition }}</td><td>{{ p.leader }}</td><td>{{ p.replicas }}</td><td>{{ p.isr }}</td></tr></tbody>
      </table>
    </template>
  </section>
</template>
