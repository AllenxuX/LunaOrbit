<script setup lang="ts">
// UX-v2: separates "re-read existing snapshot" from "re-collect" (triggers discovery).
import { shallowRef } from 'vue'
const props = defineProps<{ scopeLabel: string; busy?: boolean; canCollect?: boolean; rereadLabel?: string }>()
const emit = defineEmits<{ reread: []; collect: [] }>()
const open = shallowRef(false)
function confirm() { open.value = false; emit('collect') }
</script>
<template>
  <div class="read-actions" data-read-actions>
    <button type="button" data-reread :disabled="props.busy" @click="emit('reread')">{{ props.rereadLabel ?? '重读已有数据' }}</button>
    <button type="button" data-collect :disabled="props.busy" @click="open = true">重新采集…</button>
    <small>重读只读取已保存快照，不触发发现。</small>
    <div v-if="open" class="collect-confirm panel" role="dialog" aria-modal="false" aria-label="重新采集范围确认" data-collect-dialog>
      <h3>重新采集 · 范围确认</h3>
      <p>目标范围：<strong class="long-name">{{ props.scopeLabel || '未选择范围' }}</strong></p>
      <p>将重新读取 Kite/MM2 配置与相关元数据，而不只是重读已保存快照。按整个发现范围执行，不只重试失败的 namespace。</p>
      <p>Topic 查询的方向与深度只限制查询结果，不代表采集粒度。采集失败时保留上次成功数据，并标记过期。</p>
      <div class="collect-buttons"><button type="button" class="button-primary" data-collect-confirm :disabled="props.busy || props.canCollect === false" @click="confirm">{{ props.canCollect === false ? '当前模式未启用采集' : '确认重新采集' }}</button><button type="button" @click="open = false">取消</button></div>
    </div>
  </div>
</template>
<style scoped>
.read-actions{position:relative;display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:12px}.read-actions small{color:#64748b;font-size:12px}
.collect-confirm{position:absolute;z-index:20;top:42px;left:0;width:min(460px,92vw);padding:16px;box-shadow:0 8px 24px #0f172a22}.collect-confirm h3{margin:0 0 8px;font-size:14px}.collect-confirm p{font-size:12px;color:#475569;line-height:1.7;margin:0 0 8px}.collect-buttons{display:flex;gap:8px}
.long-name{overflow-wrap:anywhere}
</style>
