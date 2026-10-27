<script setup lang="ts">
// Re-collect entry. v3: exactly one place per state — 更多 (compact), failure alert (handle), empty state (start).
import { shallowRef } from 'vue'
const props = defineProps<{ scopeLabel: string; busy?: boolean; canCollect?: boolean; compact?: boolean; handle?: boolean; start?: boolean }>()
const emit = defineEmits<{ reread: []; collect: [] }>()
const open = shallowRef(false)
function confirm() { open.value = false; emit('collect') }
</script>
<template>
  <div class="read-actions" :class="{ 'is-compact': props.compact }" data-read-actions>
    <button v-if="props.handle" type="button" data-handle :disabled="props.busy" @click="open = !open">处理</button>
    <button v-else type="button" data-collect :class="{ 'button-primary': props.start }" :disabled="props.busy || props.canCollect === false" @click="open = true">{{ props.start ? '开始采集' : '重新采集…' }}</button>
    <div v-if="open" class="collect-confirm panel" role="dialog" aria-modal="false" aria-label="重新采集范围确认" data-collect-dialog>
      <h3>{{ props.handle ? '处理未更新的数据' : '重新采集' }}</h3>
      <p>采集范围：<strong class="long-name">{{ props.scopeLabel || '未选择范围' }}</strong></p>
      <p class="muted">完整范围重新采集，不只重试失败部分。</p>
      <div class="collect-buttons">
        <button type="button" class="button-primary" data-collect-confirm :disabled="props.busy || props.canCollect === false" @click="confirm">{{ props.canCollect === false ? '当前模式未启用采集' : '确认重新采集' }}</button>
        <button v-if="props.handle" type="button" data-reread @click="open = false; emit('reread')">重读数据</button>
        <button type="button" @click="open = false">取消</button>
      </div>
    </div>
  </div>
</template>
<style scoped>
.read-actions{position:relative;display:inline-flex;align-items:center;gap:8px}.read-actions.is-compact{display:block}.read-actions.is-compact>button{width:100%;text-align:left}
.collect-confirm{position:absolute;z-index:30;top:38px;right:0;width:min(380px,90vw);padding:14px;box-shadow:0 8px 24px #0f172a22;text-align:left}.is-compact .collect-confirm{right:auto;left:0}.collect-confirm h3{margin:0 0 8px;font-size:14px}.collect-confirm p{font-size:12px;color:#475569;line-height:1.6;margin:0 0 8px}.muted{color:#64748b}.collect-buttons{display:flex;gap:8px;flex-wrap:wrap}
.long-name{overflow-wrap:anywhere}
</style>
