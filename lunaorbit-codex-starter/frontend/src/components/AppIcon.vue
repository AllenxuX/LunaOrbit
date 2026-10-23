<script setup lang="ts">
import { computed } from 'vue'
const props = defineProps<{ name: string; size?: number }>()
const paths: Record<string, string> = {
  overview: 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z',
  database: 'M20 6c0 2-3.6 3.5-8 3.5S4 8 4 6s3.6-3.5 8-3.5S20 4 20 6z M4 6v6c0 2 3.6 3.5 8 3.5s8-1.5 8-3.5V6 M4 12v6c0 2 3.6 3.5 8 3.5s8-1.5 8-3.5v-6',
  topology: 'M3 9h5v6H3z M16 3h5v6h-5z M16 12h5v6h-5z M8 12h4V6h4 M12 12v3h4',
  shield: 'M12 3l8 3v6c0 4-4 7.5-8 9-4-1.5-8-5-8-9V6z M8.5 12l2.5 2.5 4.5-5',
  changes: 'M4 4h10l4 4v12H4z M14 4v5h5 M8 13h6 M8 17h6',
  menu: 'M4 6h16 M4 12h16 M4 18h16', close: 'M6 6l12 12 M6 18L18 6',
  'chevron-left': 'M14 5l-7 7 7 7', 'chevron-right': 'M9 5l7 7-7 7',
  'arrow-right': 'M4 12h16 M14 6l6 6-6 6',
  refresh: 'M20 10a8 8 0 0 0-14-5L3 8 M3 3v5h5 M4 14a8 8 0 0 0 14 5l3-3 M21 21v-5h-5',
  user: 'M16 7a4 4 0 1 1-8 0 4 4 0 0 1 8 0z M4 21v-2a8 8 0 0 1 16 0v2',
  search: 'M17 10a7 7 0 1 1-14 0 7 7 0 0 1 14 0z M15 15l6 6',
  check: 'M5 12l4 4L19 6', warning: 'M12 3L2 21h20z M12 9v5 M12 17v1',
  plus: 'M12 4v16 M4 12h16', clock: 'M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0z M12 7v5l3 2',
}
const path = computed(() => paths[props.name] ?? paths.overview)
</script>
<template><svg class="app-icon" :width="size ?? 18" :height="size ?? 18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.65" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path :d="path" /></svg></template>
<style scoped>.app-icon { flex: 0 0 auto; vertical-align: middle; }</style>
