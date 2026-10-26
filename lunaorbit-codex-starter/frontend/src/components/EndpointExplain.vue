<script setup lang="ts">
import { computed } from 'vue'
import { registerHref, sanitizeAddress, type EndpointState } from '../topology/unregistered'

const props=withDefaults(defineProps<{endpoint:Pick<EndpointState,'alias'|'state'|'candidateIds'>;addresses:string[];canManual?:boolean;canView?:boolean}>(),{canManual:true,canView:true})
const emit=defineEmits<{manual:[];view:[]}>()
const shown=computed(()=>props.addresses.map(sanitizeAddress).filter(Boolean))
const conclusion=computed(()=>props.endpoint.state==='unregistered'?'没找到 Cluster ID 匹配；地址无候选'
 :props.endpoint.state==='ambiguous'?`没找到 Cluster ID 匹配；地址命中多个候选（${props.endpoint.candidateIds.length} 个）`
 :'没找到 Cluster ID 匹配；地址命中 1 个候选，待确认')
const reason=computed(()=>props.endpoint.state==='unregistered'?'LunaOrbit 中没有登记与这个 MM2 端点对应的 Kafka 集群，或登记时用的地址与 MM2 配置里的地址不同（例如一边是集群内服务名，一边是转发地址）。'
 :props.endpoint.state==='ambiguous'?'多个已登记集群使用了相同的 bootstrap 地址，无法自动判断是哪一个。'
 :'只按地址匹配到一个候选；平台不会仅凭地址自动确认。')
</script>

<template>
  <div class="endpoint-explain" data-endpoint-explain>
    <h3>{{ endpoint.alias }} <span class="endpoint-tag" :class="{unregistered:endpoint.state==='unregistered'}">{{ endpoint.state==='unregistered'?'未登记':'待确认' }}</span></h3>
    <dl>
      <dt>MM2 里的地址</dt><dd data-endpoint-address>{{ shown.length?shown.join(', '):'MM2 配置中未解析出地址' }}</dd>
      <dt>匹配结论</dt><dd data-endpoint-conclusion>{{ conclusion }}</dd>
      <dt>为什么没匹配</dt><dd>{{ reason }}</dd>
      <dt>证据类型</dt><dd>配置关系（未验证）</dd>
    </dl>
    <div class="endpoint-actions">
      <RouterLink :to="registerHref(endpoint.alias,addresses)" data-endpoint-register>登记为新集群</RouterLink>
      <button v-if="canManual" type="button" data-endpoint-manual @click="emit('manual')">从已登记集群中手工指定</button>
      <button v-if="canView" type="button" data-endpoint-view @click="emit('view')">查看该任务配置</button>
    </div>
  </div>
</template>

<style scoped>
.endpoint-explain{border:1px dashed #9aa5b5;border-radius:6px;background:#f4f5f7;padding:10px 12px;margin:10px 0}
.endpoint-explain h3{font-size:14px;margin:0 0 8px;display:flex;gap:8px;align-items:center}
.endpoint-tag{font-size:11px;font-weight:500;padding:1px 6px;border-radius:4px;background:#e2e6ec;color:#4b5565}
.endpoint-tag.unregistered{background:#6b7280;color:#fff}
.endpoint-explain dl{display:grid;grid-template-columns:96px 1fr;gap:4px 8px;margin:0 0 8px;font-size:12px}
.endpoint-explain dt{color:#64748b}.endpoint-explain dd{margin:0;color:#25304a;word-break:break-all}
.endpoint-actions{display:flex;flex-wrap:wrap;gap:8px}.endpoint-actions a,.endpoint-actions button{font-size:12px}
</style>
