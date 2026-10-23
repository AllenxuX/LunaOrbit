<script setup lang="ts">
import type { Plan } from '../../api/workspace'
defineProps<{ plan: Plan }>()
const fieldName = (key: string) => ({ partitions: '分区数', replication_factor: '副本数', 'retention.ms': '保留时间（毫秒）', topic: 'Topic' })[key] ?? key
</script>
<template>
  <section class="plan-diff" data-plan-diff>
    <h3>变更预览</h3>
    <p v-if="plan.input.principal" class="helper-text">Kafka 账号：{{ plan.input.principal }}</p>
    <div class="diff-counts"><span>新增 {{ plan.additions?.length ?? 0 }} 条权限</span><span>已存在 {{ plan.existing?.length ?? 0 }} 条</span><span>撤销 {{ plan.removals?.length ?? 0 }} 条</span></div>
    <p v-if="plan.retained?.length" class="helper-text">保留 {{ plan.retained.length }} 条未选中规则，保持原样。</p><div v-if="plan.additions?.length || plan.removals?.length" class="table-scroll">
      <table class="data-table">
        <thead><tr><th>变更</th><th>资源</th><th>匹配方式</th><th>操作</th><th>效果 / Host</th></tr></thead><tbody>
          <tr v-for="(rule,index) in [...(plan.additions ?? []),...(plan.removals ?? [])]" :key="index"><td>{{ index < (plan.additions?.length ?? 0) ? '新增' : '撤销' }}</td><td>{{ ({GROUP:'消费组',TOPIC:'Topic',CLUSTER:'集群',TRANSACTIONAL_ID:'事务 ID',DELEGATION_TOKEN:'委托令牌'})[rule.resource_type] ?? rule.resource_type }} · {{ rule.resource_name }}</td><td>{{ rule.pattern_type === 'PREFIXED' ? '前缀范围' : '精确名称' }}</td><td>{{ rule.operation }}</td><td>{{ rule.permission }} / {{ rule.host }}</td></tr>
        </tbody>
      </table>
    </div>
    <dl v-if="Object.keys(plan.after ?? {}).length" class="config-diff"><template v-for="(value,key) in plan.after" :key="key"><dt>{{ fieldName(String(key)) }}</dt><dd>{{ plan.before?.[key] ?? '未存在' }} → <strong>{{ value }}</strong></dd></template></dl>
    <p v-if="plan.existing?.length" class="helper-text">已存在的规则保持不变，不会重复创建。</p>
  </section>
</template>
<style scoped>
.plan-diff { border:1px solid #dbe5f1; border-radius:6px; padding:16px; background:#f8fafc; margin:18px 0; }
h3 { margin:0 0 12px; }.diff-counts { display:flex; flex-wrap:wrap; gap:16px; font-size:12px; color:#526176; margin-bottom:12px; }
.config-diff { display:grid; grid-template-columns:minmax(100px,1fr) 2fr; gap:10px; }.config-diff dd { margin:0; overflow-wrap:anywhere; }
</style>
