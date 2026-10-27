// UX-v2: plain-language naming/matching labels. Only known Kafka classes are
// translated; unknown classes and complex regexes are never guessed.
export const IDENTITY_POLICY = 'org.apache.kafka.connect.mirror.IdentityReplicationPolicy'
export const DEFAULT_POLICY = 'org.apache.kafka.connect.mirror.DefaultReplicationPolicy'

export function policyLabel(policy?: string | null): string {
  const value = (policy ?? '').trim()
  if (!value) return '未读取命名策略'
  if (value === IDENTITY_POLICY || value === 'IdentityReplicationPolicy') return '保留源 Topic 名称'
  if (value === DEFAULT_POLICY || value === 'DefaultReplicationPolicy') return '添加源集群别名前缀（分隔符须结合配置）'
  return '未识别的命名策略，含义待确认'
}

const META = /[\\^$+?()[\]{}|]/
export function topicMatchLabel(raw?: string | null): string {
  const rules = (raw ?? '').split(',').map(s => s.trim()).filter(Boolean)
  if (!rules.length) return '未读取匹配规则'
  const parts: string[] = []
  for (const rule of rules) {
    if (rule === '.*') parts.push('全部 Topic')
    else if (rule.endsWith('.*') && !META.test(rule.slice(0, -2)) && !rule.slice(0, -2).includes('*')) parts.push(`名称以 ${rule.slice(0, -2).replace(/\\\./g, '.')} 开头`)
    else if (!META.test(rule) && !rule.includes('*') && !rule.replace(/\\\./g, '').includes('.')) parts.push(`名称为 ${rule}`)
    else return '匹配条件需查看技术详情'
  }
  return parts.join('；')
}
