import { describe, expect, it } from 'vitest'
import { DEFAULT_POLICY, IDENTITY_POLICY, policyLabel, topicMatchLabel } from './policy'

describe('policy labels', () => {
  it('translates only known classes', () => {
    expect(policyLabel(IDENTITY_POLICY)).toBe('保留源 Topic 名称')
    expect(policyLabel(DEFAULT_POLICY)).toContain('分隔符')
    expect(policyLabel('com.example.kafka.CustomReplicationPolicy')).toBe('未识别的命名策略，含义待确认')
    expect(policyLabel('')).toBe('未读取命名策略')
  })
  it('does not guess complex regex', () => {
    expect(topicMatchLabel('commerce_order_.*')).toBe('名称以 commerce_order_ 开头')
    expect(topicMatchLabel('.*')).toBe('全部 Topic')
    expect(topicMatchLabel('orders, payments')).toBe('名称为 orders；名称为 payments')
    expect(topicMatchLabel('(a|b)_[0-9]+')).toBe('匹配条件需查看技术详情')
    expect(topicMatchLabel(undefined)).toBe('未读取匹配规则')
  })
})
