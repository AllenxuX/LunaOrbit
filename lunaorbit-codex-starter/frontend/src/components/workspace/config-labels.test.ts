import { expect, it } from 'vitest'
import { configLabel, configSource, configValue, configDescription } from './config-labels'
it('provides concise Chinese explanations and an honest fallback for unknown parameters',()=>{
  expect(configDescription('min.insync.replicas')).toContain('同步副本')
  expect(configDescription('min.insync.replicas')).toContain('acks=all')
  expect(configDescription('log.retention.ms')).toContain('保留')
  expect(configDescription('future.config')).toBe('暂无中文说明。')
  expect(configDescription('min.insync.replicas')).not.toMatch(/<\/?\w|When a producer/)
})
it('uses readable names and exact units without guessing unknown configuration values', () => {
  expect(configLabel('retention.ms')).toBe('消息保留时间')
  expect(configValue('retention.ms','172800000')).toBe('2 天')
  expect(configValue('segment.bytes','1073741824')).toBe('1 GiB')
  expect(configValue('retention.ms','-1')).toBe('不限制（-1）')
  expect(configValue('segment.ms',null)).toBe('未知')
  expect(configValue('retention.ms','1501')).toBe('1501 毫秒')
  expect(configSource('NO_CLUSTER_DEFAULT_OVERRIDE')).toBe('未设置集群覆盖')
  expect(configLabel('future.config')).toBe('future.config')
})
