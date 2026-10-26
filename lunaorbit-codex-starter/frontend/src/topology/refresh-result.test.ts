import { describe, expect, it } from 'vitest'
import { refreshMessage } from './refresh-result'

const fresh = { stale: false, observed_at: '2026-10-26T10:00:00Z', issues: [] }

describe('refreshMessage', () => {
  it('success shows the new collection time', () => {
    const m = refreshMessage(['', ''], [fresh, fresh])
    expect(m.kind).toBe('success')
    expect(m.text).toMatch(/^采集完成，配置采集时间 /)
  })
  it('completed but still untrusted names the reason', () => {
    const m = refreshMessage([''], [{ stale: true, observed_at: fresh.observed_at, issues: [{ code: 'INVALID_RULE' }, { code: 'INVALID_RULE' }, { code: 'PARTIAL_COLLECTION' }] }])
    expect(m).toEqual({ kind: 'untrusted', text: '采集完成，但有 2 条规则无法解析，快照仍不可信' })
    expect(refreshMessage([''], [{ stale: true, observed_at: '', issues: [] }]).text).toBe('采集完成，但快照仍标记为过期（部分范围未完整采集）')
  })
  it('timeout never ends silently', () => {
    expect(refreshMessage(['PENDING'], [fresh])).toEqual({ kind: 'pending', text: '刷新已提交，仍在采集中，请稍后查看' })
  })
  it('failure reports the reason code in words', () => {
    expect(refreshMessage(['KITE_UNAVAILABLE'], [fresh])).toEqual({ kind: 'failed', text: '采集失败：Kite 暂不可用，请稍后重试' })
    expect(refreshMessage(['SOMETHING'], []).text).toBe('采集失败：采集未完整成功，请稍后重试')
  })
})
