import { expect, it } from 'vitest'
import { formatTime, validTime } from './time'
it('treats empty, invalid and zero times as unknown', () => {
  for (const v of ['', undefined, null, 'garbage', '0001-01-01T00:00:00Z', '1970-01-01T00:00:00Z']) expect(validTime(v)).toBe(false)
  expect(formatTime('0001-01-01T00:00:00Z', '未见成功采集')).toBe('未见成功采集')
  expect(formatTime('2026-10-27T02:20:00Z')).toMatch(/2026/)
})
