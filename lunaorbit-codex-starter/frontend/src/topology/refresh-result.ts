/** Turns the outcome of a "refresh discovery" into one visible message; never silent. */
export interface RefreshSnapshot { stale: boolean; observed_at: string; issues?: { code: string }[] }
export interface RefreshMessage { kind: 'success' | 'untrusted' | 'pending' | 'failed'; text: string }

const UNPARSED = new Set(['INVALID_RULE', 'INVALID_CONFIG', 'UNSUPPORTED_CONFIG'])
const failures: Record<string, string> = { KITE_UNAVAILABLE: 'Kite 暂不可用，请稍后重试', TIMEOUT: '采集超时，请稍后重试', PARTIAL_COLLECTION: '部分范围未完整采集，请稍后重试' }

function time(value: string) {
  const d = new Date(value)
  return !value || value.startsWith('0001-') || !Number.isFinite(d.getTime()) ? '未知' : d.toLocaleString('zh-CN', { hour12: false })
}

/** results: per-scope job outcome ('' done, 'PENDING' still running, 'SCOPE_REMOVED', or an error code). */
export function refreshMessage(results: string[], snapshots: RefreshSnapshot[]): RefreshMessage {
  if (results.includes('PENDING')) return { kind: 'pending', text: '刷新已提交，仍在采集中，请稍后查看' }
  const failed = results.find(r => r && r !== 'SCOPE_REMOVED')
  if (failed) return { kind: 'failed', text: `采集失败：${failures[failed] ?? '采集未完整成功，请稍后重试'}` }
  const unparsed = snapshots.flatMap(s => s.issues ?? []).filter(i => UNPARSED.has(i.code)).length
  if (unparsed) return { kind: 'untrusted', text: `采集完成，但有 ${unparsed} 条规则无法解析，快照仍不可信` }
  if (snapshots.some(s => s.stale)) return { kind: 'untrusted', text: '采集完成，但快照仍标记为过期（部分范围未完整采集）' }
  const latest = snapshots.map(s => s.observed_at).sort().at(-1) ?? ''
  return { kind: 'success', text: `采集完成，配置采集时间 ${time(latest)}` }
}
