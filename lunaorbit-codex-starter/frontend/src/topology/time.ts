// One rule for timestamps: empty, unparsable and backend zero values (0001-01-01…) are unknown.
export function validTime(value?: string | null): boolean {
  if (!value) return false
  const t = Date.parse(value)
  return Number.isFinite(t) && new Date(t).getUTCFullYear() > 1970
}
export function formatTime(value?: string | null, fallback = '未知'): string {
  return validTime(value) ? new Date(value!).toLocaleString('zh-CN', { hour12: false }) : fallback
}
