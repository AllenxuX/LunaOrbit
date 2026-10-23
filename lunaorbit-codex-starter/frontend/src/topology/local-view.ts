import type { View } from '../api/topology'
const key = (workspace: string, scope: string, mode: string) => `lunaorbit.information.view:${encodeURIComponent(workspace)}:${encodeURIComponent(scope)}:${mode}`
export const emptyLocalView = (): View => ({ revision: 0, positions: {}, filters: { room: '', environment: '' }, layers: { kafka: true, workloads: true } })
export function readLocalView(workspace: string, scope: string, mode: string): View | undefined {
  try {
    const raw = localStorage.getItem(key(workspace, scope, mode))
    if (!raw || raw.length > 2_000_000) return
    const view = JSON.parse(raw) as View
    if (!view || !view.positions || Array.isArray(view.positions) || typeof view.positions !== 'object' || !view.filters || typeof view.filters.room !== 'string' || typeof view.filters.environment !== 'string' || !view.layers || typeof view.layers.kafka !== 'boolean' || typeof view.layers.workloads !== 'boolean') return
    if (Object.values(view.positions).some(point => !point || !Number.isFinite(point.x) || !Number.isFinite(point.y))) return
    return { revision: 0, positions: view.positions, filters: { room: view.filters.room, environment: view.filters.environment }, layers: { kafka: view.layers.kafka, workloads: view.layers.workloads } }
  } catch { return }
}
export function saveLocalView(workspace: string, scope: string, mode: string, view: View): boolean {
  try {
    localStorage.setItem(key(workspace, scope, mode), JSON.stringify({ positions: view.positions, filters: { room: view.filters.room, environment: view.filters.environment }, layers: { kafka: view.layers.kafka, workloads: view.layers.workloads } }))
    return true
  } catch { return false }
}
