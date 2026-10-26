import type { Snapshot } from '../api/topology'
import { catalogFor, type CatalogEndpoint } from '../api/catalog'

/** Node id used for a catalog row on the graph. */
export const catalogNodeId = (row: CatalogEndpoint) => `catalog:${row.id}`

/**
 * Collapses per-MM2 alias nodes that the Endpoint Catalog resolved to one logical cluster,
 * so the same cluster is drawn once. Registered nodes and unmatched aliases are kept as-is.
 */
export function mergeByCatalog(snapshot: Snapshot, rows: CatalogEndpoint[]): Snapshot {
  if (!rows.length) return snapshot
  const links = new Map(snapshot.links.map(l => [l.id, l]))
  const rename = new Map<string, string>()
  for (const e of snapshot.edges) {
    const link = links.get(e.link_id)
    if (!link) continue
    const [nodeId, mapping] = e.relation === 'declared_input' ? [e.source, link.source] : e.relation === 'declared_output' ? [e.target, link.target] : ['', undefined]
    if (!nodeId.startsWith('alias:') || !mapping || rename.has(nodeId)) continue
    const row = catalogFor(rows.filter(r => !r.registration_id), { addresses: mapping.bootstrap_addresses ?? [] })
    if (row) rename.set(nodeId, catalogNodeId(row))
  }
  if (!rename.size) return snapshot
  const id = (x: string) => rename.get(x) ?? x
  const seen = new Set<string>()
  const nodes = snapshot.nodes.flatMap(n => { const next = id(n.id); if (seen.has(next)) return []; seen.add(next); return [{ ...n, id: next }] })
  return { ...snapshot, nodes, edges: snapshot.edges.map(e => ({ ...e, source: id(e.source), target: id(e.target) })) }
}
