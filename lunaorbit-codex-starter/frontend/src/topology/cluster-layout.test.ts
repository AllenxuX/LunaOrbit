import { describe, expect, it } from 'vitest'
import { buildClusterPresentation, directNeighbors, enabledLinkCount, layoutClusters, layoutMetrics, middleEllipsis } from './cluster-layout'
import { currentLab, cyclicGraph, generatedDag, isolatedGraph } from './fixtures'

describe('cluster presentation',()=>{
 it('draws only clusters, one labelled edge per enabled MM2 link and hides disabled links by default',()=>{
  const p=buildClusterPresentation(currentLab())
  expect(p.nodes.every(n=>n.kind==='kafka')).toBe(true)
  expect(p.nodes).toHaveLength(9)
  expect(p.edges).toHaveLength(10)
  expect(p.edges.find(e=>e.linkIds[0]==='L02')?.label).toBe('luna-mm2-k1-to-k3')
  expect(enabledLinkCount(currentLab())).toBe(10)
  const all=buildClusterPresentation(currentLab(),{showDisabled:true})
  expect(all.edges).toHaveLength(20)
  expect(all.edges.filter(e=>e.state==='disabled')).toHaveLength(10)
 })
 it('keeps isolated clusters and parallel MM2 links between the same pair',()=>{
  expect(buildClusterPresentation(isolatedGraph()).nodes.map(n=>n.id)).toContain('solo-2')
  expect(buildClusterPresentation(cyclicGraph()).edges.filter(e=>e.source==='a'&&e.target==='b')).toHaveLength(2)
 })
 it('counts direct upstream and downstream clusters, deduplicated',()=>{
  const p=buildClusterPresentation(currentLab())
  expect(directNeighbors(p,'k8')).toEqual({upstream:3,downstream:1})
  expect(directNeighbors(p,'k1')).toEqual({upstream:0,downstream:3})
  expect(directNeighbors(buildClusterPresentation(cyclicGraph()),'a')).toEqual({upstream:1,downstream:2})
 })
 it('truncates long names in the middle',()=>{
  expect(middleEllipsis('short',18)).toBe('short')
  const s=middleEllipsis('luna-mm2-k1-to-k3-extra-long-name',18)
  expect(s.length).toBe(18);expect(s.startsWith('luna')).toBe(true);expect(s.endsWith('name')).toBe(true);expect(s).toContain('…')
 })
})

const datasets={current9:currentLab(),dag20:generatedDag(20,25,3),cyclic:cyclicGraph(),isolated:isolatedGraph()}
export const crossingReport:Record<string,number>={}
describe('generic layered layout',()=>{
 for(const [name,snapshot] of Object.entries(datasets))it(`${name}: no overlaps, no edge through nodes, left-to-right except cycles`,async()=>{
  const p=buildClusterPresentation(snapshot),plan=await layoutClusters(p),m=layoutMetrics(p,plan)
  expect(Object.keys(plan.positions).sort()).toEqual(p.nodes.map(n=>n.id).sort())
  expect(m.overlaps).toBe(0)
  expect(m.edgeNodeIntersections).toBe(0)
  expect(m.backwardNonCycleEdges).toBe(0)
  crossingReport[name]=m.crossings
  console.info(`[layout] ${name}: ${p.nodes.length} clusters, ${p.edges.length} links, crossings=${m.crossings}, cycle edges=${m.cycleEdges}`)
 },20000)
 it('generated DAG has the requested size and several roots',()=>{
  const p=buildClusterPresentation(datasets.dag20)
  expect(p.nodes).toHaveLength(20);expect(p.edges).toHaveLength(25)
  expect(p.nodes.filter(n=>!p.edges.some(e=>e.target===n.id)).length).toBeGreaterThanOrEqual(3)
 })
 it('detects a backward non-cycle edge when positions are reversed',async()=>{
  const p=buildClusterPresentation(isolatedGraph()),plan=await layoutClusters(p)
  const swapped={...plan,positions:{...plan.positions,p1:plan.positions.p2!,p2:plan.positions.p1!}}
  expect(layoutMetrics(p,swapped).backwardNonCycleEdges).toBeGreaterThan(0)
 })
 it('reserves a label box for every MM2 edge that does not overlap any cluster node',async()=>{
  const p=buildClusterPresentation(currentLab()),plan=await layoutClusters(p)
  expect(Object.keys(plan.labels??{}).sort()).toEqual(p.edges.map(e=>e.id).sort())
  for(const box of Object.values(plan.labels!))for(const n of Object.values(plan.positions))
   expect(box.x<n.x+plan.nodeWidth&&n.x<box.x+box.width&&box.y<n.y+plan.nodeHeight&&n.y<box.y+box.height).toBe(false)
 })
 it('counts crossings of axis-aligned routes exactly',()=>{
  const p={nodes:[],edges:[{id:'h',source:'x',target:'y',linkIds:[],state:'verified' as const,stale:false},{id:'v',source:'u',target:'w',linkIds:[],state:'verified' as const,stale:false}]}
  const plan={positions:{},ports:{},nodeWidth:10,nodeHeight:10,routes:{h:[{x:0,y:5},{x:10,y:5}],v:[{x:5,y:0},{x:5,y:10}]}}
  expect(layoutMetrics(p,plan).crossings).toBe(1)
 })
})
