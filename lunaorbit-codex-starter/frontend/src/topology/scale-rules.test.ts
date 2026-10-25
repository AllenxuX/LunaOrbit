import { describe, expect, it } from 'vitest'
import { buildClusterPresentation, layoutClusters, layoutMetrics } from './cluster-layout'
import { cyclicGraph, currentLab, generatedDag, generatedLarge, isolatedGraph } from './fixtures'
import { applyScaleRules, groupClusters, labelBudget, markReturnEdges, needsGrouping } from './scale-rules'

describe('scale thresholds',()=>{
 it('groups only above 30 clusters or 50 enabled MM2',()=>{
  expect(needsGrouping(30,50)).toBe(false);expect(needsGrouping(31,10)).toBe(true);expect(needsGrouping(10,51)).toBe(true)
  const small=buildClusterPresentation(currentLab())
  expect(applyScaleRules(small,{}).grouped).toBe(false)
 })
})

describe('grouped overview (~40 clusters / 60 MM2)',()=>{
 const {snapshot,meta}=generatedLarge(40,60,11)
 const full=buildClusterPresentation(snapshot)
 it('dataset has the requested size',()=>{expect(full.nodes).toHaveLength(40);expect(full.edges).toHaveLength(60)})
 it('enters the grouped overview and conserves cluster and MM2 counts',()=>{
  const r=applyScaleRules(full,meta)
  expect(r.grouped).toBe(true)
  const g=r.groups!
  expect(g.groups.reduce((n,x)=>n+x.clusterIds.length,0)).toBe(40)
  expect(new Set(g.groups.flatMap(x=>x.clusterIds)).size).toBe(40)
  const internal=g.groups.reduce((n,x)=>n+x.internalLinkIds.length,0),cross=r.presentation.edges.reduce((n,e)=>n+e.linkIds.length,0)
  expect(internal+cross).toBe(60)
  expect(g.groups.every(x=>x.clusterIds.length<=12)).toBe(true)
  for(const x of g.groups)expect(x.card).toMatch(/^集群 \d+ · 组内 MM2 \d+ · 失败 0 · 未知 \d+$/)
  expect(r.presentation.edges.every(e=>e.source!==e.target)).toBe(true)
 })
 it('uses environment, room and upstream root set as keys, unknown environment listed separately',()=>{
  const g=groupClusters(full,meta)
  expect(g.basis).toEqual(['环境','机房','上游根集合'])
  expect(g.groups.some(x=>x.label.startsWith('环境未知'))).toBe(true)
 })
 it('skips a grouping level that has no metadata instead of guessing',()=>{
  const noRoom=Object.fromEntries(Object.entries(meta).map(([k,v])=>[k,{environment:v.environment}]))
  expect(groupClusters(full,noRoom).basis).toEqual(['环境','上游根集合'])
 })
 it('expanding one group shows its clusters and keeps conservation',()=>{
  const g=groupClusters(full,meta),first=g.groups[0]!
  const r=applyScaleRules(full,meta,{expanded:new Set([first.id])})
  for(const id of first.clusterIds)expect(r.presentation.nodes.some(n=>n.id===id)).toBe(true)
  const internal=r.groups!.groups.filter(x=>x.id!==first.id).reduce((n,x)=>n+x.internalLinkIds.length,0)
  expect(internal+r.presentation.edges.reduce((n,e)=>n+e.linkIds.length,0)).toBe(60)
 })
 it('grouped layout: no overlaps, nothing through nodes, no backward non-cycle edges',async()=>{
  const r=applyScaleRules(full,meta),plan=await layoutClusters(r.presentation),m=layoutMetrics(r.presentation,plan)
  expect(m.overlaps).toBe(0);expect(m.edgeNodeIntersections).toBe(0);expect(m.backwardNonCycleEdges).toBe(0)
  console.info(`[layout] large40-grouped: ${r.presentation.nodes.length} groups, ${r.presentation.edges.length} cross edges, crossings=${m.crossings}`)
 },30000)
 it('ungrouped 40/60 layout for reference',async()=>{
  const plan=await layoutClusters(full),m=layoutMetrics(full,plan)
  expect(m.overlaps).toBe(0);expect(m.edgeNodeIntersections).toBe(0);expect(m.backwardNonCycleEdges).toBe(0)
  console.info(`[layout] large40-full: 40 clusters, 60 links, crossings=${m.crossings}`)
 },30000)
})

describe('cycles and bidirectional links',()=>{
 it('labels edges drawn right-to-left as return edges, keeps both directions of a bidirectional pair',async()=>{
  const p=buildClusterPresentation(cyclicGraph()),plan=await layoutClusters(p),marked=markReturnEdges(p,plan)
  const ab=marked.edges.filter(e=>e.source==='a'&&e.target==='b'),ba=marked.edges.filter(e=>e.source==='b'&&e.target==='a')
  expect(ab.length).toBe(2);expect(ba.length).toBe(1)
  const back=marked.edges.filter(e=>plan.positions[e.target]!.x<=plan.positions[e.source]!.x)
  expect(back.length).toBeGreaterThan(0)
  expect(back.every(e=>e.label?.endsWith('· 回流/环路'))).toBe(true)
  expect(marked.edges.filter(e=>!back.includes(e)).some(e=>e.label?.includes('回流'))).toBe(false)
 })
})

describe('label budget',()=>{
 it('caps focused labels at 12 and reports shown/total',async()=>{
  const {snapshot}=generatedLarge(40,60,11),p=buildClusterPresentation(snapshot),plan=await layoutClusters(p)
  const focus=p.edges.slice(0,20).map(e=>e.id),b=labelBudget(p,plan,{focusEdgeIds:focus})
  expect(b.shown.length).toBe(12);expect(b.total).toBe(20);expect(b.notice).toBe('已显示 12/20 个标签')
  expect(b.shown.every(id=>focus.includes(id))).toBe(true)
 },30000)
 it('hides colliding labels unless selected and never hides a selected label',()=>{
  const p={nodes:[],edges:['x','y','z'].map(id=>({id,source:'a',target:'b',linkIds:[id],state:'verified' as const,stale:false,label:id}))}
  const plan={positions:{},ports:{},routes:{},nodeWidth:10,nodeHeight:10,labels:{x:{x:0,y:0,width:50,height:20},y:{x:52,y:0,width:50,height:20},z:{x:300,y:0,width:50,height:20}}}
  expect(labelBudget(p,plan,{}).shown.sort()).toEqual(['z'])
  expect(labelBudget(p,plan,{selectedEdgeIds:['y']}).shown.sort()).toEqual(['y','z'])
 })
 it('no focus and no collisions shows every label without a notice',async()=>{
  const p=buildClusterPresentation(isolatedGraph()),plan=await layoutClusters(p),b=labelBudget(p,plan,{})
  expect(b.shown.length).toBe(p.edges.length);expect(b.notice).toBe('')
 })
 it('dag20 reference stays ungrouped',()=>{expect(applyScaleRules(buildClusterPresentation(generatedDag()),{}).grouped).toBe(false)})
})
