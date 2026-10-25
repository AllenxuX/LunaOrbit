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
 it('treats enabled=false as disabled even when the collected state is unknown',async()=>{
  const {snapshotFromRows}=await import('./fixtures')
  const s=snapshotFromRows(['a','b'],[['on','a','b','mm2'],['off','b','a','mm2',false]])
  s.links.forEach(l=>{l.state='unknown'})
  expect(buildClusterPresentation(s).edges.map(e=>e.linkIds[0])).toEqual(['on'])
  expect(buildClusterPresentation(s,{showDisabled:true}).edges.find(e=>e.linkIds[0]==='off')?.state).toBe('disabled')
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

describe('full lineage',()=>{
 const ids=(r:{upstream:string[];downstream:string[]})=>({up:[...r.upstream].sort(),down:[...r.downstream].sort()})
 it('current lab: k8 and k3 examples',async()=>{
  const {lineage}=await import('./cluster-layout'),p=buildClusterPresentation(currentLab())
  expect(ids(lineage(p,'k8'))).toEqual({up:['k1','k2','k3','k4','k5','k6','k7'],down:['k9']})
  expect(ids(lineage(p,'k3'))).toEqual({up:['k1'],down:['k6','k8','k9']})
  expect(ids(lineage(p,'k1'))).toEqual({up:[],down:['k2','k3','k4','k5','k6','k7','k8','k9']})
 })
 it('ignores disabled links even when shown',async()=>{
  const {lineage}=await import('./cluster-layout')
  expect(ids(lineage(buildClusterPresentation(currentLab(),{showDisabled:true}),'k3'))).toEqual({up:['k1'],down:['k6','k8','k9']})
 })
 it('cycles terminate, exclude self and count each cluster once',async()=>{
  const {lineage}=await import('./cluster-layout'),p=buildClusterPresentation(cyclicGraph())
  expect(ids(lineage(p,'c'))).toEqual({up:['a','b','d','e'],down:['d','e','f','g']})
  expect(ids(lineage(p,'a'))).toEqual({up:['b'],down:['b','c','d','e','f','g']})
 })
 it('isolated cluster has no lineage; fan-out/fan-in on the 20 and 40 node graphs match brute force',async()=>{
  const {lineage}=await import('./cluster-layout')
  expect(ids(lineage(buildClusterPresentation(isolatedGraph()),'solo-1'))).toEqual({up:[],down:[]})
  expect(ids(lineage(buildClusterPresentation(isolatedGraph()),'p1'))).toEqual({up:[],down:['p2','p3','p4']})
  const {generatedLarge}=await import('./fixtures')
  for(const p of [buildClusterPresentation(generatedDag()),buildClusterPresentation(generatedLarge().snapshot)]){
   const reach=(from:string,fwd:boolean)=>{const seen=new Set<string>();let frontier=[from];while(frontier.length){const next:string[]=[];for(const e of p.edges){const [a,b]=fwd?[e.source,e.target]:[e.target,e.source];if(frontier.includes(a)&&!seen.has(b)){seen.add(b);next.push(b)}}frontier=next}seen.delete(from);return [...seen].sort()}
   for(const n of p.nodes)expect(ids(lineage(p,n.id))).toEqual({up:reach(n.id,false),down:reach(n.id,true)})
  }
 })
 it('lineage edges are exactly the edges on upstream or downstream paths',async()=>{
  const {lineage}=await import('./cluster-layout'),p=buildClusterPresentation(currentLab())
  const r=lineage(p,'k3'),labels=r.edgeIds.map(id=>p.edges.find(e=>e.id===id)!).map(e=>e.source+'>'+e.target).sort()
  expect(labels).toEqual(['k1>k3','k3>k6','k6>k8','k8>k9'])
 })
})

describe('label placement',()=>{
 it('maps a reserved label box to the distance along the route',async()=>{
  const {labelDistance}=await import('./cluster-layout')
  expect(labelDistance([{x:0,y:0},{x:100,y:0},{x:100,y:50}],{x:40,y:-10,width:20,height:20})).toBe(50)
  expect(labelDistance([{x:0,y:0},{x:100,y:0},{x:100,y:50}],{x:90,y:30,width:20,height:20})).toBe(140)
 })
})

describe('cluster pair relations',()=>{
 it('merges parallel MM2 into one edge labelled ×N and keeps disabled pairs separate',async()=>{
  const {buildRelationPresentation}=await import('./cluster-presentation'),{parallelPair}=await import('./fixtures')
  const p=buildRelationPresentation(parallelPair())
  expect(p.nodes).toHaveLength(9);expect(p.edges).toHaveLength(10)
  const k12=p.edges.find(e=>e.source==='k1'&&e.target==='k2')!
  expect(k12.linkIds.sort()).toEqual(['L01','L11']);expect(k12.label).toBe('×2')
  expect(p.edges.filter(e=>e.linkIds.length===1).every(e=>e.label===undefined)).toBe(true)
  expect(p.edges.reduce((n,e)=>n+e.linkIds.length,0)).toBe(11)
  const all=buildRelationPresentation(parallelPair(),{showDisabled:true})
  expect(all.edges).toHaveLength(20);expect(all.edges.filter(e=>e.state==='disabled')).toHaveLength(10)
 })
 it('lineage on the pair view equals lineage on the link view',async()=>{
  const {buildRelationPresentation,lineage}=await import('./cluster-presentation'),{parallelPair}=await import('./fixtures')
  for(const id of ['k3','k8'])expect({...lineage(buildRelationPresentation(parallelPair()),id),edgeIds:[]}).toEqual({...lineage(buildClusterPresentation(parallelPair()),id),edgeIds:[]})
 })
})

describe('evidence wording',()=>{
 it('names evidence type separately from runtime status and never calls configuration verified',async()=>{
  const {evidenceType,statusText}=await import('./cluster-presentation')
  const link={declared:true,observed:false,verified:false,state:'declared',enabled:true,stale:false}
  expect(evidenceType(link)).toBe('配置关系')
  expect(evidenceType({...link,observed:true})).toBe('配置关系 · 运行资源已发现')
  expect(statusText(link,false)).toBe('状态未知')
  expect(statusText(link,true)).toBe('数据已过期')
  expect(statusText({...link,enabled:false},false)).toBe('已禁用')
  expect(statusText({...link,verified:true},false)).toBe('已验证')
  expect(evidenceType({...link,verified:true})).toBe('配置关系')
 })
})
