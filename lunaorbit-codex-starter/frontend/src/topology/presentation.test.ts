import { expect, it } from 'vitest'
import type { GraphNode, Link, Snapshot } from '../api/topology'
import { buildPresentation, focusPresentation, presentationPositions,tracePaths } from './presentation'

function fixture(rows: [string,string,string,string,Partial<Link>?][]): Snapshot {
 const nodes=new Map<string,GraphNode>()
 const links:Link[]=[],edges:Snapshot['edges']=[]
 for(const [id,source,work,target,patch] of rows){
  for(const [node,kind] of [[source,'kafka'],[work,'workload'],[target,'kafka']] as const)nodes.set(node,{id:node,kind,label:node,replication_factor:null,rf_status:'unknown'})
  links.push({id,state:'observed',enabled:true,verified:false,stale:false,...patch} as Link)
  edges.push({id:id+'-in',source,target:work,link_id:id,relation:'declared_input',stale:false,evidence:[]},{id:id+'-out',source:work,target,link_id:id,relation:'declared_output',stale:false,evidence:[]})
 }
 return {scope_id:'synthetic',synthetic:true,stale:false,nodes:[...nodes.values()],edges,links,issues:[],observed_at:'',attempted_at:''}
}
const mesh=()=>fixture([['ab','a','original','b'],['ac','a','mesh','c'],['db','d','mesh','b'],['bc','b','mesh','c']])

it('extracts exact Kafka relationships per link without inventing shared MM2 cross products',()=>{
 const s=mesh(),before=JSON.stringify(s),p=buildPresentation(s,'relations')
 expect(p.nodes.map(n=>n.id).sort()).toEqual(['a','b','c','d'])
 expect(p.edges.map(e=>e.source+'>'+e.target).sort()).toEqual(['a>b','a>c','b>c','d>b'])
 expect(p.edges.flatMap(e=>e.linkIds).sort()).toEqual(['ab','ac','bc','db'])
 expect(JSON.stringify(s)).toBe(before)
})
it('aggregates parallel active links but preserves disabled and reverse relationships separately',()=>{
 const p=buildPresentation(fixture([['two','a','w2','b',{verified:true}],['one','a','w1','b',{verified:true}],['off','a','w3','b',{state:'disabled',enabled:false}],['reverse','b','w4','a']]),'relations')
 expect(p.edges).toHaveLength(3)
 expect(p.edges.find(e=>e.linkIds.includes('one'))).toMatchObject({source:'a',target:'b',linkIds:['one','two'],state:'verified'})
 expect(p.edges.find(e=>e.linkIds.includes('off'))?.state).toBe('disabled')
 expect(p.edges.find(e=>e.linkIds.includes('reverse'))).toMatchObject({source:'b',target:'a',state:'unverified'})
})
it('never promotes stale, unknown, or partially verified groups to verified',()=>{
 for(const patch of [{verified:false},{state:'unknown'},{stale:true}]){
  const p=buildPresentation(fixture([['one','a','w1','b',{verified:true}],['two','a','w2','b',{verified:true,...patch}]]),'relations')
  expect(p.edges[0]!.state).not.toBe('verified')
  expect(p.edges[0]!.stale).toBe(!!patch.stale)
 }
 const s=fixture([['one','a','w','b',{verified:true}]]);s.stale=true
 expect(buildPresentation(s,'relations').edges[0]).toMatchObject({state:'unknown',stale:true})
})
it('keeps deployment edges individually traceable and ignores incomplete relation pairs',()=>{
 const s=mesh(),p=buildPresentation(s,'deployment')
 expect(p.nodes).toEqual(s.nodes)
 expect(p.edges).toHaveLength(8)
 expect(p.edges.find(e=>e.id==='ac-in')?.linkIds).toEqual(['ac'])
 s.edges=s.edges.filter(e=>e.id!=='ac-out')
 expect(buildPresentation(s,'relations').edges.flatMap(e=>e.linkIds)).not.toContain('ac')
})
it('places fork, join and chain in compact deterministic layers without overlaps',()=>{
 const s=mesh(),p=buildPresentation(s,'relations'),pos=presentationPositions(p)
 expect(pos.a!.x).toBeLessThan(pos.b!.x);expect(pos.d!.x).toBeLessThan(pos.b!.x);expect(pos.b!.x).toBeLessThan(pos.c!.x)
 expect(pos.c!.x-pos.a!.x).toBeLessThanOrEqual(800)
 for(const [id,a] of Object.entries(pos))for(const [other,b] of Object.entries(pos))if(id!==other)expect(Math.abs(a.x-b.x)>=220||Math.abs(a.y-b.y)>=76).toBe(true)
 expect(presentationPositions({nodes:[...p.nodes].reverse(),edges:[...p.edges].reverse()})).toEqual(pos)
})
it('orders connected rows to avoid crossing two independent parallel paths',()=>{
 const p=buildPresentation(fixture([['one','a','w1','z'],['two','b','w2','y']]),'relations'),pos=presentationPositions(p)
 expect((pos.a!.y-pos.b!.y)*(pos.z!.y-pos.y!.y)).toBeGreaterThan(0)
})
it('keeps deployment Kafka layers directional even when shared MM2 would look cyclic',()=>{
 const p=buildPresentation(mesh(),'deployment'),pos=presentationPositions(p)
 expect(pos.a!.x).toBeLessThan(pos.b!.x);expect(pos.b!.x).toBeLessThan(pos.c!.x)
 expect(pos.original!.x).toBeGreaterThan(pos.a!.x);expect(pos.original!.x).toBeLessThan(pos.b!.x)
 for(const [id,a] of Object.entries(pos))for(const [other,b] of Object.entries(pos))if(id!==other)expect(Math.abs(a.x-b.x)>=220||Math.abs(a.y-b.y)>=76).toBe(true)
})
it('handles bidirectional cycles, self edges and disconnected nodes with finite stable positions',()=>{
 const p=buildPresentation(fixture([['ab','a','w','b'],['ba','b','w','a'],['self','c','w2','c']]),'relations')
 const pos=presentationPositions(p)
 expect(Object.keys(pos).sort()).toEqual(['a','b','c'])
 expect(Object.values(pos).every(v=>Number.isFinite(v.x)&&Number.isFinite(v.y))).toBe(true)
 expect(presentationPositions(p)).toEqual(pos)
})
it('focuses direct neighbors separately from bounded upstream and downstream traversal',()=>{
 const p=buildPresentation(mesh(),'relations')
 expect(focusPresentation(p,'b','neighbors').nodeIds).toEqual(['a','b','c','d'])
 expect(focusPresentation(p,'a','neighbors').nodeIds).toEqual(['a','b','c'])
 expect(focusPresentation(p,'b','upstream').nodeIds).toEqual(['a','b','d'])
 expect(focusPresentation(p,'a','downstream').nodeIds).toEqual(['a','b','c'])
 expect(focusPresentation(p,'missing','neighbors')).toEqual({nodeIds:[],edgeIds:[]})
 const cycle=buildPresentation(fixture([['ab','a','w','b'],['ba','b','w','a']]),'relations')
 expect(focusPresentation(cycle,'a','downstream').nodeIds).toEqual(['a','b'])
})

it('connects A through distinct MM2 tasks and one shared B into C without inventing task cross products',()=>{
 const s=mesh(),before=JSON.stringify(s),p=buildPresentation(s,'chain')
 expect(p.nodes.filter(n=>n.kind==='kafka').map(n=>n.id).sort()).toEqual(['a','b','c','d'])
 expect(p.nodes.filter(n=>n.kind==='workload')).toHaveLength(4)
 const ab=p.nodes.find(n=>n.linkId==='ab')!,bc=p.nodes.find(n=>n.linkId==='bc')!
 expect(p.edges.map(e=>e.source+'>'+e.target)).toContain('a>'+ab.id)
 expect(p.edges.map(e=>e.source+'>'+e.target)).toContain(ab.id+'>b')
 expect(p.edges.map(e=>e.source+'>'+e.target)).toContain('b>'+bc.id)
 expect(p.edges.map(e=>e.source+'>'+e.target)).toContain(bc.id+'>c')
 const paths=tracePaths(p,'a','c')
 expect(paths.paths.map(path=>path.linkIds)).toEqual([['ac'],['ab','bc']])
 const pos=presentationPositions(p)
 expect(pos.a!.x).toBeLessThan(pos[ab.id]!.x);expect(pos[ab.id]!.x).toBeLessThan(pos.b!.x)
 expect(pos.b!.x).toBeLessThan(pos[bc.id]!.x);expect(pos[bc.id]!.x).toBeLessThan(pos.c!.x)
 expect(focusPresentation(p,'a','neighbors').nodeIds).toContain('b')
 expect(focusPresentation(p,'a','neighbors').nodeIds).toContain('c')
 expect(focusPresentation(p,'a','neighbors').edgeIds).toHaveLength(4)
 expect(JSON.stringify(s)).toBe(before)
})
it('bounds configuration path enumeration, avoids cycles and excludes disabled declarations',()=>{
 const p=buildPresentation(fixture([['ab','a','w','b'],['ba','b','w','a'],['bc','b','w','c'],['off','a','w','c',{state:'disabled'}]]),'chain')
 expect(tracePaths(p,'a','c').paths.map(path=>path.linkIds)).toEqual([['ab','bc']])
 expect(tracePaths(p,'missing','c').paths).toEqual([])
 expect(tracePaths(p,'a','a').paths).toEqual([])
 const parallel=buildPresentation(fixture([['one','a','w','c'],['two','a','w','c']]),'chain')
 expect(tracePaths(parallel,'a','c',1).truncated).toBe(true)
 expect(tracePaths(parallel,'a','c',1).paths).toHaveLength(1)
})
