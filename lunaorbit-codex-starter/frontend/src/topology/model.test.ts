import { expect, it } from 'vitest'
import { filterGraph, defaultPositions, linkStatus, mappingRegistrations, nodeIdentity } from './model'
import type { Snapshot, Link } from '../api/topology'
const mapping = { alias: 'alias', status: 'candidate', method: 'bootstrap', confidence: 'low', candidate_ids: ['reg'] }
const links = [
 { id:'one', source:{...mapping,registration_id:'reg',status:'confirmed'},target:mapping,stale:false,verified:false },
 { id:'two', source:mapping,target:mapping,stale:true,verified:false },
] as Link[]
const snapshot = { links, nodes:[{id:'source',kind:'kafka'},{id:'work',kind:'workload'},{id:'target',kind:'kafka'}],edges:[{id:'e1',source:'source',target:'work',link_id:'one'},{id:'e2',source:'work',target:'target',link_id:'one'},{id:'e3',source:'source',target:'target',link_id:'two'}] } as Snapshot
it('lays out fan-out, fan-in and chained Kafka flows even when three links share one MM2 workload',()=>{
 const nodes=['a','b','c','d'].map(id=>({id,kind:'kafka',label:id})).concat([{id:'original',kind:'workload',label:'original'},{id:'mesh',kind:'workload',label:'mesh'}]) as Snapshot['nodes']
 const edges=[['ab','a','original','b'],['ac','a','mesh','c'],['db','d','mesh','b'],['bc','b','mesh','c']].flatMap(([link,from,work,to])=>[{id:link+'in',link_id:link,source:from,target:work},{id:link+'out',link_id:link,source:work,target:to}]) as Snapshot['edges']
 const before=JSON.stringify({nodes,edges}),positions=defaultPositions(nodes,edges)
 expect(positions.a!.x).toBeLessThan(positions.b!.x)
 expect(positions.d!.x).toBeLessThan(positions.b!.x)
 expect(positions.b!.x).toBeLessThan(positions.c!.x)
 expect(positions.original!.x).toBeGreaterThan(positions.a!.x)
 expect(positions.original!.x).toBeLessThan(positions.b!.x)
 for(const [id,p] of Object.entries(positions))for(const [other,q] of Object.entries(positions))if(id!==other)expect(Math.abs(p.x-q.x)>=280||Math.abs(p.y-q.y)>=100).toBe(true)
 expect(defaultPositions([...nodes].reverse(),[...edges].reverse())).toEqual(positions)
 expect(JSON.stringify({nodes,edges})).toBe(before)
})
it('filters by confirmed registry only and preserves both endpoints of each matching link',()=>{
 const graph=filterGraph(snapshot,[{id:'reg',room:'lab',environment:'uat'}],{room:'lab',environment:'uat'},{kafka:true,workloads:true})
 expect(graph.links.map(l=>l.id)).toEqual(['one']);expect(graph.edges).toHaveLength(2);expect(graph.nodes).toHaveLength(3)
 const unknown=filterGraph(snapshot,[{id:'reg',room:'lab',environment:'uat'}],{room:'__unknown__',environment:''},{kafka:true,workloads:true})
 expect(unknown.links).toHaveLength(2)
 const noMatch=filterGraph(snapshot,[{id:'reg',room:'lab',environment:'uat'}],{room:'lab',environment:'prod'},{kafka:true,workloads:true})
 expect(noMatch.links).toHaveLength(0)
})
it('layers never create synthetic edges or mutate discovery relationships',()=>{
 const before=JSON.stringify(snapshot)
 const graph=filterGraph(snapshot,[],{room:'',environment:''},{kafka:true,workloads:false})
 expect(graph.edges.map(e=>e.id)).toEqual(['e3']);expect(graph.nodes.every(n=>n.kind==='kafka')).toBe(true)
 expect(JSON.stringify(snapshot)).toBe(before)
 expect(defaultPositions(snapshot.nodes)['work']).toBeDefined()
 expect(linkStatus(links[1]!)).toContain('过期');expect(linkStatus(links[0]!)).toContain('未验证')
})
it('shows only enabled directions by default and permits explicit disabled evidence inspection',()=>{
 const forward={...links[0]!,state:'observed',enabled:true}
 const reverse={...links[1]!,state:'disabled',enabled:false,stale:false}
 const input={...snapshot,links:[forward,reverse],edges:[...snapshot.edges.slice(0,2),{id:'reverse-in',source:'target',target:'work',link_id:'two'},{id:'reverse-out',source:'work',target:'source',link_id:'two'}]} as Snapshot
 const before=JSON.stringify(input)
 const single=filterGraph(input,[],{room:'',environment:''},{kafka:true,workloads:true})
 expect(single.links.map(link=>link.id)).toEqual(['one'])
 expect(single.edges.map(edge=>edge.link_id)).toEqual(['one','one'])
 const inspect=filterGraph(input,[],{room:'',environment:''},{kafka:true,workloads:true},true)
 expect(inspect.links).toHaveLength(2);expect(inspect.edges).toHaveLength(4)
 const both=filterGraph({...input,links:[forward,{...reverse,state:'observed',enabled:true}]},[],{room:'',environment:''},{kafka:true,workloads:true})
 expect(both.links).toHaveLength(2);expect(both.edges).toHaveLength(4)
 const none=filterGraph({...input,links:[{...forward,state:'disabled'},reverse]},[],{room:'',environment:''},{kafka:true,workloads:true})
 expect(none.links).toHaveLength(0);expect(none.edges).toHaveLength(0);expect(none.nodes).toHaveLength(0)
 expect(JSON.stringify(input)).toBe(before)
})
it('distinguishes disabled declarations and unknown conflicts from ordinary unverified links',()=>{
 expect(linkStatus({...links[0]!,state:'disabled',enabled:false})).toContain('同步已禁用')
 expect(linkStatus({...links[0]!,state:'unknown',enabled:false})).toContain('状态未知')
})
it('places source and target using directional edges rather than hashed node order',()=>{
 const nodes=[{id:'target',kind:'kafka',label:'target'},{id:'work',kind:'workload',label:'MM2'},{id:'source',kind:'kafka',label:'source'}] as Snapshot['nodes']
 const positions=defaultPositions(nodes,snapshot.edges.slice(0,2))
 expect(positions.source!.x).toBeLessThan(positions.work!.x)
 expect(positions.target!.x).toBeGreaterThan(positions.work!.x)
 expect(positions.source!.y).toBe(positions.work!.y);expect(positions.target!.y).toBe(positions.work!.y)
})

it('ignores disabled reverse relationships for layout while preserving all discovery edges', () => {
 const nodes=[{id:'source',kind:'kafka',label:'source'},{id:'work',kind:'workload',label:'MM2'},{id:'target',kind:'kafka',label:'target'}] as Snapshot['nodes']
 const edges=[...snapshot.edges.slice(0,2),{id:'reverse-in',source:'target',target:'work',link_id:'reverse'},{id:'reverse-out',source:'work',target:'source',link_id:'reverse'}] as Snapshot['edges']
 const declarations=[{id:'one',state:'enabled'},{id:'reverse',state:'disabled'}]
 const before=JSON.stringify(edges)
 const positions=defaultPositions(nodes,edges,declarations)
 expect(positions.source!.x).toBeLessThan(positions.work!.x)
 expect(positions.work!.x).toBeLessThan(positions.target!.x)
 expect(Object.keys(positions).sort()).toEqual(['source','target','work'])
 expect(JSON.stringify(edges)).toBe(before)
})

it('keeps active and unknown reverse cycles finite and deterministic', () => {
 const edges=[...snapshot.edges.slice(0,2),{id:'reverse-in',source:'target',target:'work',link_id:'reverse'},{id:'reverse-out',source:'work',target:'source',link_id:'reverse'}] as Snapshot['edges']
 const declarations=[{id:'one',state:'enabled'},{id:'reverse',state:'unknown'}]
 const positions=defaultPositions(snapshot.nodes,edges,declarations)
 expect(Object.values(positions).every(p=>Number.isFinite(p.x)&&Number.isFinite(p.y))).toBe(true)
 expect(defaultPositions(snapshot.nodes,edges,declarations)).toEqual(positions)
 expect(positions.target!.x).toBe(positions.source!.x)
})

it('enriches only owner registrations and keeps candidate identity explicit',()=>{
 const rows=[{id:'reg',display_name:'Lab source',bootstrap_servers:['lab.invalid:9092'],discovered_cluster_id:'real-id'}]
 expect(mappingRegistrations(mapping,rows)).toEqual(rows)
 expect(mappingRegistrations({...mapping,candidate_ids:['foreign']},rows)).toEqual([])
 expect(nodeIdentity(snapshot,'source',rows)).toContain('Lab source')
 expect(nodeIdentity(snapshot,'target',rows)).toContain('候选')
 expect(nodeIdentity(snapshot,'source',rows)).not.toContain('real-id')
 expect(nodeIdentity(snapshot,'source',rows)).not.toContain('lab.invalid')
})
