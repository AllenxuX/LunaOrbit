import { describe, expect, it } from 'vitest'
import { createElkLayout } from './elk-layout'
import type { Presentation } from './presentation'

const graph=(pairs:[string,string][]):Presentation=>({nodes:[...new Set(pairs.flat())].map(id=>({id,kind:id.startsWith('M')?'workload':'kafka',label:id,replication_factor:null,rf_status:'unknown'})),edges:pairs.map(([source,target],i)=>({id:`edge:${i}`,source,target,linkIds:[],state:'verified',stale:false}))})
function check(plan:Awaited<ReturnType<typeof createElkLayout>>,p:Presentation){
 expect(Object.keys(plan.positions).sort()).toEqual(p.nodes.map(n=>n.id).sort())
 expect(Object.keys(plan.routes).sort()).toEqual(p.edges.map(e=>e.id).sort())
 for(const edge of p.edges){
  const route=plan.routes[edge.id]!,source=plan.positions[edge.source]!,target=plan.positions[edge.target]!,ports=plan.ports[edge.id]!
  expect(ports.source.side).toBe('right');expect(ports.target.side).toBe('left')
  expect(route[0]).toEqual({x:source.x+plan.nodeWidth,y:source.y+ports.source.offset})
  expect(route.at(-1)).toEqual({x:target.x,y:target.y+ports.target.offset})
  for(let i=1;i<route.length;i++){
   const a=route[i-1]!,b=route[i]!
   expect(a.x===b.x||a.y===b.y).toBe(true)
   for(const n of Object.values(plan.positions)){
    const crosses=a.x===b.x?a.x>n.x&&a.x<n.x+plan.nodeWidth&&Math.max(a.y,b.y)>n.y&&Math.min(a.y,b.y)<n.y+plan.nodeHeight:a.y>n.y&&a.y<n.y+plan.nodeHeight&&Math.max(a.x,b.x)>n.x&&Math.min(a.x,b.x)<n.x+plan.nodeWidth
    expect(crosses,`route ${edge.id} crosses node`).toBe(false)
   }
  }
 }
 const positions=Object.values(plan.positions)
 for(let i=0;i<positions.length;i++)for(let j=i+1;j<positions.length;j++){
  const a=positions[i]!,b=positions[j]!
  expect(a.x+plan.nodeWidth<=b.x||b.x+plan.nodeWidth<=a.x||a.y+plan.nodeHeight<=b.y||b.y+plan.nodeHeight<=a.y).toBe(true)
 }
}
describe('ELK layout',()=>{
 it('preserves chain and fork/join IDs, fixed ports and collision-free routes deterministically',async()=>{
  const p=graph([['A','M1'],['M1','B'],['B','M2'],['M2','C'],['A','M3'],['M3','C']]);const before=JSON.stringify(p)
  const result=await createElkLayout(p,180);check(result,p)
  expect(await createElkLayout({...p,nodes:[...p.nodes].reverse(),edges:[...p.edges].reverse()},180)).toEqual(result)
  expect(JSON.stringify(p)).toBe(before)
 })
 it('routes reciprocal, parallel, disabled and self-loop edges outside node bodies',async()=>{
  const p=graph([['A','B'],['B','A'],['A','A'],['A','B']]);p.edges[3]!.state='disabled';check(await createElkLayout(p,224),p)
 })
 it('handles empty and disconnected graphs',async()=>{
  expect(await createElkLayout({nodes:[],edges:[]},224)).toEqual({positions:{},ports:{},routes:{},nodeWidth:224,nodeHeight:76})
  const p=graph([['A','B']]);p.edges=[];check(await createElkLayout(p,224),p)
 })
})

it('routes dense MM2 chain with cycle and parallel tasks without intersecting nodes',async()=>{
 const {RoutingSession}=await import('./stable-routing')
 const routes=[[0,2],[0,3],[1,2],[2,3],[2,4],[4,5],[3,5],[2,0],[0,2]]
 const nodes=[...Array.from({length:6},(_,i)=>({id:'k'+i,kind:'kafka',label:'K'+i})),...routes.map((_,i)=>({id:'task:l'+i,kind:'workload',label:'MM2'}))]
 const edges=routes.flatMap(([a,b],i)=>[{id:'l'+i+':in',source:'k'+a,target:'task:l'+i,linkIds:['l'+i],state:'unverified',stale:false},{id:'l'+i+':out',source:'task:l'+i,target:'k'+b,linkIds:['l'+i],state:'unverified',stale:false}])
 const presentation={nodes,edges} as Parameters<typeof createElkLayout>[0]
 const plan=await createElkLayout(presentation,180)
 expect(()=>new RoutingSession(presentation,plan)).not.toThrow()
})

it('orders fan-in ports by upstream height instead of arbitrary edge IDs',async()=>{
 const p=graph([['D','Mmesh'],['Mmesh','B'],['A','Mmain'],['Mmain','B'],['A','Mdirect'],['Mdirect','C'],['B','Mchain'],['Mchain','C']])
 const plan=await createElkLayout(p,180);check(plan,p)
 for(const target of ['B','C']){
  const incoming=p.edges.filter(e=>e.target===target).sort((a,b)=>(plan.positions[a.source]!.y+plan.ports[a.id]!.source.offset)-(plan.positions[b.source]!.y+plan.ports[b.id]!.source.offset))
  for(let i=1;i<incoming.length;i++)expect(plan.ports[incoming[i-1]!.id]!.target.offset).toBeLessThan(plan.ports[incoming[i]!.id]!.target.offset)
 }
})
