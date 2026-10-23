import { expect,it } from 'vitest'
import type { Presentation } from './presentation'
import type { LayoutPlan } from './routing-types'
import { RoutingSession,segmentCrossesNode } from './stable-routing'
function fixture(parallel=1){
 const positions={a:{x:0,y:0},b:{x:400,y:0},c:{x:0,y:250},d:{x:400,y:250},o:{x:200,y:400}}
 const p:Presentation={nodes:Object.keys(positions).map(id=>({id,label:id,kind:'kafka',replication_factor:null,rf_status:'unknown'})),edges:[['ab','a','b'],['cd','c','d']].map(([id,source,target])=>({id:id!,source:source!,target:target!,linkIds:[],state:'verified',stale:false}))}
 const plan:LayoutPlan={positions,ports:{ab:{source:{side:'right',offset:30},target:{side:'left',offset:30}},cd:{source:{side:'right',offset:30},target:{side:'left',offset:30}}},routes:{ab:[{x:100,y:30},{x:400,y:30}],cd:[{x:100,y:280},{x:400,y:280}]},nodeWidth:100,nodeHeight:60}
 for(let i=1;i<parallel;i++){const id='parallel-'+i;p.edges.push({...p.edges[0]!,id});plan.ports[id]=plan.ports.ab!;plan.routes[id]=plan.routes.ab!}
 return new RoutingSession(p,plan)
}
for(const dy of [10,30,100])it(`preserves unrelated route during ${dy}px move`,()=>{const s=fixture(),old=s.getRoutes().cd,u=s.moveNode('a',{x:0,y:dy});expect(u.routes.cd).toBe(old);expect(u.routes.ab?.[0]).toEqual({x:100,y:dy+30});expect(u.changedEdgeIds).toEqual(['ab'])})
it('keeps fixed ports after reversal and avoids every node',()=>{const s=fixture();s.moveNode('a',{x:600,y:0});expect(s.getRoutes().ab?.[0]).toEqual({x:700,y:30});for(const route of Object.values(s.getRoutes()))for(let i=1;i<route.length;i++)for(const p of Object.values(s.getPositions()))expect(segmentCrossesNode(route[i-1]!,route[i]!,p,100,60)).toBe(false)})
it('only reroutes paths actually blocked by a moved obstacle',()=>{const s=fixture();expect(s.moveNode('o',{x:200,y:100}).changedEdgeIds).toEqual([]);expect(s.moveNode('o',{x:200,y:0}).changedEdgeIds).toEqual(['ab'])})
it('settles overlaps and rejects external overlap transactionally',()=>{const s=fixture();expect(s.moveNode('a',{x:400,y:0}).adjusted).toBe(true);const before=JSON.stringify(s.getPositions());expect(()=>s.applyPositions({a:{x:400,y:250}})).toThrow();expect(()=>s.moveNode('a',{x:Infinity,y:0})).toThrow();expect(JSON.stringify(s.getPositions())).toBe(before)})
it('previews incident edges without mutation',()=>{const s=fixture(),before=JSON.stringify(s.getRoutes());expect(Object.keys(s.previewMove('a',{x:10,y:10}))).toEqual(['ab']);expect(JSON.stringify(s.getRoutes())).toBe(before)})
it('restores interacting saved moves with fixed endpoints and clear routes',()=>{
 const original=fixture();original.moveNode('a',{x:20,y:30});original.moveNode('o',{x:210,y:20});original.moveNode('b',{x:460,y:100})
 const saved=original.getPositions(),restored=fixture().applyPositions(saved)
 expect(restored.positions).toEqual(saved)
 expect(restored.routes.ab?.[0]).toEqual({x:saved.a!.x+100,y:saved.a!.y+30})
 expect(restored.routes.ab?.at(-1)).toEqual({x:saved.b!.x,y:saved.b!.y+30})
 for(const route of Object.values(restored.routes))for(let i=1;i<route.length;i++)for(const p of Object.values(saved))expect(segmentCrossesNode(route[i-1]!,route[i]!,p,100,60)).toBe(false)
 expect(fixture().applyPositions(saved).routes).toEqual(restored.routes)
})
it('rejects infeasible bounded routes without committing positions or paths',()=>{const s=fixture(),positions=s.getPositions(),routes=s.getRoutes();expect(()=>s.moveNode('b',{x:-100000,y:0})).toThrow();expect(s.getPositions()).toEqual(positions);expect(s.getRoutes()).toEqual(routes)})
it('retains the old vertical channel for a small move',()=>{const s=fixture();s.moveNode('a',{x:0,y:10});const channel=s.getRoutes().ab?.[1]?.x;s.moveNode('a',{x:0,y:20});expect(s.getRoutes().ab?.[1]?.x).toBe(channel)})
it('bounds adversarial repeated reversal work',()=>{const s=fixture(),start=performance.now();for(let i=0;i<50;i++)s.moveNode('a',{x:i%2?0:600,y:i%3*10});expect(performance.now()-start).toBeLessThan(3000)})
it('handles 1024 incident links within a bounded drag budget',()=>{const s=fixture(1024),start=performance.now(),u=s.moveNode('a',{x:0,y:30});expect(u.changedEdgeIds).toHaveLength(1024);expect(performance.now()-start).toBeLessThan(3000)})
