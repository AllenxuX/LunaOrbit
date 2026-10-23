import type { Position } from '../api/topology'
import type { Presentation,PresentationEdge } from './presentation'
import type { LayoutPlan,RoutingUpdate } from './routing-types'

const LIMIT=100000,GAP=12
const same=(a:Position,b:Position)=>a.x===b.x&&a.y===b.y
const distance=(a:Position,b:Position)=>Math.abs(a.x-b.x)+Math.abs(a.y-b.y)
function check(p:Position){if(!Number.isFinite(p.x)||!Number.isFinite(p.y)||Math.abs(p.x)>LIMIT||Math.abs(p.y)>LIMIT)throw Error('Position outside routing bounds')}
/** Open rectangle intersection; touching the boundary or a port is permitted. */
export function segmentCrossesNode(a:Position,b:Position,p:Position,w:number,h:number):boolean{
 if(a.x===b.x)return a.x>p.x&&a.x<p.x+w&&Math.max(a.y,b.y)>p.y&&Math.min(a.y,b.y)<p.y+h
 if(a.y===b.y)return a.y>p.y&&a.y<p.y+h&&Math.max(a.x,b.x)>p.x&&Math.min(a.x,b.x)<p.x+w
 return true
}
function simplify(points:Position[]):Position[]{
 const out:Position[]=[]
 for(const p of points){if(out.length&&same(out.at(-1)!,p))continue;while(out.length>1){const a=out.at(-2)!,b=out.at(-1)!;if((a.x===b.x&&b.x===p.x)||(a.y===b.y&&b.y===p.y))out.pop();else break}out.push({...p})}
 return out
}
export class RoutingSession{
 private positions:Record<string,Position>
 private routes:Record<string,Position[]>
 constructor(private presentation:Presentation,private plan:LayoutPlan){
  this.positions=Object.fromEntries(presentation.nodes.map(n=>{const p=plan.positions[n.id];if(!p)throw Error('Missing node position');check(p);return [n.id,{...p}]}))
  this.routes={...plan.routes}
  this.validatePositions(this.positions)
  for(const edge of presentation.edges){if(!plan.ports[edge.id])throw Error('Missing fixed ports');if(!this.routes[edge.id]||!this.valid(this.routes[edge.id]!,this.positions))throw Error('Invalid initial orthogonal route')}
 }
 getPositions():Record<string,Position>{return Object.fromEntries(Object.entries(this.positions).map(([id,p])=>[id,{...p}]))}
 getRoutes():Record<string,Position[]>{return {...this.routes}}
 private validatePositions(positions:Record<string,Position>){
  const entries=Object.entries(positions)
  for(let i=0;i<entries.length;i++){const [,a]=entries[i]!;check(a);for(let j=0;j<i;j++){const [,b]=entries[j]!;if(this.overlap(a,b))throw Error('Nodes overlap')}}
 }
 private overlap(a:Position,b:Position){return a.x<b.x+this.plan.nodeWidth&&a.x+this.plan.nodeWidth>b.x&&a.y<b.y+this.plan.nodeHeight&&a.y+this.plan.nodeHeight>b.y}
 private endpoints(edge:PresentationEdge,positions:Record<string,Position>):[Position,Position]{
  const ports=this.plan.ports[edge.id]!,a=positions[edge.source]!,b=positions[edge.target]!
  return [{x:a.x+(ports.source.side==='right'?this.plan.nodeWidth:0),y:a.y+ports.source.offset},{x:b.x+(ports.target.side==='right'?this.plan.nodeWidth:0),y:b.y+ports.target.offset}]
 }
 private valid(route:Position[],positions:Record<string,Position>){return route.length>=2&&route.every(p=>Number.isFinite(p.x)&&Number.isFinite(p.y)&&Math.abs(p.x)<=LIMIT&&Math.abs(p.y)<=LIMIT)&&route.slice(1).every((b,i)=>!Object.values(positions).some(p=>segmentCrossesNode(route[i]!,b,p,this.plan.nodeWidth,this.plan.nodeHeight)))}
 private stretch(edge:PresentationEdge,positions:Record<string,Position>):Position[]{
  const old=this.routes[edge.id]!,[a,b]=this.endpoints(edge,positions)
  if(old.length<=2){const x=(old[0]!.x+old.at(-1)!.x)/2;return simplify([a,{x,y:a.y},{x,y:b.y},b])}
  const points=old.map(p=>({...p}));points[0]=a;points[points.length-1]=b
  if(old[0]!.y===old[1]!.y)points[1]!.y=a.y;else points[1]!.x=a.x
  if(old.at(-1)!.y===old.at(-2)!.y)points[points.length-2]!.y=b.y;else points[points.length-2]!.x=b.x
  return simplify(points)
 }
 previewMove(id:string,p:Position):Record<string,Position[]>{if(!this.positions[id])throw Error('Unknown node');check(p);const next={...this.positions,[id]:p};return Object.fromEntries(this.presentation.edges.filter(e=>e.source===id||e.target===id).map(e=>[e.id,this.stretch(e,next)]))}
 moveNode(id:string,p:Position):RoutingUpdate{
  if(!this.positions[id])throw Error('Unknown node');check(p)
  const others=Object.entries(this.positions).filter(([key])=>key!==id).map(([,v])=>v)
  let settled={...p}
  if(others.some(o=>this.overlap(p,o))){
   const xs=[p.x,...others.flatMap(o=>[o.x-this.plan.nodeWidth-GAP,o.x+this.plan.nodeWidth+GAP])],ys=[p.y,...others.flatMap(o=>[o.y-this.plan.nodeHeight-GAP,o.y+this.plan.nodeHeight+GAP])]
   const candidates=[...xs.map(x=>({x,y:p.y})),...ys.map(y=>({x:p.x,y}))].filter(c=>Math.abs(c.x)<=LIMIT&&Math.abs(c.y)<=LIMIT&&!others.some(o=>this.overlap(c,o))).sort((a,b)=>distance(a,p)-distance(b,p)||a.y-b.y||a.x-b.x)
   if(!candidates.length)throw Error('No free node position');settled=candidates[0]!
  }
  const result=this.applyPositions({[id]:settled});result.adjusted=!same(p,settled);return result
 }
 applyPositions(next:Record<string,Position>):RoutingUpdate{
  for(const id of Object.keys(next))if(!this.positions[id])throw Error('Unknown node')
  const positions={...this.positions,...Object.fromEntries(Object.entries(next).map(([id,p])=>[id,{...p}]))};this.validatePositions(positions)
  const changedNodeIds=Object.keys(positions).filter(id=>!same(positions[id]!,this.positions[id]!)).sort(),changed=new Set(changedNodeIds),routes={...this.routes},changedEdgeIds:string[]=[]
  for(const edge of this.presentation.edges){const old=routes[edge.id]!,incident=changed.has(edge.source)||changed.has(edge.target);if(!incident&&this.valid(old,positions))continue
   const stretched=incident?this.stretch(edge,positions):old
   routes[edge.id]=this.valid(stretched,positions)?stretched:this.search(edge,positions,old)
   if(JSON.stringify(routes[edge.id])!==JSON.stringify(old))changedEdgeIds.push(edge.id);else routes[edge.id]=old
  }
  this.positions=positions;this.routes=routes
  return {positions:this.getPositions(),routes:this.getRoutes(),changedNodeIds,changedEdgeIds:changedEdgeIds.sort(),adjusted:false}
 }
 private search(edge:PresentationEdge,positions:Record<string,Position>,old:Position[]):Position[]{
  const [a,b]=this.endpoints(edge,positions),ports=this.plan.ports[edge.id]!
  const start={x:a.x+(ports.source.side==='right'?GAP:-GAP),y:a.y},end={x:b.x+(ports.target.side==='right'?GAP:-GAP),y:b.y}
  const candidates:Position[][]=[]
  const xs=[start.x,end.x,...old.map(p=>p.x)],ys=[start.y,end.y,...old.map(p=>p.y)]
  const obstacles=Object.values(positions)
  for(const p of obstacles){xs.push(p.x-GAP,p.x+this.plan.nodeWidth+GAP);ys.push(p.y-GAP,p.y+this.plan.nodeHeight+GAP)}
  // Local one-channel candidates are cheap and keep normal dragging independent of graph size.
  const axis=(values:number[],v:number,w:number)=>[...new Set(values)].filter(x=>Math.abs(x)<=LIMIT).sort((x,y)=>Math.min(Math.abs(x-v),Math.abs(x-w))-Math.min(Math.abs(y-v),Math.abs(y-w))||x-y).slice(0,64).sort((x,y)=>x-y)
  const xx=axis(xs,start.x,end.x),yy=axis(ys,start.y,end.y)
  for(const x of xx)candidates.push(simplify([a,start,{x,y:start.y},{x,y:end.y},end,b]))
  for(const y of yy)candidates.push(simplify([a,start,{x:start.x,y},{x:end.x,y},end,b]))
  const score=(r:Position[])=>r.slice(1).reduce((n,p,i)=>n+distance(r[i]!,p),0)+(r.length-2)*45+r.filter(p=>!old.some(o=>same(p,o))).length*3
  const valid=candidates.filter(r=>this.valid(r,positions)).sort((x,y)=>score(x)-score(y))
  if(valid.length)return valid[0]!
  // Bounded visibility grid. All obstacles remain in collision checks even when distant
  // obstacle coordinates are omitted. Exhaustion rejects the transaction safely.
  const width=xx.length
  const startIndex=yy.indexOf(start.y)*width+xx.indexOf(start.x),endIndex=yy.indexOf(end.y)*width+xx.indexOf(end.x)
  type State={index:number;dir:number;cost:number;priority:number;path:number[]}
  const queue:State[]=[{index:startIndex,dir:0,cost:0,priority:0,path:[startIndex]}],best=new Map<string,number>()
  const point=(index:number)=>({x:xx[index%width]!,y:yy[Math.floor(index/width)]!})
  for(let count=0;queue.length&&count<12000;count++){
   queue.sort((x,y)=>y.priority-x.priority||y.index-x.index);const current=queue.pop()!,key=current.index+':'+current.dir
   if((best.get(key)??Infinity)<current.cost)continue
   if(current.index===endIndex){const r=simplify([a,...current.path.map(point),b]);if(this.valid(r,positions))return r;break}
   const x=current.index%width,y=Math.floor(current.index/width)
   for(const [nx,ny,dir] of [[x-1,y,1],[x+1,y,1],[x,y-1,2],[x,y+1,2]]){
    if(nx!<0||ny!<0||nx!>=width||ny!>=yy.length)continue
    const index=ny!*width+nx!,from=point(current.index),to=point(index)
    if(obstacles.some(p=>segmentCrossesNode(from,to,p,this.plan.nodeWidth,this.plan.nodeHeight)))continue
    const cost=current.cost+distance(from,to)+(current.dir&&current.dir!==dir?45:0),nextKey=index+':'+dir
    if(cost>=(best.get(nextKey)??Infinity))continue;best.set(nextKey,cost);queue.push({index,dir:dir!,cost,priority:cost+distance(to,end),path:[...current.path,index]})
   }
  }
  throw Error('No safe route within bounded local search')
 }
}
