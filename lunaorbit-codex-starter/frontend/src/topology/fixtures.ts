import type { GraphNode, Link, Snapshot } from '../api/topology'
import type { ClusterMeta } from './scale-rules'

// Deterministic synthetic topologies for layout tests and visual smoke checks.
// Each row is one MM2 replication: [linkId, source cluster, target cluster, MM2 name, enabled].
export type LinkRow=[string,string,string,string,boolean?]

export function snapshotFromRows(clusters:string[],rows:LinkRow[]):Snapshot {
 const nodes=new Map<string,GraphNode>(clusters.map(id=>[id,{id,kind:'kafka',label:id,replication_factor:null,rf_status:'unknown'}]))
 const links:Link[]=[],edges:Snapshot['edges']=[]
 for(const [id,source,target,name,enabled=true] of rows){
  for(const c of [source,target])if(!nodes.has(c))nodes.set(c,{id:c,kind:'kafka',label:c,replication_factor:null,rf_status:'unknown'})
  const work='workload:'+name
  nodes.set(work,{id:work,kind:'workload',label:name,replication_factor:null,rf_status:'unknown'})
  const mapping=(alias:string)=>({alias,status:'unknown',method:'fixture',confidence:'low',candidate_ids:[]})
  links.push({id,workload_name:name,source_alias:source,target_alias:target,source:mapping(source),target:mapping(target),state:enabled?'declared':'disabled',enabled,verified:false,stale:false} as unknown as Link)
  edges.push({id:id+':in',source,target:work,link_id:id,relation:'declared_input',stale:false,evidence:[]},{id:id+':out',source:work,target,link_id:id,relation:'declared_output',stale:false,evidence:[]})
 }
 return {scope_id:'fixture',synthetic:true,observed_at:'',attempted_at:'',stale:false,nodes:[...nodes.values()],edges,links,issues:[]}
}

function rng(seed:number){return ()=>{seed=(seed+0x6d2b79f5)|0;let t=Math.imul(seed^(seed>>>15),1|seed);t=(t+Math.imul(t^(t>>>7),61|t))^t;return ((t^(t>>>14))>>>0)/4294967296}}
const ks=(n:number)=>Array.from({length:n},(_,i)=>'k'+(i+1))

/** Current lab: fan-out k1→k2/k3/k4, one-to-one chains, fan-in k5/k6/k7→k8, k8→k9; each MM2 also has a disabled reverse rule. */
export function currentLab(withDisabled=true):Snapshot {
 const pairs:[string,string,string][]=[['k1','k2','mm2'],['k1','k3','luna-mm2-k1-to-k3'],['k1','k4','luna-mm2-k1-to-k4'],['k2','k5','luna-mm2-k2-to-k5'],['k3','k6','luna-mm2-k3-to-k6'],['k4','k7','luna-mm2-k4-to-k7'],['k5','k8','luna-mm2-k5-to-k8'],['k6','k8','luna-mm2-k6-to-k8'],['k7','k8','luna-mm2-k7-to-k8'],['k8','k9','luna-mm2-k8-to-k9']]
 const rows:LinkRow[]=pairs.map(([s,t,n],i)=>[`L${String(i+1).padStart(2,'0')}`,s,t,n])
 if(withDisabled)pairs.forEach(([s,t,n],i)=>rows.push([`R${String(i+1).padStart(2,'0')}`,t,s,n,false]))
 return snapshotFromRows(ks(9),rows)
}

/** Acyclic graph with several roots: `clusters` nodes spread over layers, exactly `links` enabled edges. */
export function generatedDag(clusters=20,links=25,roots=3,seed=7):Snapshot {
 const random=rng(seed),ids=ks(clusters),rows:LinkRow[]=[],seen=new Set<string>()
 const add=(s:string,t:string)=>{const key=s+'>'+t;if(seen.has(key))return false;seen.add(key);rows.push([`G${String(rows.length+1).padStart(2,'0')}`,s,t,`mm2-${s}-to-${t}`]);return true}
 // Every non-root node gets one parent from an earlier index, keeping the graph acyclic.
 for(let i=roots;i<clusters;i++)add(ids[Math.floor(random()*Math.min(i,Math.max(roots,i-6)))]!,ids[i]!)
 let guard=0
 while(rows.length<links&&guard++<10000){const a=Math.floor(random()*(clusters-1)),b=a+1+Math.floor(random()*Math.min(5,clusters-a-1));add(ids[a]!,ids[b]!)}
 return snapshotFromRows(ids,rows)
}

/** Bidirectional pair, a 3-cycle, a parallel MM2 pair and a long skip edge. */
export function cyclicGraph():Snapshot {
 return snapshotFromRows(['a','b','c','d','e','f','g'],[
  ['C01','a','b','mm2-a-b'],['C02','b','a','mm2-b-a'],['C03','a','b','mm2-a-b-2'],
  ['C04','b','c','mm2-b-c'],['C05','c','d','mm2-c-d'],['C06','d','e','mm2-d-e'],['C07','e','c','mm2-e-c'],
  ['C08','e','f','mm2-e-f'],['C09','a','f','mm2-a-f-long'],['C10','f','g','mm2-f-g'],
 ])
}

/** A short chain plus clusters without any MM2 link. */
export function isolatedGraph():Snapshot {
 return snapshotFromRows(['p1','p2','p3','p4','solo-1','solo-2','solo-3'],[['I01','p1','p2','mm2-p1-p2'],['I02','p2','p3','mm2-p2-p3'],['I03','p1','p4','mm2-p1-p4']])
}

/** Large graph for scale rules: multiple roots, some long edges, environment/room metadata with a few unknown environments. */
export function generatedLarge(clusters=40,links=60,seed=11):{snapshot:Snapshot;meta:Record<string,ClusterMeta>} {
 const snapshot=generatedDag(clusters,links,4,seed),random=rng(seed*31),envs=['prod','staging','test'],rooms=['sh-a','bj-b']
 const meta:Record<string,ClusterMeta>={}
 snapshot.nodes.filter(n=>n.kind==='kafka').forEach((n,i)=>{meta[n.id]={environment:i%13===5?undefined:envs[Math.floor(random()*envs.length)],room:rooms[Math.floor(random()*rooms.length)]}})
 return {snapshot,meta}
}

/** Current lab plus a second enabled and a disabled MM2 on k1→k2: 9 clusters, 10 enabled pairs, 11 enabled MM2. */
export function parallelPair():Snapshot {
 const s=currentLab()
 const extra=snapshotFromRows([],[['L11','k1','k2','mm2-k1-k2-extra'],['L12','k1','k2','mm2-k1-k2-legacy',false]])
 return {...s,nodes:[...s.nodes,...extra.nodes.filter(n=>n.kind==='workload')],edges:[...s.edges,...extra.edges],links:[...s.links,...extra.links]}
}
