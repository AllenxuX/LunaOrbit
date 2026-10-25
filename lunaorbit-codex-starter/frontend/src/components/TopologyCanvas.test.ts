import { mount } from '@vue/test-utils'
import { beforeEach,expect,it,vi } from 'vitest'
import TopologyCanvas from './TopologyCanvas.vue'
import type { Snapshot,Position } from '../api/topology'
interface CellOptions {id:string;source?:unknown;data?:unknown;x?:number;y?:number;vertices?:Position[];[key:string]:unknown}
type MockCell=CellOptions&{getData:()=>unknown;setData:(data:unknown)=>void;attr:ReturnType<typeof vi.fn>;setLabels:ReturnType<typeof vi.fn>;setVertices:ReturnType<typeof vi.fn>;position:(x?:number,y?:number)=>Position}
type EventPayload={node?:MockCell;edge?:MockCell}
const fake=vi.hoisted(()=>({nodes:[] as MockCell[],edges:[] as MockCell[],events:new Map<string,(payload:EventPayload)=>void>(),zoomToFit:vi.fn(),resetCells:vi.fn(),requests:0,failNext:false}))
vi.mock('../topology/layout-client',async()=>{
 const {createElkLayout}=await import('../topology/elk-layout')
 return {createLayoutTask:(...args:Parameters<typeof createElkLayout>)=>{fake.requests++;const result=fake.failNext?Promise.reject(new Error('synthetic worker failure')):createElkLayout(...args);fake.failNext=false;return {result,cancel:vi.fn()}}}
})
vi.mock('@antv/x6',()=>({Graph:class{
 createNode(value:CellOptions){const cell={...value,getData:()=>cell.data,setData:(data:unknown)=>{cell.data=data},attr:vi.fn(),setLabels:vi.fn(),setVertices:vi.fn((points:Position[])=>{cell.vertices=points}),position:(x?:number,y?:number)=>{if(x!==undefined){cell.x=x;cell.y=y}return {x:cell.x??0,y:cell.y??0}}};return cell}
 createEdge(value:CellOptions){return this.createNode(value)}
 resetCells(cells:MockCell[]){fake.resetCells(cells);fake.nodes=cells.filter(c=>!c.source);fake.edges=cells.filter(c=>c.source)}
 getNodes(){return fake.nodes} getEdges(){return fake.edges} zoomToFit=fake.zoomToFit
 on(name:string,handler:(payload:EventPayload)=>void){fake.events.set(name,handler)} dispose(){}
}}))
function snapshot(reverse=false):Snapshot{
 const links=[{id:'ab',state:'observed',enabled:true,verified:false,stale:false},{id:'ab2',state:'observed',enabled:true,verified:false,stale:false},...(reverse?[{id:'ba',state:'observed',enabled:true,verified:false,stale:false}]:[])]
 return {nodes:[{id:'a',kind:'kafka',label:'A'},{id:'b',kind:'kafka',label:'B'},{id:'mm',kind:'workload',label:'MM2'}],links:links.map(l=>({...l,source:{alias:'A',status:'unknown'},target:{alias:'B',status:'unknown'}})),edges:links.flatMap(l=>[{id:l.id+'i',source:l.id==='ba'?'b':'a',target:'mm',link_id:l.id,relation:'declared_input'},{id:l.id+'o',source:'mm',target:l.id==='ba'?'a':'b',link_id:l.id,relation:'declared_output'}])} as unknown as Snapshot
}

beforeEach(()=>{vi.clearAllMocks();fake.events.clear();fake.nodes=[];fake.edges=[];fake.requests=0;fake.failNext=false})
async function ready(){await vi.waitFor(()=>expect(fake.nodes.length).toBeGreaterThan(0))}
it('aggregates relationships and preserves exact link and cluster click semantics',async()=>{
 const w=mount(TopologyCanvas,{props:{snapshot:snapshot(),positions:{}}});await ready()
 expect(fake.nodes.map(n=>n.id).sort()).toEqual(['a','b']);expect(fake.edges).toHaveLength(1)
 fake.events.get('edge:click')?.({edge:fake.edges[0]});expect(w.emitted('selectLinks')).toEqual([[['ab','ab2']]])
 fake.events.get('node:click')?.({node:fake.nodes.find(n=>n.id==='a')});expect(w.emitted('selectNode')).toEqual([['a']])
 fake.events.get('blank:click')?.({});expect(w.emitted('clear')).toEqual([[]]);w.unmount()
})
it('expands status only on hover and updates health without resetting geometry or viewport',async()=>{
 const s=snapshot(),w=mount(TopologyCanvas,{props:{snapshot:s,positions:{}}});await ready()
 const edge=fake.edges[0]!;expect(JSON.stringify(edge.setLabels.mock.calls.at(-1))).not.toContain('未验证')
 fake.events.get('edge:mouseenter')?.({edge});expect(JSON.stringify(edge.setLabels.mock.calls.at(-1))).toContain('未验证')
 fake.events.get('edge:mouseleave')?.({edge});fake.resetCells.mockClear();fake.zoomToFit.mockClear()
 const next=structuredClone(s);next.links.forEach(l=>{l.verified=true});next.nodes.reverse();next.edges.reverse()
 await w.setProps({snapshot:next,focusNodeId:'a',focusDirection:'downstream'})
 expect(fake.resetCells).not.toHaveBeenCalled();expect(fake.zoomToFit).not.toHaveBeenCalled();expect(fake.requests).toBe(1)
 expect(fake.edges[0]).toBe(edge);expect(edge.attr).toHaveBeenCalledWith('line/stroke','#35744e');w.unmount()
})
it.each([10,30,100])('settles a %ipx drag incrementally and ignores parent position echo',async distance=>{
 const w=mount(TopologyCanvas,{props:{snapshot:snapshot(),positions:{}}});await ready()
 const node=fake.nodes.find(n=>n.id==='a')!,edge=fake.edges[0]!,before=node.position(),ports=JSON.stringify(node.ports)
 fake.resetCells.mockClear();fake.zoomToFit.mockClear();node.position(before.x+distance,before.y+10)
 fake.events.get('node:moved')?.({node})
 expect(w.emitted('move')?.[0]?.[0]).toBe('a');expect(fake.nodes.find(n=>n.id==='a')).toBe(node);expect(fake.edges[0]).toBe(edge)
 expect(JSON.stringify(node.ports)).toBe(ports);expect(fake.resetCells).not.toHaveBeenCalled();expect(fake.zoomToFit).not.toHaveBeenCalled()
 edge.setVertices.mockClear();await w.setProps({positions:{a:node.position()}});expect(edge.setVertices).not.toHaveBeenCalled();w.unmount()
})
it('keeps shared Kafka unique and task clicks exact; path focus remains temporary and read only',async()=>{
 const s=snapshot(),w=mount(TopologyCanvas,{props:{snapshot:s,positions:{},mode:'chain'}});await ready()
 expect(fake.nodes.filter(n=>(n.data as {kind:string}).kind==='kafka')).toHaveLength(2)
 const task=fake.nodes.find(n=>(n.data as {linkId?:string}).linkId==='ab')!;fake.events.get('node:click')?.({node:task});expect(w.emitted('select')).toEqual([['ab']])
 await w.setProps({pathNodeIds:['a','task:ab','b']});await vi.waitFor(()=>expect(fake.nodes).toHaveLength(3))
 expect(fake.edges).toHaveLength(2);fake.events.get('node:moved')?.({node:fake.nodes[0]});expect(w.emitted('move')).toBeUndefined()
 await w.setProps({pathNodeIds:[]});await vi.waitFor(()=>expect(fake.nodes).toHaveLength(4));w.unmount()
})
it('clusters mode: only cluster nodes, one MM2-labelled edge per enabled link, disabled shown dashed only on request',async()=>{
 const {currentLab}=await import('../topology/fixtures')
 const w=mount(TopologyCanvas,{props:{snapshot:currentLab(),positions:{},mode:'clusters'}});await ready()
 expect(fake.nodes).toHaveLength(9);expect(fake.edges).toHaveLength(10)
 const edge=fake.edges.find(e=>(e.data as {linkIds:string[]}).linkIds[0]==='L02')!
 expect(JSON.stringify(edge.setLabels.mock.calls.at(-1))).toContain('luna-mm2-k1-to-k3')
 const node=fake.nodes.find(n=>n.id==='k1')!;expect(JSON.stringify(node.attrs)).toContain('"title"')
 await w.setProps({showDisabled:true});await vi.waitFor(()=>expect(fake.edges).toHaveLength(20))
 const disabled=fake.edges.find(e=>(e.data as {state:string}).state==='disabled')!
 expect(JSON.stringify(disabled.attrs)).toContain('2 5');w.unmount()
})
it('clusters mode truncates long cluster names in the middle and keeps the full name as tooltip',async()=>{
 const {snapshotFromRows}=await import('../topology/fixtures')
 const long='luna-production-cluster-shanghai-a'
 const w=mount(TopologyCanvas,{props:{snapshot:snapshotFromRows([long,'b'],[['x',long,'b','mm2']]),positions:{},mode:'clusters'}});await ready()
 const attrs=JSON.stringify(fake.nodes.find(n=>n.id===long)!.attrs)
 const node=fake.nodes.find(n=>n.id===long)! as unknown as {attrs:{label:{text:string};title:{text:string}}};expect(node.attrs.label.text.length).toBe(18);expect(node.attrs.label.text).toContain('…');expect(node.attrs.title.text).toContain(long);expect(attrs).toContain(long);w.unmount()
})
it('clusters mode folds large graphs into group cards and expands a group on click',async()=>{
 const {generatedLarge}=await import('../topology/fixtures')
 const {snapshot,meta}=generatedLarge(40,60,11)
 const w=mount(TopologyCanvas,{props:{snapshot,positions:{},mode:'clusters',groupMeta:meta}});await ready()
 expect(fake.nodes.length).toBe(5)
 const group=fake.nodes.find(n=>n.id.startsWith('group:'))!
 expect(JSON.stringify((group as unknown as {attrs:unknown}).attrs)).toMatch(/集群 \d+ · 组内 MM2 \d+/)
 expect(w.text()).toContain('分组总览')
 fake.events.get('node:click')?.({node:group});expect(w.emitted('selectNode')).toBeUndefined()
 await vi.waitFor(()=>expect(fake.nodes.length).toBeGreaterThan(5));w.unmount()
})
it('clusters mode shows at most 12 labels while focusing and says how many are shown',async()=>{
 const {snapshotFromRows}=await import('../topology/fixtures')
 const targets=Array.from({length:15},(_,i)=>'t'+String(i).padStart(2,'0'))
 const s=snapshotFromRows(['h',...targets],targets.map(t=>['h-'+t,'h',t,'mm2-'+t]))
 const w=mount(TopologyCanvas,{props:{snapshot:s,positions:{},mode:'clusters',focusNodeId:'h'}});await ready()
 await vi.waitFor(()=>expect(w.text()).toContain('已显示 12/15 个标签'))
 const labelled=fake.edges.filter(e=>JSON.stringify(e.setLabels.mock.calls.at(-1)).includes('mm2-'))
 expect(labelled.length).toBe(12);w.unmount()
})
it('clusters mode focus highlights the whole upstream and downstream lineage and dims the rest',async()=>{
 const {currentLab}=await import('../topology/fixtures')
 const w=mount(TopologyCanvas,{props:{snapshot:currentLab(),positions:{},mode:'clusters',focusNodeId:'k3'}});await ready()
 const opacity=(id:string)=>fake.nodes.find(n=>n.id===id)!.attr.mock.calls.filter(c=>c[0]==='body/opacity').at(-1)?.[1]
 for(const id of ['k1','k3','k6','k8','k9'])expect(opacity(id)).toBe(1)
 for(const id of ['k2','k4','k5','k7'])expect(opacity(id)).toBe(0.3)
 const lit=fake.edges.filter(e=>e.attr.mock.calls.filter(c=>c[0]==='line/opacity').at(-1)?.[1]===1).map(e=>(e.data as {source:string;target:string})).map(e=>e.source+'>'+e.target).sort()
 expect(lit).toEqual(['k1>k3','k3>k6','k6>k8','k8>k9']);w.unmount()
})
it('pairs mode draws one line per cluster pair labelled ×N without MM2 names and clicking selects all its MM2',async()=>{
 const {parallelPair}=await import('../topology/fixtures')
 const w=mount(TopologyCanvas,{props:{snapshot:parallelPair(),positions:{},mode:'pairs'}});await ready()
 expect(fake.nodes).toHaveLength(9);expect(fake.edges).toHaveLength(10)
 const labels=fake.edges.map(e=>JSON.stringify(e.setLabels.mock.calls.at(-1)))
 expect(labels.filter(l=>l.includes('×2'))).toHaveLength(1);expect(labels.some(l=>l.includes('mm2'))).toBe(false)
 const pair=fake.edges.find(e=>(e.data as {linkIds:string[]}).linkIds.length===3)!
 fake.events.get('edge:click')?.({edge:pair});expect((w.emitted('selectLinks')?.[0]?.[0] as string[]).sort()).toEqual(['L01','L11','L12'])
 const single=fake.edges.find(e=>(e.data as {linkIds:string[]}).linkIds.length===1)!
 fake.events.get('edge:click')?.({edge:single});expect(w.emitted('selectLinks')).toHaveLength(2)
 fake.events.get('edge:mouseenter')?.({edge:single});expect(JSON.stringify(single.setLabels.mock.calls.at(-1))).not.toMatch(/mm2|状态|验证/)
 const node=fake.nodes[0]! as unknown as {attrs:{body:{rx:number};meta:{text:string}};height:number}
 expect(node.attrs.body.rx).toBe(node.height/2);expect(node.attrs.meta.text).toBe('')
 await w.setProps({showDisabled:true});await vi.waitFor(()=>expect(fake.edges).toHaveLength(20))
 const off=fake.edges.find(e=>(e.data as {state:string}).state==='disabled')!
 expect(JSON.stringify((off as unknown as {attrs:unknown}).attrs)).toContain('#a3abb8');w.unmount()
})
it('rejects incompatible saved coordinates with a visible recovery action',async()=>{
 const w=mount(TopologyCanvas,{props:{snapshot:snapshot(),positions:{a:{x:0,y:0},b:{x:0,y:0}}}});await ready()
 expect(w.text()).toContain('已保存的布局存在重叠');expect(w.find('button').text()).toBe('重新整理');w.unmount()
})
it('ignores asynchronous layout after unmount',async()=>{
 const w=mount(TopologyCanvas,{props:{snapshot:snapshot(),positions:{}}});w.unmount()
 await new Promise(resolve=>setTimeout(resolve,100));expect(fake.resetCells).not.toHaveBeenCalled();expect(fake.zoomToFit).not.toHaveBeenCalled()
})

it('discards a superseded asynchronous layout when switching views quickly',async()=>{
 const w=mount(TopologyCanvas,{props:{snapshot:snapshot(),positions:{},mode:'chain'}})
 await w.setProps({mode:'deployment'})
 await vi.waitFor(()=>expect(fake.nodes).toHaveLength(3))
 expect(fake.nodes.some(n=>n.id.startsWith('task:'))).toBe(false)
 expect(fake.edges).toHaveLength(4);w.unmount()
})

it('clears stale topology after worker failure and offers parent-managed layout recovery',async()=>{
 const w=mount(TopologyCanvas,{props:{snapshot:snapshot(),positions:{}}});await ready()
 fake.failNext=true;await w.setProps({mode:'chain'})
 await vi.waitFor(()=>expect(w.text()).toContain('自动布局暂不可用'))
 expect(fake.nodes).toHaveLength(0);await w.find('button').trigger('click');expect(w.emitted('organize')).toEqual([[]]);w.unmount()
})
