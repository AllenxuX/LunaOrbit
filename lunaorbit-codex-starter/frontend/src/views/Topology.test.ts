import { flushPromises,mount } from '@vue/test-utils'
import { afterEach,expect,it,vi } from 'vitest'
import Topology from './Topology.vue'
import { routeLocationKey } from 'vue-router'
vi.mock('../components/TopologyCanvas.vue',()=>({default:{props:['snapshot','positions','mode','focusNodeId','focusDirection','selectedLinkIds'],emits:['move','select','selectNode','selectLinks'],methods:{fit(){},async arrange(){return {source:{x:24,y:24},target:{x:400,y:24}}}},template:'<div data-canvas><button data-move @click="$emit(\'move\',\'source\',{x:200,y:300})">drag</button></div>'}}))
const testRuntime=vi.hoisted(()=>({value:{mode:'mock',capabilities:{discovery:true,observability:false}}}))
vi.mock('../composables/useRuntime',async()=>{const {ref}=await import('vue');return {useRuntime:()=>({runtime:ref(testRuntime.value)})}})
afterEach(()=>{vi.unstubAllGlobals();testRuntime.value.mode='mock'})
const mapping={alias:'mars',status:'candidate',method:'bootstrap',confidence:'low',candidate_ids:[]}
const snapshot={scope_id:'synthetic-mm2',synthetic:true,stale:false,observed_at:'2026-10-19T00:00:00Z',attempted_at:'2026-10-19T00:00:00Z',issues:[],nodes:[{id:'work',kind:'workload',label:'synthetic-deployment'},{id:'source',kind:'kafka',label:'mars'},{id:'target',kind:'kafka',label:'moon-prod'}],edges:[{id:'in',source:'source',target:'work',link_id:'link',relation:'declared_input'},{id:'out',source:'work',target:'target',link_id:'link',relation:'declared_output'}],links:[{id:'link',source_alias:'mars',target_alias:'moon-prod',namespace:'synthetic',workload_name:'synthetic-deployment',workload_uid:'uid',declared:true,observed:true,verified:false,stale:false,observed_at:'2026-10-19T00:00:00Z',source:mapping,target:mapping,topics_raw:'events.*',policy:'DefaultReplicationPolicy',runtime:{ready_replicas:1,replicas:1,pod_candidates:1,pod_association:'label_candidates'},evidence:[{configmap_name:'synthetic-config',config_key:'mm2.properties',deployment_name:'synthetic-deployment',fingerprint:'safe-hash'}],notes:[]}]}
function setup(noSnapshot=false,mapped=false,queryScope='',initialPositions:Record<string,{x:number;y:number}>={},initialView='relations'){
 vi.stubGlobal('fetch',vi.fn(async(url:string,init?:RequestInit)=>{
  if(url.endsWith('/session'))return new Response(JSON.stringify({user_id:'owner',mode:'mock'}))
  if(url.endsWith('/scopes'))return new Response(JSON.stringify({items:[...(queryScope?[{id:'other-scope',synthetic:true}]:[]),{id:'synthetic-mm2',synthetic:true}]}))
  if(url.includes('/kafka/clusters'))return new Response(JSON.stringify({items:[{id:'owner-reg',display_name:'Lab Kafka',bootstrap_servers:['lab.invalid:9092'],discovered_cluster_id:'native-lab',room:'lab',environment:'test'}],total:1}))
  if(url.endsWith('/view'))return new Response(JSON.stringify(init?.method==='PUT'?{...JSON.parse(init.body as string),revision:1}:{revision:0,positions:initialPositions,filters:{room:'',environment:''},layers:{kafka:true,workloads:true}}))
  if(url.endsWith('/bindings'))return new Response(JSON.stringify({}))
  if(url.endsWith('/runs'))return new Response(JSON.stringify({status:'pending',job_id:'job'}),{status:202})
  if(url.endsWith('/runs/latest')){noSnapshot=false;return new Response(JSON.stringify({status:'done'}))}
  const shown=mapped?{...snapshot,links:snapshot.links.map(link=>({...link,source:{...link.source,status:'confirmed',registration_id:'owner-reg',cluster_id:'native-lab'}}))}:snapshot
  return noSnapshot?new Response('{}',{status:404}):new Response(JSON.stringify({snapshot:shown,total:1,page:1,page_size:100}))
 }))
 return mount(Topology,{global:{provide:{[routeLocationKey as symbol]:{query:{scope:queryScope,view:initialView}}},stubs:{'el-tag':{template:'<span><slot /></span>'},RouterLink:{template:'<a><slot /></a>'}}}})
}
it('shows uncertain evidence and explicitly saves only personal view data',async()=>{
 const w=setup();await flushPromises()
 expect(w.text()).toContain('未验证');await w.get('[data-link="link"]').trigger('click')
 expect(w.text()).toContain('synthetic-config');expect(w.text()).toContain('候选 Pod');expect(w.text()).toContain('RF 未知')
 await w.get('[data-move]').trigger('click');await w.get('[data-save]').trigger('click');await flushPromises()
 const call=vi.mocked(fetch).mock.calls.find(c=>c[1]?.method==='PUT');const body=JSON.parse(call?.[1]?.body as string)
 expect(body.positions['relations:source']).toEqual({x:200,y:300});expect(body.links).toBeUndefined();expect(body.edges).toBeUndefined();expect(w.text()).toContain('布局已保存');w.unmount()
})
it('prioritizes cluster names and business details while keeping Kafka IDs out of text',async()=>{
 const w=setup(false,true);await flushPromises();await w.get('[data-link="link"]').trigger('click')
 const summary=w.get('[data-link-summary]')
 expect(summary.text()).toContain('Lab Kafka');expect(summary.text()).toContain('events.*')
 expect(summary.text()).toContain('就绪副本');expect(summary.text()).not.toContain('UID')
 expect(summary.text()).not.toContain('RF 未知');expect(summary.text()).not.toContain('指纹')
 expect(w.text()).not.toContain('owner-reg');expect(w.text()).not.toContain('native-lab')
 expect(w.get('[data-diagnostics]').attributes('open')).toBeUndefined()
 expect(w.get('[data-diagnostics]').text()).toContain('synthetic-config')
 w.unmount()
})
it('offers discovery when no snapshot exists and loads only after job completion',async()=>{
 const w=setup(true);await flushPromises();expect(w.text()).toContain('尚无发现快照')
 await w.get('[data-discover]').trigger('click');await flushPromises();expect(w.text()).toContain('mars');w.unmount()
})
it('keeps unsaved layout on conflict and does not auto overwrite',async()=>{
 const w=setup();await flushPromises();await w.get('[data-move]').trigger('click')
 vi.mocked(fetch).mockResolvedValue(new Response('{}',{status:409}));await w.get('[data-save]').trigger('click');await flushPromises()
 expect(w.text()).toContain('布局已被其他页面修改');expect(vi.mocked(fetch).mock.calls.filter(c=>c[1]?.method==='PUT')).toHaveLength(1);w.unmount()
})
it('preserves dirty state when coordinates change during a pending save',async()=>{
 const w=setup();await flushPromises();await w.get('[data-move]').trigger('click')
 let finish!:(response:Response)=>void
 vi.mocked(fetch).mockImplementation(()=>new Promise<Response>(resolve=>{finish=resolve}))
 await w.get('[data-save]').trigger('click');await w.get('[data-move]').trigger('click')
 // A second drag must differ from the coordinates captured by the save.
 w.findComponent({name:'TopologyCanvas'}).vm.$emit('move','work',{x:400,y:500})
 finish(new Response(JSON.stringify({revision:1})));await flushPromises()
 expect(w.text()).toContain('有未保存改动');w.unmount()
})




it('automatically discovers the first missing lab snapshot',async()=>{
 testRuntime.value.mode='admin-lab'
 const w=setup(true);await flushPromises()
 expect(vi.mocked(fetch).mock.calls.some(c=>String(c[0]).endsWith('/runs')&&c[1]?.method==='POST')).toBe(true)
 expect(w.find('[data-link="link"]').exists()).toBe(true)
 expect(w.text()).toContain('实验环境');w.unmount()
})
it('requires a visible selection and explicit confirmation before saving a mapping',async()=>{
 const w=setup();await flushPromises();await w.get('[data-link="link"]').trigger('click')
 expect(vi.mocked(fetch).mock.calls.some(c=>String(c[0]).endsWith('/bindings'))).toBe(false)
 const card=w.find('.mapping-card')
 await card.get('select').setValue('owner-reg')
 await card.get('button').trigger('click');await flushPromises()
 const binding=vi.mocked(fetch).mock.calls.find(c=>String(c[0]).endsWith('/bindings'))
 expect(binding?.[1]?.method).toBe('PUT')
 expect(JSON.parse(binding?.[1]?.body as string)).toEqual({namespace:'synthetic',workload_uid:'uid',alias:'mars',registration_id:'owner-reg'})
 expect(vi.mocked(fetch).mock.calls.some(c=>String(c[0]).endsWith('/runs'))).toBe(true)
 expect(w.text()).toContain('RF 未知');w.unmount()
})


it('restores the requested scope and retains selection alongside a separate detail link',async()=>{
 const w=setup(false,false,'synthetic-mm2');await flushPromises()
 expect(vi.mocked(fetch).mock.calls.some(([url])=>String(url).includes('/scopes/synthetic-mm2/snapshot'))).toBe(true)
 expect(vi.mocked(fetch).mock.calls.some(([url])=>String(url).includes('/scopes/other-scope/snapshot'))).toBe(false)
 expect(w.get('[data-link-detail="link"]').text()).toBe('详情')
 await w.get('[data-link="link"]').trigger('click')
 expect(w.get('.evidence-panel').text()).toContain('打开链路详情')
 w.unmount()
})

it('gives the initial canvas full width and opens the details column only after selecting a link',async()=>{
 const w=setup();await flushPromises()
 expect(w.find('section[aria-label="链路详情"]').exists()).toBe(false)
 expect(w.get('.topology-workspace').classes()).not.toContain('topology-workspace--detail')
 await w.get('[data-link="link"]').trigger('click')
 expect(w.find('section[aria-label="链路详情"]').exists()).toBe(true)
 expect(w.get('.topology-workspace').classes()).not.toContain('topology-workspace--detail')
 expect(w.get('.evidence-panel').text()).toContain('打开链路详情')
 w.unmount()
})

it('defaults to relationship view and switches deployment without losing layout drafts',async()=>{
 const w=setup();await flushPromises()
 const canvas=w.findComponent({name:'TopologyCanvas'})
 expect(canvas.props('mode')).toBe('relations')
 await w.get('[data-move]').trigger('click')
 await w.get('[data-mode="tasks"]').trigger('click')
 expect(w.find('[data-canvas]').exists()).toBe(false)
 expect(w.find('[data-task-row]').exists()).toBe(true)
 await w.get('[data-mode="relations"]').trigger('click')
 expect(canvas.props('positions').source).toEqual({x:200,y:300})
 w.unmount()
})
it('focuses a node without selecting an arbitrary link and supports direction changes',async()=>{
 const w=setup();await flushPromises()
 const canvas=w.findComponent({name:'TopologyCanvas'})
 canvas.vm.$emit('selectNode','source');await flushPromises()
 expect(canvas.props('focusNodeId')).toBe('source')
 expect(w.find('.evidence-panel').exists()).toBe(false)
 await w.get('[data-focus-direction]').setValue('downstream')
 expect(canvas.props('focusDirection')).toBe('downstream')
 await w.get('[data-clear-focus]').trigger('click')
 expect(canvas.props('focusNodeId')).toBe('')
 w.unmount()
})
it('requires a concrete task selection for an aggregated route',async()=>{
 const w=setup();await flushPromises()
 const canvas=w.findComponent({name:'TopologyCanvas'})
 canvas.vm.$emit('selectLinks',['link']);await flushPromises()
 expect(w.find('[data-link-summary]').exists()).toBe(false)
 expect(w.get('[data-task-row="link"]').text()).toContain('synthetic-deployment')
 await w.get('[data-route-choice="link"]').trigger('click')
 expect(w.get('[data-link-summary]').text()).toContain('events.*')
 w.unmount()
})


it('replaces coordinate props when a drag changes geometry',async()=>{
 const w=setup();await flushPromises()
 const canvas=w.findComponent({name:'TopologyCanvas'}),before=canvas.props('positions')
 await w.get('[data-move]').trigger('click')
 expect(canvas.props('positions')).not.toBe(before)
 w.unmount()
})
it('clears focus when a filter removes its node',async()=>{
 const w=setup();await flushPromises()
 const canvas=w.findComponent({name:'TopologyCanvas'})
 canvas.vm.$emit('selectNode','source');await flushPromises()
 await w.get('select[aria-label="机房筛选"]').setValue('lab')
 expect(canvas.props('focusNodeId')).toBe('')
 w.unmount()
})

it('retains legacy coordinates while saving relationship-only changes',async()=>{
 const w=setup(false,false,'',{'source':{x:900,y:900},'relations:source':{x:100,y:120}});await flushPromises()
 const canvas=w.findComponent({name:'TopologyCanvas'})
 expect(canvas.props('positions').source).toEqual({x:100,y:120})
 await w.get('[data-mode="tasks"]').trigger('click')
 expect(w.find('[data-canvas]').exists()).toBe(false)
 await w.get('[data-mode="relations"]').trigger('click')
 await w.get('[data-move]').trigger('click');await w.get('[data-save]').trigger('click');await flushPromises()
 const call=vi.mocked(fetch).mock.calls.find(c=>c[1]?.method==='PUT'),body=JSON.parse(call?.[1]?.body as string)
 expect(body.positions.source).toEqual({x:900,y:900})
 expect(body.positions['relations:source']).toEqual({x:200,y:300})
 w.unmount()
})
it('keeps selected details below the canvas and supports searching a cluster',async()=>{
 const w=setup();await flushPromises();await w.get('[data-link="link"]').trigger('click')
 expect(w.get('.evidence-panel').element.closest('.topology-workspace')).toBeNull()
 await w.get('[data-cluster-search]').setValue('mars')
 expect(w.findAll('[data-search-node]')).toHaveLength(1)
 await w.get('[data-search-node]').trigger('click')
 expect(w.findComponent({name:'TopologyCanvas'}).props('focusNodeId')).toBe('source')
 expect(w.get('[data-cluster-search]').element).toBeTruthy()
 w.unmount()
})

it('opens MM2 topology by default and preserves separate relationship and task views',async()=>{
 const w=setup(false,false,'',{},'');await flushPromises()
 expect(w.find('[data-mm2-instance]').exists()).toBe(false)
 expect(w.findComponent({name:'TopologyCanvas'}).props('mode')).toBe('chain')
 expect(w.find('[data-canvas]').exists()).toBe(true)
 expect(w.get('[data-mode="mm2"]').attributes('aria-pressed')).toBe('true')
 w.findComponent({name:'TopologyCanvas'}).vm.$emit('select','link');await flushPromises()
 expect(w.get('[data-link-summary]').text()).toContain('events.*')
 await w.get('[data-mode="relations"]').trigger('click')
 expect(w.find('[data-canvas]').exists()).toBe(true)
 await w.get('[data-mode="tasks"]').trigger('click')
 expect(w.find('[data-canvas]').exists()).toBe(false)
 expect(w.find('[data-task-row]').exists()).toBe(true)
 w.unmount()
})

it('keeps instance rows available separately from the default continuous topology',async()=>{
 const w=setup(false,false,'',{},'');await flushPromises()
 await w.get('[data-mode="instances"]').trigger('click')
 expect(w.find('[data-mm2-instance]').exists()).toBe(true)
 expect(w.find('[data-canvas]').exists()).toBe(false)
 await w.get('[data-mode="mm2"]').trigger('click')
 expect(w.findComponent({name:'TopologyCanvas'}).props('mode')).toBe('chain')
 expect(w.find('[data-path-source]').exists()).toBe(true)
 expect(w.find('[data-path-target]').exists()).toBe(true)
 w.unmount()
})

it('saves the actual arranged positions returned by the canvas and preserves other views',async()=>{
 const w=setup(false,false,'',{'chain:source':{x:9,y:9}});await flushPromises()
 const button=w.findAll('button').find(b=>b.text()==='整理布局')!;await button.trigger('click');await flushPromises()
 await w.get('[data-save]').trigger('click');await flushPromises()
 const call=vi.mocked(fetch).mock.calls.find(c=>c[1]?.method==='PUT'),body=JSON.parse(call?.[1]?.body as string)
 expect(body.positions['relations:source']).toEqual({x:24,y:24});expect(body.positions['relations:target']).toEqual({x:400,y:24});expect(body.positions['chain:source']).toEqual({x:9,y:9});w.unmount()
})
