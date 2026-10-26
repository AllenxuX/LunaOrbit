import { flushPromises,mount } from '@vue/test-utils'
import { afterEach,expect,it,vi } from 'vitest'
import Topology from './Topology.vue'
import { routeLocationKey } from 'vue-router'
vi.mock('../components/TopologyCanvas.vue',()=>({default:{props:['snapshot','positions','mode','focusNodeId','focusDirection','selectedLinkIds','showDisabled','groupMeta'],emits:['move','select','selectNode','selectLinks'],methods:{fit(){},async arrange(){return {source:{x:24,y:24},target:{x:400,y:24}}}},template:'<div data-canvas><button data-move @click="$emit(\'move\',\'source\',{x:200,y:300})">drag</button></div>'}}))
const testRuntime=vi.hoisted(()=>({value:{mode:'mock',capabilities:{discovery:true,observability:false}}}))
vi.mock('../composables/useRuntime',async()=>{const {ref}=await import('vue');return {useRuntime:()=>({runtime:ref(testRuntime.value)})}})
afterEach(()=>{vi.unstubAllGlobals();testRuntime.value.mode='mock'})
const mapping={alias:'mars',status:'candidate',method:'bootstrap',confidence:'low',candidate_ids:[]}
const snapshot={scope_id:'synthetic-mm2',synthetic:true,stale:false,observed_at:'2026-10-19T00:00:00Z',attempted_at:'2026-10-19T00:00:00Z',issues:[],nodes:[{id:'work',kind:'workload',label:'synthetic-deployment'},{id:'source',kind:'kafka',label:'mars'},{id:'target',kind:'kafka',label:'moon-prod'}],edges:[{id:'in',source:'source',target:'work',link_id:'link',relation:'declared_input'},{id:'out',source:'work',target:'target',link_id:'link',relation:'declared_output'}],links:[{id:'link',source_alias:'mars',target_alias:'moon-prod',namespace:'synthetic',workload_name:'synthetic-deployment',workload_uid:'uid',declared:true,observed:true,verified:false,stale:false,observed_at:'2026-10-19T00:00:00Z',source:mapping,target:mapping,topics_raw:'events.*',policy:'DefaultReplicationPolicy',runtime:{ready_replicas:1,replicas:1,pod_candidates:1,pod_association:'label_candidates'},evidence:[{configmap_name:'synthetic-config',config_key:'mm2.properties',deployment_name:'synthetic-deployment',fingerprint:'safe-hash'}],notes:[]}]}
function setup(noSnapshot=false,mapped=false,queryScope='',initialPositions:Record<string,{x:number;y:number}>={},initialView='mm2'){
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
 expect(body.positions['clusters:source']).toEqual({x:200,y:300});expect(body.links).toBeUndefined();expect(body.edges).toBeUndefined();expect(w.text()).toContain('布局已保存');w.unmount()
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
 // mars matches no registered cluster, so binding goes through the explicit manual assignment.
 const card=w.find('.mapping-card'),manual=card.get('[data-manual-assign]')
 await manual.get('[data-manual-select]').setValue('owner-reg');await manual.get('[data-manual-confirm]').setValue(true)
 await manual.get('[data-manual-submit]').trigger('click');await flushPromises()
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
 const w=setup(false,false,'',{},'relations');await flushPromises()
 const canvas=w.findComponent({name:'TopologyCanvas'})
 expect(canvas.props('mode')).toBe('pairs')
 await w.get('[data-move]').trigger('click')
 await w.get('[data-mode="tasks"]').trigger('click')
 expect(w.find('[data-canvas]').exists()).toBe(false)
 expect(w.find('[data-task-row]').exists()).toBe(true)
 await w.get('[data-mode="relations"]').trigger('click')
 expect(canvas.props('positions').source).toEqual({x:200,y:300})
 w.unmount()
})
it('focuses a node, shows the full upstream/downstream lineage and clears with 显示全部',async()=>{
 const w=setup();await flushPromises()
 const canvas=w.findComponent({name:'TopologyCanvas'})
 canvas.vm.$emit('selectNode','source');await flushPromises()
 expect(canvas.props('focusNodeId')).toBe('source')
 expect(w.find('[aria-label="链路详情"]').exists()).toBe(false);expect(w.find('[data-endpoint-panel]').exists()).toBe(true)
 expect(w.get('[data-lineage-counts]').text()).toBe('全部上游 0 个集群 · 全部下游 1 个集群')
 expect(w.find('[data-focus-direction]').exists()).toBe(false)
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
 const w=setup(false,false,'',{'source':{x:900,y:900},'relations:source':{x:100,y:120}},'relations');await flushPromises()
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
 expect(w.findComponent({name:'TopologyCanvas'}).props('mode')).toBe('clusters')
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
 expect(w.findComponent({name:'TopologyCanvas'}).props('mode')).toBe('clusters')
 expect(w.find('[data-path-source]').exists()).toBe(true)
 expect(w.find('[data-path-target]').exists()).toBe(true)
 w.unmount()
})

it('saves the actual arranged positions returned by the canvas and preserves other views',async()=>{
 const w=setup(false,false,'',{'chain:source':{x:9,y:9}},'relations');await flushPromises()
 const button=w.findAll('button').find(b=>b.text()==='整理布局')!;await button.trigger('click');await flushPromises()
 await w.get('[data-save]').trigger('click');await flushPromises()
 const call=vi.mocked(fetch).mock.calls.find(c=>c[1]?.method==='PUT'),body=JSON.parse(call?.[1]?.body as string)
 expect(body.positions['relations:source']).toEqual({x:24,y:24});expect(body.positions['relations:target']).toEqual({x:400,y:24});expect(body.positions['chain:source']).toEqual({x:9,y:9});w.unmount()
})

it('MM2 topology counts enabled links, toggles disabled links and shows full lineage counts',async()=>{
 const w=setup(false,false,'',{},'');await flushPromises()
 expect(w.get('.graph-topline').text()).toContain('0 个已登记集群 · 2 个未登记端点 · 1 条 MM2 任务 · 就绪 1/1')
 expect(w.text()).toContain('显示禁用任务（0）')
 expect(w.get('.graph-topline').text()).not.toMatch(/×|同步关系/)
 const canvas=w.findComponent({name:'TopologyCanvas'})
 expect(canvas.props('showDisabled')).toBe(false)
 await w.get('[data-show-disabled]').setValue(true)
 expect(canvas.props('showDisabled')).toBe(true)
 canvas.vm.$emit('selectNode','target');await flushPromises()
 expect(w.get('[data-lineage-counts]').text()).toBe('全部上游 1 个集群 · 全部下游 0 个集群')
 w.unmount()
})

it('passes registered environment and room per cluster for scale grouping',async()=>{
 const w=setup(false,true,'',{},'');await flushPromises()
 expect(w.findComponent({name:'TopologyCanvas'}).props('groupMeta')).toEqual({source:{environment:'test',room:'lab'}})
 w.unmount()
})

it('both graph tabs use the layered layout: relations draws cluster pairs, MM2 draws links',async()=>{
 const w=setup(false,false,'',{},'relations');await flushPromises()
 expect(w.findComponent({name:'TopologyCanvas'}).props('mode')).toBe('pairs')
 const top=w.get('.graph-topline').text()
 expect(top).toContain('0 个已登记集群 · 2 个未登记端点 · 1 对同步关系（启用）');expect(top).toContain('集群视角')
 expect(top).not.toMatch(/链路|任务/)
 expect(w.find('.task-panel').exists()).toBe(false);expect(w.find('[data-path-source]').exists()).toBe(false)
 await w.get('[data-mode="mm2"]').trigger('click')
 expect(w.findComponent({name:'TopologyCanvas'}).props('mode')).toBe('clusters')
 w.unmount()
})

it('relations page: clicking a relation opens a side panel listing member MM2 with separated facts',async()=>{
 const w=setup(false,false,'',{},'relations');await flushPromises()
 w.findComponent({name:'TopologyCanvas'}).vm.$emit('selectLinks',['link']);await flushPromises()
 const panel=w.get('[data-relation-panel]')
 expect(panel.get('[data-relation-title]').text()).toBe('mars → moon-prod')
 expect(panel.get('[data-relation-counts]').text()).toBe('启用 MM2 1 · 禁用 0')
 const row=panel.get('[data-relation-link]')
 expect(row.get('[data-relation-name]').text()).toBe('synthetic-deployment')
 expect(row.get('[data-relation-evidence]').text()).toBe('证据类型：配置关系（未验证）')
 expect(row.get('[data-relation-enablement]').text()).toBe('启用')
 expect(row.get('[data-relation-status]').text()).toBe('状态未知')
 expect(row.get('[data-relation-freshness]').text()).toBe('当前')
 expect(panel.text()).not.toContain('已验证')
 expect(w.find('[aria-label="链路详情"]').exists()).toBe(false)
 w.unmount()
})
it('MM2 page: link detail starts with evidence type and keeps enablement, status and freshness separate',async()=>{
 const w=setup(false,false,'',{},'');await flushPromises()
 w.findComponent({name:'TopologyCanvas'}).vm.$emit('select','link');await flushPromises()
 const facts=w.findAll('[data-link-facts] dt').map(d=>d.text())
 expect(facts[0]).toBe('证据类型')
 expect(w.get('[data-evidence-type]').text()).toBe('配置关系（未验证）')
 expect(w.get('[data-link-enablement]').text()).toBe('启用')
 expect(w.get('[data-link-status]').text()).toBe('状态未知')
 expect(w.get('[data-link-freshness]').text()).toBe('当前')
 expect(w.get('[data-link-facts]').text()).toContain('DefaultReplicationPolicy')
 expect(w.get('.evidence-panel').text()).not.toMatch(/×\d|已验证/)
 w.unmount()
})
it('MM2 page task panel counts only enabled MM2 tasks unless disabled are shown',async()=>{
 const w=setup(false,false,'',{},'');await flushPromises()
 expect(w.get('.task-panel').text()).toContain('1 条 MM2 任务')
 expect(w.get('.task-panel').text()).not.toContain('个任务')
 w.unmount()
})

it('更多 menu closes on outside click, Esc and after an action (LUNA-33B)',async()=>{
 const w=setup();await flushPromises()
 const more=()=>w.get('.topology-more').element as HTMLDetailsElement
 more().open=true;globalThis.document.body.dispatchEvent(new globalThis.MouseEvent('pointerdown',{bubbles:true}));await flushPromises();expect(more().open).toBe(false)
 more().open=true;more().querySelector('summary')!.dispatchEvent(new globalThis.MouseEvent('pointerdown',{bubbles:true}));expect(more().open).toBe(true)
 globalThis.document.dispatchEvent(new globalThis.KeyboardEvent('keydown',{key:'Escape'}));await flushPromises();expect(more().open).toBe(false)
 more().open=true;await w.get('[data-reload-view]').trigger('click');expect(more().open).toBe(false)
 w.unmount()
})
it('toolbar holding 更多 does not clip its menu',async()=>{
 const src=(await import('./Topology.vue?raw')).default
 expect(src).toMatch(/\.topology-toolbar\.panel\{[^}]*overflow:visible/)
})

const menuItems=(w:ReturnType<typeof setup>)=>w.findAll('.more-menu > *').map(e=>e.text().replace(/\s+/g,' ').trim().split(' ')[0]!)
const tab=async(w:ReturnType<typeof setup>,m:string)=>{await w.get(`[data-mode="${m}"]`).trigger('click');await flushPromises()}
it('LUNA-34 menu items and order per tab, mock and non-mock',async()=>{
 const w=setup(false,false,'',{},'mm2');await flushPromises()
 expect(menuItems(w)).toEqual(['显示禁用任务（0）','整理布局','保存个人视图','重新加载视图','视图版本','Mock'])
 await tab(w,'relations');expect(menuItems(w)).toEqual(['显示禁用关系（0）','整理布局','保存个人视图','重新加载视图','视图版本','Mock'])
 for(const m of ['instances','tasks']){await tab(w,m);expect(menuItems(w)).toEqual(['显示禁用任务（0）','重新加载视图','Mock'])}
 w.unmount()
 testRuntime.value.mode='readonly-lab';const r=setup(false,false,'',{},'tasks');await flushPromises()
 expect(menuItems(r)).toEqual(['显示禁用任务（0）','重新加载视图']);r.unmount()
})
it('LUNA-34 disabled toggle is shared across tabs and does not close the menu; mock select does not close either',async()=>{
 const w=setup(false,false,'',{},'mm2');await flushPromises()
 const more=()=>w.get('.topology-more').element as HTMLDetailsElement
 more().open=true;await w.get('[data-show-disabled]').setValue(true);expect(more().open).toBe(true)
 await w.get('.more-menu select').setValue('partial_failure');expect(more().open).toBe(true)
 for(const m of ['relations','instances','tasks','mm2']){await tab(w,m);expect((w.get('[data-show-disabled]').element as HTMLInputElement).checked).toBe(true)}
 w.unmount()
})
it('LUNA-34 unsaved layout notice with inline save, kept across tab switches',async()=>{
 const w=setup(false,false,'',{},'mm2');await flushPromises()
 expect(w.find('[data-unsaved]').exists()).toBe(false)
 await w.get('[data-move]').trigger('click')
 expect(w.get('[data-unsaved]').text()).toContain('有未保存的布局改动');expect(w.find('.dirty-dot').exists()).toBe(true)
 await tab(w,'tasks');expect(w.find('[data-unsaved]').exists()).toBe(false)
 await tab(w,'mm2');expect(w.get('[data-unsaved]').text()).toContain('有未保存的布局改动')
 await w.get('[data-inline-save]').trigger('click');await flushPromises()
 expect(w.find('[data-unsaved]').exists()).toBe(false);expect(w.find('.dirty-dot').exists()).toBe(false)
 w.unmount()
})
it('LUNA-34 reload with unsaved changes asks for confirmation',async()=>{
 const w=setup(false,false,'',{},'mm2');await flushPromises()
 await w.get('[data-move]').trigger('click')
 expect(w.get('[data-reload-view]').text()).toBe('重新加载视图（丢弃未保存改动）')
 const ask=vi.fn(()=>false);vi.stubGlobal('confirm',ask)
 await w.get('[data-reload-view]').trigger('click');await flushPromises()
 expect(ask).toHaveBeenCalledTimes(1);expect(w.find('[data-unsaved]').exists()).toBe(true)
 ask.mockReturnValue(true);await w.get('[data-reload-view]').trigger('click');await flushPromises()
 expect(w.find('[data-unsaved]').exists()).toBe(false)
 w.unmount()
})
