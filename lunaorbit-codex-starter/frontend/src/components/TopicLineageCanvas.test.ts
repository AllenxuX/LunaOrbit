import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, expect, it, vi } from 'vitest'
import TopicLineageCanvas from './TopicLineageCanvas.vue'
import type { LineageView } from '../topology/lineage-model'
import type { LayoutPlan } from '../topology/routing-types'
const fake=vi.hoisted(()=>({tasks:[] as {resolve:(p:LayoutPlan)=>void;reject:(e:Error)=>void;cancel:ReturnType<typeof vi.fn>}[],reset:vi.fn(),options:{} as Record<string,unknown>}))
vi.mock('../topology/layout-client',()=>({createLayoutTask:()=>{let resolve!:(p:LayoutPlan)=>void,reject!:(e:Error)=>void;const result=new Promise<LayoutPlan>((yes,no)=>{resolve=yes;reject=no});const task={resolve,reject,cancel:vi.fn()};fake.tasks.push(task);return{result,cancel:task.cancel}}}))
vi.mock('@antv/x6',()=>({Graph:class{constructor(options:Record<string,unknown>){fake.options=options}resetCells=fake.reset;createNode(value:unknown){return value}createEdge(value:unknown){return value}on(){}dispose(){}zoomToFit(){}zoom(){return 1}resize(){}}}))
const data={nodes:[{id:'root',kind:'topic',label:'orders'}],edges:[]} as unknown as LineageView
const plan={positions:{root:{x:0,y:0}},ports:{},routes:{},nodeWidth:238,nodeHeight:88}
beforeEach(()=>{fake.tasks=[];fake.reset.mockClear()})
it('keeps the graph read-only and discards a superseded layout',async()=>{const wrapper=mount(TopicLineageCanvas,{props:{view:data,rootId:'root'}});await wrapper.setProps({view:{...data,nodes:[...data.nodes]}});expect(fake.tasks[0]?.cancel).toHaveBeenCalled();fake.tasks[1]?.resolve(plan);await flushPromises();const count=fake.reset.mock.calls.length;fake.tasks[0]?.resolve(plan);await flushPromises();expect(fake.reset.mock.calls.length).toBe(count);expect(fake.options.interacting).toBe(false);wrapper.unmount()})
it('cancels the worker when the canvas unmounts',async()=>{const wrapper=mount(TopicLineageCanvas,{props:{view:data,rootId:'root'}});wrapper.unmount();expect(fake.tasks[0]?.cancel).toHaveBeenCalled();const count=fake.reset.mock.calls.length;fake.tasks[0]?.resolve(plan);await flushPromises();expect(fake.reset.mock.calls.length).toBe(count)})
it('shows a layout failure with a retry action',async()=>{const wrapper=mount(TopicLineageCanvas,{props:{view:data,rootId:'root'}});fake.tasks[0]?.reject(new Error('worker failed'));await flushPromises();expect(wrapper.get('[role="alert"]').text()).toContain('自动布局失败');await wrapper.get('[role="alert"] button').trigger('click');expect(fake.tasks).toHaveLength(2);wrapper.unmount()})
