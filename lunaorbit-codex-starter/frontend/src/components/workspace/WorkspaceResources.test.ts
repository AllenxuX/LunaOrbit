import { flushPromises, mount, RouterLinkStub } from '@vue/test-utils'
import { afterEach, expect, it, vi } from 'vitest'
import WorkspaceResources from './WorkspaceResources.vue'
const markers = { synthetic: true, observed_at: '2026-10-20T00:00:00Z', stale: false, state: 'verified', capabilities: [] }
afterEach(() => vi.unstubAllGlobals())
it('opens consumer groups on a dedicated page without inline offset requests', async () => {
  const fetcher = vi.fn(async () => new Response(JSON.stringify({items:[{id:'consumer/a',state:'Stable'}],total:1,...markers})))
  vi.stubGlobal('fetch',fetcher)
  const w=mount(WorkspaceResources,{props:{clusterId:'one',section:'groups',canManageTopics:false},global:{stubs:{RouterLink:RouterLinkStub}}});await flushPromises()
  expect(w.findComponent(RouterLinkStub).props('to')).toBe('/kafka/clusters/one/groups/consumer%2Fa')
  expect(fetcher).toHaveBeenCalledTimes(1)
  expect(w.find('[data-offsets]').exists()).toBe(false)
  w.unmount()
})
it('ignores late responses when the cluster or tab changes', async () => {
  let finish!: (r: Response) => void
  vi.stubGlobal('fetch', vi.fn(async (input: string) => String(input).includes('/old/topics') ? new Promise<Response>(resolve => { finish = resolve }) : new Response(JSON.stringify({ items: [{ id: 'new-group' }], total: 1, ...markers }))))
  const w = mount(WorkspaceResources, { props: { clusterId: 'old', section: 'topics', canManageTopics: true } }); await flushPromises()
  await w.setProps({ clusterId: 'new', section: 'groups' }); await flushPromises()
  finish(new Response(JSON.stringify({ items: [{ name: 'old-topic' }], total: 1, ...markers }))); await flushPromises()
  expect(w.text()).toContain('new-group'); expect(w.text()).not.toContain('old-topic')
  w.unmount()
})
it('emits create intent without writing to Kafka', async () => {
  vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ items: [], total: 0, ...markers }))))
  const w = mount(WorkspaceResources, { props: { clusterId: 'one', section: 'topics', canManageTopics: true } }); await flushPromises()
  await w.get('[data-create-topic]').trigger('click')
  expect(w.emitted('intent')).toEqual([[{ kind: 'topic_create' }]])
  w.unmount()
})
it('navigates from each Topic to its dedicated detail page without fetching inline detail', async () => {
 const fetcher=vi.fn(async()=>new Response(JSON.stringify({items:[{name:'events.test',partitions:[],rf_min:1,rf_max:1}],total:1,...markers})));vi.stubGlobal('fetch',fetcher)
 const w=mount(WorkspaceResources,{props:{clusterId:'one',section:'topics',canManageTopics:true},global:{stubs:{RouterLink:RouterLinkStub}}});await flushPromises()
 expect(w.findComponent(RouterLinkStub).props('to')).toBe('/kafka/clusters/one/topics/events.test')
 expect(w.find('.resource-detail').exists()).toBe(false)
 expect(fetcher).toHaveBeenCalledTimes(1)
 w.unmount()
})
it('links Broker entries to their resource pages',async()=>{
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>new Response(JSON.stringify({items:String(input).includes('/brokers')?[{id:2,host:'broker.test',port:9092}]:[],total:1,...markers}))))
 const w=mount(WorkspaceResources,{props:{clusterId:'one',section:'overview',canManageTopics:false},global:{stubs:{RouterLink:RouterLinkStub}}});await flushPromises()
 expect(w.findComponent(RouterLinkStub).props('to')).toBe('/kafka/clusters/one/brokers/2')
 w.unmount()
})
