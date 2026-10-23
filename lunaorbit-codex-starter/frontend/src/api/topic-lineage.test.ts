import { describe, expect, it, vi } from 'vitest'
import { evidenceLabel, lineagePresentation, resourcePath, readLineage, observedTime } from './topic-lineage'
describe('topic lineage contract', () => {
 it('keeps configured, observed and historical evidence distinct', () => {
  expect(evidenceLabel('configured')).toContain('配置')
  expect(evidenceLabel('observed')).toContain('观测')
  expect(evidenceLabel('historical')).toContain('历史')
 })
 it('preserves cross-cluster identity and never promotes existence to verification', () => {
  const graph = lineagePresentation({ nodes: [{id:'a',kind:'topic',label:'orders',exists:true},{id:'b',kind:'topic',label:'orders',exists:true}],edges:[{id:'e',source:'a',target:'b',kind:'sync',evidence:'configured'}] })
  expect(graph.nodes.map(n=>n.id)).toEqual(['a','b'])
  expect(graph.edges[0]?.state).toBe('unverified')
 })
 it('escapes resource names and avoids restricted application governance routes', () => {
  expect(resourcePath({id:'x',kind:'topic',label:'a/b',topic:'a/b',cluster_id:'c'})).toBe('/kafka/clusters/c/topics/a%2Fb')
  expect(resourcePath({id:'x',kind:'application',label:'app',application_id:'a'})).toBe('/applications#application-a')
 })
 it('passes cancellation and root query to the read-only endpoint', async () => {
  const fetch = vi.fn().mockResolvedValue({ok:true,json:async()=>({nodes:[],edges:[]})}); vi.stubGlobal('fetch',fetch)
  const controller = new AbortController()
  await readLineage('c','a/b',2,'both',controller.signal)
  expect(fetch.mock.calls[0]?.[0]).toContain('topic=a%2Fb')
  expect(fetch.mock.calls[0]?.[1].signal).toBe(controller.signal)
  vi.unstubAllGlobals()
 })
})

it('formats valid collection timestamps and hides invalid sentinel dates',()=>{expect(observedTime('0001-01-01T00:00:00Z')).toBe('未知');expect(observedTime('invalid')).toBe('未知');expect(observedTime('2026-10-23T08:00:00Z')).toContain('2026')})
