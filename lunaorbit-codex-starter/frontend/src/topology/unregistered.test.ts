import { describe, expect, it } from 'vitest'
import { registeredLab, labRegistrations } from './fixtures'
import { endpointStates, unregisteredSummary, registerHref, addressesOverlap, endpointMarks, manualReviewKey } from './unregistered'

describe('unregistered endpoints',()=>{
 it('finds an endpoint with no registration and no candidate as unregistered, with its enabled links',()=>{
  const s=registeredLab(['k9']),states=endpointStates(s,labRegistrations(['k9']))
  expect([...states.keys()]).toEqual(['alias:k9'])
  const k9=states.get('alias:k9')!
  expect(k9).toMatchObject({alias:'k9',state:'unregistered',candidateIds:[]})
  expect(k9.linkIds).toEqual(['L10'])
 })
 it('reports nothing when every cluster is registered',()=>{
  expect(endpointStates(registeredLab(),labRegistrations()).size).toBe(0)
  expect(unregisteredSummary(registeredLab(),labRegistrations())).toBeUndefined()
 })
 it('separates address-ambiguous endpoints from unregistered ones',()=>{
  const states=endpointStates(registeredLab(['k9'],['k5']),labRegistrations(['k9']))
  expect(states.get('alias:k5')).toMatchObject({state:'ambiguous',candidateIds:['k1','k2']})
  expect(states.get('alias:k9')!.state).toBe('unregistered')
 })
 it('summarises counts and the banner sentence',()=>{
  const sum=unregisteredSummary(registeredLab(['k9']),labRegistrations(['k9']))!
  expect(sum).toMatchObject({registered:8,unregistered:1,aliases:['k9']})
  expect(sum.banner).toBe('1 条链路的对端 k9 为自动发现端点，尚未合并登记信息；点击节点查看访问状态')
  const two=unregisteredSummary(registeredLab(['k5','k9']),labRegistrations(['k5','k9']))!
  expect(two.banner).toBe('3 条链路的对端 k5、k9 为自动发现端点，尚未合并登记信息；点击节点查看访问状态')
  expect(two).toMatchObject({registered:7,unregistered:2})
 })
 it('marks unregistered nodes and manually bound endpoints that still need review',()=>{
  const s=registeredLab(['k9'])
  expect(endpointMarks(s,labRegistrations(['k9']),new Set())).toEqual({'alias:k9':'自动发现'})
  const reviewed=new Set([manualReviewKey('lab','uid-L10','k8')])
  expect(endpointMarks(s,labRegistrations(['k9']),reviewed)).toEqual({'alias:k9':'自动发现','kafka:k8':'人工绑定 · 需复核'})
  expect(endpointMarks(registeredLab(),labRegistrations(),new Set())).toEqual({})
 })
 it('builds a registration link prefilled with the alias and only host:port addresses',()=>{
  expect(registerHref('k9')).toBe('/kafka/clusters?register=1&name=k9')
  expect(registerHref('k9',['SASL_SSL://user:pw@k9-kafka.luna-kafka.svc:9092','k9-b:9092'])).toBe('/kafka/clusters?register=1&name=k9&bootstrap=k9-kafka.luna-kafka.svc%3A9092%2Ck9-b%3A9092')
 })
 it('detects whether two bootstrap lists share an address',()=>{
  expect(addressesOverlap(['a:9092','B:9093'],['b:9093'])).toBe(true)
  expect(addressesOverlap(['a:9092'],['127.0.0.1:19100'])).toBe(false)
 })
})
