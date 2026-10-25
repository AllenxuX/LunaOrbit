import { afterEach, expect, it, vi } from 'vitest'
import { registryRequest, ensureSession, probeMessage, probeStatus, capabilityStatus } from './registry'

afterEach(() => {vi.unstubAllGlobals();vi.useRealTimers()})
it('preserves only the recognized ACL capability error code without upstream text', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ error: { code: 'AUTHORIZER_UNVERIFIED', message: 'private-upstream' } }), { status: 501 })))
  await expect(registryRequest('/kafka/clusters/id/acls')).rejects.toMatchObject({ code: 'AUTHORIZER_UNVERIFIED', status: 501 })
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ error: { code: 'private-upstream' } }), { status: 501 })))
  await expect(registryRequest('/kafka/clusters/id/acls')).rejects.toMatchObject({ code: 'REGISTRY_ERROR' })
})
it('distinguishes unprobed and unknown capabilities from explicitly unsupported mock evidence', () => {
  expect(capabilityStatus({ supported: false, reason: 'not probed' })).toBe('未探测')
  expect(capabilityStatus({ supported: false, reason: 'not_probed' })).toBe('未探测')
  expect(capabilityStatus({ supported: false, reason: 'unknown reason' })).toBe('待确认')
  expect(capabilityStatus({ supported: false, reason: 'mock unsupported' })).toBe('不支持')
  expect(capabilityStatus({ supported: false, reason: 'synthetic unsupported response fixture' })).toBe('不支持')
  expect(capabilityStatus({ supported: true, reason: 'mock supported' })).toBe('支持')
})
it('maps server PascalCase and mock snake case errors to safe localized labels', () => {
  const pairs = [
    ['AuthenticationFailed', 'authentication_failed'], ['TLSHandshakeFailed', 'tls_failed'],
    ['BrokerUnreachable', 'broker_unreachable'], ['AdvertisedBrokerUnreachable', 'advertised_unreachable'],
    ['AuthorizationDenied', 'authorization_denied'], ['UnsupportedAPI', 'unsupported_api'], ['Timeout', 'timeout'],
  ]
  for (const [server, mock] of pairs) expect(probeMessage(server)).toBe(probeMessage(mock))
  expect(probeMessage('UnknownSensitiveError')).toBe('模拟探测失败')
  expect(probeStatus('mock_connected')).toBe('模拟连接成功（未验证真实 Kafka）')
})
it('replaces unknown server details with a fixed safe error', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ error: { code: 'SECRET_FAILURE', message: 'private-password' } }), { status: 500 })))
  await expect(registryRequest('/kafka/clusters')).rejects.toThrow('操作失败，请稍后重试')
})
it('establishes a cookie session only after session lookup is unauthorized', async () => {
  const fetcher = vi.fn().mockResolvedValueOnce(new Response('{}', { status: 401 })).mockResolvedValueOnce(new Response(JSON.stringify({ user_id: 'synthetic-user', mode: 'mock', persistent: false })))
  vi.stubGlobal('fetch', fetcher)
  expect((await ensureSession()).mode).toBe('mock')
  expect(fetcher.mock.calls[1]?.[0]).toBe('/api/v1/dev/session')
  expect(fetcher.mock.calls[1]?.[1].credentials).toBe('same-origin')
})
it('initializes configured lab registrations in the current existing browser session', async () => {
  const session = { user_id: 'current-browser-owner', mode: 'mock', persistent: false, lab_catalog: true }
  const fetcher = vi.fn().mockResolvedValueOnce(new Response(JSON.stringify(session)))
    .mockResolvedValueOnce(new Response(JSON.stringify({ items: [{ id: 'source', status: 'connected' }, { id: 'target', status: 'connected' }] })))
  vi.stubGlobal('fetch', fetcher)
  expect((await ensureSession()).user_id).toBe('current-browser-owner')
  expect(fetcher.mock.calls[1]?.[0]).toBe('/api/v1/lab/registrations')
  expect(fetcher.mock.calls[1]?.[1].method).toBe('POST')
  expect(fetcher.mock.calls[1]?.[1].body).toBe('{}')
})
it('waits for actual asynchronous lab probes instead of inventing successful states', async () => {
  const fetcher = vi.fn().mockResolvedValueOnce(new Response(JSON.stringify({ user_id: 'owner', mode: 'mock', lab_catalog: true })))
    .mockResolvedValueOnce(new Response(JSON.stringify({ items: [{ id: 'source', status: 'probing' }, { id: 'target', status: 'connected' }] })))
    .mockResolvedValueOnce(new Response(JSON.stringify({ status: 'done', error_code: 'authentication_failed' })))
  vi.stubGlobal('fetch', fetcher)
  await ensureSession()
  expect(fetcher.mock.calls[2]?.[0]).toBe('/api/v1/kafka/clusters/source/probe')
  expect(fetcher.mock.calls[2]?.[1].method).toBe('GET')
})

it('accepts the four configured lab clusters without truncating catalog initialization',async()=>{
 const items=Array.from({length:4},(_,index)=>({id:`synthetic-${index}`,status:'connected'}))
 const fetcher=vi.fn().mockResolvedValueOnce(new Response(JSON.stringify({user_id:'owner',mode:'mock',lab_catalog:true}))).mockResolvedValueOnce(new Response(JSON.stringify({items})))
 vi.stubGlobal('fetch',fetcher)
 expect((await ensureSession()).user_id).toBe('owner')
 expect(fetcher.mock.calls).toHaveLength(2)
})
it.each([0,1,17])('rejects an invalid lab catalog length of %i before probing',async(length)=>{
 const fetcher=vi.fn().mockResolvedValueOnce(new Response(JSON.stringify({user_id:'owner',mode:'mock',lab_catalog:true}))).mockResolvedValueOnce(new Response(JSON.stringify({items:Array.from({length},(_,index)=>({id:`synthetic-${index}`,status:'probing'}))})))
 vi.stubGlobal('fetch',fetcher)
 await expect(ensureSession()).rejects.toMatchObject({code:'INVALID_RESPONSE'})
 expect(fetcher.mock.calls).toHaveLength(2)
})
it('accepts the bounded maximum of sixteen lab clusters',async()=>{
 const fetcher=vi.fn().mockResolvedValueOnce(new Response(JSON.stringify({user_id:'owner',mode:'mock',lab_catalog:true}))).mockResolvedValueOnce(new Response(JSON.stringify({items:Array.from({length:16},(_,index)=>({id:`synthetic-${index}`,status:'connected'}))})))
 vi.stubGlobal('fetch',fetcher)
 await expect(ensureSession()).resolves.toMatchObject({user_id:'owner'})
})

it('dispatches all four lab registrations in bounded batches after the first probes finish',async()=>{
 let batches=0
 const events:string[]=[]
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>{
  const url=String(input)
  if(url.endsWith('/session'))return new Response(JSON.stringify({user_id:'owner',mode:'mock',lab_catalog:true}))
  if(url.endsWith('/lab/registrations')){
   batches++;events.push(`batch-${batches}`)
   return new Response(JSON.stringify({items:['a','b','c','d'].map((id,index)=>({id,status:batches===1?(index<2?'probing':'unverified'):(index<2?'connected':'probing')}))}))
  }
  events.push(url);return new Response(JSON.stringify({status:'done'}))
 }))
 await ensureSession()
 expect(batches).toBe(2)
 expect(events).toEqual(['batch-1','/api/v1/kafka/clusters/a/probe','/api/v1/kafka/clusters/b/probe','batch-2','/api/v1/kafka/clusters/c/probe','/api/v1/kafka/clusters/d/probe'])
})
it('bounds repeated initialization when the pending registrations cannot obtain a probe slot',async()=>{
 let batches=0
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>new Response(JSON.stringify(String(input).endsWith('/session')?{user_id:'owner',mode:'mock',lab_catalog:true}:(batches++,{items:['a','b','c','d'].map(id=>({id,status:'unverified'}))})))))
 await ensureSession()
 expect(batches).toBe(4)
})
it('does not keep dispatching pending registrations while an earlier probe is still running',async()=>{
 vi.useFakeTimers();let batches=0,probes=0
 vi.stubGlobal('fetch',vi.fn(async(input:string)=>{
  const url=String(input)
  if(url.endsWith('/session'))return new Response(JSON.stringify({user_id:'owner',mode:'mock',lab_catalog:true}))
  if(url.endsWith('/lab/registrations')){batches++;return new Response(JSON.stringify({items:[{id:'a',status:'probing'},{id:'b',status:'unverified'}]}))}
  probes++;return new Response(JSON.stringify({status:'running'}))
 }))
 const pending=ensureSession();await vi.runAllTimersAsync();await pending
 expect(batches).toBe(1);expect(probes).toBe(20)
})
it('does not retry failed registrations while continuing pending registrations',async()=>{
 let batches=0
 const fetcher=vi.fn(async(input:string)=>{
  const url=String(input)
  if(url.endsWith('/session'))return new Response(JSON.stringify({user_id:'owner',mode:'mock',lab_catalog:true}))
  if(url.endsWith('/lab/registrations')){batches++;return new Response(JSON.stringify({items:[{id:'failed',status:'failed'},{id:'pending',status:batches===1?'unverified':'probing'}]}))}
  return new Response(JSON.stringify({status:'done',error_code:'authentication_failed'}))
 })
 vi.stubGlobal('fetch',fetcher);await ensureSession()
 expect(batches).toBe(2)
 expect(fetcher.mock.calls.filter(([url])=>url.includes('/failed/probe'))).toHaveLength(0)
 expect(fetcher.mock.calls.filter(([url])=>url.includes('/pending/probe'))).toHaveLength(1)
})

it('forwards SCOPE_REMOVED only for a 409 and leaves other codes generic', async () => {
  const reply = (body: unknown, status: number) => vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(body), { status })))
  reply({ error: { code: 'SCOPE_REMOVED', message: 'private-upstream' } }, 409)
  await expect(registryRequest('/discovery/scopes/x/bindings', 'PUT', {})).rejects.toMatchObject({ code: 'SCOPE_REMOVED', status: 409 })
  reply({ error: { code: 'CONFLICT' } }, 409)
  await expect(registryRequest('/discovery/scopes/x/bindings', 'PUT', {})).rejects.toMatchObject({ code: 'REGISTRY_ERROR', status: 409, message: '添加已更新，请关闭表单并刷新后重试' })
  reply({ error: { code: 'SCOPE_REMOVED' } }, 500)
  await expect(registryRequest('/x')).rejects.toMatchObject({ code: 'REGISTRY_ERROR', status: 500 })
  reply({ error: { code: 'SCOPE_REMOVED' } }, 501)
  await expect(registryRequest('/x')).rejects.toMatchObject({ code: 'REGISTRY_ERROR' })
})
