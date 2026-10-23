import { afterEach,expect,it,vi } from 'vitest'
import { readSnapshot } from './topology'
afterEach(()=>vi.unstubAllGlobals())
function page(n:number,total:number,attempt='now') {
 const links=Array.from({length:n===1?100:1},(_,i)=>({id:String((n-1)*100+i)}))
 return {page:n,page_size:100,total,snapshot:{scope_id:'scope',observed_at:'then',attempted_at:attempt,links,
 nodes:[{id:'source',kind:'kafka'},{id:'work',kind:'workload'},{id:'target',kind:'kafka'}],
 edges:links.flatMap(l=>[{id:l.id+'-in',link_id:l.id,source:'source',target:'work',relation:'declared_input'},{id:l.id+'-out',link_id:l.id,source:'work',target:'target',relation:'declared_output'}])}}
}
it('loads every page and deduplicates shared nodes',async()=>{
 vi.stubGlobal('fetch',vi.fn(async(url:string)=>new Response(JSON.stringify(page(url.includes('page=1&')?1:2,101)))))
 const result=await readSnapshot('scope');expect(result.links).toHaveLength(101);expect(result.nodes).toHaveLength(3)
})
it('rejects mixed revisions instead of presenting a partial graph',async()=>{
 vi.stubGlobal('fetch',vi.fn(async(url:string)=>new Response(JSON.stringify(page(url.includes('page=1&')?1:2,101,url.includes('page=1&')?'before':'after')))))
 await expect(readSnapshot('scope')).rejects.toThrow('快照已更新')
})
it('rejects truncated pages',async()=>{
 const data=page(1,101);data.snapshot.links=[]
 vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify(data))))
 await expect(readSnapshot('scope')).rejects.toThrow('响应不完整')
})

it('rejects dangling edge endpoints instead of presenting an apparently complete graph',async()=>{
 const data=page(1,100);data.snapshot.nodes.pop()
 vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify(data))))
 await expect(readSnapshot('scope')).rejects.toThrow('响应不完整')
})
it('rejects missing directional edges',async()=>{
 const data=page(1,100);data.snapshot.edges.pop()
 vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify(data))))
 await expect(readSnapshot('scope')).rejects.toThrow('响应不完整')
})
