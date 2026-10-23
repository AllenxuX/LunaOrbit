import { afterEach, expect, it, vi } from 'vitest'
import { saveInformationApplication } from './information'
afterEach(()=>vi.unstubAllGlobals())
it('sends identity metadata and discards stale producer group fields',async()=>{
 const fetcher=vi.fn().mockResolvedValue({ok:true,status:200,json:async()=>({})});vi.stubGlobal('fetch',fetcher)
 await saveInformationApplication({name:'orders',owner:'',environment:'',description:'',location:'service-a',relations:[{cluster_id:'a',topic:'orders',mode:'produce',account:' user-a ',client_id:' client-a ',consumer_group:'stale-consumer'}]})
 const body=JSON.parse(fetcher.mock.calls[0]![1].body)
 expect(body.location).toBe('service-a')
 expect(body.relations).toEqual([{cluster_id:'a',topic:'orders',mode:'produce',account:'user-a',client_id:'client-a'}])
})
