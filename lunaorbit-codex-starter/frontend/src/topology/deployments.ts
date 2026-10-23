import type { Link } from '../api/topology'

export function deploymentGroups(links:Link[],stale=false){
 const result=new Map<string,{id:string;name:string;namespace:string;links:Link[];runtime:string}>()
 for(const link of links){
  const id=JSON.stringify([link.namespace,link.workload_uid||link.workload_name])
  let group=result.get(id)
  if(!group){group={id,name:link.workload_name,namespace:link.namespace,links:[],runtime:''};result.set(id,group)}
  group.links.push(link)
 }
 for(const group of result.values()){
  const values=[...new Set(group.links.map(l=>`${l.runtime.ready_replicas} / ${l.runtime.replicas}`))]
  group.runtime=stale||group.links.some(l=>l.stale)?'运行数据已过期':values.length===1?`就绪 ${values[0]}`:'运行数据不一致，请刷新'
 }
 return [...result.values()].sort((a,b)=>a.name.localeCompare(b.name)||a.id.localeCompare(b.id))
}
