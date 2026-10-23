#!/usr/bin/env python3
"""Opt-in synthetic lab lifecycle; never targets a business database or Kafka."""
import http.cookiejar,json,os,secrets,subprocess,sys,time,urllib.error,urllib.parse,urllib.request
from pathlib import Path
base=Path.cwd().resolve()
server=Path(sys.argv[1]).resolve()
if server.parent!=base or not server.is_file():raise SystemExit('Dedicated lab executable required')
values=dict(line.strip().split('=',1) for line in (base/'.env').read_text().splitlines() if '=' in line)
if values.get('POSTGRES_DB')!='lunaorbit_phase02_test':raise SystemExit('Dedicated database required')
port=int(values['POSTGRES_PORT'])
if port!=25432:raise SystemExit('Dedicated loopback PostgreSQL port required')
env=dict(os.environ,HTTP_ADDR='127.0.0.1:18081',REGISTRY_MODE='postgres-mock',DISCOVERY_MODE='mock',CHANGES_MODE='mock',OBSERVABILITY_MODE='mock',IDENTITY_MODE='dev',DEV_SESSION_KEY=secrets.token_hex(32),CHANGE_SNAPSHOT_KEY=secrets.token_hex(32))
env['DATABASE_URL']='postgresql://'+urllib.parse.quote(values['POSTGRES_USER'])+':'+urllib.parse.quote(values['POSTGRES_PASSWORD'])+'@127.0.0.1:25432/lunaorbit_phase02_test?sslmode=disable'
url='http://127.0.0.1:18081'
clients=[urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())) for _ in range(2)]
process=None
log=open(base/'lifecycle-server.log','wb');os.chmod(base/'lifecycle-server.log',0o600)
def call(who,method,path,body=None,expected=200):
 raw=None if body is None else json.dumps(body).encode()
 req=urllib.request.Request(url+'/api/v1'+path,data=raw,method=method,headers={'Content-Type':'application/json','Origin':url})
 try:r=clients[who].open(req,timeout=12)
 except urllib.error.HTTPError as e:r=e
 data=r.read()
 if r.code!=expected:raise AssertionError('Unexpected status for '+method+' '+path+': '+str(r.code))
 return json.loads(data) if data else None
def stop():
 global process
 if process is not None:
  process.terminate()
  try:process.wait(timeout=10)
  except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
  process=None
def start():
 global process
 process=subprocess.Popen([str(server)],env=env,stdout=log,stderr=log,cwd=base)
 for _ in range(100):
  if process.poll() is not None:raise AssertionError('Lab application failed to start')
  try:
   if urllib.request.urlopen(url+'/healthz',timeout=1).status==200:return
  except (OSError,urllib.error.URLError):pass
  time.sleep(.1)
 raise AssertionError('Lab application startup timeout')
def discover():
 call(0,'POST','/discovery/scopes/synthetic-mm2/runs',{},202)
 for _ in range(100):
  job=call(0,'GET','/discovery/scopes/synthetic-mm2/runs/latest')
  if job['status']=='done':
   if job.get('error_code'):raise AssertionError('Synthetic discovery failed')
   return
  time.sleep(.05)
 raise AssertionError('Synthetic worker timeout')
try:
 start()
 a=call(0,'POST','/dev/session',{});b=call(1,'POST','/dev/session',{})
 cluster=call(0,'POST','/kafka/clusters',{'display_name':'Synthetic lifecycle','bootstrap_servers':['broker.synthetic.test:9092'],'enabled':True,'security_protocol':'PLAINTEXT','sasl_mechanism':'','tls_server_name':''},201)
 discover()
 snapshot=call(0,'GET','/discovery/scopes/synthetic-mm2/snapshot?page_size=100')['snapshot']
 view={'revision':0,'positions':{snapshot['nodes'][0]['id']:{'x':123,'y':456}},'filters':{'room':'','environment':''},'layers':{'kafka':True,'workloads':True}}
 saved=call(0,'PUT','/topology/scopes/synthetic-mm2/view',view)
 confirmation=call(0,'POST','/observability/scopes/synthetic-mm2/lineage-confirmations',{'link_id':snapshot['links'][0]['id'],'topic':'synthetic-events','description':'Synthetic business declaration'},201)
 rule={'principal':'User:synthetic-lifecycle','host':'*','resource_type':'TOPIC','resource_name':'synthetic-events','pattern_type':'LITERAL','operation':'READ','permission':'ALLOW'}
 acl=call(0,'POST','/kafka/clusters/'+cluster['id']+'/acl-requests',{'rule':rule,'preview':False})
 target=call(0,'GET','/changes/targets')['items'][0]
 change=call(0,'POST','/changes',{'target_id':target['id'],'topic_rule':'synthetic.events.*','expected_resource_version':target['resource_version'],'preview':False})
 env['ACL_APPROVAL_SCOPES']=json.dumps({b['user_id']:[cluster['id']]})
 env['CHANGE_APPROVAL_SCOPES']=json.dumps({b['user_id']:[target['id']]})
 stop();start()
 assert call(0,'GET','/kafka/clusters/'+cluster['id'])['id']==cluster['id']
 assert call(0,'GET','/topology/scopes/synthetic-mm2/view')==saved
 assert call(0,'GET','/observability/scopes/synthetic-mm2/lineage-confirmations')['items'][0]['id']==confirmation['id']
 call(1,'GET','/kafka/clusters/'+cluster['id'],expected=404)
 inbox=call(1,'GET','/acl-requests/inbox?page=1&page_size=20')
 assert any(x['id']==acl['id'] and x['can_approve'] for x in inbox['items'])
 approved=call(1,'POST','/acl-requests/'+acl['id']+'/approve',{'revision':acl['revision']})
 assert approved['status']=='approved_not_executed'
 approved_change=call(1,'POST','/changes/'+change['id']+'/approve',{'revision':change['revision']})
 assert approved_change['status']=='approved_not_executed'
 executed=call(0,'POST','/changes/'+change['id']+'/execute',{'revision':approved_change['revision'],'scenario':'success'})
 assert executed['synthetic'] and executed['status']=='succeeded'
 after=call(0,'GET','/discovery/scopes/synthetic-mm2/snapshot?page_size=100')['snapshot']
 assert after==snapshot and not any(link['verified'] for link in after['links'])
 stop();start()
 assert call(0,'GET','/changes/'+change['id'])['status']=='succeeded'
 assert call(0,'GET','/kafka/clusters/'+cluster['id']+'/acl-requests')['items'][0]['status']=='approved_not_executed'
 print('PASS: PostgreSQL application restarts preserve sessions, registry, discovery, private layout, lineage, ACL approval and encrypted synthetic change workflow')
finally:stop();log.close()
