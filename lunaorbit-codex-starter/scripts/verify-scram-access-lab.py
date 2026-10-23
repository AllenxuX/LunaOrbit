"""Opt-in end-to-end checks on the guarded synthetic lab; never prints credentials."""
import http.cookiejar
from importlib.machinery import SourceFileLoader
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
import uuid

os.umask(0o077)
root = Path('/root/lunaorbit-minikube-20261020')
lab = SourceFileLoader('scram_acceptance_guard', str(root / 'kafka-acl-lab.py')).load_module()
lab.guard()
origin = 'http://127.0.0.1:18088'

def client():
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

def request(opener, path, data=None):
    req = urllib.request.Request(origin + '/api/v1' + path,
        data=json.dumps(data).encode() if data is not None else None,
        headers={'Origin': origin, 'Content-Type': 'application/json'})
    with opener.open(req, timeout=40) as response:
        return json.load(response)

def command(args, payload=None):
    result = subprocess.run(['kubectl','--context','lunaorbit-lab','-n','lunaorbit-integration',*args],
        input=payload, capture_output=True, timeout=90)
    if result.returncode:
        # No stderr: Kafka and shell tools can include input configuration.
        for line in result.stdout.decode(errors='replace').splitlines():
            if line.startswith(('PASS ', 'FAIL ')) and all(c.isupper() or c in ' _' for c in line):
                print(line, flush=True)
        raise RuntimeError('synthetic command failed')
    return result.stdout.decode()

def topic(opener, base, name):
    intent={'kind':'topic_create','topic':name,'partitions':1,'replication_factor':1,'retention_ms':3600000}
    plan=request(opener,base+'/plans',intent)
    change=request(opener,base+'/requests',dict(intent,fingerprint=plan['fingerprint']))
    assert change['status']=='approved'
    result=request(opener,base+'/requests/'+change['id']+'/execute',{'revision':change['revision']})
    assert result['status']=='executed'

def main():
    owner=client();request(owner,'/dev/session',{})
    request(owner,'/lab/registrations',{})
    clusters=request(owner,'/kafka/clusters')['items']
    payloads=[]
    for role,label in [('source','实验源集群'),('target','实验目标集群')]:
        cluster=next(c for c in clusters if c['display_name']==label)
        base='/kafka/clusters/'+cluster['id']+'/workspace'
        suffix=uuid.uuid4().hex[:12]
        allowed='lunaorbit-scram-'+suffix;denied=allowed+'-private';group=allowed+'-consumer'
        topic(owner,base,allowed);topic(owner,base,denied)
        app=request(owner,base+'/apps',{'name':'Synthetic SCRAM acceptance '+role,'owner':'Synthetic laboratory','request_id':str(uuid.uuid4())})
        intent={'mode':'create','request_id':str(uuid.uuid4()),'application_id':app['id'],'access_mode':'produce_consume',
            'topics':[{'name':allowed,'pattern_type':'LITERAL'}],'group':{'name':group,'pattern_type':'LITERAL'}}
        created=request(owner,base+'/accounts',intent)['account']
        again=request(owner,base+'/accounts',intent)['account']
        assert again['id']==created['id']
        account=request(owner,base+'/accounts/'+created['id']+'/execute',{'revision':created['revision']})['account']
        assert account['operation']['status']=='executed', 'account did not finish provisioning'
        password=request(owner,base+'/accounts/'+account['id']+'/reveal',{'revision':account['revision']})['password']
        assert password and password not in json.dumps(request(owner,base+'/accounts?page_size=100'))
        payload={'bootstrap_servers':[f'kafka-{role}.lunaorbit-integration.svc.cluster.local:9094'],
            'username':account['username'],'password':password,'topic':allowed,'denied_topic':denied,'group':group,'denied_group':group+'-forbidden'}
        (root/'scram-current-private.json').write_text(json.dumps(payload))
        binary=(root/'scramcheck-linux').read_bytes()
        command(['exec','-i','deploy/kafka-'+role,'--','sh','-c','umask 077; cat > /tmp/lunaorbit-scramcheck; chmod 700 /tmp/lunaorbit-scramcheck'],binary)
        output=command(['exec','-i','deploy/kafka-'+role,'--','/tmp/lunaorbit-scramcheck','--lab'],json.dumps(payload).encode())
        assert output.count('PASS ')==9
        print('SCRAM_BUSINESS_PERMISSION_MATRIX_PASS:',role,'auth good/bad; produce/fetch allow; topic/group deny')
        payloads.append({'cluster':cluster['id'],'account':account['id'],'revision':account['revision'], 'payload':payload})
        # A separate platform owner imports an existing broker identity. Neither
        # SCRAM nor ACL mutation is involved in import.
        importer=client();request(importer,'/dev/session',{});request(importer,'/lab/registrations',{})
        other=next(c for c in request(importer,'/kafka/clusters')['items'] if c['display_name']==label)
        otherbase='/kafka/clusters/'+other['id']+'/workspace'
        otherapp=request(importer,otherbase+'/apps',{'name':'Synthetic existing identity import','owner':'Synthetic import team'})
        import_intent={'mode':'import','application_id':otherapp['id'],'username':account['username'],'password':password}
        bad=dict(import_intent,password=password+'-wrong')
        try:
            request(importer,otherbase+'/accounts',bad)
            raise AssertionError('incorrect existing credential accepted')
        except urllib.error.HTTPError as error:
            assert error.code in (400,403,503)
        imported=request(importer,otherbase+'/accounts',import_intent)['account']
        assert imported['observation']=='verified'
        assert request(importer,otherbase+'/accounts/'+imported['id']+'/reveal',{'revision':imported['revision']})['password']==password
        # Recheck unchanged original credential and permission matrix.
        assert command(['exec','-i','deploy/kafka-'+role,'--','/tmp/lunaorbit-scramcheck','--lab'],json.dumps(payload).encode()).count('PASS ')==9
        print('SCRAM_EXISTING_ACCOUNT_IMPORT_PASS:',role,'wrong password rejected; original identity and permissions preserved')
    (root/'scram-acceptance-private.json').write_text(json.dumps(payloads))
    print('SCRAM_ACCESS_LAB_ACCEPTANCE_PASS')

if __name__=='__main__':
    try: main()
    except Exception as error:
        print('SCRAM_ACCESS_LAB_ACCEPTANCE_FAILED:',type(error).__name__)
        raise SystemExit(1)
