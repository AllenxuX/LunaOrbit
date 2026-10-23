"""Private cookie persistence check across the synthetic console pod recreation."""
import http.cookiejar
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid
import urllib.request

os.umask(0o077)
root = Path('/root/lunaorbit-minikube-20261020')
jar = http.cookiejar.LWPCookieJar(str(root / 'workspace-restart.cookies'))
statefile = root / 'workspace-restart.json'
origin = 'http://127.0.0.1:18088'
if sys.argv[1] == 'verify':
    jar.load(ignore_discard=True)
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

def request(path, data=None):
    req = urllib.request.Request(origin + path, data=json.dumps(data).encode() if data is not None else None,
                                 headers={'Origin': origin, 'Content-Type': 'application/json'})
    with opener.open(req, timeout=20) as response:
        return json.load(response)

if sys.argv[1] == 'prepare':
    session = request('/api/v1/dev/session', {})
    clusters = request('/api/v1/lab/registrations', {})['items']
    cluster = clusters[0]['id']
    app = request('/api/v1/kafka/clusters/' + cluster + '/workspace/apps',
                  {'name': 'Synthetic restart verification', 'owner': 'Synthetic laboratory',
                   'principal': 'User:synthetic-restart-check', 'description': 'Pod recreation persistence check'})
    base='/api/v1/kafka/clusters/'+cluster+'/workspace'
    account=request(base+'/accounts', {'mode':'create','request_id':str(uuid.uuid4()),'application_id':app['id']})['account']
    account=request(base+'/accounts/'+account['id']+'/execute',{'revision':account['revision']})['account']
    assert account['operation']['status']=='executed'
    password=request(base+'/accounts/'+account['id']+'/reveal',{'revision':account['revision']})['password']
    statefile.write_text(json.dumps({'user_id': session['user_id'], 'cluster': cluster, 'app': app, 'account':account,
                                    'password_hash':hashlib.sha256(password.encode()).hexdigest()}))
    jar.save(ignore_discard=True)
    print('WORKSPACE_RESTART_PREPARED: private cookie and expected synthetic metadata saved')
else:
    expected = json.loads(statefile.read_text())
    assert request('/api/v1/session')['user_id'] == expected['user_id']
    actual = request('/api/v1/kafka/clusters/' + expected['cluster'] + '/workspace/apps/' + expected['app']['id'])
    assert actual == expected['app']
    if 'account' in expected:
        account=expected['account']
        password=request('/api/v1/kafka/clusters/'+expected['cluster']+'/workspace/accounts/'+account['id']+'/reveal',{'revision':account['revision']})['password']
        assert hashlib.sha256(password.encode()).hexdigest()==expected['password_hash']
        print('SCRAM_ENCRYPTED_CREDENTIAL_POD_RECREATION_PASS')
    print('WORKSPACE_POD_RECREATION_PERSISTENCE_PASS: same signed session, registration, application and revision')
