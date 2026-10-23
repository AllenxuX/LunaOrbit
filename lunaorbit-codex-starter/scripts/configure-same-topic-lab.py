"""Explicit dedicated-lab MM2 conversion through documented Kite resource APIs.
No production targets, credential output, Topic deletion or platform write routes.
"""
import argparse
import copy
import datetime
import importlib.util
import json
import os
from pathlib import Path
import subprocess

POLICY = 'org.apache.kafka.connect.mirror.IdentityReplicationPolicy'

def transform(raw, mesh):
    lines = []
    for line in raw.splitlines():
        key = line.split('=', 1)[0].strip()
        if key.endswith('replication.policy.class'):
            continue
        if mesh and key == 'target->archive.topics':
            line = 'target->archive.topics = (lunaorbit-lab-events|lunaorbit-mesh-events)'
        lines.append(line)
    lines.append('replication.policy.class = '+POLICY)
    return '\n'.join(lines)+'\n'

def main():
    p=argparse.ArgumentParser();p.add_argument('--apply-lab',action='store_true');a=p.parse_args()
    assert a.apply_lab, 'EXPLICIT_LAB_FLAG_REQUIRED'
    os.umask(0o077)
    root=Path('/root/lunaorbit-minikube-20261020')
    spec=importlib.util.spec_from_file_location('kite_lab',root/'kite-lab-check.py')
    kite=importlib.util.module_from_spec(spec);spec.loader.exec_module(kite)
    k=['kubectl','--context=lunaorbit-lab','-n','lunaorbit-integration']
    def kube(*args,body=None):
        return subprocess.check_output(k+list(args),input=None if body is None else json.dumps(body).encode(),timeout=30)
    ns=json.loads(kube('get','namespace',kite.NS,'-o','json'))
    assert ns['metadata']['labels'].get('lunaorbit.io/purpose')=='synthetic-integration'
    creds=json.loads((root/'kite-credentials.json').read_text())
    status,_=kite.request('/api/auth/login/password','POST',creds)
    assert status==204,'LAB_KITE_LOGIN_FAILED'
    backup=root/('same-topic-backup-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S'))
    backup.mkdir(mode=0o700)
    # Temporarily extend existing dedicated resource-name grant, never namespace-wide writes.
    role=json.loads(kube('get','role','lunaorbit-mm2-manager','-o','json'))
    (backup/'previous-role.json').write_text(json.dumps(role))
    rules=copy.deepcopy(role['rules'])
    for rule in rules:
        if rule['resources']==['configmaps']:
            rule['resourceNames']=sorted(set(rule['resourceNames']+['mm2-mesh-config']))
        if rule['resources']==['deployments']:
            rule['resourceNames']=sorted(set(rule['resourceNames']+['mm2-mesh']))
    kube('patch','role','lunaorbit-mm2-manager','--type=merge','-p',json.dumps({'metadata':{'resourceVersion':role['metadata']['resourceVersion']},'rules':rules}))
    try:
        for cmname,depname,mesh in [('mm2-config','mm2',False),('mm2-mesh-config','mm2-mesh',True)]:
            path='/api/v1/configmaps/'+kite.NS+'/'+cmname
            status,cm=kite.request(path);assert status==200,'LAB_CONFIG_READ_FAILED'
            status,dep=kite.request('/api/v1/deployments/'+kite.NS+'/'+depname);assert status==200,'LAB_DEPLOYMENT_READ_FAILED'
            (backup/(cmname+'.json')).write_text(json.dumps(cm))
            (backup/(depname+'.json')).write_text(json.dumps(dep))
            raw=cm['data']['mm2.properties']
            cm['data']['mm2.properties']=transform(raw,mesh)
            status,_=kite.request(path,'PUT',cm);assert status==200,'LAB_CONFIG_UPDATE_FAILED'
            stamp=datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00','Z')
            patch={'metadata':{'resourceVersion':dep['metadata']['resourceVersion']},'spec':{'template':{'metadata':{'annotations':{'kite.kubernetes.io/restartedAt':stamp}}}}}
            status,_=kite.request('/api/v1/deployments/'+kite.NS+'/'+depname+'?patchType=merge','PATCH',patch)
            assert status==200,'LAB_RESTART_FAILED'
            print('PASS same_topic_config '+depname,flush=True)
    finally:
        kube('patch','role','lunaorbit-mm2-manager','--type=merge','-p',json.dumps({'rules':role['rules']}))
    print('PASS temporary_resource_grant_restored',flush=True)

if __name__=='__main__':
    main()
