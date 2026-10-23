"""Exercise a deliberately throttled synthetic reassignment and native cancellation.

Runs only on the authorized isolated host. Saves/restores every selected throttle
override and removes its own Topic. Never handles production resources or secrets.
"""
import http.cookiejar
import importlib.util
import json
import re
from pathlib import Path
import time
import urllib.request
import urllib.error
import uuid

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('resource_broker_lab', ROOT / 'setup-resource-broker-lab.py')
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
guard.inspect()
origin = 'http://127.0.0.1:18088'
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))


def request(path, data=None):
    req = urllib.request.Request(origin + path, data=None if data is None else json.dumps(data).encode(),
                                 headers={'Origin': origin, 'Content-Type': 'application/json'})
    try:
        with opener.open(req, timeout=35) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        body = json.load(error)
        raise RuntimeError('Synthetic API failure: HTTP ' + str(error.code) + ' code=' + str(body.get('error', {}).get('code', body.get('code', 'unknown')))) from None


request('/api/v1/dev/session', {})
assert request('/api/runtime')['mode'] == 'admin-lab'
request('/api/v1/lab/registrations', {})
clusters = request('/api/v1/kafka/clusters')['items']
source = next(c for c in clusters if c['display_name'] == '实验源集群')
assert all(host.startswith('kafka-source.') for host in source['bootstrap_servers'])
base = '/api/v1/kafka/clusters/' + source['id'] + '/workspace'
topic = 'lunaorbit-resource-cancel-' + uuid.uuid4().hex[:12]
target = {'type': 'TOPIC', 'name': topic}


def submit(intent):
    plan = request(base + '/resource-plans', intent)
    op = request(base + '/resource-operations', {'intent': intent, 'fingerprint': plan['fingerprint'], 'request_id': str(uuid.uuid4())})
    assert op['status'] == 'approved'
    op = request(base + '/resource-operations/' + op['id'] + '/execute', {'revision': op['revision']})
    assert op['status'] == 'observing', op['status']
    return op


def verify(op):
    for attempt in range(40):
        op = request(base + '/resource-operations/' + op['id'] + '/refresh', {'revision': op['revision']})
        if op['status'] == 'succeeded':
            return op
        assert op['status'] in ('observing', 'unknown'), op['status']
        time.sleep(1)
    raise AssertionError('Resource did not converge: ' + op['status'])


def change(resource, changes):
    return verify(submit({'kind': 'configs', 'resource': resource, 'changes': changes}))


def entries(resource):
    from urllib.parse import urlencode
    return request(base + '/resource-configs?' + urlencode({'resource_type': resource['type'], 'resource_name': resource['name']}))['entries']


create = {'kind': 'topic_create', 'topic': topic, 'partitions': 1, 'replication_factor': 1}
plan = request(base + '/plans', create)
op = request(base + '/requests', dict(create, fingerprint=plan['fingerprint']))
assert request(base + '/requests/' + op['id'] + '/execute', {'revision': op['revision']})['status'] == 'executed'
restore = []
broker_restore = []
def broker_cli(broker, *args):
    return guard.scram.quiet_kubectl('exec', 'deploy/kafka-source', '--', 'env', 'KAFKA_HEAP_OPTS=-Xms32m -Xmx64m',
        '/opt/kafka/bin/kafka-configs.sh', '--bootstrap-server', 'localhost:9092', '--entity-type', 'brokers', '--entity-name', broker, *args)
try:
    initial = request(base + '/resource-plans', {'kind': 'reassign', 'resource': target, 'assignments': [{'partition': 0, 'replicas': [1]}]})
    if initial['topic']['partitions'][0]['replicas'] != [1]:
        verify(submit({'kind': 'reassign', 'resource': target, 'assignments': [{'partition': 0, 'replicas': [1]}]}))
    # Bounded data, small CLI heap; only this random synthetic Topic receives it.
    guard.scram.quiet_kubectl('exec', '-i', 'deploy/kafka-source', '--', 'env', 'KAFKA_HEAP_OPTS=-Xms32m -Xmx64m',
        '/opt/kafka/bin/kafka-console-producer.sh', '--bootstrap-server', 'localhost:9092', '--topic', topic,
        data=('synthetic-cancellation-' + 'x' * 131072 + '\n') * 64)
    print('LAB_RESOURCE_CANCEL_SYNTHETIC_DATA_READY')
    offsets = guard.scram.quiet_kubectl('exec', 'deploy/kafka-source', '--', 'env', 'KAFKA_HEAP_OPTS=-Xms32m -Xmx64m', '/opt/kafka/bin/kafka-get-offsets.sh', '--bootstrap-server', 'localhost:9092', '--topic', topic)
    assert re.search(re.escape(topic) + r':0:64\b', offsets), 'Synthetic producer did not persist all 64 records'
    for broker in ('1', '2'):
        before = broker_cli(broker, '--describe')
        undo = []
        for key in ('leader.replication.throttled.rate', 'follower.replication.throttled.rate'):
            match = re.search(r'(?<![\w.])' + re.escape(key) + r'=(\d+)', before)
            undo.append((key, match.group(1) if match else None))
        broker_restore.append((broker, undo))
        broker_cli(broker, '--alter', '--add-config', 'leader.replication.throttled.rate=1,follower.replication.throttled.rate=1')
    print('LAB_RESOURCE_CANCEL_BROKER_THROTTLES_SET')
    change(target, [{'name': 'leader.replication.throttled.replicas', 'operation': 'SET', 'value': '0:1'},
                    {'name': 'follower.replication.throttled.replicas', 'operation': 'SET', 'value': '0:2'}])
    migration = submit({'kind': 'reassign', 'resource': target, 'assignments': [{'partition': 0, 'replicas': [2]}]})
    print('LAB_RESOURCE_CANCEL_MIGRATION_ACCEPTED')
    active = request(base + '/resource-operations/' + migration['id'] + '/refresh', {'revision': migration['revision']})
    assert active['status'] == 'observing'
    ongoing = guard.scram.quiet_kubectl('exec', 'deploy/kafka-source', '--', 'env', 'KAFKA_HEAP_OPTS=-Xms32m -Xmx64m', '/opt/kafka/bin/kafka-reassign-partitions.sh', '--bootstrap-server', 'localhost:9092', '--list')
    assert topic in ongoing, 'Kafka CLI reports no active reassignment for synthetic Topic'
    state = request(base + '/resource-plans', {'kind':'topic_delete','resource':target,'confirmation_name':topic})
    print('LAB_CANCEL_METADATA', json.dumps(state['topic']['partitions']))
    cancellation = submit({'kind': 'reassign_cancel', 'resource': target, 'partitions': [0]})
    verify(cancellation)
    print('LAB_RESOURCE_INFLIGHT_CANCELLATION_PASS: actual Kafka returned to original replicas and ISR')
finally:
    # Restore only selected Broker overrides, even when a test assertion fails.
    try:
        for resource, undo in reversed(restore):
            change(resource, undo)
        for broker, undo in reversed(broker_restore):
            for key, value in undo:
                broker_cli(broker, '--alter', '--delete-config' if value is None else '--add-config', key if value is None else key+'='+value)
    finally:
        verify(submit({'kind': 'topic_delete', 'resource': target, 'confirmation_name': topic}))
    print('LAB_RESOURCE_THROTTLES_RESTORED_AND_TOPIC_REMOVED')
