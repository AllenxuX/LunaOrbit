"""Explicit synthetic experiment only. No production resources or GitOps.

remote-brokers requires --host to match a local IPv4 address on the second host.
mesh requires the existing labeled LunaOrbit Minikube namespace on the first host.
Run with --host SECOND_LAB_IPV4; only the two named containers/new MM2 are managed.
"""
import argparse
import base64
import hashlib
from importlib.machinery import SourceFileLoader
import ipaddress
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess

ROOT = Path('/root/lunaorbit-topology-lab')
LABEL = 'lunaorbit.io.purpose=synthetic-topology'
IMAGE = 'apache/kafka:3.9.2'


def run(args, **kwargs):
    result = subprocess.run(args, capture_output=True, text=True, timeout=120, **kwargs)
    if result.returncode:
        raise RuntimeError('Synthetic topology operation failed: ' + args[0])
    return result.stdout


def remote_brokers(host):
    addresses = json.loads(run(['ip', '-j', '-4', 'address']))
    assert any(a['local'] == host for i in addresses for a in i.get('addr_info', [])), 'Host address mismatch'
    assert run(['docker', 'image', 'inspect', IMAGE, '--format', '{{.Id}}']).strip()
    ROOT.mkdir(mode=0o700, exist_ok=True)
    assert not ROOT.is_symlink()
    marker = ROOT / 'purpose'
    if marker.exists():
        assert marker.read_text() == LABEL
    else:
        marker.write_text(LABEL)
    ids_file = ROOT / 'cluster-ids.json'
    ids = json.loads(ids_file.read_text()) if ids_file.exists() else {
        alias: base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip('=') for alias in ('archive', 'upstream')}
    ids_file.write_text(json.dumps(ids))
    for alias, port in [('archive', 19092), ('upstream', 29092)]:
        name = 'lunaorbit-topology-' + alias
        previous = subprocess.run(['docker', 'inspect', name], capture_output=True, text=True)
        if previous.returncode == 0:
            entry = json.loads(previous.stdout)[0]
            assert entry['Config']['Labels'].get('lunaorbit.io.purpose') == 'synthetic-topology'
            assert entry['Config']['Image'] == IMAGE
            assert entry['State']['Running'], 'Existing experimental container is stopped; inspect before restarting'
            print('LAB_BROKER_ALREADY_RUNNING', alias)
            continue
        with socket.socket() as s:
            s.bind((host, port))
        controller = port + 1
        values = {
            'CLUSTER_ID': ids[alias], 'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
            'KAFKA_CONTROLLER_QUORUM_VOTERS': f'1@127.0.0.1:{controller}',
            'KAFKA_LISTENERS': f'PLAINTEXT://{host}:{port},CONTROLLER://127.0.0.1:{controller}',
            'KAFKA_ADVERTISED_LISTENERS': f'PLAINTEXT://{host}:{port}',
            'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT',
            'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER', 'KAFKA_INTER_BROKER_LISTENER_NAME': 'PLAINTEXT',
            'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1', 'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1',
            'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1', 'KAFKA_NUM_PARTITIONS': '1',
            'KAFKA_LOG_RETENTION_HOURS': '6', 'KAFKA_LOG_SEGMENT_BYTES': '16777216',
            'KAFKA_LOG_CLEANER_DEDUPE_BUFFER_SIZE': '16777216', 'KAFKA_LOG_DIRS': '/var/lib/kafka/data',
            'KAFKA_HEAP_OPTS': '-Xms128m -Xmx192m',
            'KAFKA_JVM_PERFORMANCE_OPTS': '-XX:+UseG1GC -XX:MaxMetaspaceSize=128m -XX:MaxDirectMemorySize=64m -Xss512k',
        }
        args = ['docker', 'run', '-d', '--name', name, '--label', LABEL, '--restart', 'unless-stopped',
                '--network', 'host', '--memory', '640m', '--cpus', '1', '--mount',
                f'type=volume,src={name}-data,dst=/var/lib/kafka/data']
        for key, value in values.items():
            args += ['-e', key + '=' + value]
        run(args + [IMAGE])
        print('LAB_BROKER_CREATED', alias)
    print('SECOND_HOST_BROKERS_READY_FOR_PROBE')


def mesh(host):
    root = Path('/root/lunaorbit-minikube-20261020')
    lab = SourceFileLoader('topology_existing_lab', str(root / 'minikube-lab.py')).load_module()
    ns = json.loads(lab.kubectl('get', 'namespace', lab.NS, '-o', 'json'))
    assert ns['metadata']['labels'].get('lunaorbit.io/purpose') == 'synthetic-integration'
    for kind, name in [('configmap', 'mm2-mesh-config'), ('deployment', 'mm2-mesh')]:
        previous = subprocess.run(['kubectl', '--context', lab.PROFILE, '-n', lab.NS, 'get', kind, name, '-o', 'json'], capture_output=True, text=True)
        if previous.returncode == 0:
            assert json.loads(previous.stdout)['metadata'].get('labels', {}).get('lunaorbit.io/purpose') == 'synthetic-topology'
    seeds = {'source': f'kafka-source.{lab.NS}.svc.cluster.local:9092', 'target': f'kafka-target.{lab.NS}.svc.cluster.local:9092',
             'archive': f'{host}:19092', 'upstream': f'{host}:29092'}
    flows = [('source', 'archive', 'lunaorbit-lab-events'), ('upstream', 'target', 'lunaorbit-mesh-events'),
             ('target', 'archive', r'(source\.lunaorbit-lab-events|upstream\.lunaorbit-mesh-events)')]
    lines = ['clusters = ' + ', '.join(seeds), 'emit.heartbeats.enabled = false',
             'producer.buffer.memory = 1048576', 'producer.max.request.size = 1048576',
             'consumer.max.partition.fetch.bytes = 1048576'] + [f'{alias}.bootstrap.servers = {seed}' for alias, seed in seeds.items()]
    for source, target, topics in flows:
        prefix = source + '->' + target
        values = {'enabled': 'true', 'topics': topics, 'groups': '.^', 'emit.heartbeats.enabled': 'false',
                  'emit.checkpoints.enabled': 'false', 'sync.group.offsets.enabled': 'false', 'sync.topic.acls.enabled': 'false',
                  'refresh.topics.interval.seconds': '5', 'refresh.groups.interval.seconds': '60',
                  'consumer.auto.offset.reset': 'earliest', 'producer.max.request.size': '1048576'}
        lines += [f'{prefix}.{key} = {value}' for key, value in values.items()]
    lines += [f'{key} = 1' for key in ('replication.factor', 'checkpoints.topic.replication.factor', 'heartbeats.topic.replication.factor',
              'offset-syncs.topic.replication.factor', 'offset.storage.replication.factor', 'config.storage.replication.factor', 'status.storage.replication.factor', 'tasks.max')]
    # MM2's defaults isolate storage/group by source within each target Kafka.
    # A target-level group.id override would incorrectly merge different herders.
    cm = lab.item('ConfigMap', 'mm2-mesh-config', data={'mm2.properties': '\n'.join(lines) + '\n'})
    cm['metadata']['labels'] = {'lunaorbit.io/purpose': 'synthetic-topology'}
    lab.apply(cm)
    # Reuse guarded manifest construction but label the new deployment before apply.
    original_apply = lab.apply
    def labeled_apply(obj):
        obj['metadata'].setdefault('labels', {})['lunaorbit.io/purpose'] = 'synthetic-topology'
        if obj['kind'] == 'Deployment':
            obj['spec']['template']['metadata']['annotations'] = {'lunaorbit.io/config-sha': hashlib.sha256(cm['data']['mm2.properties'].encode()).hexdigest()}
            obj['spec']['template']['spec']['automountServiceAccountToken'] = False
            obj['spec']['template']['spec']['dnsConfig'] = {'options': [{'name': 'ndots', 'value': '1'}]}
        original_apply(obj)
    lab.apply = labeled_apply
    lab.deploy('mm2-mesh', IMAGE, command=['/opt/kafka/bin/connect-mirror-maker.sh', '/etc/mm2/mm2.properties', '--clusters', 'target', 'archive'],
               values={'KAFKA_HEAP_OPTS': '-Xms128m -Xmx256m',
                       'KAFKA_JVM_PERFORMANCE_OPTS': '-XX:+UseG1GC -XX:MaxMetaspaceSize=128m -XX:MaxDirectMemorySize=64m -Xss512k'},
               memory='512Mi', volumes=[{'name': 'config', 'configMap': {'name': 'mm2-mesh-config'}}], mounts=[{'name': 'config', 'mountPath': '/etc/mm2', 'readOnly': True}])
    (root / 'topology-lab.json').write_text(json.dumps({'host': host, 'seeds': seeds, 'workload': 'mm2-mesh'}))
    print('MM2_MESH_CONFIGURED', len(flows), 'flows; original MM2 unchanged')


if __name__ == '__main__':
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['remote-brokers', 'mesh'])
    parser.add_argument('--host', required=True)
    args = parser.parse_args()
    assert isinstance(ipaddress.ip_address(args.host), ipaddress.IPv4Address) and ipaddress.ip_address(args.host).is_private
    {'remote-brokers': remote_brokers, 'mesh': mesh}[args.action](args.host)
