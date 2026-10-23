"""Guarded CoreDNS fix for invalid search completions in the isolated lab.

The host's localdomain upstream is unreachable. Answer only the synthetic
namespace's invalid *.svc.cluster.local.localdomain completions with NXDOMAIN.
Existing Kafka/MM2 pods keep running; CoreDNS reloads its configuration in place.
"""
import json
import os
from importlib.machinery import SourceFileLoader
from pathlib import Path
import subprocess

os.umask(0o077)
root = Path(__file__).resolve().parent
guard = SourceFileLoader('resource_dns_guard', str(root / 'setup-resource-broker-lab.py')).load_module()
guard.inspect()
base = ['kubectl', '--context', guard.lab.PROFILE, '-n', 'kube-system']
obj = json.loads(subprocess.check_output(base + ['get', 'configmap', 'coredns', '-o', 'json']))
assert obj['metadata']['namespace'] == 'kube-system' and obj['metadata']['name'] == 'coredns'
original = obj['data']['Corefile']
zone = guard.lab.NS + '.svc.cluster.local.localdomain'
block = '    template IN ANY ' + zone + ' {\n        rcode NXDOMAIN\n    }\n'
if block not in original:
    assert zone not in original and original.count('    forward . /etc/resolv.conf {') == 1
    backup = guard.lab.ROOT / 'resource-coredns-before.json'
    if not backup.exists():
        backup.write_text(json.dumps(obj))
        backup.chmod(0o600)
    plugins = subprocess.check_output(base + ['exec', 'deployment/coredns', '--', '/coredns', '-plugins'], text=True)
    assert 'template' in plugins.splitlines()
    obj['data']['Corefile'] = original.replace('    forward . /etc/resolv.conf {', block + '    forward . /etc/resolv.conf {')
    subprocess.run(base + ['replace', '-f', '-'], input=json.dumps(obj), text=True, capture_output=True, check=True)
print('LAB_RESOURCE_DNS_SEARCH_FIX_APPLIED: only invalid synthetic namespace search completions')
