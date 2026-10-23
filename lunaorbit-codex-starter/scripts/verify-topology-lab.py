"""Verify the explicitly configured synthetic mesh; prints only PASS labels."""
import json
from pathlib import Path
import subprocess

root = Path('/root/lunaorbit-minikube-20261020')
prefix = ['kubectl', '--context=lunaorbit-lab', '-n', 'lunaorbit-integration']
namespace = json.loads(subprocess.check_output(prefix + ['get', 'namespace', 'lunaorbit-integration', '-o', 'json']))
assert namespace['metadata']['labels'].get('lunaorbit.io/purpose') == 'synthetic-integration'
settings = json.loads((root / 'topology-lab.json').read_text())
assert settings['workload'] == 'mm2-mesh'
binary = root / 'lab-topology-check-linux'
assert binary.is_file() and not binary.is_symlink()
command = prefix + ['exec', '-i', 'deployment/lunaorbit-console', '-c', 'web', '--']
result = subprocess.run(command + ['sh', '-c', 'umask 077; cat > /tmp/lab-topology-check; chmod 700 /tmp/lab-topology-check'],
                        input=binary.read_bytes(), capture_output=True, timeout=30)
assert result.returncode == 0, 'LAB_CHECK_UPLOAD_FAILED'
result = subprocess.run(command + ['/tmp/lab-topology-check', '--lab'], input=json.dumps({'remote_host': settings['host']}).encode(),
                        capture_output=True, timeout=140)
for line in result.stdout.decode().splitlines():
    if line.startswith('PASS ') and all(c.isalnum() or c in ' _=' for c in line):
        print(line, flush=True)
if result.returncode:
    raise SystemExit('LAB_REPLICATION_CHECK_FAILED')
