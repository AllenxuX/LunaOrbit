"""Explicit operator maintenance of the user-selected synthetic registration.

The private lab key/DB password travel only over subprocess stdin. Never use
against production, reset a Kafka password, or expose plaintext in diagnostics.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import quote

ROOT = Path('/root/lunaorbit-minikube-20261020')
PREFIX = ['kubectl', '--context=lunaorbit-lab', '-n', 'lunaorbit-integration']


def command(args, payload=None):
    result = subprocess.run(PREFIX + args, input=payload, capture_output=True, timeout=150)
    if result.returncode:
        raise RuntimeError('LAB_IMPORT_OPERATION_FAILED')
    return result.stdout


def main(registration):
    assert re.fullmatch('[a-f0-9]{32}', registration)
    ns = json.loads(command(['get', 'namespace', 'lunaorbit-integration', '-o', 'json']))
    assert ns['metadata']['labels'].get('lunaorbit.io/purpose') == 'synthetic-integration'
    binary = ROOT / 'lab-account-import-linux'
    assert binary.is_file() and not binary.is_symlink()
    private = ROOT / 'workspace-credentials.json'
    assert private.stat().st_mode & 0o077 == 0
    credentials = json.loads(private.read_text())
    payload = {'registration_id': registration, 'account_master_key': credentials['account_key'],
               'database_url': 'postgresql://lunaorbit_lab:' + quote(credentials['password'], safe='') + '@127.0.0.1:5432/lunaorbit_phase02_test?sslmode=disable'}
    command(['exec', '-i', 'deployment/lunaorbit-console', '-c', 'web', '--', 'sh', '-c',
             'umask 077; cat > /tmp/lunaorbit-lab-account-import; chmod 700 /tmp/lunaorbit-lab-account-import'], binary.read_bytes())
    result = json.loads(command(['exec', '-i', 'deployment/lunaorbit-console', '-c', 'web', '--',
                                 '/tmp/lunaorbit-lab-account-import', '--lab'], json.dumps(payload).encode()))
    assert set(result) == {'existing', 'imported', 'missing', 'failed'}
    print('LAB_EXISTING_ACCOUNT_IMPORT', json.dumps(result), flush=True)


if __name__ == '__main__':
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registration', required=True)
    args = parser.parse_args()
    try:
        main(args.registration)
    except Exception:
        print('LAB_EXISTING_ACCOUNT_IMPORT_FAILED; details suppressed', flush=True)
        raise SystemExit(1)
