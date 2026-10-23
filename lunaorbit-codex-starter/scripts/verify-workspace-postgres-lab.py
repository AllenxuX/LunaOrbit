"""Run guarded optional suites using the private synthetic lab database only."""
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import quote

root = Path('/root/lunaorbit-minikube-20261020')
credentials = json.loads((root / 'workspace-credentials.json').read_text())
dsn = 'postgresql://lunaorbit_lab:' + quote(credentials['password'], safe='') + '@127.0.0.1:15432/lunaorbit_phase02_test?sslmode=disable'
env = dict(os.environ, WORKSPACE_TEST_DATABASE_URL=dsn, REGISTRY_TEST_DATABASE_URL=dsn)
for binary, pattern in [('workspace-tests-linux', 'TestWorkspacePostgres|TestAccountsPostgresIntegration|TestResourcesPostgresIntegration'), ('registry-tests-linux', 'TestPostgresGrantsIntegration')]:
    path = root / binary
    path.chmod(0o700)
    subprocess.run([str(path), '-test.run', pattern, '-test.v', '-test.timeout=90s'], env=env, check=True, timeout=100)
print('WORKSPACE_POSTGRES_INTEGRATION_PASS')
