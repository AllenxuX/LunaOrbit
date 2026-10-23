"""Upgrade ONLY the pre-existing dedicated LunaOrbit lab to information mode.

Inputs: compiled Linux API, frontend archive, explicit existing workspace ID.
No owner merging, Kafka/MM2 mutation or secret output. Keeps old database and
stores a private rollback manifest and database backup on the lab host.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile


def runtime_has_image(raw, image):
    tags = {tag.removeprefix("docker.io/library/") for row in json.loads(raw).get("images", []) for tag in row.get("repoTags", [])}
    return image in tags


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    parser.add_argument("archive", type=Path)
    parser.add_argument("workspace")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-f0-9]{32}", args.workspace):
        raise SystemExit("Expected an explicit existing lab workspace ID")
    os.umask(0o077)
    module_path = Path(__file__).with_name("deploy-readonly-lab.py")
    spec = importlib.util.spec_from_file_location("lab_deployer", module_path)
    lab = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lab)
    namespace = json.loads(lab.kube("get", "ns", lab.NS, "-o", "json").stdout)
    assert namespace["metadata"]["labels"].get("lunaorbit.io/purpose") == "synthetic-integration"
    deployment = json.loads(lab.kube("get", "deployment", "lunaorbit-console", "-o", "json").stdout)
    assert deployment["spec"]["template"]["spec"]["automountServiceAccountToken"] is False
    pg = ["exec", "-i", "deployment/lunaorbit-console", "-c", "postgres", "--", "psql", "-v", "ON_ERROR_STOP=1", "-U", "lunaorbit_lab", "-d", "lunaorbit_phase02_test", "-At"]
    # Preview/adopt exactly one existing namespace. Never UPDATE owners or merge data.
    preview = lab.kube(*pg, data="SELECT count(*),count(DISTINCT payload->>'DiscoveredClusterID') FROM kafka_cluster_registrations WHERE deleted_at IS NULL AND created_by='" + args.workspace + "';").stdout.strip()
    count, native = map(int, preview.split("|"))
    assert 1 <= count <= 16 and native == count
    snapshots = lab.kube(*pg, data="SELECT count(*) FROM mm2_discovery_snapshots WHERE owner_id='" + args.workspace + "' AND scope_id='readonly-lab';").stdout.strip()
    assert snapshots == "1"
    digest = hashlib.sha256(args.binary.read_bytes() + args.archive.read_bytes()).hexdigest()[:12]
    build = lab.ROOT / ("information-build-" + digest)
    build.mkdir(mode=0o700, exist_ok=True)
    rollback = build / "previous-deployment.json"
    if not rollback.exists():
        rollback.write_text(json.dumps(deployment))
    backup = build / "database-before.sql"
    if not backup.exists():
        with backup.open("wb") as output:
            subprocess.run(["kubectl", "--context", lab.PROFILE, "-n", lab.NS,
                "exec", "deployment/lunaorbit-console", "-c", "postgres", "--",
                "pg_dump", "-U", "lunaorbit_lab", "-d", "lunaorbit_phase02_test"],
                stdout=output, stderr=subprocess.PIPE, check=True, timeout=60)
    migration = lab.ROOT / "migrations" / "010_information_applications.sql"
    assert migration.is_file()
    lab.kube(*pg, data=migration.read_text())
    shutil.copyfile(args.binary, build / "api")
    (build / "api").chmod(0o755)
    ui = build / "frontend"
    ui.mkdir(exist_ok=True)
    with tarfile.open(args.archive) as archive:
        for member in archive.getmembers():
            assert (ui / member.name).resolve().is_relative_to(ui.resolve())
            assert not member.issym() and not member.islnk()
        archive.extractall(ui)
    (build / "Dockerfile.api").write_text('FROM scratch\nCOPY api /api\nENTRYPOINT ["/api"]\n')
    # Reuse the known console proxy configuration without changing any target service.
    web = next(c for c in deployment["spec"]["template"]["spec"]["containers"] if c["name"] == "web")
    config = lab.kube("exec", "deployment/lunaorbit-console", "-c", "web", "--", "cat", "/etc/nginx/conf.d/default.conf").stdout
    (build / "default.conf").write_text(config)
    (build / "Dockerfile.ui").write_text('FROM nginx:1.27-alpine\nCOPY frontend/ /usr/share/nginx/html/\nCOPY default.conf /etc/nginx/conf.d/default.conf\n')
    images = {"api": "lunaorbit-api:information-" + digest, "ui": "lunaorbit-ui:information-" + digest}
    for target, image in images.items():
        lab.run(["docker", "build", "--pull=false", "-f", str(build / ("Dockerfile." + target)), "-t", image, str(build)])
        lab.ensure_image_loaded(image)
        # minikube image load may exit successfully after a partial disk-full load.
        # Verify the runtime inventory before replacing the working console.
        probe = lab.run(["docker", "exec", lab.PROFILE, "crictl", "images", "-o", "json"], text=True, capture_output=True)
        assert runtime_has_image(probe.stdout, image), "Console image absent from dedicated runtime; deployment left unchanged"
    api = next(c for c in deployment["spec"]["template"]["spec"]["containers"] if c["name"] == "api")
    drop = {"ACCOUNT_MASTER_KEY", "ACCOUNT_SCRAM_ENDPOINTS", "DEV_SESSION_KEY", "CHANGE_SNAPSHOT_KEY", "KITE_WRITE_API_KEY", "CHANGE_LAB_CATALOG", "CHANGE_LAB_ALLOW_SELF_APPROVAL", "KAFKA_LAB_ACL_SELF_APPROVAL", "ACL_APPROVAL_SCOPES", "IDENTITY_MODE"}
    override = {"PLATFORM_MODE": "information", "INFORMATION_WORKSPACE_ID": args.workspace,
                "ENABLE_KAFKA_WRITES": "false", "ENABLE_KITE_WRITES": "false", "CHANGES_MODE": "disabled",
                "LUNAORBIT_RUNTIME_MODE": "readonly-lab",
                "INFORMATION_BROWSER_ORIGINS": "http://127.0.0.1:5173"}
    api["env"] = [entry for entry in api["env"] if entry["name"] not in drop and entry["name"] not in override]
    api["env"] += [{"name": key, "value": value} for key, value in override.items()]
    api["image"] = images["api"]
    web["image"] = images["ui"]
    # Replace only the console pod spec; PostgreSQL PVC, Kafka and MM2 stay intact.
    patch = {"spec": {"template": deployment["spec"]["template"]}}
    lab.kube("patch", "deployment", "lunaorbit-console", "--type=merge", "-p", json.dumps(patch))
    lab.kube("rollout", "status", "deployment/lunaorbit-console", "--timeout=120s")
    (build / "verification.json").write_text(json.dumps({"workspace": args.workspace, "registrations": count, "snapshots": 1, "digest": digest}))
    print("INFORMATION_LAB_DEPLOYED: one explicit workspace; no owner merge; private rollback backup retained")


if __name__ == "__main__":
    main()
