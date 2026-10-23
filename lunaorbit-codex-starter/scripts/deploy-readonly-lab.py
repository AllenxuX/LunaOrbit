"""Deploy the console ONLY into the previously provisioned synthetic Minikube lab.

Usage: python3 deploy-readonly-lab.py /private/path/api /private/path/frontend.tar
Both artifacts must already exist on this authorized Linux host. No credentials print.
"""
import hashlib
import json
import os
import secrets
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
from urllib.parse import quote

PROFILE = "lunaorbit-lab"
NS = "lunaorbit-integration"
ROOT = Path("/root/lunaorbit-minikube-20261020")


def run(args, **kwargs):
    return subprocess.run(args, check=True, timeout=180, **kwargs)


def kube(*args, data=None):
    return run(["kubectl", "--context", PROFILE, "-n", NS, *args], input=data, text=True, capture_output=True)


def apply(obj):
    kube("apply", "-f", "-", data=json.dumps(obj))


def ensure_image_loaded(image):
    # Immutable console tags and the fixed PG version can be reused on retries.
    # Re-exporting an existing image fills the host's Minikube cache needlessly.
    probe = subprocess.run(["docker", "exec", PROFILE, "crictl", "images", "-o", "json"],
                           text=True, capture_output=True, timeout=20)
    if probe.returncode == 0:
        try:
            tags = {tag.removeprefix("docker.io/library/") for row in json.loads(probe.stdout).get("images", [])
                    for tag in row.get("repoTags", [])}
            if image in tags:
                return
        except (ValueError, TypeError):
            pass
    run(["minikube", "-p", PROFILE, "image", "load", image])


def main():
    os.umask(0o077)
    binary, archive = map(lambda value: Path(value).resolve(), sys.argv[1:])
    assert binary.is_file() and archive.is_file()
    namespace = json.loads(kube("get", "ns", NS, "-o", "json").stdout)
    assert namespace["metadata"]["labels"].get("lunaorbit.io/purpose") == "synthetic-integration"
    digest = hashlib.sha256(binary.read_bytes() + archive.read_bytes()).hexdigest()[:12]
    build = ROOT / ("console-build-" + digest)
    build.mkdir(mode=0o700, exist_ok=True)
    shutil.copyfile(binary, build / "api")
    (build / "api").chmod(0o755)
    ui = build / "frontend"
    ui.mkdir(exist_ok=True)
    with tarfile.open(archive) as tar:
        for member in tar.getmembers():
            resolved = (ui / member.name).resolve()
            assert resolved.is_relative_to(ui.resolve()) and not member.issym() and not member.islnk()
        tar.extractall(ui)
    assert (ui / "index.html").is_file()
    config = f"""server {{
  listen 8088;
  server_name localhost;
  root /usr/share/nginx/html;
  index index.html;
  client_max_body_size 300k;
  location / {{ try_files $uri $uri/ /index.html; add_header Cache-Control "no-cache"; }}
  location /api/ {{ proxy_pass http://127.0.0.1:18081; proxy_set_header Host $http_host; proxy_read_timeout 40s; }}
  location = /healthz {{ proxy_pass http://127.0.0.1:18081; proxy_set_header Host $http_host; }}
}}
server {{
  listen 127.0.0.1:28080;
  location / {{ proxy_pass http://kite.{NS}.svc.cluster.local:8080; proxy_read_timeout 10s; }}
}}
"""
    (build / "default.conf").write_text(config)
    (build / "Dockerfile.api").write_text('FROM scratch\nCOPY api /api\nENTRYPOINT ["/api"]\n')
    (build / "Dockerfile.ui").write_text('FROM nginx:1.27-alpine\nCOPY frontend/ /usr/share/nginx/html/\nCOPY default.conf /etc/nginx/conf.d/default.conf\n')
    images = {"api": "lunaorbit-api:lab-" + digest, "ui": "lunaorbit-ui:lab-" + digest}
    for target, image in images.items():
        run(["docker", "build", "--pull=false", "-f", str(build / ("Dockerfile." + target)), "-t", image, str(build)])
        ensure_image_loaded(image)
    # Durable laboratory metadata, with private stable signing/encryption keys.
    # This database is pod-local and cannot be selected by browser input.
    credentials_file = ROOT / "workspace-credentials.json"
    if not credentials_file.exists():
        credentials_file.write_text(json.dumps({"password": secrets.token_urlsafe(32), "session_key": secrets.token_hex(32), "snapshot_key": secrets.token_hex(32)}))
        credentials_file.chmod(0o600)
    credentials = json.loads(credentials_file.read_text())
    if "account_key" not in credentials:
        credentials["account_key"] = secrets.token_hex(32)
        credentials_file.write_text(json.dumps(credentials))
        credentials_file.chmod(0o600)
    migrations = {path.name: path.read_text() for path in sorted((ROOT / "migrations").glob("*.sql"))}
    assert "009_workspace.sql" in migrations and "001_audit.sql" in migrations
    ensure_image_loaded("postgres:17-alpine")
    apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "lunaorbit-migrations", "namespace": NS}, "data": migrations})
    apply({"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": "lunaorbit-workspace-db", "namespace": NS},
           "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}}})
    key = json.loads((ROOT / "kite-reader-key.json").read_text())["key"]
    private_values = {"ACCOUNT_MASTER_KEY": credentials["account_key"], "KITE_API_KEY": key, "POSTGRES_PASSWORD": credentials["password"], "DEV_SESSION_KEY": credentials["session_key"], "CHANGE_SNAPSHOT_KEY": credentials["snapshot_key"],
                      "DATABASE_URL": "postgresql://lunaorbit_lab:" + quote(credentials["password"], safe="") + "@127.0.0.1:5432/lunaorbit_phase02_test?sslmode=disable"}
    apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "lunaorbit-runtime", "namespace": NS},
           "type": "Opaque", "stringData": private_values})
    values = {"HTTP_ADDR": "127.0.0.1:18081", "REGISTRY_MODE": "postgres-mock", "DISCOVERY_MODE": "readonly-lab",
              "LUNAORBIT_RUNTIME_MODE": "admin-lab", "KITE_LAB_VERSION": "v0.16.0",
              "KITE_BASE_URL": "http://127.0.0.1:28080", "KITE_CLUSTER_NAME": PROFILE, "KITE_NAMESPACE": NS,
              "KAFKA_LAB_ALLOWED_CIDRS": "10.96.0.0/12,10.244.0.0/16",
              "KAFKA_LAB_ALLOWED_HOSTS": f"kafka-source.{NS}.svc.cluster.local,kafka-target.{NS}.svc.cluster.local,kafka-source-extra.{NS}.svc.cluster.local",
              "KAFKA_LAB_ALLOWED_PORTS": "9092,9094", "ENABLE_KITE_WRITES": "true", "ENABLE_KAFKA_WRITES": "true",
              "CHANGES_MODE": "lab", "OBSERVABILITY_MODE": "disabled", "DEV_BROWSER_ORIGIN": "http://127.0.0.1:5173",
              "GOMEMLIMIT": "80MiB"}
    values["ACCOUNT_SCRAM_ENDPOINTS"] = json.dumps({f"kafka-{alias}.{NS}.svc.cluster.local:9092": f"kafka-{alias}.{NS}.svc.cluster.local:9094" for alias in ("source", "target")})
    values["KAFKA_LAB_REGISTRATIONS"] = json.dumps([
        {"display_name": "实验源集群", "room": "Minikube", "environment": "实验环境",
         "description": "已部署的源端 Kafka 3.7.2，配置版本不替代实际能力探测",
         "bootstrap_servers": [f"kafka-source.{NS}.svc.cluster.local:9092"], "enabled": True,
         "security_protocol": "PLAINTEXT", "sasl_mechanism": "", "tls_server_name": ""},
        {"display_name": "实验目标集群", "room": "Minikube", "environment": "实验环境",
         "description": "已部署的目标端 Kafka 3.9.2，配置版本不替代实际能力探测",
         "bootstrap_servers": [f"kafka-target.{NS}.svc.cluster.local:9092"], "enabled": True,
         "security_protocol": "PLAINTEXT", "sasl_mechanism": "", "tls_server_name": ""},
    ])
    values["KAFKA_LAB_ALIAS_BINDINGS"] = json.dumps([
        {"namespace": NS, "workload_name": "mm2", "alias": alias,
         "bootstrap": f"kafka-{alias}.{NS}.svc.cluster.local:9092"} for alias in ("source", "target")])
    # Optional, host-generated synthetic topology inventory. It contains no secrets
    # and never expands production targets or browser-controlled network policy.
    topology_file = ROOT / "topology-lab.json"
    if topology_file.exists():
        import ipaddress
        topology = json.loads(topology_file.read_text())
        host = str(ipaddress.IPv4Address(topology["host"]))
        assert ipaddress.ip_address(host).is_private
        assert topology["workload"] == "mm2-mesh"
        expected_seeds = {"source": f"kafka-source.{NS}.svc.cluster.local:9092",
                          "target": f"kafka-target.{NS}.svc.cluster.local:9092",
                          "archive": host + ":19092", "upstream": host + ":29092"}
        assert topology["seeds"] == expected_seeds
        values["KAFKA_LAB_ALLOWED_CIDRS"] += "," + host + "/32"
        values["KAFKA_LAB_ALLOWED_HOSTS"] += "," + host
        values["KAFKA_LAB_ALLOWED_PORTS"] += ",19092,29092"
        registrations = json.loads(values["KAFKA_LAB_REGISTRATIONS"])
        for alias, label in [("archive", "实验归档集群 C"), ("upstream", "实验上游集群 D")]:
            registrations.append({"display_name": label, "room": "第二实验主机", "environment": "实验环境",
                                  "description": "独立 Docker Kafka 3.9.2 · 合成拓扑实验",
                                  "bootstrap_servers": [expected_seeds[alias]], "enabled": True,
                                  "security_protocol": "PLAINTEXT", "sasl_mechanism": "", "tls_server_name": ""})
        values["KAFKA_LAB_REGISTRATIONS"] = json.dumps(registrations)
        bindings = json.loads(values["KAFKA_LAB_ALIAS_BINDINGS"])
        bindings += [{"namespace": NS, "workload_name": "mm2-mesh", "alias": alias, "bootstrap": seed}
                     for alias, seed in expected_seeds.items()]
        values["KAFKA_LAB_ALIAS_BINDINGS"] = json.dumps(bindings)
    cm = json.loads(kube("get", "configmap", "mm2-config", "-o", "json").stdout)
    mm2 = json.loads(kube("get", "deployment", "mm2", "-o", "json").stdout)
    values["CHANGE_LAB_CATALOG"] = json.dumps([{
        "ID": "lab-mm2-source-target", "Name": "实验 MM2 · 源 → 目标", "ScopeID": "readonly-lab",
        "Namespace": NS, "ConfigMapName": "mm2-config", "DeploymentName": "mm2",
        "ConfigDataKey": "mm2.properties", "TopicRuleKey": "source->target.topics",
        "ConfigMapUID": cm["metadata"]["uid"], "DeploymentUID": mm2["metadata"]["uid"]}])
    values["CHANGE_LAB_ALLOW_SELF_APPROVAL"] = "true"
    values["KAFKA_LAB_ACL_SELF_APPROVAL"] = "true"
    dns = json.loads(kube("get", "svc", "kube-dns", "-n", "kube-system", "-o", "json").stdout)["spec"]["clusterIP"]
    containers = [
        {"name": "api", "image": images["api"], "imagePullPolicy": "Never",
         "env": [{"name": key, "value": value} for key, value in values.items()] + [{"name": name, "valueFrom": {"secretKeyRef": {"name": "lunaorbit-runtime", "key": "KITE_API_KEY" if name == "KITE_WRITE_API_KEY" else name}}} for name in ("ACCOUNT_MASTER_KEY", "KITE_API_KEY", "KITE_WRITE_API_KEY", "DATABASE_URL", "DEV_SESSION_KEY", "CHANGE_SNAPSHOT_KEY")],
         "resources": {"requests": {"cpu": "30m", "memory": "32Mi"}, "limits": {"memory": "128Mi"}}},
        {"name": "web", "image": images["ui"], "imagePullPolicy": "Never", "ports": [{"containerPort": 8088}],
         "resources": {"requests": {"cpu": "20m", "memory": "16Mi"}, "limits": {"memory": "48Mi"}},
         "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8088}, "initialDelaySeconds": 3, "periodSeconds": 5}},
        {"name": "postgres", "image": "postgres:17-alpine", "imagePullPolicy": "Never",
         "args": ["postgres", "-c", "listen_addresses=127.0.0.1", "-c", "shared_buffers=16MB", "-c", "max_connections=12", "-c", "work_mem=1MB", "-c", "maintenance_work_mem=16MB", "-c", "max_wal_size=128MB"],
         "env": [{"name": "POSTGRES_USER", "value": "lunaorbit_lab"}, {"name": "POSTGRES_DB", "value": "lunaorbit_phase02_test"}, {"name": "PGDATA", "value": "/var/lib/postgresql/data/pgdata"}, {"name": "POSTGRES_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "lunaorbit-runtime", "key": "POSTGRES_PASSWORD"}}}],
         "volumeMounts": [{"name": "database", "mountPath": "/var/lib/postgresql/data"}, {"name": "migrations", "mountPath": "/docker-entrypoint-initdb.d", "readOnly": True}],
         "resources": {"requests": {"cpu": "20m", "memory": "32Mi"}, "limits": {"memory": "128Mi"}},
         "readinessProbe": {"exec": {"command": ["pg_isready", "-h", "127.0.0.1", "-U", "lunaorbit_lab", "-d", "lunaorbit_phase02_test"]}, "initialDelaySeconds": 3, "periodSeconds": 5}},
    ]
    apply({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "lunaorbit-console", "namespace": NS},
           "spec": {"replicas": 1, "strategy": {"type": "Recreate"}, "selector": {"matchLabels": {"app": "lunaorbit-console"}},
                    "template": {"metadata": {"labels": {"app": "lunaorbit-console"}}, "spec": {
                        "automountServiceAccountToken": False, "dnsPolicy": "None", "dnsConfig": {"nameservers": [dns],
                        "searches": [NS + ".svc.cluster.local", "svc.cluster.local", "cluster.local"], "options": [{"name": "ndots", "value": "1"}]},
                        "containers": containers, "volumes": [{"name": "database", "persistentVolumeClaim": {"claimName": "lunaorbit-workspace-db"}}, {"name": "migrations", "configMap": {"name": "lunaorbit-migrations"}}]}}}})
    apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "lunaorbit-console", "namespace": NS},
           "spec": {"selector": {"app": "lunaorbit-console"}, "ports": [{"port": 8088, "targetPort": 8088}]}})
    kube("rollout", "status", "deployment/lunaorbit-console", "--timeout=120s")
    for sql in migrations.values():
        kube("exec", "-i", "deploy/lunaorbit-console", "-c", "postgres", "--", "psql", "-v", "ON_ERROR_STOP=1", "-U", "lunaorbit_lab", "-d", "lunaorbit_phase02_test", data=sql)
    print("LAB_CONSOLE_DEPLOYED: persistent workspace database; dedicated lab only; no service-account token")


if __name__ == "__main__":
    main()
