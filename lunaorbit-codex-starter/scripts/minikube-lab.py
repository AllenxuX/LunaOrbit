"""Opt-in synthetic integration lab. Run on the authorized Linux Docker host.

Does not start/delete profiles, change kubectl context, or access existing namespaces.
Secrets are generated on the host and never printed. Runtime files must stay private.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import subprocess
import time

PROFILE = "lunaorbit-lab"
NS = "lunaorbit-integration"
ROOT = Path("/root/lunaorbit-minikube-20261020")
KITE_IMAGE = "swr.cn-north-4.myhuaweicloud.com/ddn-k8s/ghcr.io/kite-org/kite:v0.16.0"


def kubectl(*args, data=None):
    # A second full broker-sized JVM for CLI verification can OOM the lab
    # container. Bound only the short-lived CLI process, not the running broker.
    args = list(args)
    if args and args[0] == "exec" and "--" in args:
        index = args.index("--") + 1
        if index < len(args) and args[index].startswith("/opt/kafka/bin/"):
            args[index:index] = ["env", "KAFKA_HEAP_OPTS=-Xms32m -Xmx64m"]
    return subprocess.check_output(
        ["kubectl", "--context", PROFILE, "-n", NS, *args],
        input=data, text=True, timeout=120,
    )


def apply(obj):
    kubectl("apply", "-f", "-", data=json.dumps(obj))


def item(kind, name, spec=None, **extra):
    obj = {"apiVersion": "v1", "kind": kind, "metadata": {"name": name, "namespace": NS}}
    if spec is not None:
        obj["spec"] = spec
    obj.update(extra)
    return obj


def env(values):
    return [{"name": k, "value": str(v)} for k, v in values.items()]


def deploy(name, image, command=None, values=None, memory="384Mi", volumes=None, mounts=None, secret_env=()):
    container = {"name": name, "image": image, "imagePullPolicy": "Never",
                 "resources": {"requests": {"cpu": "50m", "memory": "96Mi"},
                               "limits": {"memory": memory}}, "env": env(values or {})}
    if command:
        container["command"] = command
    container["env"].extend({"name": key, "valueFrom": {"secretKeyRef": {
        "name": "kite-config", "key": key}}} for key in secret_env)
    if mounts:
        container["volumeMounts"] = mounts
    pod = {"containers": [container], "terminationGracePeriodSeconds": 30}
    if volumes:
        pod["volumes"] = volumes
    if name == "kite":
        pod["serviceAccountName"] = "kite"
        container["readinessProbe"] = {"httpGet": {"path": "/healthz", "port": 8080},
                                       "initialDelaySeconds": 5, "periodSeconds": 5}
    elif name == "mm2":
        # The host's external localdomain DNS is unreachable. Keep this lab's
        # fully qualified service lookups on cluster DNS without external searches.
        dns = json.loads(subprocess.check_output(["kubectl", "--context", PROFILE, "-n", "kube-system",
                         "get", "svc", "kube-dns", "-o", "json"], text=True, timeout=15))
        pod["dnsPolicy"] = "None"
        pod["dnsConfig"] = {"nameservers": [dns["spec"]["clusterIP"]],
                            "searches": [NS + ".svc.cluster.local", "svc.cluster.local", "cluster.local"],
                            "options": [{"name": "ndots", "value": "1"}]}
    elif name.startswith("kafka-"):
        container["readinessProbe"] = {"tcpSocket": {"port": 9092},
                                       "initialDelaySeconds": 10, "periodSeconds": 5}
    obj = item("Deployment", name, {"replicas": 1, "strategy": {"type": "Recreate"},
               "selector": {"matchLabels": {"app": name}},
               "template": {"metadata": {"labels": {"app": name}}, "spec": pod}})
    obj["apiVersion"] = "apps/v1"
    apply(obj)


def setup():
    os.umask(0o077)
    ROOT.mkdir(mode=0o700, exist_ok=True)
    profile = json.loads(Path("/root/.minikube/profiles/lunaorbit-lab/config.json").read_text())
    assert profile["Name"] == PROFILE and profile["Driver"] == "docker"
    ns = {"apiVersion": "v1", "kind": "Namespace", "metadata": {
        "name": NS, "labels": {"lunaorbit.io/purpose": "synthetic-integration"}}}
    existing = subprocess.run(["kubectl", "--context", PROFILE, "get", "ns", NS, "-o", "json"],
                              capture_output=True, text=True, timeout=15)
    if existing.returncode == 0:
        assert json.loads(existing.stdout)["metadata"]["labels"].get("lunaorbit.io/purpose") == "synthetic-integration"
    apply(ns)
    ids_file = ROOT / "kafka-cluster-ids.json"
    if not ids_file.exists():
        ids_file.write_text(json.dumps({name: base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip("=")
                                       for name in ("kafka-source", "kafka-target")}))
    cluster_ids = json.loads(ids_file.read_text())
    assert cluster_ids["kafka-source"] != cluster_ids["kafka-target"]
    for name, version in [("kafka-source", "3.7.2"), ("kafka-target", "3.9.2")]:
        apply(item("Service", name, {"selector": {"app": name}, "ports": [{"port": 9092}]}))
        values = {"CLUSTER_ID": cluster_ids[name], "KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
                  "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
                  "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
                  "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{name}.{NS}.svc.cluster.local:9092",
                  "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
                  "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
                  "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
                  "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
                  "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
                  "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1", "KAFKA_NUM_PARTITIONS": "1",
                  "KAFKA_LOG_RETENTION_HOURS": "1", "KAFKA_LOG_SEGMENT_BYTES": "16777216",
                  "KAFKA_LOG_CLEANER_DEDUPE_BUFFER_SIZE": "16777216",
                  "KAFKA_HEAP_OPTS": "-Xms128m -Xmx192m",
                  "KAFKA_JVM_PERFORMANCE_OPTS": "-XX:+UseG1GC -XX:MaxMetaspaceSize=128m -XX:MaxDirectMemorySize=64m -Xss512k"}
        deploy(name, f"apache/kafka:{version}", values=values)
    properties = f"""clusters = source, target
source.bootstrap.servers = kafka-source.{NS}.svc.cluster.local:9092
target.bootstrap.servers = kafka-target.{NS}.svc.cluster.local:9092
source->target.enabled = true
target->source.enabled = false
source->target.topics = lunaorbit-lab-events
source->target.groups = .^
source->target.emit.heartbeats.enabled = false
source->target.emit.checkpoints.enabled = false
source->target.sync.group.offsets.enabled = false
source->target.sync.topic.acls.enabled = false
source->target.refresh.topics.interval.seconds = 5
source->target.refresh.groups.interval.seconds = 60
source->target.consumer.auto.offset.reset = earliest
replication.factor = 1
checkpoints.topic.replication.factor = 1
heartbeats.topic.replication.factor = 1
offset-syncs.topic.replication.factor = 1
offset.storage.replication.factor = 1
config.storage.replication.factor = 1
status.storage.replication.factor = 1
tasks.max = 1
"""
    apply(item("ConfigMap", "mm2-config", data={"mm2.properties": properties}))
    deploy("mm2", "apache/kafka:3.9.2", command=["/opt/kafka/bin/connect-mirror-maker.sh", "/etc/mm2/mm2.properties", "--clusters", "target"],
           values={"KAFKA_HEAP_OPTS": "-Xms128m -Xmx192m",
                   "KAFKA_JVM_PERFORMANCE_OPTS": "-XX:+UseG1GC -XX:MaxMetaspaceSize=128m -XX:MaxDirectMemorySize=64m -Xss512k"},
           memory="448Mi", volumes=[{"name": "config", "configMap": {"name": "mm2-config"}}],
           mounts=[{"name": "config", "mountPath": "/etc/mm2", "readOnly": True}])
    credentials_file = ROOT / "kite-credentials.json"
    if credentials_file.exists():
        credentials = json.loads(credentials_file.read_text())
    else:
        credentials = {"username": "lunaorbit-lab-admin", "password": secrets.token_urlsafe(32),
                       "jwt": secrets.token_hex(32), "encrypt": secrets.token_hex(16)}
        credentials_file.write_text(json.dumps(credentials))
        credentials_file.chmod(0o600)
    config = "superUser:\n  username: ${KITE_USERNAME}\n  password: ${KITE_PASSWORD}\nclusters:\n  - name: lunaorbit-lab\n    inCluster: true\n    default: true\n"
    apply(item("Secret", "kite-config", type="Opaque", stringData={
        "config.yaml": config, "KITE_USERNAME": credentials["username"],
        "KITE_PASSWORD": credentials["password"], "JWT_SECRET": credentials["jwt"],
        "KITE_ENCRYPT_KEY": credentials["encrypt"]}))
    apply(item("ServiceAccount", "kite"))
    role = item("Role", "kite-lab", rules=[{"apiGroups": ["", "apps"],
        "resources": ["configmaps", "pods", "deployments", "services", "events"],
        "verbs": ["get", "list", "watch"]}])
    role["apiVersion"] = "rbac.authorization.k8s.io/v1"
    apply(role)
    binding = item("RoleBinding", "kite-lab", roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "kite-lab"},
                   subjects=[{"kind": "ServiceAccount", "name": "kite", "namespace": NS}])
    binding["apiVersion"] = "rbac.authorization.k8s.io/v1"
    apply(binding)
    role = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
            "metadata": {"name": "lunaorbit-lab-namespaces"}, "rules": [{"apiGroups": [""],
            "resources": ["namespaces"], "verbs": ["get", "list", "watch"]}]}
    apply(role)
    apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
           "metadata": {"name": "lunaorbit-lab-namespaces"},
           "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "lunaorbit-lab-namespaces"},
           "subjects": [{"kind": "ServiceAccount", "name": "kite", "namespace": NS}]})
    # Secret contents are sent through kubectl stdin, never command arguments or logs.
    deploy("kite", KITE_IMAGE, memory="192Mi", values={"KITE_CONFIG_FILE": "/etc/kite/config.yaml",
           "DB_DSN": "/data/kite.sqlite", "ANONYMOUS_USER_ENABLED": "false", "ENABLE_ANALYTICS": "false",
           "DISABLE_CACHE": "true"},
           volumes=[{"name": "config", "secret": {"secretName": "kite-config"}}, {"name": "data", "emptyDir": {}}],
           mounts=[{"name": "config", "mountPath": "/etc/kite", "readOnly": True}, {"name": "data", "mountPath": "/data"}],
           secret_env=("KITE_USERNAME", "KITE_PASSWORD", "JWT_SECRET", "KITE_ENCRYPT_KEY"))
    apply(item("Service", "kite", {"selector": {"app": "kite"}, "ports": [{"port": 8080}]}))
    print("LAB_MANIFESTS_APPLIED: synthetic namespace, 2 brokers, MM2, authenticated read-only Kite")


def replicate():
    topic = "lunaorbit-lab-events"
    marker = "lunaorbit-synthetic-" + secrets.token_hex(8)
    create = ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--create", "--if-not-exists", "--topic", topic, "--partitions", "1", "--replication-factor", "1"]
    kubectl("exec", "deploy/kafka-source", "--", *create)
    kubectl("exec", "-i", "deploy/kafka-source", "--", "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "localhost:9092", "--topic", topic, data=marker + "\n")
    # Explicit partition avoids creating a consumer group just to inspect synthetic data.
    for attempt in range(6):
        time.sleep(5)
        try:
            offsets = kubectl("exec", "deploy/kafka-target", "--", "/opt/kafka/bin/kafka-get-offsets.sh",
                              "--bootstrap-server", "localhost:9092", "--topic", "source." + topic)
        except subprocess.CalledProcessError:
            continue  # Remote Topic may not exist until the connector refreshes.
        rows = [line for line in offsets.splitlines() if line.startswith("source." + topic + ":0:")]
        if not rows:
            continue
        count = int(rows[0].rsplit(":", 1)[1])
        assert 0 <= count <= 1000, "synthetic topic exceeded inspection bound"
        if not count:
            continue
        output = kubectl("exec", "deploy/kafka-target", "--", "/opt/kafka/bin/kafka-console-consumer.sh",
                         "--bootstrap-server", "localhost:9092", "--topic", "source." + topic,
                         "--partition", "0", "--offset", "0", "--max-messages", str(count), "--timeout-ms", "15000")
        if marker in output.splitlines():
            print("MM2_REPLICATION_PASS: unique synthetic marker arrived source Kafka3.7.2 -> MM2 -> target Kafka3.9.2")
            return
    raise AssertionError("synthetic marker did not arrive through MM2")


def prepare_mm2():
    """Initialize only this lab's target Connect topics before running MM2."""
    kubectl("scale", "deployment/mm2", "--replicas=0")
    for topic in ("mm2-configs.source.internal", "mm2-offsets.source.internal", "mm2-status.source.internal"):
        kubectl("exec", "deploy/kafka-target", "--", "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "localhost:9092", "--create", "--if-not-exists", "--topic", topic,
                "--partitions", "1", "--replication-factor", "1", "--config", "cleanup.policy=compact")
        kubectl("exec", "deploy/kafka-target", "--", "/opt/kafka/bin/kafka-configs.sh",
                "--bootstrap-server", "localhost:9092", "--entity-type", "topics", "--entity-name", topic,
                "--alter", "--add-config", "cleanup.policy=compact")
    kubectl("scale", "deployment/mm2", "--replicas=1")
    print("MM2_INTERNAL_TOPICS_PREPARED: dedicated target topics have cleanup.policy=compact")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["setup", "prepare-mm2", "replicate"])
    args = parser.parse_args()
    {"setup": setup, "prepare-mm2": prepare_mm2, "replicate": replicate}[args.action]()
