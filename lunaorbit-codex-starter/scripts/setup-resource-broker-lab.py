"""Add one broker to the guarded synthetic source KRaft cluster; no source rollout.

Run on the authorized Linux Docker host. The source cluster ID is reused and
node 2 registers with source controller 1, so Metadata lists both IDs in the
same cluster. Keeps the extra broker for subsequent platform tests; no cleanup.
"""
import argparse
from importlib.machinery import SourceFileLoader
import json
import os
from pathlib import Path
import re
import subprocess
import time

scram = SourceFileLoader("resource_scram_lab", str(Path(__file__).with_name("setup-scram-lab.py"))).load_module()
acl, lab = scram.acl, scram.lab
NAME = "kafka-source-extra"
IMAGE = "apache/kafka:3.7.2"
PURPOSE = "synthetic-resource-broker"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def read_optional(kind, name):
    # --ignore-not-found distinguishes absence from connection/permission errors.
    value = scram.quiet_kubectl("get", kind, name, "--ignore-not-found", "-o", "json")
    return json.loads(value) if value.strip() else None


def inspect():
    acl.guard()
    source = json.loads(scram.quiet_kubectl("get", "deployment", "kafka-source", "-o", "json"))
    container = source["spec"]["template"]["spec"]["containers"][0]
    env = {entry["name"]: entry.get("value") for entry in container.get("env", [])}
    require(env.get("KAFKA_NODE_ID") == "1" and env.get("KAFKA_PROCESS_ROLES") == "broker,controller", "Unexpected source KRaft roles")
    require(re.fullmatch(r"[A-Za-z0-9_-]{22}", env.get("CLUSTER_ID", "")) is not None, "Missing source cluster ID")
    require(env.get("KAFKA_CONTROLLER_QUORUM_VOTERS") == "1@localhost:9093", "Unexpected source quorum")
    for key, value in scram.desired_env("kafka-source").items():
        require(env.get(key) == value, "Source does not match established synthetic listener configuration")
    require(env.get("KAFKA_INTER_BROKER_LISTENER_NAME") == "PLAINTEXT" and env.get("KAFKA_CONTROLLER_LISTENER_NAMES") == "CONTROLLER", "Unexpected inter-broker/controller listeners")
    require(all(entry.get("valueFrom") is None and isinstance(entry.get("value"), str) for entry in container.get("env", [])), "Unexpected source environment reference")
    service = json.loads(scram.quiet_kubectl("get", "service", "kafka-source", "-o", "json"))
    require(service["metadata"]["namespace"] == lab.NS and service["spec"]["selector"] == {"app": "kafka-source"} and service["spec"].get("type", "ClusterIP") == "ClusterIP", "Unexpected source service scope")
    ports = service["spec"]["ports"]
    require({p["port"] for p in ports} in ({9092, 9094}, {9092, 9093, 9094}), "Unexpected source service ports")
    require(all(p.get("targetPort", p["port"]) == p["port"] and p.get("protocol", "TCP") == "TCP" for p in ports), "Unexpected source service target")
    return env, ports


def objects(env):
    values = dict(env)
    values.update({"KAFKA_NODE_ID": "2", "KAFKA_PROCESS_ROLES": "broker",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@kafka-source.{lab.NS}.svc.cluster.local:9093",
        "KAFKA_LISTENERS": "PLAINTEXT://:9092,SASL_PLAINTEXT://:9094",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{NAME}.{lab.NS}.svc.cluster.local:9092,SASL_PLAINTEXT://{NAME}.{lab.NS}.svc.cluster.local:9094",
        "KAFKA_HEAP_OPTS": "-Xms128m -Xmx128m", "KAFKA_LOG_DIRS": "/tmp/kraft-broker-logs"})
    labels = {"app": NAME, "lunaorbit.io/purpose": PURPOSE}
    metadata = {"name": NAME, "namespace": lab.NS, "labels": labels}
    service = {"apiVersion": "v1", "kind": "Service", "metadata": metadata,
        "spec": {"type": "ClusterIP", "selector": {"app": NAME}, "ports": [
            {"name": "plaintext", "port": 9092, "targetPort": 9092},
            {"name": "sasl-plaintext", "port": 9094, "targetPort": 9094}]}}
    deployment = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": metadata,
        "spec": {"replicas": 1, "strategy": {"type": "Recreate"}, "selector": {"matchLabels": {"app": NAME}},
        "template": {"metadata": {"labels": labels}, "spec": {"automountServiceAccountToken": False,
            "containers": [{"name": NAME, "image": IMAGE, "env": [{"name": k, "value": v} for k, v in sorted(values.items())],
                "ports": [{"containerPort": 9092}, {"containerPort": 9094}],
                "resources": {"requests": {"memory": "64Mi", "cpu": "50m"}, "limits": {"memory": "512Mi"}},
                "readinessProbe": {"tcpSocket": {"port": 9092}, "initialDelaySeconds": 10, "periodSeconds": 5},
                "volumeMounts": [{"name": "data", "mountPath": "/tmp/kraft-broker-logs"}]}],
            "volumes": [{"name": "data", "emptyDir": {}}]}}}}
    return service, deployment


def inspect_existing(desired):
    existing = read_optional(desired["kind"].lower(), NAME)
    if existing is None:
        return False
    require(existing["metadata"]["namespace"] == lab.NS and all(existing["metadata"].get("labels", {}).get(k) == v for k, v in desired["metadata"]["labels"].items()), "Existing extra resource is not owned by this fixture")
    if desired["kind"] == "Service":
        require(existing["spec"]["selector"] == {"app": NAME} and existing["spec"].get("type", "ClusterIP") == "ClusterIP", "Existing extra service differs")
        require({p["port"] for p in existing["spec"]["ports"]} == {9092, 9094} and all(p.get("targetPort", p["port"]) == p["port"] for p in existing["spec"]["ports"]), "Existing extra ports differ")
    else:
        spec = existing["spec"]
        require(spec["selector"]["matchLabels"] == {"app": NAME} and spec["strategy"]["type"] == "Recreate" and spec["replicas"] == 1, "Existing extra deployment differs")
        pod = spec["template"]["spec"]
        require(pod.get("automountServiceAccountToken") is False and spec["template"]["metadata"]["labels"] == desired["spec"]["template"]["metadata"]["labels"], "Existing extra pod scope differs")
        require(len(pod["containers"]) == 1, "Unexpected extra sidecar")
        actual, expected = pod["containers"][0], desired["spec"]["template"]["spec"]["containers"][0]
        for key in ("name", "image", "env", "resources", "volumeMounts"):
            require(actual.get(key) == expected[key], "Existing extra broker differs; refusing replacement")
        require(pod.get("volumes") == [{"name": "data", "emptyDir": {}}], "Unexpected extra storage")
    return True


def wait_registration():
    for attempt in range(24):
        try:
            output = scram.quiet_kubectl("exec", "deploy/kafka-source", "--", "env", "KAFKA_HEAP_OPTS=-Xms32m -Xmx64m",
                "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092")
            if all(re.search(r"\(id:\s*" + str(node) + r"\b", output) for node in (1, 2)):
                print("LAB_RESOURCE_TWO_BROKERS_PASS")
                return
        except RuntimeError:
            pass
        if attempt < 23:
            time.sleep(3)
    raise RuntimeError("Synthetic source cluster did not expose both broker IDs; diagnostics suppressed")


def setup():
    os.umask(0o077)
    env, ports = inspect()
    desired = objects(env)
    # Inspect all collision targets before the first mutation.
    exists = [inspect_existing(item) for item in desired]
    if not any(port["port"] == 9093 for port in ports):
        published = [{**p, "name": p.get("name", "plaintext" if p["port"] == 9092 else "sasl-plaintext")} for p in ports]
        published.append({"name": "controller", "port": 9093, "targetPort": 9093, "protocol": "TCP"})
        scram.quiet_kubectl("patch", "service", "kafka-source", "--type=merge", "--patch-file=/dev/stdin", data=json.dumps({"spec": {"ports": published}}))
    for item, present in zip(desired, exists):
        if not present:
            scram.quiet_kubectl("create", "-f", "-", data=json.dumps(item))
    scram.quiet_kubectl("rollout", "status", "deployment/" + NAME, "--timeout=150s")
    wait_registration()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("guard", "setup"))
    if parser.parse_args().action == "guard":
        env, _ = inspect()
        for item in objects(env):
            inspect_existing(item)
        print("LAB_RESOURCE_BROKER_SCOPE_PASS")
    else:
        setup()
