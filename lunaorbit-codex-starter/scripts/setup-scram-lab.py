"""Opt-in SASL listener setup for the explicitly authorized synthetic lab.

Run only on its Linux Docker host. Preserves PLAINTEXT:9092 for MM2 and
ANONYMOUS administration; touches only the two guarded broker deployments and
services. Broker pods use ephemeral storage, so a rollout can remove lab topics.
Business credentials are created through the platform native Kafka API; this
script never creates or changes Kafka credentials.
"""
import argparse
from importlib.machinery import SourceFileLoader
import json
import os
from pathlib import Path
import re
import time
import subprocess

acl = SourceFileLoader("scram_acl_lab", str(Path(__file__).with_name("kafka-acl-lab.py"))).load_module()
lab = acl.lab
CREDENTIALS = lab.ROOT / "scram-lab-credentials.json"


def quiet_kubectl(*args, data=None):
    result = subprocess.run(
        ["kubectl", "--context", lab.PROFILE, "-n", lab.NS, *args],
        input=data, text=True, capture_output=True, timeout=150,
    )
    if result.returncode:
        # Kafka tools can include configuration text in diagnostics. Never
        # forward stdout/stderr or a credential-bearing exception to callers.
        raise RuntimeError("Synthetic lab kubectl operation failed; credentials suppressed")
    return result.stdout


def legacy_superusers():
    # Earlier versions generated an unused management identity. Accept only
    # exact values from that private file when removing the obsolete grant.
    if not CREDENTIALS.exists():
        return {}
    assert CREDENTIALS.is_file() and not CREDENTIALS.is_symlink()
    assert CREDENTIALS.stat().st_mode & 0o077 == 0
    entries = json.loads(CREDENTIALS.read_text())
    assert set(entries) == set(acl.BROKERS)
    result = {}
    for name, entry in entries.items():
        username = entry["username"]
        assert re.fullmatch(r"lunaorbit-lab-admin-[a-f0-9]{24}", username)
        result[name] = "User:ANONYMOUS;User:" + username
    return result

def desired_env(name):
    return {
        "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093,SASL_PLAINTEXT://:9094",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{name}.{lab.NS}.svc.cluster.local:9092,SASL_PLAINTEXT://{name}.{lab.NS}.svc.cluster.local:9094",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT",
        "KAFKA_SASL_ENABLED_MECHANISMS": "SCRAM-SHA-256",
        "KAFKA_OPTS": "-Djava.security.auth.login.config=/dev/null",
        "KAFKA_LISTENER_NAME_SASL__PLAINTEXT_SCRAM___SHA___256_SASL_JAAS_CONFIG": "org.apache.kafka.common.security.scram.ScramLoginModule required;",
        "KAFKA_AUTHORIZER_CLASS_NAME": "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
        "KAFKA_SUPER_USERS": "User:ANONYMOUS",
    }


def preflight(name, legacy):
    deployment = json.loads(quiet_kubectl("get", "deployment", name, "-o", "json"))
    values = {value["name"]: value.get("value") for value in deployment["spec"]["template"]["spec"]["containers"][0].get("env", [])}
    expected = desired_env(name)
    original = {
        "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{name}.{lab.NS}.svc.cluster.local:9092",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_SUPER_USERS": "User:ANONYMOUS",
    }
    assert values.get("KAFKA_INTER_BROKER_LISTENER_NAME") == "PLAINTEXT"
    assert values.get("KAFKA_CONTROLLER_LISTENER_NAMES") == "CONTROLLER"
    for key, wanted in expected.items():
        allowed = (None, "", wanted, original.get(key))
        if key == "KAFKA_SUPER_USERS" and name in legacy:
            allowed += (legacy[name],)
        assert values.get(key) in allowed, "unexpected broker listener/auth configuration"
    service = json.loads(quiet_kubectl("get", "service", name, "-o", "json"))
    assert service["metadata"]["namespace"] == lab.NS
    assert service["spec"]["selector"] == {"app": name}
    assert service["spec"].get("type", "ClusterIP") == "ClusterIP"
    ports = service["spec"]["ports"]
    assert {port["port"] for port in ports} in ({9092}, {9092, 9094})
    for port in ports:
        assert port.get("targetPort", port["port"]) == port["port"] and port.get("protocol", "TCP") == "TCP"
    return values, ports


def wait_ready(name):
    # TCP/Deployment readiness can precede Kafka controller initialization.
    # Retry only this bounded, read-only check; never retry credential writes.
    for attempt in range(8):
        try:
            quiet_kubectl("exec", "-i", "deploy/" + name, "--", "env", "KAFKA_HEAP_OPTS=-Xms32m -Xmx64m",
                "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list")
            print("LAB_SCRAM_BROKER_READY:", name)
            return
        except RuntimeError:
            if attempt == 7:
                raise RuntimeError("Synthetic lab broker did not become ready; diagnostics suppressed") from None
            time.sleep(2)

def setup():
    os.umask(0o077)
    acl.guard()
    legacy = legacy_superusers()
    inspected = {name: preflight(name, legacy) for name in acl.BROKERS}
    for name in acl.BROKERS:
        expected = desired_env(name)
        values, ports = inspected[name]
        if any(values.get(key) != value for key, value in expected.items()):
            patch = {"spec": {"template": {"spec": {"containers": [{"name": name,
                "env": [{"name": key, "value": value} for key, value in expected.items()]}]}}}}
            quiet_kubectl("patch", "deployment", name, "--type=strategic", "--patch-file=/dev/stdin", data=json.dumps(patch))
        quiet_kubectl("rollout", "status", "deployment/" + name, "--timeout=110s")
        if not any(port["port"] == 9094 for port in ports):
            # Multiple service ports require names, including the existing port.
            published = [{**port, "name": port.get("name", "plaintext")} for port in ports]
            published.append({"name": "sasl-plaintext", "port": 9094, "targetPort": 9094, "protocol": "TCP"})
            quiet_kubectl("patch", "service", name, "--type=merge", "--patch-file=/dev/stdin", data=json.dumps({"spec": {"ports": published}}))
        wait_ready(name)
        print("LAB_SCRAM_LISTENER_CONFIGURED:", name, "9094; PLAINTEXT 9092 preserved")
    print("LAB_SCRAM_AUTH_MODEL: ANONYMOUS management on 9092; platform-created restricted SCRAM business identities on 9094")
    print("LAB_SCRAM_NEXT: verify SASL authentication via platform; prepare and replicate MM2 synthetic topics after broker rollouts")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("guard", "setup"))
    action = parser.parse_args().action
    if action == "guard":
        acl.guard()
    else:
        setup()
