"""Opt-in ACL CRUD verification on the authorized synthetic Kafka lab only.

ANONYMOUS is a lab superuser so existing PLAINTEXT MM2 remains connected.
This proves ACL administration, not SASL identity or permission enforcement.
"""
import json
from importlib.machinery import SourceFileLoader
from pathlib import Path
import secrets
import subprocess

lab = SourceFileLoader("lab_acl", str(Path(__file__).with_name("minikube-lab.py"))).load_module()
BROKERS = {"kafka-source": "apache/kafka:3.7.2", "kafka-target": "apache/kafka:3.9.2"}


def guard():
    profile = json.loads(Path("/root/.minikube/profiles/lunaorbit-lab/config.json").read_text())
    assert profile["Name"] == lab.PROFILE and profile["Driver"] == "docker"
    ns = json.loads(lab.kubectl("get", "namespace", lab.NS, "-o", "json"))
    assert ns["metadata"]["name"] == lab.NS
    assert ns["metadata"]["labels"].get("lunaorbit.io/purpose") == "synthetic-integration"
    for name, image in BROKERS.items():
        deployment = json.loads(lab.kubectl("get", "deployment", name, "-o", "json"))
        assert deployment["metadata"]["namespace"] == lab.NS
        assert deployment["spec"]["selector"]["matchLabels"] == {"app": name}
        assert deployment["spec"]["template"]["metadata"]["labels"]["app"] == name
        containers = deployment["spec"]["template"]["spec"]["containers"]
        assert len(containers) == 1 and containers[0]["name"] == name and containers[0]["image"] == image
    print("LAB_SCOPE_GUARDS_PASS: named Docker profile, synthetic namespace label, exact broker selectors/images")


def cli(name, *args):
    return lab.kubectl("exec", "deploy/" + name, "--", "/opt/kafka/bin/kafka-acls.sh", "--bootstrap-server", "localhost:9092", *args)


def enable():
    guard()
    for name in BROKERS:
        deployment = json.loads(lab.kubectl("get", "deployment", name, "-o", "json"))
        values = {e["name"]: e.get("value") for e in deployment["spec"]["template"]["spec"]["containers"][0].get("env", [])}
        expected = {"KAFKA_AUTHORIZER_CLASS_NAME": "org.apache.kafka.metadata.authorizer.StandardAuthorizer", "KAFKA_SUPER_USERS": "User:ANONYMOUS"}
        existing = values.get("KAFKA_AUTHORIZER_CLASS_NAME")
        assert existing in (None, "", expected["KAFKA_AUTHORIZER_CLASS_NAME"]), "unexpected authorizer configuration"
        assert values.get("KAFKA_SUPER_USERS") in (None, "", "User:ANONYMOUS"), "unexpected superuser configuration"
        if any(values.get(k) != v for k, v in expected.items()):
            lab.kubectl("set", "env", "deployment/" + name, *(k + "=" + v for k, v in expected.items()))
            print("LAB_AUTHORIZER_ENV_ENABLED:", name)
        else:
            print("LAB_AUTHORIZER_ALREADY_ENABLED:", name)
        lab.kubectl("rollout", "status", "deployment/" + name, "--timeout=110s")
        # Leave native/JVM overhead for a bounded 64 MiB CLI alongside the
        # broker; 384 MiB was insufficient during repeated integration checks.
        limit = deployment["spec"]["template"]["spec"]["containers"][0].get("resources", {}).get("limits", {}).get("memory")
        if limit != "640Mi":
            lab.kubectl("set", "resources", "deployment/" + name, "--containers=" + name, "--limits=memory=640Mi")
            lab.kubectl("rollout", "status", "deployment/" + name, "--timeout=110s")
        cli(name, "--list")
        print("LAB_AUTHORIZER_QUERY_PASS:", name)


def verify():
    guard()
    principal = "User:lunaorbit-acl-synthetic-" + secrets.token_hex(4)
    topic = "lunaorbit-acl-fixture-" + secrets.token_hex(4)
    for name in BROKERS:
        created = []
        try:
            for pattern in ("literal", "prefixed"):
                args = ("--topic", topic, "--resource-pattern-type", pattern)
                cli(name, "--add", "--allow-principal", principal, "--allow-host", "*", "--operation", "Read", *args)
                created.append(args)
            listed = cli(name, "--list", "--principal", principal)
            assert topic in listed and "LITERAL" in listed and "PREFIXED" in listed
            literal = created[0]
            cli(name, "--remove", "--force", "--allow-principal", principal, "--allow-host", "*", "--operation", "Read", *literal)
            created.remove(literal)
            remaining = cli(name, "--list", "--principal", principal)
            assert "PREFIXED" in remaining and "LITERAL" not in remaining, "exact literal deletion affected other pattern"
            prefixed = created[0]
            cli(name, "--remove", "--force", "--allow-principal", principal, "--allow-host", "*", "--operation", "Read", *prefixed)
            created.remove(prefixed)
            assert topic not in cli(name, "--list", "--principal", principal)
            print("LAB_ACL_CREATE_QUERY_EXACT_DELETE_PASS:", name, "literal+prefixed; unrelated pattern preserved; synthetic rules removed")
        finally:
            for args in created:
                cli(name, "--remove", "--force", "--allow-principal", principal, "--allow-host", "*", "--operation", "Read", *args)
    print("LAB_AUTHORIZATION_LIMIT: PLAINTEXT ANONYMOUS superuser validates CRUD only; SASL and permission enforcement not validated")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("guard", "enable", "verify"))
    action = parser.parse_args().action
    {"guard": guard, "enable": enable, "verify": verify}[action]()
