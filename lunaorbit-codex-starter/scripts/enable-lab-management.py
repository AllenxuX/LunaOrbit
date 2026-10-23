"""Explicitly authorized lab writes, scoped to the single MM2 fixture only.

Kite role API/verbs: https://kitehq.dev/api/rbac-management
Run on the lab host. Credentials stay in its private files; never print them.
"""
import json
import os
from importlib.machinery import SourceFileLoader
from pathlib import Path

check = SourceFileLoader("check", str(Path(__file__).with_name("kite-lab-check.py"))).load_module()
lab = check.lab


def main():
    os.umask(0o077)
    ns = json.loads(lab.kubectl("get", "ns", lab.NS, "-o", "json"))
    assert ns["metadata"]["labels"].get("lunaorbit.io/purpose") == "synthetic-integration"
    # Extend the existing SA with write rights on exactly these fixture names.
    obj = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
           "metadata": {"name": "lunaorbit-mm2-manager", "namespace": lab.NS}, "rules": [
        {"apiGroups": [""], "resources": ["configmaps"], "resourceNames": ["mm2-config"], "verbs": ["get", "update", "patch"]},
        {"apiGroups": ["apps"], "resources": ["deployments"], "resourceNames": ["mm2"], "verbs": ["get", "update", "patch"]}]}
    lab.apply(obj)
    lab.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
        "metadata": {"name": "lunaorbit-mm2-manager", "namespace": lab.NS},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "lunaorbit-mm2-manager"},
        "subjects": [{"kind": "ServiceAccount", "name": "kite", "namespace": lab.NS}]})
    credentials = json.loads((lab.ROOT / "kite-credentials.json").read_text())
    status, _ = check.request("/api/auth/login/password", "POST", credentials)
    check.require(status, [204], "login")
    status, roles = check.request("/api/v1/admin/roles/")
    check.require(status, [200], "roles")
    role = next((r for r in roles["roles"] if r["name"] == "lunaorbit-lab-mm2-manager"), None)
    if role is None:
        status, response = check.request("/api/v1/admin/roles/", "POST", {
            "name": "lunaorbit-lab-mm2-manager", "clusters": [lab.PROFILE], "namespaces": [lab.NS],
            "resources": ["configmaps", "deployments"], "verbs": ["get", "update"]})
        check.require(status, [201], "create role")
        role = response["role"]
    status, _ = check.request(f'/api/v1/admin/roles/{role["id"]}/assign', "POST", {
        "subjectType": "user", "subject": "lunaorbit-synthetic-reader"})
    check.require(status, [200, 201], "assign scoped role")
    print("LAB_MM2_MANAGEMENT_ENABLED: only mm2-config and mm2 writable; no production resources")


if __name__ == "__main__":
    main()
