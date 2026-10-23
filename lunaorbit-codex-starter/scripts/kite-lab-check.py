"""Authenticate to the dedicated Kite lab and verify only documented resource GETs.

Admin bootstrap routes were checked against Kite v0.16.0 routes.go and handlers.
No key, cookie, password, or raw ConfigMap is printed or copied into the repository.
"""
import http.cookiejar
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.request

from importlib.machinery import SourceFileLoader

lab = SourceFileLoader("lab", str(Path(__file__).with_name("minikube-lab.py"))).load_module()
BASE = "http://127.0.0.1:28080"
ROOT = lab.ROOT
NS = lab.NS
cookies = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookies))


def request(path, method="GET", data=None, key=None, authenticated=True):
    headers = {"Content-Type": "application/json", "x-cluster-name": lab.PROFILE}
    if key:
        headers["Authorization"] = key
    req = urllib.request.Request(BASE + path, method=method, headers=headers,
        data=None if data is None else json.dumps(data).encode())
    client = opener if authenticated else urllib.request.build_opener()
    try:
        with client.open(req, timeout=30) as response:
            raw = response.read(1024 * 1024 + 1)
            assert len(raw) <= 1024 * 1024, "response exceeded limit"
            return response.status, None if not raw else json.loads(raw)
    except urllib.error.HTTPError as error:
        return error.code, None


def require(status, allowed, step):
    assert status in allowed, f"{step}: unexpected HTTP status {status}"


def main():
    os.umask(0o077)
    credentials = json.loads((ROOT / "kite-credentials.json").read_text())
    status, _ = request("/api/auth/login/password", "POST", {
        "username": credentials["username"], "password": credentials["password"]})
    require(status, [204], "password login")
    keyfile = ROOT / "kite-reader-key.json"
    # The lab uses ephemeral SQLite; a recreated Kite Pod requires a fresh key.
    status, keys = request("/api/v1/admin/apikeys/")
    require(status, [200], "key inventory")
    keyname = "lunaorbit-synthetic-reader"
    if keyfile.exists() and any(k["username"] == keyname for k in keys["apiKeys"]):
        key = json.loads(keyfile.read_text())["key"]
    else:
        status, result = request("/api/v1/admin/apikeys/", "POST", {"name": keyname})
        require(status, [201], "create lab key")
        user = result["apiKey"]
        key = f'kite{user["id"]}-{user["apiKey"]}'
        keyfile.write_text(json.dumps({"key": key}))
        keyfile.chmod(0o600)
    status, roles = request("/api/v1/admin/roles/")
    require(status, [200], "roles")
    role = next((r for r in roles["roles"] if r["name"] == "lunaorbit-lab-reader"), None)
    if role is None:
        status, result = request("/api/v1/admin/roles/", "POST", {
            "name": "lunaorbit-lab-reader", "clusters": [lab.PROFILE], "namespaces": [NS],
            "resources": ["configmaps", "deployments", "pods", "namespaces"], "verbs": ["get"]})
        require(status, [201], "create limited lab role")
        role = result["role"]
    status, _ = request(f'/api/v1/admin/roles/{role["id"]}/assign', "POST", {
        "subjectType": "user", "subject": keyname})
    require(status, [200, 201], "assign reader")
    time.sleep(1)
    status, _ = request(f"/api/v1/configmaps/{NS}", authenticated=False)
    require(status, [401], "anonymous denied")
    status, _ = request(f"/api/v1/configmaps/{NS}", key="Bearer " + key, authenticated=False)
    require(status, [401], "Bearer prefix denied")
    status, _ = request("/api/v1/admin/clusters/", key=key, authenticated=False)
    require(status, [403], "reader admin denied")
    status, _ = request("/api/v1/configmaps/kube-system", key=key, authenticated=False)
    require(status, [403], "reader foreign namespace denied")
    pods = json.loads(lab.kubectl("get", "pods", "-l", "app=mm2", "-o", "json"))["items"]
    pod = pods[0]["metadata"]["name"]
    paths = ["/api/v1/namespaces/_all", f"/api/v1/configmaps/{NS}",
             f"/api/v1/configmaps/{NS}/mm2-config", f"/api/v1/deployments/{NS}",
             f"/api/v1/deployments/{NS}/mm2", f"/api/v1/pods/{NS}", f"/api/v1/pods/{NS}/{pod}"]
    summary = []
    for path in paths:
        status, body = request(path, key=key, authenticated=False)
        require(status, [200], path)
        shape = {"path": path, "status": status, "type": type(body).__name__}
        if isinstance(body, dict):
            shape["keys"] = sorted(body)
            if "items" in body:
                shape["items"] = len(body["items"])
        elif isinstance(body, list):
            shape["items"] = len(body)
        summary.append(shape)
        # Only synthetic lab responses remain on the lab host with mode0600.
        (ROOT / ("response-" + str(len(summary)) + ".json")).write_text(json.dumps(body))
    (ROOT / "kite-response-summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    print("KITE_LAB_GETS_PASS: seven existing resource contracts, anonymous/Bearer/admin/foreign-namespace denial")


if __name__ == "__main__":
    main()
