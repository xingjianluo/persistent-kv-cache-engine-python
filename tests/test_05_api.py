"""HTTP integration: status/error codes, TTL via the API, atomic batch,
recovery endpoint, restart through the app."""

from __future__ import annotations

import importlib

from fastapi.testclient import TestClient

from conftest import get_logger

LOG = get_logger("test.api")


def _client(monkeypatch, tmp_path):
    monkeypatch.setenv("KV_DATA_DIR", str(tmp_path / "kv"))
    monkeypatch.setenv("KV_CAPACITY_PER_NAMESPACE", "2")
    import main
    importlib.reload(main)
    return TestClient(main.app), main


def test_api_error_codes_and_batch(monkeypatch, tmp_path):
    client, main = _client(monkeypatch, tmp_path)
    LOG.info("PUT a (no preconditions)")
    r = client.put("/v1/namespaces/ns/keys/a", json={"value": 1})
    assert r.status_code == 200 and r.json()["version"] == 1

    LOG.info("PUT a again with if_not_exists -> 409 key_exists")
    r = client.put("/v1/namespaces/ns/keys/a", json={"value": 2, "if_not_exists": True})
    assert r.status_code == 409 and r.json()["error"]["code"] == "key_exists"

    LOG.info("PUT a with stale expected_version=1 while at v1 succeeds once; "
             "repeat -> 409 version_mismatch")
    r = client.put("/v1/namespaces/ns/keys/a", json={"value": 2, "expected_version": 1})
    assert r.status_code == 200 and r.json()["version"] == 2
    r = client.put("/v1/namespaces/ns/keys/a", json={"value": 3, "expected_version": 1})
    assert r.status_code == 409 and r.json()["error"]["code"] == "version_mismatch"

    LOG.info("GET missing vs deleted produce distinct codes")
    assert client.get("/v1/namespaces/ns/keys/nope").status_code == 404
    assert client.get("/v1/namespaces/ns/keys/nope").json()["error"]["code"] == "not_found"
    client.delete("/v1/namespaces/ns/keys/a")
    r = client.get("/v1/namespaces/ns/keys/a")
    assert r.status_code == 404 and r.json()["error"]["code"] == "deleted"

    LOG.info("atomic batch: one invalid precondition rejects the whole batch")
    r = client.post("/v1/batch", json={"mutations": [
        {"op": "put", "namespace": "ns", "key": "b", "value": 1},
        {"op": "delete", "namespace": "ns", "key": "missing"},
    ]})
    assert r.status_code == 404 and r.json()["error"]["code"] == "not_found"
    assert client.get("/v1/namespaces/ns/keys/b").status_code == 404

    LOG.info("successful batch applies all results")
    r = client.post("/v1/batch", json={"mutations": [
        {"op": "put", "namespace": "ns", "key": "b", "value": 1},
        {"op": "put", "namespace": "ns", "key": "c", "value": 2},
    ]})
    assert r.status_code == 200 and r.json()["count"] == 2 and r.json()["atomic"] is True

    LOG.info("capacity 2 reached: next insert -> 507 capacity_exceeded")
    r = client.put("/v1/namespaces/ns/keys/d", json={"value": 9})
    assert r.status_code == 507 and r.json()["error"]["code"] == "capacity_exceeded"

    LOG.info("ttl_ms invalid -> 422")
    r = client.put("/v1/namespaces/ns/keys/t", json={"value": 1, "ttl_ms": 0})
    assert r.status_code == 422

    rep = client.get("/v1/admin/recovery-report").json()
    LOG.info("recovery report via API (fresh dir -> 0 frames replayed): %s", rep)
    assert rep["frames_replayed"] == 0 and rep["truncated"] is None
    # after a restart, confirmed frames are replayed and counted
    main.app.state.store.close()
    client2, main2 = _client(monkeypatch, tmp_path)
    rep2 = client2.get("/v1/admin/recovery-report").json()
    LOG.info("recovery report after restart: %s", rep2)
    assert rep2["frames_replayed"] >= 1 and rep2["truncated"] is None
    assert client2.get("/v1/namespaces/ns/keys/c").json()["value"] == 2
    main2.app.state.store.close()
