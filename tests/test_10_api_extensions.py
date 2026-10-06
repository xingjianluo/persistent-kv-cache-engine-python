"""HTTP surface for the new capabilities: manual compaction endpoint,
compaction report, and the read_only / data_dir_locked error codes."""

from __future__ import annotations

import importlib

from fastapi.testclient import TestClient

from conftest import get_logger

LOG = get_logger("test.api_ext")


def _client(monkeypatch, tmp_path, **env):
    monkeypatch.setenv("KV_DATA_DIR", str(tmp_path / "kv"))
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import main
    importlib.reload(main)
    return TestClient(main.app), main


def test_compact_endpoint_and_report(monkeypatch, tmp_path):
    client, main = _client(monkeypatch, tmp_path)
    for i in range(20):
        r = client.put("/v1/namespaces/ns/keys/churn", json={"value": i})
        assert r.status_code == 200
    LOG.info("20 overwrites done; triggering POST /v1/admin/compact")
    r = client.post("/v1/admin/compact")
    assert r.status_code == 200
    body = r.json()
    LOG.info("compact response: %s", body)
    assert body["compacted"] is True
    assert body["bytes_after"] < body["bytes_before"]

    r = client.get("/v1/admin/compaction-report")
    assert r.json()["last_compaction"]["compacted"] is True

    # state is untouched by compaction
    assert client.get("/v1/namespaces/ns/keys/churn").json()["version"] == 20
    main.app.state.store.close()
    LOG.info("basis: manual compaction via HTTP preserves state")


def test_compact_on_startup_env(monkeypatch, tmp_path):
    client, main = _client(monkeypatch, tmp_path)
    client.put("/v1/namespaces/ns/keys/a", json={"value": 1})
    main.app.state.store.close()
    client2, main2 = _client(monkeypatch, tmp_path, KV_COMPACT_ON_STARTUP="1")
    rep = client2.get("/v1/admin/compaction-report").json()
    LOG.info("startup compaction report: %s", rep)
    assert rep["last_compaction"] is not None
    assert client2.get("/v1/namespaces/ns/keys/a").json()["value"] == 1
    main2.app.state.store.close()


def test_read_only_mode_via_env(monkeypatch, tmp_path):
    client, main = _client(monkeypatch, tmp_path)
    client.put("/v1/namespaces/ns/keys/a", json={"value": 1})
    main.app.state.store.close()

    client2, main2 = _client(monkeypatch, tmp_path, KV_READ_ONLY="1")
    LOG.info("read-only instance up; health check and reads must work")
    assert client2.get("/healthz").json() == {"status": "ok"}
    assert client2.get("/v1/namespaces/ns/keys/a").json()["value"] == 1
    r = client2.put("/v1/namespaces/ns/keys/b", json={"value": 2})
    assert r.status_code == 403 and r.json()["error"]["code"] == "read_only"
    r = client2.post("/v1/admin/compact")
    assert r.status_code == 403 and r.json()["error"]["code"] == "read_only"
    LOG.info("basis: writes -> 403 read_only; health/reads unaffected")
    main2.app.state.store.close()
