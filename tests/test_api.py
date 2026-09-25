"""HTTP API 集成测试：TTL 真实到期、重启恢复、损坏快照、并发路由。"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import time
import unittest

from fastapi.testclient import TestClient

from kvapp.api import create_app
from kvapp.wal import SNAPSHOT_FILE

log = logging.getLogger("kvtest.api")


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="kvapi-")

    def tearDown(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    def client(self) -> TestClient:
        return TestClient(create_app(self.dir))

    def test_ttl_expiry_over_http_then_distinct_reasons(self) -> None:
        with self.client() as c:
            r = c.put("/v1/namespaces/n/keys/t",
                      json={"value": "v", "ttl_ms": 120})
            self.assertEqual(r.status_code, 200, r.text)
            log.info("PUT t ttl=120ms -> %s", r.json())
            r = c.get("/v1/namespaces/n/keys/t")
            self.assertEqual(r.status_code, 200)
            time.sleep(0.15)
            r = c.get("/v1/namespaces/n/keys/t")
            self.assertEqual(r.status_code, 404)
            self.assertEqual(r.json()["error"]["code"], "expired")
            log.info("verdict after 150ms: expired=%r", r.json())
            # 显式删除另一个键，得到 deleted 原因，与 expired 区分
            c.put("/v1/namespaces/n/keys/d", json={"value": 1})
            c.request("DELETE", "/v1/namespaces/n/keys/d")
            r = c.get("/v1/namespaces/n/keys/d")
            self.assertEqual(r.json()["error"]["code"], "deleted")
            r = c.get("/v1/namespaces/n/keys/never")
            self.assertEqual(r.json()["error"]["code"], "not_found")

    def test_restart_preserves_version_and_ttl(self) -> None:
        with self.client() as c:
            c.put("/v1/namespaces/n/keys/a", json={"value": 1})
            c.put("/v1/namespaces/n/keys/a",
                  json={"value": 2, "expected_version": 1})
            c.put("/v1/namespaces/n/keys/t",
                  json={"value": "x", "ttl_ms": 600_000})
            log.info("wrote a@v2 and t with ttl before restart")
        with self.client() as c:
            r = c.get("/v1/namespaces/n/keys/a")
            self.assertEqual((r.json()["value"], r.json()["version"]), (2, 2))
            r = c.get("/v1/namespaces/n/keys/t")
            self.assertEqual(r.status_code, 200)
            self.assertIsNotNone(r.json()["expires_at_ns"])
            log.info("verdict after restart: a=2@v2, ttl preserved=%s",
                     r.json()["expires_at_ns"])

    def test_truncated_snapshot_is_hard_error(self) -> None:
        # 低压缩阈值 + 多次写入，强制产生 snapshot.kvs
        with TestClient(create_app(self.dir, compaction_threshold_bytes=300)) as c:
            for i in range(40):
                r = c.put(f"/v1/namespaces/n/keys/k{i:02d}",
                          json={"value": i})
                self.assertEqual(r.status_code, 200, r.text)
        snap = os.path.join(self.dir, SNAPSHOT_FILE)
        self.assertTrue(os.path.exists(snap), "compaction should create snapshot")
        with open(snap, "rb") as f:
            good = f.read()
        with open(snap, "wb") as f:
            f.write(good[:-3])  # 截断快照尾
        log.info("truncated snapshot by 3 bytes")
        with self.assertRaises(Exception):
            with TestClient(create_app(self.dir,
                                       compaction_threshold_bytes=300)):
                pass
        log.info("verdict: truncated snapshot prevents startup")

    def test_capacity_and_value_limit_over_http(self) -> None:
        with TestClient(create_app(self.dir, max_entries_per_namespace=2,
                                   max_value_bytes=8)) as c:
            c.put("/v1/namespaces/n/keys/a", json={"value": 1})
            c.put("/v1/namespaces/n/keys/b", json={"value": 2})
            r = c.put("/v1/namespaces/n/keys/c", json={"value": 3})
            self.assertEqual(r.status_code, 507)
            self.assertEqual(r.json()["error"]["code"],
                             "capacity_exceeded")
            log.info("capacity reject over HTTP=%s", r.json())
            # 已确认数据未被淘汰
            self.assertEqual(c.get("/v1/namespaces/n/keys/a").status_code, 200)
            self.assertEqual(c.get("/v1/namespaces/n/keys/b").status_code, 200)
            r = c.put("/v1/namespaces/n/keys/b", json={"value": "abcdefghij"})
            self.assertEqual(r.status_code, 413)
            self.assertEqual(r.json()["error"]["code"], "value_too_large")
            log.info("value-too-large reject over HTTP=%s", r.json())

    def test_ttl_expires_after_restart(self) -> None:
        with TestClient(create_app(self.dir)) as c:
            c.put("/v1/namespaces/n/keys/t",
                  json={"value": "v", "ttl_ms": 120})
        time.sleep(0.15)
        with TestClient(create_app(self.dir)) as c:
            r = c.get("/v1/namespaces/n/keys/t")
            self.assertEqual(r.status_code, 404)
            self.assertEqual(r.json()["error"]["code"], "expired")
            log.info("verdict: ttl expired across restart, code=expired")

    def test_batch_consistent_results(self) -> None:
        with self.client() as c:
            r = c.post("/v1/batch", json={"ops": [
                {"op": "put", "ns": "n", "key": "a", "value": 1},
                {"op": "put", "ns": "n", "key": "b", "value": 2,
                 "expected_version": 0},
            ]})
            self.assertEqual(r.status_code, 200, r.text)
            versions = [x["version"] for x in r.json()["results"]]
            self.assertEqual(versions, [1, 1])
            log.info("batch consistent results=%s", r.json()["results"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    unittest.main(verbosity=2)
