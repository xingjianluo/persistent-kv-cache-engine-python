"""KVState 纯状态机测试：CAS、TTL、删除原因区分、批量原子性、容量规则。

每个用例通过日志打印操作序列、输入与判定依据，便于复现。
"""

from __future__ import annotations

import logging
import unittest

from kvapp.engine import (
    KVError,
    KVState,
    REASON_CAPACITY_EXCEEDED,
    REASON_DELETED,
    REASON_EXPIRED,
    REASON_NOT_FOUND,
    REASON_VERSION_MISMATCH,
)

log = logging.getLogger("kvtest.state")

T0 = 1_000_000_000  # 固定基准时钟（ns），保证断言可复现


class StateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.st = KVState(max_entries_per_namespace=2)

    def commit(self, ops, now: int = T0):
        log.info("COMMIT now=%d ops=%r", now, ops)
        p = self.st.prepare_commit(ops, now)
        for kind, pl in p.frames:
            self.st.apply_frame(kind, pl, now)
        log.info("COMMIT results=%r frames=%r", p.results,
                 [(k.name, pl) for k, pl in p.frames])
        return p.results

    def test_put_get_version_and_not_found(self) -> None:
        r = self.commit([{"op": "put", "ns": "n", "key": "a", "value": 1}])
        self.assertEqual(r, [{"index": 0, "version": 1}])
        self.assertEqual(self.st.classify("n", "a", T0), "visible")
        self.assertEqual(self.st.classify("n", "missing", T0),
                         REASON_NOT_FOUND)
        log.info("verdict: a visible=%s, missing=%s",
                 self.st.classify("n", "a", T0),
                 self.st.classify("n", "missing", T0))

    def test_cas_mismatch_is_rejected_with_distinct_reason(self) -> None:
        self.commit([{"op": "put", "ns": "n", "key": "a", "value": 1}])
        with self.assertRaises(KVError) as cm:
            self.st.prepare_commit(
                [{"op": "put", "ns": "n", "key": "a", "value": 2,
                  "expected_version": 5}], T0)
        self.assertEqual(cm.exception.code, REASON_VERSION_MISMATCH)
        self.assertEqual(cm.exception.details["current_version"], 1)
        self.assertEqual(cm.exception.details["state"], "visible")
        log.info("verdict: expected=5 rejected, details=%r", cm.exception.details)

    def test_cas_match_succeeds(self) -> None:
        self.commit([{"op": "put", "ns": "n", "key": "a", "value": 1}])
        r = self.commit([{"op": "put", "ns": "n", "key": "a", "value": 2,
                          "expected_version": 1}])
        self.assertEqual(r[0]["version"], 2)

    def test_ttl_expiry_is_distinct_from_delete_and_missing(self) -> None:
        self.commit([{"op": "put", "ns": "n", "key": "t", "value": "x",
                      "ttl_ms": 100}])
        self.assertEqual(self.st.classify("n", "t", T0 + 99_000_000),
                         "visible")
        self.assertEqual(self.st.classify("n", "t", T0 + 100_000_000),
                         REASON_EXPIRED)
        log.info("verdict: ttl 100ms, visible@99ms, expired@>=100ms")
        # 过期后以 expected_version=0 重建：版本从 1 重新开始
        r = self.commit(
            [{"op": "put", "ns": "n", "key": "t", "value": "y",
              "expected_version": 0}], T0 + 200_000_000)
        self.assertEqual(r[0]["version"], 1)

    def test_delete_is_distinct_and_version_continues(self) -> None:
        self.commit([{"op": "put", "ns": "n", "key": "a", "value": 1}])
        self.commit([{"op": "put", "ns": "n", "key": "a", "value": 2,
                      "expected_version": 1}])
        r = self.commit([{"op": "delete", "ns": "n", "key": "a",
                          "expected_version": 2}])
        self.assertEqual(r[0]["version"], 3)
        self.assertEqual(self.st.classify("n", "a", T0), REASON_DELETED)
        with self.assertRaises(KVError) as cm:
            self.st.prepare_commit(
                [{"op": "delete", "ns": "n", "key": "a"}], T0)
        self.assertEqual(cm.exception.code, REASON_DELETED)
        # 显式删除后重建：版本沿历史继续（4），不会与旧版本重复
        r = self.commit([{"op": "put", "ns": "n", "key": "a", "value": 9,
                          "expected_version": 0}])
        self.assertEqual(r[0]["version"], 4)
        log.info("verdict: deleted->reason=%s, recreate version=4",
                 REASON_DELETED)

    def test_delete_expired_key_reports_expired(self) -> None:
        st = KVState()
        p = st.prepare_commit(
            [{"op": "put", "ns": "n", "key": "t", "value": 1, "ttl_ms": 1}], T0)
        for k, pl in p.frames:
            st.apply_frame(k, pl, T0)
        with self.assertRaises(KVError) as cm:
            st.prepare_commit(
                [{"op": "delete", "ns": "n", "key": "t"}], T0 + 2_000_000)
        self.assertEqual(cm.exception.code, REASON_EXPIRED)

    def test_capacity_rejects_new_keys_without_eviction(self) -> None:
        st = KVState(max_entries_per_namespace=1)
        p = st.prepare_commit(
            [{"op": "put", "ns": "n", "key": "a", "value": 1}], T0)
        for k, pl in p.frames:
            st.apply_frame(k, pl, T0)
        with self.assertRaises(KVError) as cm:
            st.prepare_commit(
                [{"op": "put", "ns": "n", "key": "b", "value": 1}], T0)
        self.assertEqual(cm.exception.code, REASON_CAPACITY_EXCEEDED)
        self.assertEqual(cm.exception.details["projected"], 2)
        # 被拒绝后已有数据不丢失
        self.assertEqual(st.live_count("n", T0), 1)
        log.info("verdict: capacity reject details=%r, a still present",
                 cm.exception.details)

    def test_expired_keys_free_capacity(self) -> None:
        st = KVState(max_entries_per_namespace=1)
        p = st.prepare_commit(
            [{"op": "put", "ns": "n", "key": "t", "value": 1, "ttl_ms": 10}],
            T0)
        for k, pl in p.frames:
            st.apply_frame(k, pl, T0)
        p = st.prepare_commit(
            [{"op": "put", "ns": "n", "key": "g", "value": 1}],
            T0 + 50_000_000)
        self.assertEqual(p.results[0]["version"], 1)
        log.info("verdict: expired t freed slot, g accepted")

    def test_batch_is_atomic_on_later_failure(self) -> None:
        self.commit([{"op": "put", "ns": "n", "key": "a", "value": 1}])
        with self.assertRaises(KVError) as cm:
            self.st.prepare_commit([
                {"op": "put", "ns": "n", "key": "z", "value": 1},
                {"op": "delete", "ns": "n", "key": "missing"},
            ], T0)
        self.assertEqual(cm.exception.code, REASON_NOT_FOUND)
        self.assertEqual(cm.exception.details["index"], 1)
        self.assertEqual(self.st.classify("n", "z", T0), REASON_NOT_FOUND)
        log.info("verdict: batch rejected at index=1, z never visible")

    def test_delete_then_recreate_same_key_in_batch(self) -> None:
        st = KVState(max_entries_per_namespace=1)
        p = st.prepare_commit(
            [{"op": "put", "ns": "n", "key": "a", "value": 1}], T0)
        for k, pl in p.frames:
            st.apply_frame(k, pl, T0)
        p = st.prepare_commit([
            {"op": "delete", "ns": "n", "key": "a"},
            {"op": "put", "ns": "n", "key": "a", "value": 2},
        ], T0)
        for k, pl in p.frames:
            st.apply_frame(k, pl, T0)
        self.assertEqual(p.results, [
            {"index": 0, "version": 2},
            {"index": 1, "version": 3},
        ])

    def test_value_too_large_rejected(self) -> None:
        st = KVState(max_value_bytes=4)
        with self.assertRaises(KVError) as cm:
            st.prepare_commit(
                [{"op": "put", "ns": "n", "key": "a", "value": "toolong"}], T0)
        self.assertEqual(cm.exception.code, "value_too_large")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    unittest.main(verbosity=2)
