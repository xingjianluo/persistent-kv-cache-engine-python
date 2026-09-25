"""引擎持久化、崩溃恢复与损坏检测测试。"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tempfile
import unittest

from kvapp.engine import Engine, EngineInUseError, KVError
from kvapp.wal import WAL_FILE, CorruptLogError, decode_frames

log = logging.getLogger("kvtest.persistence")


class PersistenceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="kvtest-")
        log.info("SETUP data_dir=%s", self.dir)

    def tearDown(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    async def _amain(self, coro):
        return await coro

    def run_async(self, coro):
        return asyncio.run(coro)

    def test_commit_survives_restart(self) -> None:
        async def scenario():
            eng = Engine(self.dir)
            v = await eng.put("ns1", "a", {"hello": "world"}, ttl_ms=5000)
            log.info("WRITE put ns1/a ttl=5000 -> version=%d", v)
            await eng.put("ns1", "b", [1, 2, 3], expected_version=0)
            await eng.batch([
                {"op": "put", "ns": "ns2", "key": "x", "value": "x"},
                {"op": "delete", "ns": "ns1", "key": "b"},
            ])
            report = eng.recovery_report()
            await eng.close()
            return report

        self.run_async(scenario())

        async def verify():
            eng = Engine(self.dir)
            val, entry = await eng.get("ns1", "a")
            log.info("READ after restart ns1/a=%r version=%d exp=%r",
                     val, entry.version, entry.expires_at_ns)
            self.assertEqual(val, {"hello": "world"})
            self.assertEqual(entry.version, 1)
            self.assertIsNotNone(entry.expires_at_ns)
            with self.assertRaises(KVError) as cm:
                await eng.get("ns1", "b")
            self.assertEqual(cm.exception.code, "deleted")
            val2, e2 = await eng.get("ns2", "x")
            self.assertEqual(val2, "x")
            await eng.close()
            log.info("verdict: values/ttl/version/tombstone all preserved")

        self.run_async(verify())

    def test_second_process_is_rejected(self) -> None:
        async def scenario():
            eng = Engine(self.dir)
            with self.assertRaises(EngineInUseError):
                Engine(self.dir)
            log.info("verdict: concurrent engine on same dir rejected")
            await eng.close()
        self.run_async(scenario())

    def test_torn_tail_after_fake_crash_is_truncated_and_reported(self) -> None:
        async def write():
            eng = Engine(self.dir, compaction_threshold_bytes=10**9)
            await eng.put("n", "a", 1)
            await eng.put("n", "b", 2)
            await eng.close()
        self.run_async(write())

        wal = os.path.join(self.dir, WAL_FILE)
        with open(wal, "rb") as rf:
            data = rf.read()
        # 人为截断最后一个事务（至少切掉最后一帧的一部分）
        cut = len(data) - 5
        log.info("CORRUPT truncate wal %d -> %d bytes", len(data), cut)
        with open(wal, "wb") as f:
            f.write(data[:cut])

        async def verify():
            eng = Engine(self.dir, compaction_threshold_bytes=10**9)
            rep = eng.recovery_report()
            log.info("RECOVERY report=%r", rep.as_dict())
            self.assertGreater(rep.torn_tail_bytes, 0)
            # a 仍在；最后一次被截断的写入（b）可能丢失，但已确认的 a 不损坏
            val, _ = await eng.get("n", "a")
            self.assertEqual(val, 1)
            await eng.close()
        self.run_async(verify())

    def test_mid_log_corruption_is_hard_error(self) -> None:
        async def write():
            eng = Engine(self.dir, compaction_threshold_bytes=10**9)
            await eng.put("n", "a", 1)
            await eng.put("n", "b", 2)
            await eng.put("n", "c", 3)
            await eng.close()
        self.run_async(write())

        wal = os.path.join(self.dir, WAL_FILE)
        with open(wal, "rb") as rf:
            data = bytearray(rf.read())
        # 破坏第二个事务某帧的载荷，保持末尾帧完好（可重同步）
        frames, _ = decode_frames(bytes(data), source=WAL_FILE)
        target_seq = frames[2].seq  # 第二个 BEGIN 附近
        # 找到该 seq 帧起始位置并翻转其载荷字节
        idx = data.find(b"KVRW")
        positions = []
        while idx != -1:
            positions.append(idx)
            idx = data.find(b"KVRW", idx + 1)
        # 损坏倒数第 3 帧（中间帧），最后一帧保持完好
        bad_pos = positions[-3] + 20
        log.info("CORRUPT flip byte at %d (mid frame, last frame intact)", bad_pos)
        data[bad_pos] ^= 0xFF
        with open(wal, "wb") as f:
            f.write(bytes(data))

        async def verify():
            with self.assertRaises(CorruptLogError):
                Engine(self.dir, compaction_threshold_bytes=10**9)
            log.info("verdict: mid-log corruption raised CorruptLogError")
        self.run_async(verify())

    def test_uncommitted_txn_after_crash_is_discarded(self) -> None:
        async def write():
            eng = Engine(self.dir, compaction_threshold_bytes=10**9)
            await eng.put("n", "a", 1)
            await eng.close()
        self.run_async(write())

        from kvapp.wal import encode_frame, FrameKind
        wal = os.path.join(self.dir, WAL_FILE)
        # 追加一个只有 BEGIN+PUT、没有 COMMIT 的事务（模拟崩溃）
        tail = (
            encode_frame(900, FrameKind.BEGIN, {"txn": 900, "n": 1})
            + encode_frame(901, FrameKind.PUT,
                           {"ns": "n", "k": "ghost", "v": 1, "ver": 1,
                            "exp": None, "txn": 900})
        )
        with open(wal, "ab") as f:
            f.write(tail)
        log.info("CRASH injected txn=900 without COMMIT")

        async def verify():
            eng = Engine(self.dir, compaction_threshold_bytes=10**9)
            rep = eng.recovery_report()
            self.assertEqual(rep.discarded_incomplete_txns, 1)
            log.info("verdict: uncommitted txn discarded, report=%r",
                     rep.as_dict())
            with self.assertRaises(KVError) as cm:
                await eng.get("n", "ghost")
            self.assertEqual(cm.exception.code, "not_found")
            val, _ = await eng.get("n", "a")
            self.assertEqual(val, 1)
            await eng.close()
        self.run_async(verify())


    def test_snapshot_plus_new_wal_commits_both_recovered(self) -> None:
        async def first():
            eng = Engine(self.dir, compaction_threshold_bytes=300)
            for i in range(30):
                await eng.put("n", f"k{i:02d}", i)
            await eng.close()
        self.run_async(first())
        self.assertTrue(os.path.exists(os.path.join(self.dir, "snapshot.kvs")))

        async def second():
            # 阈值调大，启动时只加载快照、不再压缩；再追加新提交
            eng = Engine(self.dir, compaction_threshold_bytes=10**9)
            await eng.put("n", "after", "NEW")
            await eng.delete("n", "k00")
            await eng.close()
        self.run_async(second())

        async def verify():
            eng = Engine(self.dir, compaction_threshold_bytes=10**9)
            rep = eng.recovery_report()
            log.info("snapshot+wal recovery report=%r", rep.as_dict())
            self.assertGreaterEqual(rep.snapshot_frames, 1)
            # 快照中的旧数据
            v, e = await eng.get("n", "k01")
            self.assertEqual((v, e.version), (1, 1))
            # 快照之后的新写入与删除
            v2, e2 = await eng.get("n", "after")
            self.assertEqual(v2, "NEW")
            with self.assertRaises(KVError) as cm:
                await eng.get("n", "k00")
            self.assertEqual(cm.exception.code, "deleted")
            await eng.close()
        self.run_async(verify())
        log.info("verdict: snapshot base + incremental wal correctly merged")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    unittest.main(verbosity=2)
