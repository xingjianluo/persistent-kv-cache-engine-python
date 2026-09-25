"""并发争用测试：同一键大量并发条件写，不得丢失更新。

使用可注入时钟的 Engine，并发通过多线程事件循环压测提交路径；
日志打印每个工作线程的尝试/重试/成功序列，便于复现判定。
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
import threading
import unittest

from kvapp.engine import Engine, KVError

log = logging.getLogger("kvtest.concurrency")


class ConcurrencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="kvconc-")

    def tearDown(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_concurrent_cas_increments_never_lost(self) -> None:
        eng = Engine(self.dir, compaction_threshold_bytes=10**9)
        per_worker = 25
        workers = 8
        total = per_worker * workers

        barrier = threading.Barrier(workers)
        outcomes: list[tuple[int, int, int]] = []  # (worker, attempt, version)
        lock = threading.Lock()

        def worker(wid: int) -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def run() -> None:
                barrier.wait()
                successes = 0
                attempts = 0
                while successes < per_worker:
                    attempts += 1
                    _, entry = await eng.get("ns", "counter")
                    cur = entry.version
                    try:
                        ver = await eng.put(
                            "ns", "counter",
                            {"by": wid, "n": successes + 1},
                            expected_version=cur,
                        )
                    except KVError as e:
                        # 并发下版本冲突是预期结果：重试而不是覆盖
                        log.info("worker=%d attempt=%d cur=%d -> %s",
                                 wid, attempts, cur, e.code)
                        continue
                    successes += 1
                    with lock:
                        outcomes.append((wid, attempts, ver))
                    log.info("worker=%d attempt=%d CAS ok ver=%d",
                             wid, attempts, ver)
                log.info("worker=%d done attempts=%d successes=%d",
                         wid, attempts, successes)

            loop.run_until_complete(run())
            loop.close()

        # 初始版本 0
        init = asyncio.run(eng.put("ns", "counter", {"by": -1, "n": 0}))
        self.assertEqual(init, 1)

        threads = [threading.Thread(target=worker, args=(w,))
                   for w in range(workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        final = asyncio.run(eng.get("ns", "counter"))
        log.info("FINAL version=%d expected=%d outcomes=%d",
                 final[1].version, total + 1, len(outcomes))
        # 初始版本 1 + 每个 worker 25 次严格递增 → 最终版本必须精确等于 total+1
        self.assertEqual(final[1].version, total + 1)
        self.assertEqual(len(outcomes), total)
        # 每个版本号恰好被一个成功者拿到（无重复、无覆盖）
        versions = sorted(v for _, _, v in outcomes)
        self.assertEqual(versions, list(range(2, total + 2)))
        asyncio.run(eng.close())
        log.info("verdict: %d concurrent CAS writes, no lost update", total)

    def test_batch_concurrent_is_serialized(self) -> None:
        eng = Engine(self.dir, max_entries_per_namespace=1000,
                     compaction_threshold_bytes=10**9)
        n = 50

        def worker(wid: int) -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def run() -> None:
                await eng.batch([
                    {"op": "put", "ns": "n", "key": f"w{wid}", "value": wid},
                    {"op": "put", "ns": "n", "key": f"k{wid}", "value": wid},
                ])
            loop.run_until_complete(run())
            loop.close()

        threads = [threading.Thread(target=worker, args=(w,)) for w in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        async def verify():
            for w in range(n):
                v, e = await eng.get("n", f"w{w}")
                assert v == w and e.version == 1
            s = await eng.stats("n")
            assert s["live_entries"] == 2 * n, s
            await eng.close()
        asyncio.run(verify())
        log.info("verdict: %d concurrent batches, all keys present exactly once", n)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    unittest.main(verbosity=2)
