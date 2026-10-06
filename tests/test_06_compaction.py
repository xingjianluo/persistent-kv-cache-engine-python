"""Log compaction: disk usage and recovery time converge; observable state
(values, versions, TTL remaining, tombstones, the never-written/deleted/
expired distinction, CAS preconditions, capacity accounting) is identical
before and after; concurrent confirmed writes survive; a crash mid-pass
lands on a consistent pre- or post-compaction state."""

from __future__ import annotations

import os
import threading

import pytest

import kvstore.compact as compact_mod
from kvstore.errors import (
    CapacityExceeded,
    KeyDeleted,
    KeyExpired,
    KeyNotFound,
    VersionMismatch,
)
from kvstore.store import KVStore, WAL_NAME

from conftest import get_logger

LOG = get_logger("test.compaction")


def _open(data_dir, capacity=10_000, t=1000, **kw):
    clock = {"t": t}
    store = KVStore(data_dir, capacity_per_namespace=capacity,
                    clock_ms=lambda: clock["t"], **kw)
    store._test_clock = clock
    return store


def _status(store, ns, key):
    try:
        store.get(ns, key)
        return "live"
    except KeyNotFound:
        return "not_found"
    except KeyDeleted:
        return "deleted"
    except KeyExpired:
        return "expired"


def test_compaction_preserves_all_observable_state(tmp_path):
    data = str(tmp_path / "d")
    s = _open(data, capacity=3, t=10_000)
    s.put("ns", "live", {"v": 1})
    s.put("ns", "live2", "x", ttl_ms=60_000)          # expires at 70000
    s.put("ns", "gone", 1)
    s.delete("ns", "gone")                            # tombstone v2
    s.put("ns", "short", "y", ttl_ms=5)               # expires at 10005
    s._test_clock["t"] = 10_006                       # 'short' now expired
    for i in range(40):                               # churn: 40 dead frames
        s.put("ns", "live", {"v": i + 2})
    before = {k: _status(s, "ns", k)
              for k in ("live", "live2", "gone", "short", "never")}
    wal_before = os.path.getsize(os.path.join(data, WAL_NAME))
    LOG.info("pre-compaction status=%s wal_bytes=%d", before, wal_before)

    rep = s.compact()
    wal_after = os.path.getsize(os.path.join(data, WAL_NAME))
    LOG.info("compact report=%s wal_bytes %d -> %d", rep, wal_before, wal_after)
    assert wal_after < wal_before, "log must converge, not grow with history"

    after = {k: _status(s, "ns", k)
             for k in ("live", "live2", "gone", "short", "never")}
    LOG.info("post-compaction status=%s (must equal pre)", after)
    assert after == before
    assert after["live"] == "live" and after["gone"] == "deleted"
    assert after["short"] == "expired" and after["never"] == "not_found"

    # versions & TTL timestamps survive exactly
    assert s.get("ns", "live")["version"] == 41
    assert s.get("ns", "live2")["expires_at_ms"] == 70_000
    # TTL *remaining* time is unchanged: same absolute deadline
    s._test_clock["t"] = 70_000
    assert _status(s, "ns", "live2") == "expired"

    # CAS on pre-compaction versions still lines up, both directions
    with pytest.raises(VersionMismatch):
        s.put("ns", "live", 0, expected_version=40)
    r = s.put("ns", "live", 0, expected_version=41)
    assert r["version"] == 42
    # CAS that revives a tombstone still needs the tombstone's version
    with pytest.raises(KeyDeleted):
        s.put("ns", "gone", 9, expected_version=1)
    r = s.put("ns", "gone", 9, expected_version=2)
    assert r["version"] == 3
    LOG.info("basis: version chain and delete/expired reasons survive compaction")

    # capacity accounting is neither over- nor under-counted
    s2 = _open(str(tmp_path / "d"), capacity=3, t=70_000)
    live_now = sum(1 for k in ("live", "live2", "gone", "short")
                   if _status(s2, "ns", k) == "live")
    LOG.info("live keys after restart at t=70000: %d (capacity=3)", live_now)
    assert live_now == 2  # live, gone(revived); live2 & short expired
    s2.put("ns", "extra", 1)                       # 3rd live key: ok
    with pytest.raises(CapacityExceeded):
        s2.put("ns", "overflow", 1)                # 4th: rejected
    LOG.info("basis: capacity counts live keys exactly as before compaction")
    s2.close()
    s.close()


def test_compaction_converges_recovery_work(tmp_path):
    data = str(tmp_path / "d")
    s = _open(data)
    for i in range(300):
        s.put("ns", f"k{i % 10}", i)               # 300 frames, 10 live keys
    s.delete("ns", "k0")
    s.close()
    s = _open(data)
    assert s.last_recovery["frames_replayed"] == 301
    rep = s.compact()
    s.close()
    s = _open(data)
    LOG.info("recovery after compaction replays %d frames (was 301)",
             s.last_recovery["frames_replayed"])
    assert s.last_recovery["frames_replayed"] == 10  # 9 live + 1 tombstone
    assert rep["entries"] == 10
    s.close()


def test_compaction_on_startup(tmp_path):
    data = str(tmp_path / "d")
    s = _open(data)
    for i in range(50):
        s.put("ns", "churn", i)
    s.close()
    s = _open(data, compact_on_startup=True)
    LOG.info("compact_on_startup report=%s", s.last_compaction)
    assert s.last_compaction is not None and s.last_compaction["compacted"]
    assert s.get("ns", "churn")["version"] == 50
    s.close()
    s = _open(data)
    assert s.last_recovery["frames_replayed"] == 1
    s.close()


def test_concurrent_writes_during_compaction_are_not_lost(tmp_path, monkeypatch):
    """A write confirmed while the snapshot is being serialized is carried
    into the new log via the delta phase (deterministically injected)."""
    data = str(tmp_path / "d")
    s = _open(data)
    for i in range(20):
        s.put("ns", f"base{i}", i)

    orig_append_put = compact_mod.WriteAheadLog.append_put
    injected = {"done": False}

    def append_put_and_inject(self, record):
        # During phase 2 (snapshot copy, no store lock held), land one
        # confirmed write and one confirmed delete on the *old* WAL.
        if self.path.endswith(".compact") and not injected["done"]:
            injected["done"] = True
            s.put("ns", "during", "concurrent")
            s.delete("ns", "base0")
            LOG.info("injected confirmed put+delete while snapshot copy runs")
        return orig_append_put(self, record)

    monkeypatch.setattr(compact_mod.WriteAheadLog, "append_put",
                        append_put_and_inject)
    rep = s.compact()
    monkeypatch.undo()
    LOG.info("report=%s (delta_carried must cover the injected writes)", rep)
    assert injected["done"] and rep["delta_carried"] == 2

    assert s.get("ns", "during")["value"] == "concurrent"
    assert _status(s, "ns", "base0") == "deleted"
    s.close()
    # ... and they survive a restart on the compacted log
    s = _open(data)
    assert s.get("ns", "during")["value"] == "concurrent"
    assert _status(s, "ns", "base0") == "deleted"
    assert s.get("ns", "base1")["value"] == 1
    LOG.info("basis: delta-carried mutations are durable after the swap")
    s.close()


def test_reads_and_writes_stay_available_during_compaction(tmp_path):
    """Many keys make the copy phase long; a writer thread must keep
    succeeding throughout, with zero lost confirmed writes."""
    data = str(tmp_path / "d")
    s = _open(data)
    for i in range(400):
        s.put("ns", f"k{i}", i)
    stop = threading.Event()
    failures: list[BaseException] = []
    confirmed = []

    def writer():
        n = 0
        while not stop.is_set():
            try:
                s.put("ns", "hot", n)
                confirmed.append(n)
                n += 1
            except BaseException as exc:  # noqa: BLE001
                failures.append(exc)
                return

    t = threading.Thread(target=writer)
    t.start()
    for _ in range(3):
        s.compact()
    stop.set()
    t.join()
    LOG.info("writer completed %d confirmed puts across 3 compactions; "
             "failures=%s", len(confirmed), failures)
    assert not failures
    assert len(confirmed) > 0
    # the last confirmed write is what the store shows — nothing lost
    assert s.get("ns", "hot")["value"] == confirmed[-1]
    s.close()
    s = _open(data)
    assert s.get("ns", "hot")["value"] == confirmed[-1]
    LOG.info("basis: every confirmed write survived compaction + restart")
    s.close()


def test_crash_before_swap_leaves_pre_compaction_state(tmp_path):
    """Leftover wal.log.compact (power loss mid-copy) is discarded at
    startup; the old WAL stays authoritative."""
    data = str(tmp_path / "d")
    s = _open(data)
    s.put("ns", "a", 1)
    s.put("ns", "b", 2)
    s.close()
    wal_path = os.path.join(data, WAL_NAME)
    wal_bytes = os.path.getsize(wal_path)
    # simulate an interrupted compaction: half-written copy left behind
    with open(wal_path + ".compact", "wb") as f:
        f.write(b"PKV1partial-garbage")
    LOG.info("simulated crash: leftover .compact next to wal.log (%d bytes)",
             wal_bytes)

    s = _open(data)
    assert s.get("ns", "a")["value"] == 1
    assert s.get("ns", "b")["value"] == 2
    assert not os.path.exists(wal_path + ".compact"), "stale copy discarded"
    assert os.path.getsize(wal_path) == wal_bytes, "old WAL untouched"
    LOG.info("basis: crash before the atomic swap => pre-compaction state")
    s.close()


def test_failed_swap_keeps_old_log_and_store_healthy(tmp_path, monkeypatch):
    """If the atomic replace itself fails, compaction aborts: the old log
    is untouched, no leftover file, and the store keeps serving writes."""
    data = str(tmp_path / "d")
    s = _open(data)
    s.put("ns", "a", 1)
    wal_path = os.path.join(data, WAL_NAME)
    wal_bytes = os.path.getsize(wal_path)

    monkeypatch.setattr(compact_mod.os, "replace",
                        lambda *a: (_ for _ in ()).throw(OSError("power off")))
    with pytest.raises(OSError):
        s.compact()
    monkeypatch.undo()
    LOG.info("swap failed -> old wal intact=%s leftover=%s",
             os.path.getsize(wal_path) == wal_bytes,
             os.path.exists(wal_path + ".compact"))
    assert os.path.getsize(wal_path) == wal_bytes
    assert not os.path.exists(wal_path + ".compact")
    # store is still fully functional on the old log
    r = s.put("ns", "b", 2)
    assert r["version"] == 1
    s.close()
    s = _open(data)
    assert s.get("ns", "a")["value"] == 1 and s.get("ns", "b")["value"] == 2
    LOG.info("basis: failed compaction never half-applies; writes continue")
    s.close()


def test_compacted_log_is_a_consistent_post_state(tmp_path):
    """Crash *after* the swap: the compacted log alone is a complete,
    consistent state (values, versions, tombstones, expiry)."""
    data = str(tmp_path / "d")
    s = _open(data, t=1000)
    s.put("ns", "x", {"n": 1}, ttl_ms=5000)
    s.put("ns", "y", 2)
    s.delete("ns", "y")
    s.compact()
    s.close()
    # nothing but wal.log (+ writer.lock) may remain: no .compact, no backup
    leftovers = [f for f in os.listdir(data) if f.endswith(".compact")]
    LOG.info("post-swap directory listing: %s", os.listdir(data))
    assert leftovers == []
    s = _open(data, t=2000)
    assert s.get("ns", "x")["value"] == {"n": 1}
    assert s.get("ns", "x")["expires_at_ms"] == 6000
    assert _status(s, "ns", "y") == "deleted"
    LOG.info("basis: post-swap state is complete and consistent on its own")
    s.close()
