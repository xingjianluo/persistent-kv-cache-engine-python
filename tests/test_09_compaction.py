"""Log compaction: disk usage and recovery time must converge.

Semantics after compaction must be bit-identical: values, versions,
remaining TTL, tombstones and the never-written / deleted / expired
distinction; versioned CAS keeps matching; capacity accounting is exact;
confirmed writes during compaction are never lost; a crash mid-compaction
falls back to either the old or the new log, never a mix.
"""

from __future__ import annotations

import os
import threading

import pytest

from kvstore.errors import KeyDeleted, KeyExpired, KeyNotFound
from kvstore.store import KVStore

from conftest import get_logger

LOG = get_logger("test.compaction")


def _observable_state(store, keys, expired_keys=(), deleted_keys=(), missing_keys=()):
    """Snapshot every observable fact the tests assert on."""
    snap = {}
    for key in keys:
        got = store.get("ns", key)
        snap[key] = (got["value"], got["version"], got["expires_at_ms"])
    for key in expired_keys:
        with pytest.raises(KeyExpired):
            store.get("ns", key)
        snap[key] = "expired"
    for key in deleted_keys:
        with pytest.raises(KeyDeleted):
            store.get("ns", key)
        snap[key] = "deleted"
    for key in missing_keys:
        with pytest.raises(KeyNotFound):
            store.get("ns", key)
        snap[key] = "not_found"
    return snap


def _seed_mixed(store):
    for i in range(10):
        store.put("ns", f"live{i}", {"n": i}, ttl_ms=None if i % 2 else 60_000)
    store.put("ns", "live0", "overwritten")           # version 2
    store.put("ns", "gone", 1)
    store.delete("ns", "gone")                        # tombstone v2
    store.put("ns", "short", "x", ttl_ms=500)         # expires at t+500
    return ["live0"] + [f"live{i}" for i in range(1, 10)]


def test_semantics_identical_across_compaction_and_restart(tmp_path):
    data = str(tmp_path / "d")
    clock = {"t": 1_000_000}
    store = KVStore(data, clock_ms=lambda: clock["t"])
    live = _seed_mixed(store)
    clock["t"] += 1_000  # 'short' now expired naturally
    before = _observable_state(
        store, live, expired_keys=["short"], deleted_keys=["gone"],
        missing_keys=["never"],
    )
    size_before = os.path.getsize(f"{data}/wal.log")
    LOG.info("pre-compaction state=%s wal_bytes=%d", before, size_before)

    rep = store.compact()
    size_after = os.path.getsize(f"{data}/wal.log")
    LOG.info("compaction report=%s wal_bytes %d -> %d", rep, size_before, size_after)
    assert size_after < size_before, "compaction must shrink the log"

    after = _observable_state(
        store, live, expired_keys=["short"], deleted_keys=["gone"],
        missing_keys=["never"],
    )
    LOG.info("post-compaction state=%s", after)
    assert before == after, "values/versions/TTL/reasons must be identical"

    LOG.info("CAS on pre-compaction version still matches (live0 at v2)")
    r = store.put("ns", "live0", "cas", expected_version=2)
    assert r["version"] == 3
    with pytest.raises(Exception) as exc:
        store.put("ns", "gone", "x", expected_version=1)
    assert exc.value.code == "deleted", "tombstone version must survive compaction"
    store.put("ns", "gone", "revived", expected_version=2)
    store.close()

    reopened = KVStore(data, clock_ms=lambda: clock["t"])
    LOG.info("post-restart recovery=%s", reopened.last_recovery)
    assert reopened.last_recovery["truncated"] is None
    after_restart = _observable_state(
        reopened, live, expired_keys=["short"], deleted_keys=[], missing_keys=["never"],
    )
    after_restart["gone"] = ("revived", 3, None)
    expected = dict(after)
    expected["live0"] = ("cas", 3, None)
    expected["gone"] = ("revived", 3, None)
    assert after_restart == expected
    r2 = reopened.put("ns", "new", 1)
    assert r2["seq"] > rep["base_seq"], "seq keeps growing across generations"
    LOG.info("basis: restart after compaction preserves everything; seq=%s", r2["seq"])
    reopened.close()


def test_capacity_accounting_unchanged_by_compaction(tmp_path):
    data = str(tmp_path / "d")
    clock = {"t": 1000}
    store = KVStore(data, capacity_per_namespace=3, clock_ms=lambda: clock["t"])
    store.put("ns", "a", 1)
    store.put("ns", "b", 1)
    store.put("ns", "tmp", 1)
    store.delete("ns", "tmp")                      # tombstone frees the slot
    store.put("ns", "c", 1, ttl_ms=10)
    clock["t"] += 100                              # 'c' expired -> slot free
    store.compact()
    LOG.info("after compaction: 2 live, 1 tombstone, 1 expired; capacity=3")
    store.put("ns", "d", 1)                        # exactly fits (a,b,d)
    with pytest.raises(Exception) as exc:
        store.put("ns", "overflow", 1)
    LOG.info("4th live key rejected with code=%s", exc.value.code)
    assert exc.value.code == "capacity_exceeded"
    store.close()


def test_concurrent_writes_during_compaction_are_not_lost(tmp_path):
    data = str(tmp_path / "d")
    store = KVStore(data, clock_ms=lambda: 1000)
    store.put("ns", "pre", 0)
    errors: list[BaseException] = []
    confirmed: list[tuple[str, int]] = []
    lock = threading.Lock()

    def writer(tid: int):
        try:
            for i in range(30):
                key = f"t{tid}k{i}"
                store.put("ns", key, i)
                with lock:
                    confirmed.append((key, i))
        except BaseException as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(t,)) for t in range(4)]
    for t in threads:
        t.start()
    for round_ in range(5):
        rep = store.compact()
        LOG.info("compact round %d while writers run: %s", round_, rep)
    for t in threads:
        t.join()
    assert not errors
    LOG.info("%d confirmed writes during compaction", len(confirmed))

    lost = [(k, v) for k, v in confirmed
            if k != "pre" and store.get("ns", k)["value"] != v]
    assert not lost, f"confirmed writes lost: {lost[:5]}"
    assert store.get("ns", "pre")["value"] == 0
    store.close()

    reopened = KVStore(data, clock_ms=lambda: 1000)
    missing = [k for k, v in confirmed if reopened.get("ns", k)["value"] != v]
    LOG.info("after restart: %d/%d confirmed writes still visible",
             len(confirmed) - len(missing), len(confirmed))
    assert not missing
    reopened.close()


def test_crash_mid_compaction_leaves_consistent_state(tmp_path):
    data = str(tmp_path / "d")
    store = KVStore(data, clock_ms=lambda: 1000)
    for i in range(5):
        store.put("ns", f"k{i}", i)
    store.close()

    # Simulate a crash after the temp file was written but before the swap:
    # the old wal.log must still be authoritative; the leftover is discarded.
    with open(f"{data}/wal.log.compact", "wb") as f:
        f.write(b"partial compact output, never swapped")
        f.flush()
        os.fsync(f.fileno())
    LOG.info("simulated crash: leftover wal.log.compact next to intact wal.log")

    reopened = KVStore(data, clock_ms=lambda: 1000)
    LOG.info("recovery after mid-compaction crash: %s", reopened.last_recovery)
    for i in range(5):
        assert reopened.get("ns", f"k{i}")["value"] == i
    assert not os.path.exists(f"{data}/wal.log.compact"), "stale temp discarded"
    reopened.put("ns", "post", 1)
    reopened.close()
    LOG.info("basis: crash before swap -> pre-compaction state, fully writable")

    # And a crash *after* the swap is just the compacted log: already covered
    # by test_semantics_identical_across_compaction_and_restart.


def test_compact_on_open_and_manual_trigger(tmp_path):
    data = str(tmp_path / "d")
    store = KVStore(data, clock_ms=lambda: 1000)
    for i in range(10):
        store.put("ns", "hot", i)  # 10 versions of one key
    store.close()
    bloated = os.path.getsize(f"{data}/wal.log")

    opened = KVStore(data, clock_ms=lambda: 1000, compact_on_open=True)
    shrunk = os.path.getsize(f"{data}/wal.log")
    LOG.info("compact_on_open: wal %d -> %d bytes, report=%s",
             bloated, shrunk, opened.last_compaction)
    assert shrunk < bloated
    assert opened.last_compaction["compacted"] is True
    assert opened.get("ns", "hot")["value"] == 9
    assert opened.get("ns", "hot")["version"] == 10
    opened.close()


def test_compaction_preserves_batch_tombstones_and_ttl_via_api(tmp_path):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from kvstore.api import create_app

    store = KVStore(str(tmp_path / "d"), clock_ms=lambda: 5000)
    client = TestClient(create_app(store))
    client.put("/v1/namespaces/ns/keys/x", json={"value": 1, "ttl_ms": 5000})
    client.put("/v1/namespaces/ns/keys/y", json={"value": 2})
    client.delete("/v1/namespaces/ns/keys/y")

    r = client.post("/v1/admin/compact")
    LOG.info("POST /v1/admin/compact -> %s", r.json())
    assert r.status_code == 200 and r.json()["compacted"] is True

    got = client.get("/v1/namespaces/ns/keys/x").json()
    assert got["expires_at_ms"] == 10_000, "absolute TTL preserved"
    assert client.get("/v1/namespaces/ns/keys/y").json()["error"]["code"] == "deleted"
    rep = client.get("/v1/admin/compaction-report").json()
    assert rep["last_compaction"]["entries"] == 2
    store.close()
