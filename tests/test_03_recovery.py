"""Restart recovery: confirmed writes/expiry/versions survive; torn trailing
frame and transaction without COMMIT are reported then deterministically
truncated; confirmed state is never damaged."""

from __future__ import annotations

import os

from kvstore.errors import KeyDeleted, KeyExpired
from kvstore.store import KVStore, WAL_NAME

from conftest import get_logger

LOG = get_logger("test.recovery")


def _open_new(data_dir, capacity=10_000, t=5_000):
    clock = {"t": t}
    store = KVStore(data_dir, capacity_per_namespace=capacity,
                    clock_ms=lambda: clock["t"])
    store._test_clock = clock
    return store


def test_confirmed_state_survives_restart(tmp_path):
    data = tmp_path / "d1"
    s = _open_new(str(data), t=1000)
    s.put("ns", "a", {"v": 1})
    s.put("ns", "a", {"v": 2}, expected_version=1)       # v2
    s.put("ns", "b", "ttl", ttl_ms=500)                 # expires 1500
    s.put("ns", "c", 3)
    s.delete("ns", "c")                                  # tombstone v2
    rep = s.batch([
        {"op": "put", "namespace": "ns", "key": "d", "value": 4},
        {"op": "put", "namespace": "ns2", "key": "e", "value": 5},
    ])
    seq_before = rep["results"][-1]["seq"]
    s.close()
    LOG.info("wrote confirmed frames through seq=%s, then closed", seq_before)

    s2 = _open_new(str(data), t=1200)
    assert s2.get("ns", "a")["value"] == {"v": 2}
    assert s2.get("ns", "a")["version"] == 2
    assert s2.get("ns", "b")["value"] == "ttl"
    assert s2.get("ns", "d")["value"] == 4
    assert s2.get("ns2", "e")["value"] == 5
    try:
        s2.get("ns", "c")
        raise AssertionError
    except KeyDeleted:
        LOG.info("tombstone survived restart")
    # expiry is an absolute timestamp: at t=1600 b is gone
    s2._test_clock["t"] = 1600
    try:
        s2.get("ns", "b")
        raise AssertionError
    except KeyExpired:
        LOG.info("absolute expires_at_ms survived restart; key now expired")

    # sequence numbers continue after restart (no reuse/collision):
    # 9 frames were replayed (incl. BEGIN/COMMIT), next seq must be 10
    replayed = s2.last_recovery["frames_replayed"]
    r = s2.put("ns", "a", {"v": 3}, expected_version=2)
    assert r["seq"] == replayed + 1
    assert r["version"] == 3
    s2.close()
    s3 = _open_new(str(data), t=2000)
    assert s3.get("ns", "a")["value"] == {"v": 3}
    LOG.info("basis: values/versions/expiry/tombstones identical across restarts")


def test_torn_trailing_frame_is_truncated_and_reported(tmp_path):
    """Simulate a crash mid-append: confirmed prefix stays, torn suffix cut."""
    data = tmp_path / "d2"
    s = _open_new(str(data))
    s.put("ns", "confirmed", 1)
    s.put("ns", "will_be_torn", 2)
    s.close()
    path = str(data / WAL_NAME)
    good_size = os.path.getsize(path)
    LOG.info("clean WAL size=%d", good_size)

    with open(path, "r+b") as f:
        f.truncate(good_size - 3)  # tear the last frame
    torn_size = good_size - 3
    s2 = _open_new(str(data))
    rep = s2.last_recovery
    LOG.info("recovery report on torn suffix: %s", rep)
    assert rep["truncated"] is not None
    assert rep["truncated"]["reason"] == "truncated_frame"
    assert rep["truncated"]["offset"] < torn_size
    assert s2.get("ns", "confirmed")["value"] == 1
    from kvstore.errors import KeyNotFound
    try:
        s2.get("ns", "will_be_torn")
        raise AssertionError
    except KeyNotFound:
        LOG.info("unconfirmed torn record invisible; confirmed prefix intact")
    # the bad suffix is physically gone, so the next recovery is clean
    s2.close()
    s3 = _open_new(str(data))
    assert s3.last_recovery["truncated"] is None
    assert s3.last_recovery["frames_replayed"] == 1
    # and new appends continue sequence numbering after the kept frame
    r = s3.put("ns", "after", 9)
    assert r["seq"] == 2
    LOG.info("basis: truncation is fsync'd; re-replay clean, seq continues")


def test_transaction_without_commit_is_rolled_back_on_restart(tmp_path):
    """Crash between BEGIN and COMMIT: whole batch is unconfirmed."""
    data = tmp_path / "d3"
    s = _open_new(str(data))
    s.put("ns", "before", 1)

    # Simulate a crash between BEGIN and COMMIT via the same live WAL so
    # frame sequence numbers stay contiguous (as in a real process).
    from kvstore.records import PutRecord
    s.wal.append_begin()
    s.wal.append_put(PutRecord("ns", "inside_tx", "x", 2, None))
    s.wal.close()  # process killed before COMMIT

    s2 = _open_new(str(data))
    rep = s2.last_recovery
    LOG.info("recovery with dangling transaction: %s", rep)
    assert rep["truncated"] is not None
    assert "transaction" in rep["truncated"]["detail"]
    assert s2.get("ns", "before")["value"] == 1
    from kvstore.errors import KeyNotFound
    try:
        s2.get("ns", "inside_tx")
        raise AssertionError
    except KeyNotFound:
        LOG.info("basis: no COMMIT => entire transaction invisible after restart")
    # committed batches after restart still work atomically
    out = s2.batch([
        {"op": "put", "namespace": "ns", "key": "p", "value": 1},
        {"op": "put", "namespace": "ns", "key": "q", "value": 2},
    ])
    assert out["applied"] is True
    assert s2.get("ns", "p")["value"] == 1 and s2.get("ns", "q")["value"] == 2
