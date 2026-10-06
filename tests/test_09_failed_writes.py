"""Failed writes leave zero trace: a put/batch that fails (unserializable
value, I/O error mid-batch) persists nothing, consumes no durable sequence
numbers, and never puts the store into a state a later recovery would
reject. Versions and WAL seqs stay continuous across the failure and the
next restart."""

from __future__ import annotations

import os

import pytest

from kvstore.store import KVStore, WAL_NAME
from kvstore.wal import WriteAheadLog

from conftest import get_logger

LOG = get_logger("test.failed_writes")


def _open(data, t=1000):
    clock = {"t": t}
    store = KVStore(data, clock_ms=lambda: clock["t"])
    store._test_clock = clock
    return store


def test_unserializable_put_leaves_no_trace(tmp_path):
    data = str(tmp_path / "d")
    s = _open(data)
    r = s.put("ns", "a", 1)
    wal = os.path.join(data, WAL_NAME)
    size_before = os.path.getsize(wal)
    LOG.info("confirmed put seq=%s; wal=%d bytes", r["seq"], size_before)

    with pytest.raises(ValueError) as ei:
        s.put("ns", "bad", object())  # not JSON-serializable
    LOG.info("unserializable put rejected: %s", ei.value)
    assert os.path.getsize(wal) == size_before, "zero frames for a failed put"

    # subsequent writes continue with the very next sequence number
    r2 = s.put("ns", "b", 2)
    LOG.info("next confirmed put seq=%s (must be previous+1)", r2["seq"])
    assert r2["seq"] == r["seq"] + 1
    s.close()

    s = _open(data)
    assert s.last_recovery["truncated"] is None, "startup must not choke"
    assert s.get("ns", "a")["version"] == 1
    assert s.get("ns", "b")["version"] == 1
    r3 = s.put("ns", "a", 9, expected_version=1)
    assert r3["version"] == 2 and r3["seq"] == r2["seq"] + 1
    LOG.info("basis: restart clean; versions and seqs continuous, none reused")
    s.close()


def test_unserializable_batch_member_rejects_whole_batch_before_writing(tmp_path):
    data = str(tmp_path / "d")
    s = _open(data)
    s.put("ns", "base", 1)
    wal = os.path.join(data, WAL_NAME)
    size_before = os.path.getsize(wal)

    with pytest.raises(ValueError):
        s.batch([
            {"op": "put", "namespace": "ns", "key": "x", "value": 1},
            {"op": "put", "namespace": "ns", "key": "y", "value": {"a", "b"}},
        ])
    assert os.path.getsize(wal) == size_before, "not even BEGIN may be written"
    with pytest.raises(Exception) as ei:
        s.get("ns", "x")
    assert ei.value.code == "not_found"
    LOG.info("failed batch persisted nothing; wal still %d bytes", size_before)

    out = s.batch([
        {"op": "put", "namespace": "ns", "key": "x", "value": 1},
        {"op": "put", "namespace": "ns", "key": "y", "value": 2},
    ])
    assert out["applied"] is True
    s.close()
    s = _open(data)
    assert s.get("ns", "x")["value"] == 1 and s.get("ns", "y")["value"] == 2
    LOG.info("basis: later batch applies and survives restart")
    s.close()


def test_io_failure_mid_batch_truncates_and_seq_stays_continuous(tmp_path):
    """Force a failure *after* BEGIN is on disk: the dangling transaction is
    rolled back immediately, and the next write reuses only numbers that
    never reached a confirmed frame — recovery sees no gap, ever."""
    data = str(tmp_path / "d")
    s = _open(data)
    r1 = s.put("ns", "a", 1)
    wal = os.path.join(data, WAL_NAME)

    real_append_put = WriteAheadLog.append_put
    state = {"armed": True}

    def flaky_append_put(self, record):
        if state["armed"] and self.path == s.wal.path:
            state["armed"] = False
            raise OSError("simulated disk failure mid-batch")
        return real_append_put(self, record)

    WriteAheadLog.append_put = flaky_append_put
    try:
        with pytest.raises(OSError):
            s.batch([
                {"op": "put", "namespace": "ns", "key": "t1", "value": 1},
                {"op": "put", "namespace": "ns", "key": "t2", "value": 2},
            ])
    finally:
        WriteAheadLog.append_put = real_append_put
    LOG.info("mid-batch failure: dangling BEGIN rolled back; wal=%d bytes",
             os.path.getsize(wal))

    # the failed transaction is invisible and the log is immediately usable
    with pytest.raises(Exception) as ei:
        s.get("ns", "t1")
    assert ei.value.code == "not_found"
    out = s.batch([
        {"op": "put", "namespace": "ns", "key": "t1", "value": 1},
        {"op": "put", "namespace": "ns", "key": "t2", "value": 2},
    ])
    seqs = [r["seq"] for r in out["results"]]
    LOG.info("retried batch seqs=%s (continue right after seq=%s)",
             seqs, r1["seq"])
    assert seqs[0] > r1["seq"], "monotonic: never below confirmed numbers"
    s.close()

    # the crucial property: recovery must not see a sequence gap
    s = _open(data)
    assert s.last_recovery["truncated"] is None
    assert s.get("ns", "t1")["value"] == 1 and s.get("ns", "t2")["value"] == 2
    r = s.put("ns", "a", 2, expected_version=1)
    assert r["version"] == 2
    LOG.info("basis: restart after failed batch is clean; versions/seqs "
             "monotonic, service never refuses to start")
    s.close()


def test_failed_delete_attempt_does_not_consume_versions(tmp_path):
    data = str(tmp_path / "d")
    s = _open(data)
    s.put("ns", "k", 1)                       # v1
    with pytest.raises(Exception) as ei:
        s.delete("ns", "missing")             # rejected: nothing persisted
    assert ei.value.code == "not_found"
    r = s.put("ns", "k", 2, expected_version=1)  # CAS on v1 still valid
    assert r["version"] == 2
    s.close()
    s = _open(data)
    assert s.get("ns", "k")["version"] == 2
    LOG.info("basis: rejected writes consume neither versions nor seqs")
    s.close()
