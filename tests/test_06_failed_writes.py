"""Failed writes must not poison the log: after a put/batch fails because a
value is not JSON-serializable, later writes and a subsequent restart keep
versions and frame sequences monotonic (never reused) and the service still
starts."""

from __future__ import annotations

import pytest

from kvstore.store import KVStore

from conftest import get_logger

LOG = get_logger("test.failed_writes")


def _wal_seqs(data_dir: str) -> list[int]:
    from kvstore.records import HEADER_SIZE, HEADER_STRUCT

    with open(f"{data_dir}/wal.log", "rb") as f:
        blob = f.read()
    seqs, pos = [], 0
    while pos + HEADER_SIZE <= len(blob):
        _magic, plen, _crc, seq, _op = HEADER_STRUCT.unpack_from(blob, pos)
        seqs.append(seq)
        pos += HEADER_SIZE + plen
    return seqs


def test_failed_single_put_then_write_and_restart(tmp_path):
    data = str(tmp_path / "d")
    store = KVStore(data, clock_ms=lambda: 1000)
    r1 = store.put("ns", "a", 1)
    LOG.info("confirmed put seq=%s; now failing a put with unserializable value", r1["seq"])
    with pytest.raises(TypeError):
        store.put("ns", "bad", {"x": object()})
    LOG.info("failed put raised TypeError as expected; continuing with a normal put")
    r2 = store.put("ns", "b", 2)
    LOG.info("next confirmed put seq=%s (must be > %s, no reuse)", r2["seq"], r1["seq"])
    assert r2["seq"] > r1["seq"]
    seqs = _wal_seqs(data)
    LOG.info("on-disk seqs after failure: %s", seqs)
    assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))
    store.close()

    reopened = KVStore(data, clock_ms=lambda: 1000)
    LOG.info("restarted after failed write: recovery=%s", reopened.last_recovery)
    assert reopened.get("ns", "a")["value"] == 1
    assert reopened.get("ns", "b")["value"] == 2
    r3 = reopened.put("ns", "c", 3)
    assert r3["seq"] > r2["seq"]
    reopened.close()


def test_failed_batch_then_write_and_restart(tmp_path):
    data = str(tmp_path / "d")
    store = KVStore(data, clock_ms=lambda: 1000)
    store.put("ns", "a", 1)
    LOG.info("failing a batch whose 2nd mutation carries an unserializable value")
    with pytest.raises(TypeError):
        store.batch([
            {"op": "put", "namespace": "ns", "key": "x", "value": 1},
            {"op": "put", "namespace": "ns", "key": "y", "value": object()},
        ])
    LOG.info("batch failed; dangling BEGIN must be truncated without seq reuse")
    r = store.put("ns", "b", 2)
    seqs = _wal_seqs(data)
    LOG.info("on-disk seqs after failed batch: %s (next put seq=%s)", seqs, r["seq"])
    assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))
    store.close()

    reopened = KVStore(data, clock_ms=lambda: 1000)
    LOG.info("restarted after failed batch: recovery=%s", reopened.last_recovery)
    assert reopened.last_recovery["truncated"] is None
    assert reopened.get("ns", "a")["value"] == 1
    assert reopened.get("ns", "b")["value"] == 2
    with pytest.raises(Exception) as exc:
        reopened.get("ns", "x")
    assert exc.value.code == "not_found"
    reopened.close()
