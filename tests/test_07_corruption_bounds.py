"""Tightened corruption boundary: only damage that provably sits at the very
end of the log (crash-time unwritten residue) is auto-truncated and reported;
an incomplete frame with valid confirmed frames after it is mid-log
corruption and must raise a locatable CorruptLogError without touching the
file or the confirmed state."""

from __future__ import annotations

import os

import pytest

from kvstore.errors import CorruptLogError
from kvstore.records import HEADER_SIZE, HEADER_STRUCT
from kvstore.store import KVStore

from conftest import get_logger

LOG = get_logger("test.corruption_bounds")


def _seed(data: str) -> list[int]:
    """Write 3 confirmed puts; return the byte offset of each frame."""
    store = KVStore(data, clock_ms=lambda: 1000)
    offsets, pos = [], 0
    for i in range(3):
        store.put("ns", f"k{i}", i)
    store.close()
    with open(f"{data}/wal.log", "rb") as f:
        blob = f.read()
    while pos < len(blob):
        offsets.append(pos)
        _magic, plen, _crc, _seq, _op = HEADER_STRUCT.unpack_from(blob, pos)
        pos += HEADER_SIZE + plen
    LOG.info("seeded 3 frames at offsets %s, file size=%d", offsets, len(blob))
    return offsets


def test_torn_tail_is_truncated_and_reported(tmp_path):
    data = str(tmp_path / "d")
    _seed(data)
    path = f"{data}/wal.log"
    size = os.path.getsize(path)
    # Crash-mid-write residue: half of a 4th frame's header appended.
    with open(path, "ab") as f:
        f.write(b"PKV1\x10\x00")
    LOG.info("appended %d torn bytes at offset %d (tail)", 6, size)

    store = KVStore(data, clock_ms=lambda: 1000)
    rep = store.last_recovery
    LOG.info("recovery report: %s", rep)
    assert rep["truncated"] is not None
    assert rep["truncated"]["offset"] == size
    assert store.get("ns", "k2")["value"] == 2
    store.close()
    assert os.path.getsize(path) == size, "torn suffix must be truncated away"
    LOG.info("basis: tail residue auto-cleaned, confirmed frames intact")


def test_mid_log_torn_frame_with_valid_frames_after_is_corruption(tmp_path):
    data = str(tmp_path / "d")
    offsets = _seed(data)
    path = f"{data}/wal.log"
    size_before = os.path.getsize(path)
    # Corrupt the *middle* frame's length field so it looks incomplete,
    # while a perfectly valid confirmed frame (k2) still follows it.
    with open(path, "r+b") as f:
        f.seek(offsets[1] + 4)  # payload_len field of frame #2
        f.write((10_000_000).to_bytes(4, "little"))
    LOG.info("corrupted frame#2 length at offset %d; frame#3 stays valid", offsets[1] + 4)

    with pytest.raises(CorruptLogError) as exc:
        KVStore(data, clock_ms=lambda: 1000)
    e = exc.value
    LOG.info("raised code=%s reason=%s position=%d", e.code, e.reason, e.position)
    assert e.code == "corrupt_log"
    assert e.reason == "truncated_mid_log"
    assert e.position == offsets[1]
    assert os.path.getsize(path) == size_before, "corrupt log must not be modified"
    LOG.info("basis: mid-log damage reported, file untouched, no silent data loss")


def test_mid_log_bad_crc_still_raises_and_preserves_file(tmp_path):
    data = str(tmp_path / "d")
    offsets = _seed(data)
    path = f"{data}/wal.log"
    size_before = os.path.getsize(path)
    with open(path, "r+b") as f:
        f.seek(offsets[1] + HEADER_SIZE + 1)  # inside frame #2 payload
        f.write(b"\xff\xff")
    with pytest.raises(CorruptLogError) as exc:
        KVStore(data, clock_ms=lambda: 1000)
    LOG.info("bad crc mid-log -> reason=%s position=%d", exc.value.reason, exc.value.position)
    assert exc.value.reason == "bad_crc"
    assert exc.value.position == offsets[1]
    assert os.path.getsize(path) == size_before
