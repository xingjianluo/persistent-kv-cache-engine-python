"""Corruption handling: genuine damage (bad CRC/magic/sequence/payload in a
complete frame) must raise an identifiable CorruptLogError with byte offset
and reason, and must not be silently skipped. An incomplete final frame is
the single recoverable case (unconfirmed torn suffix)."""

from __future__ import annotations

import json
import os
import zlib

from kvstore.errors import CorruptLogError
from kvstore.records import FRAME_MAGIC, HEADER_SIZE, HEADER_STRUCT, OpCode
from kvstore.store import KVStore, WAL_NAME

from conftest import get_logger

LOG = get_logger("test.corruption")


def _write_frame(path, seq: int, op: OpCode, obj: dict, magic=FRAME_MAGIC,
                 bad_crc=False) -> int:
    data = json.dumps(obj, separators=(",", ":")).encode()
    crc = 0xDEADBEEF if bad_crc else zlib.crc32(data)
    header = HEADER_STRUCT.pack(magic, len(data), crc, seq, op.value)
    with open(path, "ab") as f:
        f.write(header + data)
    return HEADER_SIZE + len(data)


def _seed(tmp_path):
    data = tmp_path / "d"
    store = KVStore(str(data), clock_ms=lambda: 1000)
    store.put("ns", "ok", 1)
    store.put("ns", "ok2", 2)
    store.close()
    return str(data / WAL_NAME)


def test_bad_crc_raises_identifiable_error_and_keeps_file_untouched(tmp_path):
    path = _seed(tmp_path)
    size_before = os.path.getsize(path)
    with open(path, "r+b") as f:
        f.seek(HEADER_SIZE + 10)  # inside first frame payload
        f.write(b"\x00\x00")
    try:
        KVStore(str(tmp_path / "d"), clock_ms=lambda: 1000)
        raise AssertionError("corrupt frame must not be silently skipped")
    except CorruptLogError as e:
        LOG.info("bad CRC -> code=%s reason=%s position=%d message=%s",
                 e.code, e.reason, e.position, e)
        assert e.code == "corrupt_log"
        assert e.reason == "bad_crc"
        assert e.position == 0
    assert os.path.getsize(path) == size_before, "failed recovery mutates nothing"
    LOG.info("basis: complete frame with bad CRC is fatal and non-destructive")


def test_bad_magic_raises(tmp_path):
    path = _seed(tmp_path)
    with open(path, "r+b") as f:
        f.seek(0)
        f.write(b"XXXX")
    try:
        KVStore(str(tmp_path / "d"), clock_ms=lambda: 1000)
        raise AssertionError
    except CorruptLogError as e:
        LOG.info("bad magic -> reason=%s position=%d", e.reason, e.position)
        assert e.reason == "bad_magic" and e.position == 0


def test_sequence_gap_raises(tmp_path):
    data = tmp_path / "d2"
    os.makedirs(data, exist_ok=True)
    path = str(data / WAL_NAME)
    _write_frame(path, 1, OpCode.PUT,
                 {"namespace": "ns", "key": "a", "value": 1,
                  "version": 1, "expires_at_ms": None})
    _write_frame(path, 3, OpCode.PUT,  # gap: expected 2
                 {"namespace": "ns", "key": "b", "value": 2,
                  "version": 2, "expires_at_ms": None})
    try:
        KVStore(data, clock_ms=lambda: 1000)
        raise AssertionError
    except CorruptLogError as e:
        LOG.info("seq gap -> reason=%s position=%d", e.reason, e.position)
        assert e.reason == "bad_sequence"
        assert e.position > 0  # offset of the second frame is reported


def test_invalid_payload_raises(tmp_path):
    data = tmp_path / "d3"
    os.makedirs(data, exist_ok=True)
    path = str(data / WAL_NAME)
    _write_frame(path, 1, OpCode.PUT,
                 {"namespace": "ns", "key": "a", "value": 1,
                  "version": "zero-not-int", "expires_at_ms": None})
    try:
        KVStore(data, clock_ms=lambda: 1000)
        raise AssertionError
    except CorruptLogError as e:
        LOG.info("semantically bad payload -> reason=%s position=%d",
                 e.reason, e.position)
        assert e.reason == "bad_payload" and e.position == 0


def test_unknown_opcode_raises(tmp_path):
    data = tmp_path / "d4"
    os.makedirs(data, exist_ok=True)
    path = str(data / WAL_NAME)
    _write_frame(path, 1, OpCode(99) if False else OpCode.PUT,
                 {"namespace": "ns", "key": "a", "value": 1,
                  "version": 1, "expires_at_ms": None})
    # overwrite op byte (last header byte) with 99
    with open(path, "r+b") as f:
        f.seek(HEADER_SIZE - 1)
        f.write(bytes([99]))
    try:
        KVStore(data, clock_ms=lambda: 1000)
        raise AssertionError
    except CorruptLogError as e:
        LOG.info("unknown op -> reason=%s position=%d", e.reason, e.position)
        assert e.reason == "bad_payload"


def test_incomplete_final_frame_is_the_recoverable_case(tmp_path):
    """A torn trailing frame is reported, truncated, and service resumes;
    frames before it remain confirmed."""
    path = _seed(tmp_path)
    good = os.path.getsize(path)
    with open(path, "r+b") as f:
        f.truncate(good - 2)
    store = KVStore(str(tmp_path / "d"), clock_ms=lambda: 1000)
    rep = store.last_recovery
    LOG.info("torn tail report=%s", rep)
    assert rep["truncated"]["reason"] == "truncated_frame"
    assert store.get("ns", "ok")["value"] == 1
    store.close()
    store2 = KVStore(str(tmp_path / "d"), clock_ms=lambda: 1000)
    assert store2.last_recovery["truncated"] is None
    LOG.info("basis: torn suffix auto-repaired; every other damage is fatal")


def test_constructor_surfaces_corruption(tmp_path):
    path = _seed(tmp_path)
    with open(path, "r+b") as f:
        f.seek(0)
        f.write(FRAME_MAGIC[:3] + b"?")
    try:
        KVStore(str(tmp_path / "d"), clock_ms=lambda: 1000)
        raise AssertionError("service must not start on a corrupt log")
    except CorruptLogError as e:
        LOG.info("constructor surfaces corruption: %s", e)
        assert e.code == "corrupt_log" and e.reason == "bad_magic"
