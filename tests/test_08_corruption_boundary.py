"""Corruption boundary: only damage that is provably crash residue at the
very tail of the log (an incomplete final frame / a transaction without
COMMIT) is auto-cleaned and reported. Damage in the middle — with intact
confirmed frames after it — is reported as corruption: identifiable error,
untouched file, confirmed data never silently dropped."""

from __future__ import annotations

import os

import pytest

from kvstore.errors import CorruptLogError
from kvstore.records import FRAME_MAGIC, HEADER_SIZE, HEADER_STRUCT, OpCode
from kvstore.store import KVStore, WAL_NAME

from conftest import get_logger

LOG = get_logger("test.corruption_boundary")


def _seed(data: str, n: int = 3) -> str:
    """Write n confirmed single puts; return the wal path."""
    s = KVStore(data, clock_ms=lambda: 1000)
    for i in range(n):
        s.put("ns", f"k{i}", i)
    s.close()
    return os.path.join(data, WAL_NAME)


def _frame_spans(path: str) -> list[tuple[int, int]]:
    spans, pos = [], 0
    blob = open(path, "rb").read()
    while pos < len(blob):
        _, plen, _, _, _ = HEADER_STRUCT.unpack_from(blob, pos)
        spans.append((pos, HEADER_SIZE + plen))
        pos += HEADER_SIZE + plen
    return spans


def test_mid_log_damage_with_valid_tail_is_fatal_and_non_destructive(tmp_path):
    """Corrupt the *first* of three frames: two intact confirmed frames
    follow the damage, so this can never be mistaken for crash residue."""
    data = str(tmp_path / "d")
    path = _seed(data, 3)
    spans = _frame_spans(path)
    before = open(path, "rb").read()
    # flip a byte inside frame #1's payload (frame #2 and #3 stay intact)
    with open(path, "r+b") as f:
        f.seek(spans[0][0] + HEADER_SIZE + 2)
        f.write(b"\xff")
    LOG.info("damaged frame 1 of %d; frames after it are intact", len(spans))

    with pytest.raises(CorruptLogError) as ei:
        KVStore(data, clock_ms=lambda: 1000)
    e = ei.value
    LOG.info("mid-log damage -> code=%s reason=%s position=%d",
             e.code, e.reason, e.position)
    assert e.code == "corrupt_log" and e.reason == "bad_crc"
    assert e.position == spans[0][0], "error must locate the damage"
    assert open(path, "rb").read() != before[:0]  # sanity
    assert open(path, "rb").read()[: spans[0][0]] == before[: spans[0][0]]
    assert os.path.getsize(path) == len(before), "file must not be modified"
    LOG.info("basis: confirmed frames after the damage are never silently "
             "dropped; the file is left for manual repair")


def test_complete_but_corrupt_final_frame_is_not_treated_as_residue(tmp_path):
    """A *complete-length* final frame with a bad CRC is not 'crash didn't
    finish writing' — it is corruption and must raise, not be truncated."""
    data = str(tmp_path / "d")
    path = _seed(data, 2)
    size = os.path.getsize(path)
    with open(path, "r+b") as f:
        f.seek(size - 1)  # last payload byte of the final frame
        f.write(b"\x00")
    with pytest.raises(CorruptLogError) as ei:
        KVStore(data, clock_ms=lambda: 1000)
    LOG.info("corrupt-but-complete tail -> reason=%s position=%d",
             ei.value.reason, ei.value.position)
    assert ei.value.reason == "bad_crc"
    assert os.path.getsize(path) == size, "no auto-truncation of real damage"


def test_tail_residue_is_auto_cleaned_with_reason(tmp_path):
    """The mirror case: an incomplete final frame *is* crash residue and is
    truncated, fsync'd, and reported with its reason."""
    data = str(tmp_path / "d")
    path = _seed(data, 2)
    good = os.path.getsize(path)
    with open(path, "r+b") as f:
        f.truncate(good - 4)  # tear the final frame
    s = KVStore(data, clock_ms=lambda: 1000)
    rep = s.last_recovery["truncated"]
    LOG.info("torn tail report: %s", rep)
    assert rep["reason"] == "truncated_frame" and rep["repaired"] is True
    assert "incomplete" in rep["detail"]
    assert s.get("ns", "k0")["value"] == 0
    s.close()
    s = KVStore(data, clock_ms=lambda: 1000)
    assert s.last_recovery["truncated"] is None
    LOG.info("basis: residue auto-cleaned once; next startup is clean")
    s.close()


def test_read_only_recovery_reports_but_never_modifies(tmp_path):
    """A read-only open (health-check peer) must report the torn tail
    without repairing it: the file belongs to the writer."""
    data = str(tmp_path / "d")
    path = _seed(data, 2)
    good = os.path.getsize(path)
    with open(path, "r+b") as f:
        f.truncate(good - 4)
    torn = open(path, "rb").read()
    ro = KVStore(data, read_only=True, clock_ms=lambda: 1000)
    rep = ro.last_recovery["truncated"]
    LOG.info("read-only recovery report: %s", rep)
    assert rep["reason"] == "truncated_frame" and rep["repaired"] is False
    assert open(path, "rb").read() == torn, "reader must not change the file"
    assert ro.get("ns", "k0")["value"] == 0  # confirmed prefix still served
    ro.close()
    # the writer then repairs it on its own startup
    s = KVStore(data, clock_ms=lambda: 1000)
    assert s.last_recovery["truncated"]["repaired"] is True
    s.close()
    LOG.info("basis: read-only observes; only the writer repairs")


def test_junk_after_valid_frames_is_distinguished(tmp_path):
    """Appended garbage that forms neither a complete frame nor a valid
    header: incomplete-header residue at the tail is cleaned; garbage that
    parses as a full-length frame is corruption."""
    data = str(tmp_path / "d")
    path = _seed(data, 1)
    with open(path, "ab") as f:
        f.write(b"\x01\x02\x03")  # 3 bytes: not even a header
    s = KVStore(data, clock_ms=lambda: 1000)
    LOG.info("3 stray bytes -> %s", s.last_recovery["truncated"])
    assert s.last_recovery["truncated"]["reason"] == "truncated_frame"
    s.close()

    # now append a full-header-sized lie: magic ok, length plausible, crc bad
    data2 = str(tmp_path / "d2")
    path2 = _seed(data2, 1)
    payload = b'{"namespace":"ns"}'
    with open(path2, "ab") as f:
        f.write(HEADER_STRUCT.pack(FRAME_MAGIC, len(payload), 0xDEADBEEF, 2,
                                   OpCode.PUT.value) + payload)
    with pytest.raises(CorruptLogError) as ei:
        KVStore(data2, clock_ms=lambda: 1000)
    LOG.info("full-length lying frame -> reason=%s (fatal, not cleaned)",
             ei.value.reason)
    assert ei.value.reason == "bad_crc"
