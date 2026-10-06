"""Multi-instance protection: one writer process per data directory.

A second writer opening the same directory is rejected with an identifiable
``lock_held`` error; after the holder is kill -9'd the lock is released by
the OS and a new writer starts cleanly. Read-only access is unaffected.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from kvstore.errors import LockHeld, StoreReadOnly
from kvstore.store import KVStore

from conftest import get_logger

LOG = get_logger("test.multinstance")

_HOLDER_SRC = """
import sys, time
from kvstore.store import KVStore
store = KVStore(sys.argv[1])
store.put("ns", "from_holder", 1)
print("READY", flush=True)
time.sleep(60)
"""


def _start_holder(data_dir: str) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-c", _HOLDER_SRC, data_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout.readline().strip() == "READY"
    LOG.info("holder process pid=%s opened %s and wrote one key", proc.pid, data_dir)
    return proc


def test_second_writer_rejected_then_starts_after_holder_killed(tmp_path):
    data = str(tmp_path / "d")
    holder = _start_holder(data)
    try:
        with pytest.raises(LockHeld) as exc:
            KVStore(data)
        LOG.info("second writer rejected: code=%s message=%s",
                 exc.value.code, exc.value)
        assert exc.value.code == "lock_held"
    finally:
        holder.kill()  # kill -9: no cleanup, OS must release the lock
        holder.wait()
    LOG.info("holder killed with SIGKILL (rc=%s)", holder.returncode)
    assert holder.returncode != 0

    reopened = KVStore(data)
    LOG.info("new writer acquired the directory after SIGKILL; recovery=%s",
             reopened.last_recovery)
    assert reopened.get("ns", "from_holder")["value"] == 1
    reopened.close()
    LOG.info("basis: stale lock state never blocks a fresh writer")


def test_read_only_open_bypasses_lock_and_cannot_write(tmp_path):
    data = str(tmp_path / "d")
    holder = _start_holder(data)
    try:
        reader = KVStore(data, read_only=True)
        LOG.info("read-only store opened while writer holds the lock")
        assert reader.get("ns", "from_holder")["value"] == 1
        with pytest.raises(StoreReadOnly) as exc:
            reader.put("ns", "nope", 1)
        LOG.info("write on read-only store rejected: code=%s", exc.value.code)
        assert exc.value.code == "read_only"
        reader.close()
    finally:
        holder.kill()
        holder.wait()
    LOG.info("basis: read-only access unaffected by the single-writer lock")


def test_lock_released_on_close_allows_reopen(tmp_path):
    data = str(tmp_path / "d")
    first = KVStore(data)
    first.put("ns", "a", 1)
    first.close()
    LOG.info("first store closed; lock must be gone for the next open")
    second = KVStore(data)
    assert second.get("ns", "a")["value"] == 1
    second.close()
