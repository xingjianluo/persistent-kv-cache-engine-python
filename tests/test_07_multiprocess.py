"""Single-writer rule: one data directory admits exactly one writer process
at a time. A second writer is rejected with a recognizable error; a holder
killed -9 never leaves the directory locked; read-only access and health
checks are unaffected by the lock."""

from __future__ import annotations

import os
import signal
import subprocess
import sys

import pytest

from kvstore.errors import DataDirLocked, StoreReadOnly
from kvstore.lockfile import LOCK_NAME
from kvstore.store import KVStore

from conftest import get_logger

LOG = get_logger("test.multiprocess")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_HOLDER_SCRIPT = """
import sys, time
from kvstore.store import KVStore
s = KVStore(sys.argv[1])
s.put("ns", "holder", os.getpid() if False else 1)
print("READY", flush=True)
time.sleep(60)
"""


def _start_holder(data_dir: str) -> subprocess.Popen:
    env = dict(os.environ, PYTHONPATH=REPO_ROOT)
    proc = subprocess.Popen(
        [sys.executable, "-c", _HOLDER_SCRIPT, data_dir],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
    )
    assert proc.stdout.readline().strip() == "READY", (
        f"holder failed to start: {proc.stderr.read()}"
    )
    return proc


def test_same_process_handoff_and_reopen_after_close(tmp_path):
    """Inside one process a re-open hands ownership to the new store (this
    is how restart-without-process-exit works); the flock's exclusion is a
    *cross-process* guarantee, covered by the subprocess tests below."""
    data = str(tmp_path / "d")
    s = KVStore(data)
    s.put("ns", "k", 1)
    s2 = KVStore(data)  # same-process handoff: s is usurped
    with pytest.raises(DataDirLocked):
        s.put("ns", "k", 2)
    LOG.info("usurped predecessor write -> data_dir_locked")
    assert s2.get("ns", "k")["value"] == 1
    s.close()
    s2.close()
    # after a clean close the directory is openable again
    s3 = KVStore(data)
    assert s3.get("ns", "k")["value"] == 1
    s3.close()
    LOG.info("basis: handoff while held, free after release")


def test_cross_process_contention_and_kill9_recovery(tmp_path):
    data = str(tmp_path / "d")
    holder = _start_holder(data)
    try:
        with pytest.raises(DataDirLocked) as ei:
            KVStore(data)
        LOG.info("while holder pid=%d lives: code=%s (%s)",
                 holder.pid, ei.value.code, ei.value)
        assert ei.value.code == "data_dir_locked"

        # read-only access is NOT blocked by the writer lock
        ro = KVStore(data, read_only=True)
        assert ro.get("ns", "holder")["value"] == 1
        ro.close()
        LOG.info("read-only open + read succeeded while writer lock held")

        # kill -9: no cleanup, lock file left behind
        holder.kill()
        holder.wait()
        assert holder.returncode == -signal.SIGKILL
        assert os.path.exists(os.path.join(data, LOCK_NAME))
        LOG.info("holder killed -9; stale lock file remains on disk")

        # the kernel released the flock with the process: new writer starts
        s = KVStore(data)
        assert s.get("ns", "holder")["value"] == 1
        s.put("ns", "after_kill", "ok")
        s.close()
        LOG.info("basis: stale lock never blocks a new process after kill -9")
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.wait()


def test_read_only_store_rejects_writes_but_serves_reads(tmp_path):
    data = str(tmp_path / "d")
    s = KVStore(data)
    s.put("ns", "k", {"v": 1})
    # writer stays open; a read-only peer coexists (health-check style)
    ro = KVStore(data, read_only=True)
    assert ro.get("ns", "k")["value"] == {"v": 1}
    for op in (
        lambda: ro.put("ns", "x", 1),
        lambda: ro.delete("ns", "k"),
        lambda: ro.batch([{"op": "put", "namespace": "ns", "key": "y",
                           "value": 1}]),
        lambda: ro.compact(),
    ):
        with pytest.raises(StoreReadOnly) as ei:
            op()
        assert ei.value.code == "read_only"
    LOG.info("read-only: get ok; put/delete/batch/compact -> read_only")
    # a read-only open must not create or modify anything in the directory
    empty = str(tmp_path / "empty")
    ro2 = KVStore(empty, read_only=True)
    assert not os.path.exists(empty), "read-only open creates no directory"
    with pytest.raises(Exception) as ei:
        ro2.get("ns", "k")
    assert ei.value.code == "not_found"
    ro.close()
    ro2.close()
    s.close()
    LOG.info("basis: read-only is side-effect free; health checks unaffected")


def test_usurped_same_process_store_cannot_write(tmp_path):
    """Same-process re-open (restart without process exit) hands the lock
    over; the abandoned store's writes then fail loudly instead of
    corrupting the log."""
    data = str(tmp_path / "d")
    s1 = KVStore(data)
    s1.put("ns", "a", 1)
    s2 = KVStore(data)  # takes over the directory
    with pytest.raises(DataDirLocked):
        s1.put("ns", "b", 2)
    LOG.info("usurped store write -> data_dir_locked; new owner unaffected")
    s2.put("ns", "b", 2)
    assert s2.get("ns", "b")["value"] == 2
    s1.close()  # closing the usurped store must not release s2's lock
    assert s2.get("ns", "a")["value"] == 1
    s2.close()
