"""WAL compaction: rewrite the log as a minimal snapshot without changing
any observable state.

Rules
-----
* The compacted log preserves, for every key, its value, version, absolute
  ``expires_at_ms`` and deletion flag, so the never-written / deleted /
  expired distinction, version-based CAS and capacity accounting are
  identical before and after. Expired-but-unpurged entries and tombstones
  are carried over verbatim (their *reasons* must survive).
* Readers and writers are not blocked while the snapshot is serialized:
  the snapshot is written to ``wal.log.compact`` outside the store lock;
  only a short final phase (append the delta of mutations confirmed during
  the copy, fsync, atomic ``os.replace``, dir fsync) runs under the lock.
* A crash at any point lands on a consistent state: before the atomic
  replace the old ``wal.log`` is authoritative and a leftover
  ``wal.log.compact`` is discarded on startup; after it, the new log is
  complete (it was fsync'd before the replace).
"""

from __future__ import annotations

import os

from .records import DeleteRecord, PutRecord
from .wal import WriteAheadLog

COMPACT_SUFFIX = ".compact"


def discard_stale_compact(wal_path: str) -> bool:
    """Remove a leftover ``wal.log.compact`` from an interrupted pass.

    Only ever called by the single writer at startup, before recovery: the
    old ``wal.log`` is still authoritative, so the half-written copy is
    unconfirmed garbage. Returns True if a file was removed.
    """
    stale = wal_path + COMPACT_SUFFIX
    if not os.path.exists(stale):
        return False
    os.remove(stale)
    dir_fd = os.open(os.path.dirname(os.path.abspath(wal_path)), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
    return True


class LogCompactor:
    """Runs one compaction pass over a :class:`KVStore`."""

    def __init__(self, store):
        self._store = store

    def compact(self) -> dict:
        store = self._store
        wal_path = store.wal.path
        compact_path = wal_path + COMPACT_SUFFIX

        # Phase 1 (under the store lock): snapshot the index and start
        # mirroring every confirmed mutation into a delta list. The lock is
        # released immediately afterwards — writers are not blocked while
        # the (potentially large) snapshot is serialized in phase 2.
        with store._lock:
            store._assert_writable()
            if store._compaction_delta is not None:
                raise RuntimeError("compaction already in progress")
            snapshot = {ns: dict(t) for ns, t in store._tables.items()}
            entries = sum(len(t) for t in snapshot.values())
            bytes_before = store.wal.size
            store._compaction_delta = delta = []

        tmp: WriteAheadLog | None = None
        try:
            if os.path.exists(compact_path):
                os.remove(compact_path)
            # Phase 2 (no lock): serialize the snapshot. Every frame keeps
            # the entry's version and absolute expiry so CAS preconditions
            # and TTL reasons are unchanged after the swap.
            tmp = WriteAheadLog(compact_path)
            for ns, table in snapshot.items():
                for key, st in table.items():
                    if st.deleted:
                        tmp.append_delete(DeleteRecord(ns, key, st.version))
                    else:
                        tmp.append_put(
                            PutRecord(ns, key, st.value, st.version,
                                      st.expires_at_ms)
                        )

            # Phase 3 (under the store lock): carry the delta of mutations
            # confirmed while phase 2 was running, fsync, then swap
            # atomically. This is the only phase that blocks writers, and
            # it is proportional to the (usually tiny) delta.
            with store._lock:
                for item in delta:
                    if item["op"] == "put":
                        tmp.append_put(
                            PutRecord(item["namespace"], item["key"],
                                      item["value"], item["version"],
                                      item.get("expires_at_ms"))
                        )
                    else:
                        tmp.append_delete(
                            DeleteRecord(item["namespace"], item["key"],
                                         item["version"])
                        )
                next_seq = tmp._next_seq
                tmp.close()
                tmp = None
                # Atomic point of no return. The store's current WAL handle
                # stays valid until this succeeds, so a failed swap leaves
                # a fully functional store on the untouched old log.
                os.replace(compact_path, wal_path)
                dir_fd = os.open(
                    os.path.dirname(os.path.abspath(wal_path)), os.O_RDONLY
                )
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
                store.wal.close()
                store.wal = WriteAheadLog(wal_path)
                store.wal._next_seq = next_seq
                store._compaction_delta = None
                report = {
                    "compacted": True,
                    "entries": entries,
                    "delta_carried": len(delta),
                    "bytes_before": bytes_before,
                    "bytes_after": store.wal.size,
                }
        except BaseException:
            # The old WAL is still authoritative; drop the half-written copy.
            with store._lock:
                store._compaction_delta = None
            if tmp is not None:
                tmp.close()
            if os.path.exists(compact_path):
                os.remove(compact_path)
            raise
        return report
