"""Exclusive advisory lock guarding a data directory against multiple writers.

The lock is a ``flock(LOCK_EX | LOCK_NB)`` on ``<data_dir>/kvstore.lock``.
A second writer *process* opening the same directory fails immediately with
:class:`LockHeld` instead of silently interleaving WAL appends. If the
holder is killed (even ``kill -9``) the OS releases the lock, so the next
process can start without any manual cleanup.

The exclusion is per process: within one process a registry refcounts the
held lock, so reopening a directory the process already owns (e.g. an
in-process restart) is allowed. Read-only stores do not take the lock at
all.
"""

from __future__ import annotations

import fcntl
import os
import threading

from .errors import LockHeld

LOCK_NAME = "kvstore.lock"


class _OsLock:
    """One held flock plus a reference count."""

    def __init__(self, path: str):
        self.refs = 1
        self._fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self._fd)
            self._fd = -1
            raise LockHeld(
                f"data directory {os.path.dirname(path)!r} is already opened "
                "by another writer process"
            ) from exc
        # Record the holder for diagnostics (best effort, not load-bearing).
        try:
            os.ftruncate(self._fd, 0)
            os.write(self._fd, str(os.getpid()).encode())
        except OSError:
            pass

    def release(self) -> None:
        if self._fd >= 0:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = -1


_REGISTRY: dict[str, _OsLock] = {}
_REGISTRY_LOCK = threading.Lock()


class DirLock:
    """Held for the lifetime of a writer :class:`KVStore`."""

    def __init__(self, data_dir: str):
        os.makedirs(data_dir, exist_ok=True)
        self._key = os.path.join(os.path.abspath(data_dir), LOCK_NAME)
        with _REGISTRY_LOCK:
            holder = _REGISTRY.get(self._key)
            if holder is None:
                holder = _OsLock(self._key)  # raises LockHeld if another process owns it
                _REGISTRY[self._key] = holder
            else:
                holder.refs += 1  # same process already owns the directory
        self._holder: _OsLock | None = holder

    def release(self) -> None:
        with _REGISTRY_LOCK:
            holder = getattr(self, "_holder", None)
            if holder is None:
                return
            self._holder = None
            holder.refs -= 1
            if holder.refs == 0:
                _REGISTRY.pop(self._key, None)
                holder.release()

    def __del__(self):  # pragma: no cover - safety net
        self.release()
