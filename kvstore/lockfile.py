"""Single-writer advisory lock for a data directory.

Rule
----
At most one *writer* process may open a data directory at a time. The lock
is an ``fcntl.flock(LOCK_EX | LOCK_NB)`` on ``<data_dir>/writer.lock``:

* a second writer opening the same directory fails immediately with
  :class:`DataDirLocked` instead of two processes corrupting one WAL;
* the lock is held by the kernel on the open file description, so when the
  holder is killed (-9) the lock is released automatically — a leftover
  lock *file* never blocks a new process;
* read-only opens and health checks do not take the lock at all.
"""

from __future__ import annotations

import errno
import fcntl
import os
import threading

from .errors import DataDirLocked

LOCK_NAME = "writer.lock"

# Process-local registry: data dir abspath -> the lock currently owning it.
# The flock excludes *other processes*; this registry handles a same-process
# re-open (e.g. a restarted store object whose predecessor was abandoned
# without close()). The predecessor's lock is released and flagged usurped
# so its writes fail loudly instead of corrupting the log.
_OPEN: dict[str, ProcessLock] = {}
_OPEN_LOCK = threading.RLock()


class ProcessLock:
    """Held for the lifetime of a writer :class:`KVStore`."""

    def __init__(self, data_dir: str):
        self.data_dir = str(data_dir)
        self.path = os.path.join(self.data_dir, LOCK_NAME)
        self._fd: int | None = None
        self.usurped = False

    def acquire(self) -> None:
        """Take the exclusive lock or raise :class:`DataDirLocked`.

        The lock file is created if missing (its directory entry is fsync'd
        once so the file itself survives a crash); the flock itself is what
        excludes other writers, never the mere presence of the file.
        """
        if self._fd is not None:
            return
        key = os.path.abspath(self.data_dir)
        with _OPEN_LOCK:
            prev = _OPEN.get(key)
            if prev is not None and prev is not self:
                # Same-process handoff: the abandoned predecessor must not
                # write through its (still open) WAL ever again. Releasing
                # its flock first is what lets our own flock below succeed.
                prev.usurped = True
                prev.release()
        os.makedirs(self.data_dir, exist_ok=True)
        created = not os.path.exists(self.path)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                holder = self._read_holder()
                raise DataDirLocked(
                    f"data directory {self.data_dir!r} is already opened for "
                    f"writing by another process{holder}"
                ) from exc
            raise
        if created:
            dir_fd = os.open(self.data_dir, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        self._fd = fd
        self._write_holder()
        with _OPEN_LOCK:
            _OPEN[key] = self

    def release(self) -> None:
        """Release the lock (idempotent). The lock file itself is kept."""
        key = os.path.abspath(self.data_dir)
        with _OPEN_LOCK:
            if _OPEN.get(key) is self:
                del _OPEN[key]
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None

    @property
    def held(self) -> bool:
        return self._fd is not None

    # ------------------------------------------------------------------ #
    # Diagnostics only: the recorded pid is informational, never consulted
    # for the locking decision (the kernel flock is the source of truth).
    # ------------------------------------------------------------------ #
    def _write_holder(self) -> None:
        try:
            os.ftruncate(self._fd, 0)
            os.write(self._fd, f"pid={os.getpid()}\n".encode())
        except OSError:
            pass

    def _read_holder(self) -> str:
        try:
            with open(self.path, "rb") as f:
                note = f.read(64).decode(errors="replace").strip()
            return f" ({note})" if note else ""
        except OSError:
            return ""
