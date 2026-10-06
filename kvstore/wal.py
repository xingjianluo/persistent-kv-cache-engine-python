"""Append-only write-ahead log with CRC-protected, framed JSON records.

Durability rule
---------------
``append_*`` opens the file once, writes one frame, flushes the Python
buffer and calls ``os.fsync`` before returning. A frame that did not reach
disk before a crash simply does not exist: on recovery it shows up as a
torn suffix (or a transaction missing COMMIT) and is truncated. A complete
frame with a bad CRC/magic/sequence is genuine corruption and raises
:class:`CorruptLogError`.
"""

from __future__ import annotations

import json
import os
import zlib

from .errors import CorruptLogError, TruncatedFrame
from .records import (
    HEADER_SIZE,
    HEADER_STRUCT,
    FRAME_MAGIC,
    DeleteRecord,
    Frame,
    OpCode,
    PutRecord,
)

_MAX_PAYLOAD = 64 * 1024 * 1024  # sanity bound: a single 64 MiB frame max


class WriteAheadLog:
    """File-backed ordered log of confirmed mutations."""

    def __init__(self, path: str, read_only: bool = False):
        self.path = path
        self.read_only = read_only
        self._next_seq = 1
        if read_only:
            # A reader must not create or modify anything: no directory, no
            # file, and certainly no truncation during recovery.
            self._file = open(path, "rb") if os.path.exists(path) else None
            return
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._file = open(path, "ab+")
        # Make sure the directory entry of a brand-new WAL is durable too.
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    @property
    def size(self) -> int:
        if self._file is None:
            return 0
        return self._file.tell()

    # ------------------------------------------------------------------ #
    # Writing
    # ------------------------------------------------------------------ #
    def append_put(self, record: PutRecord) -> int:
        return self._append(
            OpCode.PUT,
            {
                "namespace": record.namespace,
                "key": record.key,
                "value": record.value,
                "version": record.version,
                "expires_at_ms": record.expires_at_ms,
            },
        )

    def append_delete(self, record: DeleteRecord) -> int:
        return self._append(
            OpCode.DELETE,
            {
                "namespace": record.namespace,
                "key": record.key,
                "version": record.version,
            },
        )

    def append_begin(self) -> int:
        return self._append(OpCode.BEGIN, {})

    def append_commit(self) -> int:
        return self._append(OpCode.COMMIT, {})

    def _append(self, op: OpCode, payload_dict: dict) -> int:
        if self.read_only or self._file is None:  # pragma: no cover - guarded by store
            raise OSError("WAL opened read-only")
        payload = json.dumps(payload_dict, ensure_ascii=False, separators=(",", ":"))
        data = payload.encode("utf-8")
        seq = self._next_seq
        header = HEADER_STRUCT.pack(FRAME_MAGIC, len(data), zlib.crc32(data), seq, op.value)
        self._file.write(header + data)
        self._file.flush()
        os.fsync(self._file.fileno())
        self._next_seq += 1
        return seq

    # ------------------------------------------------------------------ #
    # Recovery
    # ------------------------------------------------------------------ #
    def recover(self) -> tuple[list[Frame], TruncatedFrame | None]:
        """Validate and replay every frame.

        Returns ``(frames, truncation)``. On a torn final frame, returns the
        complete frames plus a :class:`TruncatedFrame` describing the
        unconfirmed suffix. Any other structural/checksum failure raises
        :class:`CorruptLogError` and changes nothing. On success the next
        frame sequence is advanced past the last frame on disk.
        """
        if not os.path.exists(self.path):
            return [], None
        with open(self.path, "rb") as f:
            blob = f.read()

        frames: list[Frame] = []
        pos = 0
        total = len(blob)
        expected_seq = 1

        while pos < total:
            frame_start = pos
            if total - pos < HEADER_SIZE:
                return frames, TruncatedFrame(
                    frame_start,
                    f"incomplete header ({total - pos} of {HEADER_SIZE} bytes)",
                    frames,
                )

            magic, plen, crc, seq, op_byte = HEADER_STRUCT.unpack_from(blob, pos)
            if magic != FRAME_MAGIC:
                raise CorruptLogError(
                    "bad_magic", frame_start, f"expected {FRAME_MAGIC!r}, got {magic!r}"
                )
            if seq != expected_seq:
                raise CorruptLogError(
                    "bad_sequence",
                    frame_start,
                    f"expected seq {expected_seq}, got {seq}",
                )
            if plen > _MAX_PAYLOAD:
                raise CorruptLogError(
                    "bad_length", frame_start, f"payload length {plen} exceeds limit"
                )
            pos += HEADER_SIZE
            if total - pos < plen:
                return frames, TruncatedFrame(
                    frame_start,
                    f"incomplete payload ({total - pos} of {plen} bytes)",
                    frames,
                )

            payload = blob[pos : pos + plen]
            pos += plen
            if zlib.crc32(payload) != crc:
                raise CorruptLogError(
                    "bad_crc", frame_start, "frame payload checksum mismatch"
                )

            try:
                obj = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CorruptLogError(
                    "bad_payload", frame_start, f"undecodable JSON: {exc}"
                ) from exc

            try:
                op = OpCode(op_byte)
            except ValueError as exc:
                raise CorruptLogError(
                    "bad_payload", frame_start, f"unknown op byte {op_byte}"
                ) from exc

            record = self._record_for(op, obj, frame_start)
            frames.append(Frame(seq=seq, op=op, record=record, offset=frame_start))
            expected_seq += 1

        self._next_seq = expected_seq
        return frames, None

    @staticmethod
    def _record_for(op: OpCode, obj: object, frame_start: int):
        if op in (OpCode.BEGIN, OpCode.COMMIT):
            if obj != {}:
                raise CorruptLogError(
                    "bad_payload", frame_start, f"{op.name} frame must carry '{{}}'"
                )
            return None

        if not isinstance(obj, dict):
            raise CorruptLogError("bad_payload", frame_start, "payload is not an object")
        ns = obj.get("namespace")
        key = obj.get("key")
        version = obj.get("version")
        if not isinstance(ns, str) or not isinstance(key, str):
            raise CorruptLogError("bad_payload", frame_start, "namespace/key must be strings")
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            raise CorruptLogError("bad_payload", frame_start, "version must be int >= 1")

        if op is OpCode.PUT:
            if "value" not in obj:
                raise CorruptLogError("bad_payload", frame_start, "PUT missing 'value'")
            expires = obj.get("expires_at_ms")
            if expires is not None and (not isinstance(expires, int) or isinstance(expires, bool)):
                raise CorruptLogError(
                    "bad_payload", frame_start, "expires_at_ms must be int or null"
                )
            return PutRecord(ns, key, obj["value"], version, expires)

        if op is OpCode.DELETE:
            return DeleteRecord(ns, key, version)

        raise CorruptLogError("bad_payload", frame_start, f"unhandled op {op!r}")

    # ------------------------------------------------------------------ #
    # Maintenance
    # ------------------------------------------------------------------ #
    def truncate_from(self, offset: int) -> None:
        """Drop every byte at/after ``offset`` and fsync the new EOF.

        ``_next_seq`` is recomputed from the surviving prefix (last frame's
        seq + 1), never reset blindly: frames still on disk keep their
        numbers, so later appends stay continuous and a subsequent recovery
        sees no sequence gap.
        """
        if self.read_only:  # pragma: no cover - guarded by store
            raise OSError("WAL opened read-only")
        self._file.close()
        fd = os.open(self.path, os.O_RDWR)
        try:
            os.ftruncate(fd, offset)
            os.fsync(fd)
        finally:
            os.close(fd)
        self._file = open(self.path, "ab+")
        self._next_seq = self._scan_next_seq()

    def _scan_next_seq(self) -> int:
        """Header-only walk of the file: seq after the last complete frame."""
        next_seq = 1
        with open(self.path, "rb") as f:
            while True:
                header = f.read(HEADER_SIZE)
                if len(header) < HEADER_SIZE:
                    break
                _, plen, _, seq, _ = HEADER_STRUCT.unpack(header)
                if plen > _MAX_PAYLOAD:
                    break
                if len(f.read(plen)) < plen:
                    break
                next_seq = seq + 1
        return next_seq

    def close(self) -> None:
        if self._file is not None and not self._file.closed:
            if not self.read_only:
                self._file.flush()
                os.fsync(self._file.fileno())
            self._file.close()
