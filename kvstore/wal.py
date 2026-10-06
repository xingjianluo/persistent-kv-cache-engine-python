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

from .errors import CorruptLogError, StoreReadOnly, TruncatedFrame
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
_OP_VALUES = {op.value for op in OpCode}


def encode_frame(op: OpCode, payload_dict: dict, seq: int) -> bytes:
    """Serialize one frame (header + JSON payload) exactly as appended live."""
    payload = json.dumps(payload_dict, ensure_ascii=False, separators=(",", ":"))
    data = payload.encode("utf-8")
    header = HEADER_STRUCT.pack(FRAME_MAGIC, len(data), zlib.crc32(data), seq, op.value)
    return header + data


def _valid_frame_follows(blob: bytes, start: int, min_seq: int) -> bool:
    """Look for a well-formed frame at/after ``start``.

    A frame with valid magic, in-bounds length, matching CRC and a plausible
    sequence/op is proof that confirmed records live past this point, so an
    incomplete frame here is mid-log corruption — not a crash-time torn
    tail that may be truncated away.
    """
    total = len(blob)
    pos = start
    while True:
        idx = blob.find(FRAME_MAGIC, pos)
        if idx < 0:
            return False
        if idx + HEADER_SIZE <= total:
            _magic, plen, crc, seq, op_byte = HEADER_STRUCT.unpack_from(blob, idx)
            end = idx + HEADER_SIZE + plen
            if (
                plen <= _MAX_PAYLOAD
                and end <= total
                and seq >= min_seq
                and op_byte in _OP_VALUES
                and zlib.crc32(blob[idx + HEADER_SIZE : end]) == crc
            ):
                return True
        pos = idx + 1


class WriteAheadLog:
    """File-backed ordered log of confirmed mutations."""

    def __init__(self, path: str, read_only: bool = False):
        self.path = path
        self.read_only = read_only
        directory = os.path.dirname(os.path.abspath(path))
        self._next_seq = 1
        if read_only:
            # A read-only store never creates or modifies the log file.
            self._file = None
        else:
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
            return os.path.getsize(self.path) if os.path.exists(self.path) else 0
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
        if self._file is None:
            raise StoreReadOnly("cannot append to a read-only WAL")
        seq = self._next_seq
        self._file.write(encode_frame(op, payload_dict, seq))
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
        if os.path.exists(self.path):
            with open(self.path, "rb") as f:
                blob = f.read()
        else:  # read-only store over a not-yet-created log
            blob = b""

        frames: list[Frame] = []
        pos = 0
        total = len(blob)
        expected_seq = 1

        while pos < total:
            frame_start = pos
            if total - pos < HEADER_SIZE:
                if _valid_frame_follows(blob, frame_start, expected_seq):
                    raise CorruptLogError(
                        "truncated_mid_log",
                        frame_start,
                        "incomplete header but valid frames follow; "
                        "refusing to drop confirmed records",
                    )
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
            if plen > _MAX_PAYLOAD:
                raise CorruptLogError(
                    "bad_length", frame_start, f"payload length {plen} exceeds limit"
                )
            pos += HEADER_SIZE
            if total - pos < plen:
                if _valid_frame_follows(blob, frame_start, expected_seq):
                    raise CorruptLogError(
                        "truncated_mid_log",
                        frame_start,
                        "incomplete payload but valid frames follow; "
                        "refusing to drop confirmed records",
                    )
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

            if op is OpCode.META:
                # Generation header written by compaction: carries the first
                # sequence number of this log generation. Only valid as the
                # very first frame, with seq 0; old logs have no META frame
                # and implicitly start at seq 1.
                if frame_start != 0 or seq != 0:
                    raise CorruptLogError(
                        "bad_payload", frame_start,
                        "META frame is only valid at offset 0 with seq 0",
                    )
                base = obj.get("base_seq") if isinstance(obj, dict) else None
                if not isinstance(base, int) or isinstance(base, bool) or base < 1:
                    raise CorruptLogError(
                        "bad_payload", frame_start,
                        "META frame requires integer base_seq >= 1",
                    )
                expected_seq = base
                continue

            if seq != expected_seq:
                raise CorruptLogError(
                    "bad_sequence",
                    frame_start,
                    f"expected seq {expected_seq}, got {seq}",
                )
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

        The next sequence is recomputed from the surviving frames so a
        failed write never causes sequence numbers of *confirmed* frames
        to be reused (which a later recovery would reject as corruption).
        """
        if self._file is None:
            raise StoreReadOnly("cannot truncate a read-only WAL")
        self._file.close()
        fd = os.open(self.path, os.O_RDWR)
        try:
            os.ftruncate(fd, offset)
            os.fsync(fd)
        finally:
            os.close(fd)
        self._file = open(self.path, "ab+")
        self._resync_next_seq()

    def _resync_next_seq(self) -> None:
        """Set ``_next_seq`` just past the last complete frame on disk."""
        with open(self.path, "rb") as f:
            blob = f.read()
        pos = 0
        total = len(blob)
        expected = 1
        while pos + HEADER_SIZE <= total:
            magic, plen, _crc, seq, op_byte = HEADER_STRUCT.unpack_from(blob, pos)
            if magic != FRAME_MAGIC or pos + HEADER_SIZE + plen > total:
                break
            if pos == 0 and op_byte == OpCode.META.value:
                try:
                    base = json.loads(
                        blob[HEADER_SIZE : HEADER_SIZE + plen].decode("utf-8")
                    ).get("base_seq", 1)
                    if isinstance(base, int) and not isinstance(base, bool):
                        expected = max(expected, base)
                except (ValueError, AttributeError):
                    pass
            else:
                expected = max(expected, seq + 1)
            pos += HEADER_SIZE + plen
        self._next_seq = expected

    def close(self) -> None:
        if self._file is not None and not self._file.closed:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
