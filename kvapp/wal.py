"""WAL/快照帧编解码与崩溃恢复。

帧布局（全部小端序）::

    magic       4B  = b"KVRW"
    seq         8B  帧序号（单调递增）
    payload_len 4B  uint32
    kind        1B
    payload     payload_len B（kind 决定其 JSON 结构，UTF-8 JSON）
    crc32       4B  = zlib.crc32(seq + payload_len_bytes + kind + payload)

崩溃恢复规则（见 decode_frames）：

* 帧流中间出现任何无法解释的字节（magic 不符、CRC 不符、未知 kind、长度
  越过后续帧）→ 抛 :class:`CorruptLogError`，绝不静默跳过；损坏位置之后
  是否还存在一串严格完好到 EOF 的帧，用“重同步探测”判定；
* 只有“帧流末尾”那个不完整/CRC 不符且后面没有可重同步帧的帧被视为崩溃
  时刻的半截写（torn tail），整段丢弃并计入 RecoveryReport.torn_tail_bytes。
"""

from __future__ import annotations

import enum
import json
import struct
import zlib
from dataclasses import dataclass, field
from typing import Any


MAGIC = b"KVRW"
HEADER_LEN = 4 + 8 + 4 + 1  # magic + seq + payload_len + kind
TRAILER_LEN = 4  # crc32
MAX_PAYLOAD = 64 * 1024 * 1024  # 防御性上限：单帧 64MiB

SNAPSHOT_FILE = "snapshot.kvs"
SNAPSHOT_TMP = "snapshot.kvs.tmp"
WAL_FILE = "wal.log"
WAL_OLD_FILE = "wal.old"
LOCK_FILE = "lock"


class FrameKind(enum.Enum):
    BEGIN = b"B"
    COMMIT = b"C"
    PUT = b"P"
    DELETE = b"D"
    EXPIRE = b"E"
    SNAPSHOT = b"S"


_KIND_BY_VALUE: dict[bytes, FrameKind] = {k.value: k for k in FrameKind}


class CorruptLogError(Exception):
    """WAL/快照中发现无法解释的损坏（CRC 错、magic 错、快照截断等）。

    唯一允许“丢弃”的情况是帧流末尾由崩溃造成的半截帧（torn tail），
    其余位置的损坏一律报出本错误而不是静默跳过。
    """


@dataclass
class Frame:
    seq: int
    kind: FrameKind
    payload: dict[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.payload, dict):
            raise CorruptLogError(f"frame {self.seq} payload is not an object")


@dataclass
class RecoveryReport:
    source_opened: str = ""
    snapshot_frames: int = 0
    wal_frames: int = 0
    committed_txns: int = 0
    discarded_incomplete_txns: int = 0
    torn_tail_bytes: int = 0
    warnings: list[str] = field(default_factory=list)
    last_seq: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_opened": self.source_opened,
            "snapshot_frames": self.snapshot_frames,
            "wal_frames": self.wal_frames,
            "committed_txns": self.committed_txns,
            "discarded_incomplete_txns": self.discarded_incomplete_txns,
            "torn_tail_bytes": self.torn_tail_bytes,
            "warnings": list(self.warnings),
            "last_seq": self.last_seq,
        }


def encode_frame(seq: int, kind: FrameKind, payload: dict[str, Any]) -> bytes:
    payload_bytes = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    if len(payload_bytes) > MAX_PAYLOAD:
        raise ValueError(f"frame payload too large: {len(payload_bytes)}")
    head = MAGIC + struct.pack("<QI", seq, len(payload_bytes)) + kind.value
    crc = zlib.crc32(
            struct.pack("<QI", seq, len(payload_bytes)) + kind.value + payload_bytes
        ) & 0xFFFFFFFF
    return head + payload_bytes + struct.pack("<I", crc)


def _strict_decode_from(data: bytes, pos: int, total: int) -> bool:
    """从 ``pos`` 起严格顺序解码直到 EOF，成功返回 True，任何瑕疵返回 False。

    仅用于损坏路径的“重同步探测”：判断当前坏帧之后是否还存在一串完好
    帧——若存在，说明当前损坏位于日志中间，绝不能按末尾截断静默丢弃。
    """
    while pos < total:
        if total - pos < HEADER_LEN + TRAILER_LEN:
            return False
        if data[pos : pos + 4] != MAGIC:
            return False
        seq, payload_len = struct.unpack_from("<QI", data, pos + 4)
        if payload_len > MAX_PAYLOAD:
            return False
        frame_end = pos + HEADER_LEN + payload_len + TRAILER_LEN
        if frame_end > total:
            return False
        kind_byte = data[pos + 16 : pos + 17]
        kind = _KIND_BY_VALUE.get(kind_byte)
        if kind is None:
            return False
        payload_bytes = data[pos + HEADER_LEN : pos + HEADER_LEN + payload_len]
        (crc_stored,) = struct.unpack_from("<I", data, frame_end - TRAILER_LEN)
        crc_calc = zlib.crc32(
            struct.pack("<QI", seq, payload_len) + kind_byte + payload_bytes
        ) & 0xFFFFFFFF
        if crc_stored != crc_calc:
            return False
        try:
            obj = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return False
        if not isinstance(obj, dict):
            return False
        pos = frame_end
    return True


def _resync_exists(data: bytes, after: int) -> bool:
    """``after`` 之后是否存在某个 MAGIC 位置，能严格解码到 EOF。"""
    total = len(data)
    probe = after + 1
    while True:
        hit = data.find(MAGIC, probe, total)
        if hit < 0:
            return False
        if _strict_decode_from(data, hit, total):
            return True
        probe = hit + 1


def decode_frames(data: bytes, *, source: str) -> tuple[list[Frame], int]:
    """解码整段帧流。

    返回 ``(frames, torn_tail_bytes)``。``source`` 仅用于错误信息
    （如 ``"wal.log"``）。
    """
    frames: list[Frame] = []
    pos = 0
    total = len(data)

    while pos < total:
        frame_start = pos
        remaining = total - pos

        # 末尾连完整帧头都凑不齐：半截帧（崩溃残留）。
        if remaining < HEADER_LEN + TRAILER_LEN:
            return frames, total - frame_start

        if data[pos : pos + 4] != MAGIC:
            raise CorruptLogError(
                f"{source}: bad magic at byte {pos} (expected frame boundary)"
            )

        seq, payload_len = struct.unpack_from("<QI", data, pos + 4)
        kind_byte = data[pos + 16 : pos + 17]
        kind = _KIND_BY_VALUE.get(kind_byte)

        if payload_len > MAX_PAYLOAD:
            raise CorruptLogError(
                f"{source}: frame at byte {pos} declares implausible "
                f"payload_len={payload_len}"
            )

        frame_end = pos + HEADER_LEN + payload_len + TRAILER_LEN

        if frame_end > total:
            # 越过 EOF：可能是崩溃时写了一半，也可能是长度字段被改坏。
            # 若其后还能找到一串严格完好到 EOF 的帧，则属于中间损坏。
            if _resync_exists(data, frame_start):
                raise CorruptLogError(
                    f"{source}: frame at byte {pos} (seq={seq}) length "
                    f"{payload_len} overruns but valid frames follow; "
                    f"refusing to truncate"
                )
            return frames, total - frame_start

        payload_bytes = data[pos + HEADER_LEN : pos + HEADER_LEN + payload_len]
        (crc_stored,) = struct.unpack_from("<I", data, frame_end - TRAILER_LEN)
        crc_calc = zlib.crc32(
            struct.pack("<QI", seq, payload_len) + kind_byte + payload_bytes
        ) & 0xFFFFFFFF

        if crc_stored != crc_calc:
            if frame_end < total and _resync_exists(data, frame_start):
                raise CorruptLogError(
                    f"{source}: crc mismatch at byte {pos} (seq={seq}); "
                    f"valid frames follow, refusing to skip"
                )
            # 末尾帧 CRC 不符且后面没有可重同步的帧：崩溃时写了一半。
            return frames, total - frame_start

        if kind is None:
            raise CorruptLogError(
                f"{source}: unknown frame kind {kind_byte!r} at byte {pos}"
            )

        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CorruptLogError(
                f"{source}: invalid JSON payload at byte {pos} (seq={seq}): {exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise CorruptLogError(
                f"{source}: payload is not a JSON object at byte {pos} (seq={seq})"
            )

        frames.append(Frame(seq=seq, kind=kind, payload=payload))
        pos = frame_end

    return frames, 0
