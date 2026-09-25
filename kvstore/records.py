"""On-disk record types and frame constants for the write-ahead log.

Frame layout (little-endian), repeated:

    magic       4 bytes  = FRAME_MAGIC
    frame_len   4 bytes  uint32, length of payload
    crc32       4 bytes  uint32, CRC-32 of payload
    seq         8 bytes  uint64, monotonically increasing frame sequence
    op          1 byte   OpCode
    payload     frame_len bytes (JSON, ``{}`` for BEGIN/COMMIT)

Frames are only ever appended; a crash leaves either complete frames, a torn
suffix, or a transaction missing its COMMIT. The latter two are unconfirmed
suffixes and are deterministically truncated at recovery time.
"""

import enum
import struct
from dataclasses import dataclass

FRAME_MAGIC = b"PKV1"
HEADER_STRUCT = struct.Struct("<4sIIQB")
HEADER_SIZE = HEADER_STRUCT.size  # magic(4) + len(4) + crc(4) + seq(8) + op(1)


class OpCode(enum.Enum):
    PUT = 1
    DELETE = 2
    BEGIN = 3
    COMMIT = 4


@dataclass(frozen=True)
class PutRecord:
    namespace: str
    key: str
    value: object
    version: int
    expires_at_ms: int | None  # absolute epoch ms; None == never expires


@dataclass(frozen=True)
class DeleteRecord:
    namespace: str
    key: str
    version: int  # version of the new tombstone


@dataclass(frozen=True)
class Frame:
    seq: int
    op: OpCode
    record: PutRecord | DeleteRecord | None
    offset: int  # byte offset of this frame within the WAL
