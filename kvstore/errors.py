"""Error hierarchy.

Every public failure maps to a stable machine-readable ``code`` so callers
can distinguish: key never written / explicitly deleted / naturally expired,
version mismatch vs precondition failure, capacity rejection and on-disk
corruption.
"""


class KVError(Exception):
    code = "kv_error"


class KeyNotFound(KVError):
    """Key has never been written (or is unknown to the store)."""

    code = "not_found"


class KeyDeleted(KVError):
    """Key was explicitly deleted; a tombstone is retained."""

    code = "deleted"


class KeyExpired(KVError):
    """Key exists with data but its TTL has elapsed."""

    code = "expired"


class VersionMismatch(KVError):
    """expected_version did not match the current version."""

    code = "version_mismatch"


class KeyExists(KVError):
    """if_not_exists was requested but the key is present."""

    code = "key_exists"


class CapacityExceeded(KVError):
    """Write rejected because the namespace capacity limit would be crossed."""

    code = "capacity_exceeded"


class LockHeld(KVError):
    """The data directory is already opened by another writer process."""

    code = "lock_held"


class StoreReadOnly(KVError):
    """A mutating operation was attempted on a read-only store."""

    code = "read_only"


class CorruptLogError(KVError):
    """WAL framing/checksum/structure is invalid.

    ``position`` is the byte offset at which corruption was detected and
    ``reason`` gives a stable sub-code (bad_magic, bad_crc, bad_length,
    bad_payload, bad_sequence, truncated_frame, truncated_mid_log).
    ``truncated_mid_log`` means an incomplete frame was found while valid
    confirmed frames still follow it — genuine mid-log damage, reported
    instead of truncated.
    """

    code = "corrupt_log"

    def __init__(self, reason: str, position: int, detail: str = ""):
        self.reason = reason
        self.position = position
        msg = f"corrupt WAL at byte {position}: {reason}"
        if detail:
            msg += f" ({detail})"
        super().__init__(msg)


class TruncatedFrame(CorruptLogError):
    """The WAL ends with an incomplete frame (crash mid-write).

    Complete frames before it are confirmed; the suffix is unconfirmed and
    is deterministically truncated during recovery. Unlike other corruption,
    this condition is recoverable and is surfaced in the recovery report.
    """

    code = "truncated_frame"

    def __init__(self, position: int, detail: str = "", frames: list | None = None):
        self.frames = frames or []
        super().__init__("truncated_frame", position, detail)
