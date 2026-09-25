"""In-memory index over a fsync'd WAL: TTL, tombstones, optimistic CAS.

Visibility rules
----------------
* a key never written            -> :class:`KeyNotFound`   ("not_found")
* a key explicitly deleted       -> :class:`KeyDeleted`    ("deleted"),
                                    a versioned tombstone is kept forever
* a key whose TTL has elapsed    -> :class:`KeyExpired`    ("expired");
                                    natural expiry never produces a tombstone
* otherwise the value is returned.

Concurrency rules
-----------------
One :class:`threading.RLock` guards the index and WAL appends; every
successful put/delete is linearizable and durable (frame fsync'd) before the
call returns. ``expected_version`` (``0`` == create) and ``if_not_exists``
are checked under that lock, so concurrent writes can never silently
overwrite each other.

Capacity rule
-------------
Each namespace holds at most ``capacity_per_namespace`` *live* keys
(present, not deleted, not expired). Expired keys and tombstones do not
occupy a slot. Overwriting a live key never grows the namespace; any write
that would exceed the limit is rejected with :class:`CapacityExceeded`
before anything is persisted. Confirmed data is never silently evicted.

Batch rule
----------
A batch is validated (preconditions + final capacity) before any frame is
written, then persisted as BEGIN ... COMMIT. All mutations succeed together
or none is visible; a crash before COMMIT leaves a dangling transaction
whose suffix is unconfirmed and is deterministically truncated on restart.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from .errors import (
    CapacityExceeded,
    KeyDeleted,
    KeyExists,
    KeyExpired,
    KeyNotFound,
    TruncatedFrame,
    VersionMismatch,
)
from .records import DeleteRecord, OpCode, PutRecord
from .wal import WriteAheadLog

WAL_NAME = "wal.log"


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass
class _State:
    value: object
    version: int
    expires_at_ms: int | None
    deleted: bool


class KVStore:
    """Namespaced persistent key-value cache (see module docstring)."""

    def __init__(
        self,
        data_dir: str,
        capacity_per_namespace: int = 10_000,
        clock_ms: Callable[[], int] | None = None,
    ):
        if capacity_per_namespace < 1:
            raise ValueError("capacity_per_namespace must be >= 1")
        self.data_dir = str(data_dir)
        self.capacity_per_namespace = capacity_per_namespace
        self.clock_ms = clock_ms or _now_ms
        # namespace -> key -> state
        self._tables: dict[str, dict[str, _State]] = {}
        self._lock = threading.RLock()
        self.wal = WriteAheadLog(f"{self.data_dir.rstrip('/')}/{WAL_NAME}")
        self.last_recovery = self.recover()

    # ------------------------------------------------------------------ #
    # Lifecycle / recovery
    # ------------------------------------------------------------------ #
    def recover(self) -> dict:
        """Replay the WAL into the index and return a recovery report.

        Complete frames are replayed; a torn final frame or a transaction
        left without COMMIT (crash mid-write / mid-batch) is an unconfirmed
        suffix: it is truncated and fsync'd away, then reported. Any other
        checksum/framing failure raises :class:`CorruptLogError` and changes
        neither file nor index.
        """
        with self._lock:
            frames, truncation = self.wal.recover()
            if truncation is None:
                # Clean log bytes, but a crash could still sit between BEGIN
                # and COMMIT: that whole transaction is unconfirmed.
                begin_offset = self._open_transaction_offset(frames)
                if begin_offset is not None:
                    truncation = TruncatedFrame(
                        begin_offset,
                        "transaction without COMMIT (crash mid-batch)",
                        frames,
                    )

            if truncation is not None:
                self.wal.truncate_from(truncation.position)
                frames, second = self.wal.recover()
                if second is not None:  # pragma: no cover - defensive
                    raise second

            self._tables = {}
            tx_records: list = []
            in_tx = False
            for frame in frames:
                if frame.op is OpCode.BEGIN:
                    in_tx = True
                    tx_records = []
                elif frame.op is OpCode.COMMIT:
                    for rec in tx_records:
                        self._apply_record(rec)
                    tx_records = []
                    in_tx = False
                elif frame.record is not None:
                    if in_tx:
                        tx_records.append(frame.record)
                    else:
                        self._apply_record(frame.record)
            return {
                "frames_replayed": len(frames),
                "truncated": None
                if truncation is None
                else {
                    "reason": truncation.reason,
                    "offset": truncation.position,
                    "detail": str(truncation).split(": ", 1)[-1],
                },
                "namespaces": len(self._tables),
                "entries": sum(len(t) for t in self._tables.values()),
            }

    @staticmethod
    def _open_transaction_offset(frames) -> int | None:
        depth = 0
        begin_offset = None
        for frame in frames:
            if frame.op is OpCode.BEGIN:
                if depth == 0:
                    begin_offset = frame.offset
                depth += 1
            elif frame.op is OpCode.COMMIT:
                depth = 0
                begin_offset = None
        return begin_offset if depth > 0 else None

    def close(self) -> None:
        with self._lock:
            self.wal.close()

    # ------------------------------------------------------------------ #
    # Reads
    # ------------------------------------------------------------------ #
    def get(self, namespace: str, key: str) -> dict:
        with self._lock:
            state = self._tables.get(namespace, {}).get(key)
            self._assert_visible(namespace, key, state)
            return {
                "namespace": namespace,
                "key": key,
                "value": state.value,
                "version": state.version,
                "expires_at_ms": state.expires_at_ms,
            }

    # ------------------------------------------------------------------ #
    # Writes
    # ------------------------------------------------------------------ #
    def put(
        self,
        namespace: str,
        key: str,
        value: object,
        ttl_ms: int | None = None,
        expected_version: int | None = None,
        if_not_exists: bool = False,
    ) -> dict:
        self._validate_name(namespace, "namespace")
        self._validate_name(key, "key")
        if ttl_ms is not None and (not isinstance(ttl_ms, int) or ttl_ms < 1):
            raise ValueError("ttl_ms must be a positive int or None")
        result = self._commit([
            {
                "op": "put",
                "namespace": namespace,
                "key": key,
                "value": value,
                "ttl_ms": ttl_ms,
                "expected_version": expected_version,
                "if_not_exists": if_not_exists,
            }
        ])
        return result["results"][0]

    def delete(self, namespace: str, key: str) -> dict:
        self._validate_name(namespace, "namespace")
        self._validate_name(key, "key")
        result = self._commit([{"op": "delete", "namespace": namespace, "key": key}])
        return result["results"][0]

    def batch(self, mutations: list[dict]) -> dict:
        if not isinstance(mutations, list) or not mutations:
            raise ValueError("batch requires a non-empty list of mutations")
        for m in mutations:
            if not isinstance(m, dict):
                raise ValueError("each mutation must be an object")
            self._validate_name(m.get("namespace", ""), "namespace")
            self._validate_name(m.get("key", ""), "key")
            if m.get("op") not in ("put", "delete"):
                raise ValueError("mutation op must be 'put' or 'delete'")
            ttl = m.get("ttl_ms")
            if ttl is not None and (not isinstance(ttl, int) or ttl < 1):
                raise ValueError("ttl_ms must be a positive int or None")
        return self._commit(mutations)

    # ------------------------------------------------------------------ #
    # Core: stage -> validate -> persist -> apply
    # ------------------------------------------------------------------ #
    def _commit(self, mutations: list[dict]) -> dict:
        atomic = len(mutations) > 1
        with self._lock:
            now = self.clock_ms()
            # Work on a private overlay so a rejected batch mutates nothing.
            plan = self._stage(mutations, now)
            # Everything validated, including final capacity: persist.
            if atomic:
                begin_offset = self.wal.size
                self.wal.append_begin()
            try:
                for item in plan:
                    if item["op"] == "put":
                        seq = self.wal.append_put(
                            PutRecord(
                                item["namespace"],
                                item["key"],
                                item["value"],
                                item["version"],
                                item["expires_at_ms"],
                            )
                        )
                    else:
                        seq = self.wal.append_delete(
                            DeleteRecord(item["namespace"], item["key"], item["version"])
                        )
                    item["seq"] = seq
                if atomic:
                    self.wal.append_commit()
            except BaseException:
                # A write error must not leave a dangling transaction that a
                # later successful write could appear to commit.
                if atomic:
                    self.wal.truncate_from(begin_offset)
                raise
            for item in plan:
                self._apply_staged(item)
            return {
                "applied": True,
                "atomic": atomic,
                "count": len(plan),
                "results": [self._result_of(item) for item in plan],
            }

    def _stage(self, mutations: list[dict], now: int) -> list[dict]:
        """Validate mutations against current state and build the plan.

        Later mutations in the same batch observe earlier ones via
        ``overrides``. Capacity is checked against the final overlay.
        """
        overrides: dict[tuple[str, str], _State] = {}
        plan: list[dict] = []

        def cur(ns: str, key: str) -> _State | None:
            hit = overrides.get((ns, key))
            if hit is not None:
                return hit
            return self._tables.get(ns, {}).get(key)

        for m in mutations:
            ns, key = m["namespace"], m["key"]
            state = cur(ns, key)
            item = {"op": m["op"], "namespace": ns, "key": key}
            new_version = 1 if state is None else state.version + 1

            if m["op"] == "put":
                self._check_preconditions(ns, key, state, m)
                expires = None if m.get("ttl_ms") is None else now + m["ttl_ms"]
                new_state = _State(m["value"], new_version, expires, False)
                item.update(value=m["value"], version=new_version,
                           expires_at_ms=expires)
            else:
                # Unconditional delete; the current status decides whether it
                # is permitted and records a distinct reason otherwise.
                self._assert_visible(ns, key, state)
                new_state = _State(None, new_version, None, True)
                item.update(version=new_version, expires_at_ms=None)

            overrides[(ns, key)] = new_state
            plan.append(item)

        self._check_capacity(mutations, overrides, now)
        return plan

    def _check_preconditions(self, ns: str, key: str, state: _State | None, m: dict):
        expected = m.get("expected_version")
        if_not_exists = bool(m.get("if_not_exists"))
        now = self.clock_ms()

        if if_not_exists:
            if state is None:
                pass
            elif state.deleted:
                raise KeyDeleted(f"{ns}/{key} was explicitly deleted")
            elif self._is_expired(state, now):
                raise KeyExpired(f"{ns}/{key} is expired")
            else:
                raise KeyExists(f"{ns}/{key} already exists at version {state.version}")

        if expected is not None:
            if state is None:
                if expected != 0:
                    # Distinct reason: the key simply does not exist.
                    raise KeyNotFound(
                        f"{ns}/{key} not found (expected_version {expected} "
                        "requires an existing key)"
                    )
            elif state.deleted:
                # A matching version explicitly revives a tombstone; a wrong
                # one reports the delete reason, not a generic mismatch.
                if expected != state.version:
                    raise KeyDeleted(
                        f"{ns}/{key} was explicitly deleted at version {state.version}"
                    )
            elif self._is_expired(state, now):
                if expected != state.version:
                    raise KeyExpired(
                        f"{ns}/{key} expired at {state.expires_at_ms}"
                    )
            elif expected != state.version:
                raise VersionMismatch(
                    f"{ns}/{key}: expected_version {expected} != current "
                    f"{state.version} (status=live)"
                )

    def _check_capacity(self, mutations, overrides, now: int) -> None:
        namespaces = {m["namespace"] for m in mutations}
        for ns in namespaces:
            merged: dict[str, _State] = dict(self._tables.get(ns, {}))
            for (o_ns, o_key), st in overrides.items():
                if o_ns == ns:
                    merged[o_key] = st
            live = sum(1 for st in merged.values() if self._is_live(st, now))
            if live > self.capacity_per_namespace:
                raise CapacityExceeded(
                    f"namespace {ns!r}: {live} live keys > capacity "
                    f"{self.capacity_per_namespace}"
                )

    # ------------------------------------------------------------------ #
    # Index helpers
    # ------------------------------------------------------------------ #
    def _apply_staged(self, item: dict) -> None:
        table = self._tables.setdefault(item["namespace"], {})
        table[item["key"]] = _State(
            None if item["op"] == "delete" else item["value"],
            item["version"],
            item.get("expires_at_ms"),
            item["op"] == "delete",
        )

    def _apply_record(self, record) -> None:
        from .records import DeleteRecord as _D

        table = self._tables.setdefault(record.namespace, {})
        if isinstance(record, _D):
            table[record.key] = _State(None, record.version, None, True)
        else:
            table[record.key] = _State(
                record.value, record.version, record.expires_at_ms, False
            )

    def _is_expired(self, state: _State, now: int) -> bool:
        return state.expires_at_ms is not None and now >= state.expires_at_ms

    def _is_live(self, state: _State, now: int) -> bool:
        return not state.deleted and not self._is_expired(state, now)

    def _assert_visible(self, ns: str, key: str, state: _State | None) -> None:
        if state is None:
            raise KeyNotFound(f"{ns}/{key} not found")
        if state.deleted:
            raise KeyDeleted(f"{ns}/{key} was explicitly deleted")
        if self._is_expired(state, self.clock_ms()):
            raise KeyExpired(f"{ns}/{key} expired at {state.expires_at_ms}")

    @staticmethod
    def _validate_name(value: object, label: str) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError(f"{label} must be a non-empty string")
        if len(value) > 4096:
            raise ValueError(f"{label} too long (max 4096 chars)")

    @staticmethod
    def _result_of(item: dict) -> dict:
        return {
            "op": item["op"],
            "namespace": item["namespace"],
            "key": item["key"],
            "version": item["version"],
            "expires_at_ms": item.get("expires_at_ms"),
            "seq": item.get("seq"),
            "status": "deleted" if item["op"] == "delete" else "stored",
        }
