"""存储引擎：命名空间、TTL、容量、版本/CAS、原子批量与崩溃恢复。

状态机（:class:`KVState`）与磁盘 I/O（:class:`WalWriter` 等）分离：
状态变更以“已提交事务”的帧序列为唯一事实来源，WAL 回放与正常写入走
同一条 apply 路径，保证重启后语义一致。

所有公共方法均为 async；同一事件循环内按命名空间串行化写操作，读操作
无锁（GIL 下 dict 读原子）。本引擎只支持单进程多协程访问（启动时以
``lock`` 文件互斥）。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable

from kvapp.wal import (
    Frame,
    FrameKind,
    RecoveryReport,
    decode_frames,
)


@dataclass
class Entry:
    value: Any
    version: int
    expires_at_ns: int | None  # None 表示不过期


@dataclass
class Namespace:
    entries: dict[str, Entry] = field(default_factory=dict)
    # 已自然过期但尚未被覆盖/删除的键 -> 过期时刻（ns）
    expired: dict[str, int] = field(default_factory=dict)
    # 显式删除墓碑：key -> 版本号
    tombstones: dict[str, int] = field(default_factory=dict)


# ---- 拒绝原因（对外可区分的错误码；HTTP 状态见 api 层）-----------------

REASON_NOT_FOUND = "not_found"
REASON_EXPIRED = "expired"
REASON_DELETED = "deleted"
REASON_VERSION_MISMATCH = "version_mismatch"
REASON_CAPACITY_EXCEEDED = "capacity_exceeded"
REASON_VALUE_TOO_LARGE = "value_too_large"
REASON_BAD_REQUEST = "bad_request"


class KVError(Exception):
    """业务可预期拒绝。code 用于区分原因，http_status 供 API 层映射。"""

    def __init__(self, code: str, message: str, *, http_status: int = 409,
                 details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.http_status = http_status
        self.details = details or {}


@dataclass
class _Prepared:
    """一次提交（单键或批量）在 dry-run 阶段算出的确定性结果。"""

    frames: list[tuple[FrameKind, dict[str, Any]]]
    # apply 之后每个操作的返回：{"index", "version"} 或 {"index", "code", ...}
    results: list[dict[str, Any]]


class KVState:
    """纯内存命名空间状态 + 帧回放/构造。不做任何磁盘 I/O。"""

    def __init__(self, *, max_entries_per_namespace: int = 10_000,
                 max_value_bytes: int = 1 << 20) -> None:
        self.namespaces: dict[str, Namespace] = {}
        self.max_entries_per_namespace = max_entries_per_namespace
        self.max_value_bytes = max_value_bytes

    def ns(self, name: str) -> Namespace:
        n = self.namespaces.get(name)
        if n is None:
            n = Namespace()
            self.namespaces[name] = n
        return n

    # ---- TTL / 可见性 --------------------------------------------------

    def _expire_if_due(self, n: Namespace, key: str, now_ns: int) -> None:
        e = n.entries.get(key)
        if e is not None and e.expires_at_ns is not None and now_ns >= e.expires_at_ns:
            del n.entries[key]
            n.expired[key] = e.expires_at_ns
            # 过期不产生墓碑：与显式删除区分

    def classify(self, namespace: str, key: str, now_ns: int) -> str:
        """键当前对外可见性分类：visible / expired / deleted / not_found。"""
        n = self.ns(namespace)
        self._expire_if_due(n, key, now_ns)
        if key in n.entries:
            return "visible"
        if key in n.expired:
            return REASON_EXPIRED
        if key in n.tombstones:
            return REASON_DELETED
        return REASON_NOT_FOUND

    def current_version(self, namespace: str, key: str, now_ns: int) -> int:
        """可见键返回当前版本（>=1），否则返回 0。"""
        n = self.ns(namespace)
        self._expire_if_due(n, key, now_ns)
        e = n.entries.get(key)
        return e.version if e is not None else 0

    def live_count(self, namespace: str, now_ns: int) -> int:
        n = self.ns(namespace)
        return sum(
            1
            for e in n.entries.values()
            if e.expires_at_ns is None or now_ns < e.expires_at_ns
        )

    # ---- apply（回放与正常提交的唯一路径）------------------------------

    def apply_frame(self, kind: FrameKind, p: dict[str, Any], now_ns: int) -> None:
        if kind is FrameKind.PUT:
            ns = self.ns(p["ns"])
            ver = int(p["ver"])
            # 幂等保护：只有版本严格更新才覆盖（seq 单调递增，重复回放安全）。
            # 条目无论是否已过绝对到期时刻都进入 entries；到期判定始终按读取
            # 时刻惰性进行（expired 标记只是历史状态，用于区分 expired/deleted）。
            cur = ns.entries.get(p["k"])
            if cur is not None and cur.version >= ver:
                return
            ns.entries[p["k"]] = Entry(
                value=p["v"], version=ver, expires_at_ns=p.get("exp"),
            )
            ns.tombstones.pop(p["k"], None)
            ns.expired.pop(p["k"], None)
        elif kind is FrameKind.DELETE:
            ns = self.ns(p["ns"])
            ver = int(p["ver"])
            cur = ns.entries.get(p["k"])
            if ns.tombstones.get(p["k"], 0) >= ver and cur is None:
                return
            ns.entries.pop(p["k"], None)
            ns.expired.pop(p["k"], None)
            ns.tombstones[p["k"]] = ver
        elif kind is FrameKind.EXPIRE:
            ns = self.ns(p["ns"])
            # 回放时仍尊重版本：只在版本一致时做过期迁移
            cur = ns.entries.get(p["k"])
            if cur is not None and cur.version == int(p["ver"]):
                del ns.entries[p["k"]]
                ns.expired[p["k"]] = int(p["at"])
                ns.tombstones.pop(p["k"], None)
        else:
            # BEGIN / COMMIT / SNAPSHOT 由恢复流程解释，不改变状态
            return

    # ---- 构造提交 ------------------------------------------------------

    def prepare_commit(
        self, ops: list[dict[str, Any]], now_ns: int
    ) -> _Prepared:
        """对一批操作做完整 dry-run：全成功才生成帧；任何一个前置条件/容量
        不满足则整体拒绝（批量原子性）。

        每个 op::
            {"op": "put", "ns": str, "key": str, "value": json,
             "ttl_ms": int|None, "expected_version": int|None}
            {"op": "delete", "ns": str, "key": str,
             "expected_version": int|None}
        """
        if not isinstance(ops, list) or not ops:
            raise KVError(REASON_BAD_REQUEST, "ops must be a non-empty list",
                          http_status=400)

        # 第一步：结构校验（不触碰任何状态）
        for index, raw in enumerate(ops):
            if not isinstance(raw, dict):
                raise KVError(REASON_BAD_REQUEST,
                              f"ops[{index}] must be an object", http_status=400)
            if raw.get("op") not in ("put", "delete"):
                raise KVError(REASON_BAD_REQUEST,
                              f"ops[{index}].op must be 'put' or 'delete'",
                              http_status=400)
            if not isinstance(raw.get("ns"), str) or not raw["ns"]:
                raise KVError(REASON_BAD_REQUEST,
                              f"ops[{index}].ns must be a non-empty string",
                              http_status=400)
            if not isinstance(raw.get("key"), str) or not raw["key"]:
                raise KVError(REASON_BAD_REQUEST,
                              f"ops[{index}].key must be a non-empty string",
                              http_status=400)

        # 第二步：准备 dry-run 工作副本。自然过期完全惰性——按提交时刻
        # now_ns 逐键判断，绝不把“未来时刻的 TTL”提前搬移，因此时钟前进
        # 后回到较早 now_ns 的容量计算也不会把过期键误算为存活。

        # 工作副本（只记录本批内的覆盖/删除），dry-run 期间只读真实状态
        sim_entries: dict[tuple[str, str], Entry] = {}
        sim_deleted: set[tuple[str, str]] = set()
        # 同批删除时记录墓碑版本（真实 tombstone 要等 apply 才写入）
        sim_deleted_ver: dict[tuple[str, str], int] = {}
        # dry-run 视图中按 now_ns 判定已到期的真实键（用于区分 expired）
        lazy_expired: set[tuple[str, str]] = set()

        def _is_live(e: Entry | None) -> bool:
            return e is not None and (
                e.expires_at_ns is None or now_ns < e.expires_at_ns
            )

        frames: list[tuple[FrameKind, dict[str, Any]]] = []
        results: list[dict[str, Any]] = []

        for index, raw in enumerate(ops):
            opname = raw["op"]
            ns_name = raw["ns"]
            key = raw["key"]

            n = self.ns(ns_name)
            full = (ns_name, key)

            # 模拟视图读取：按 now_ns 惰性判过期，不搬移真实状态。
            def _sim_entry() -> Entry | None:
                if full in sim_deleted:
                    return None
                e = sim_entries.get(full, n.entries.get(key))
                if not _is_live(e):
                    if e is not None and full not in sim_entries:
                        lazy_expired.add(full)
                    return None
                return e

            def _sim_state() -> tuple[str, int]:
                """返回 (状态, 最近版本号)；状态 ∈ visible/expired/deleted/not_found。"""
                e = _sim_entry()
                if e is not None:
                    return "visible", e.version
                if full in sim_deleted:
                    return REASON_DELETED, sim_deleted_ver[full]
                if full in lazy_expired or key in n.expired:
                    return REASON_EXPIRED, 0
                if key in n.tombstones:
                    return REASON_DELETED, n.tombstones[key]
                return REASON_NOT_FOUND, 0

            expected = raw.get("expected_version")
            if expected is not None and (
                not isinstance(expected, int) or isinstance(expected, bool)
                or expected < 0
            ):
                raise KVError(REASON_BAD_REQUEST,
                              f"ops[{index}].expected_version must be int >= 0",
                              http_status=400)

            if opname == "put":
                value = raw.get("value", None)
                ttl_ms = raw.get("ttl_ms")
                if ttl_ms is not None and (
                    not isinstance(ttl_ms, int) or isinstance(ttl_ms, bool)
                    or ttl_ms <= 0
                ):
                    raise KVError(REASON_BAD_REQUEST,
                                  f"ops[{index}].ttl_ms must be a positive int",
                                  http_status=400)
                value_bytes = len(
                    json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode(
                        "utf-8"
                    )
                )
                if value_bytes > self.max_value_bytes:
                    raise KVError(
                        REASON_VALUE_TOO_LARGE,
                        f"ops[{index}] value {value_bytes}B exceeds limit "
                        f"{self.max_value_bytes}B",
                        http_status=413,
                        details={"index": index, "size_bytes": value_bytes,
                                 "limit_bytes": self.max_value_bytes},
                    )
                expires_at = (
                    now_ns + ttl_ms * 1_000_000 if ttl_ms is not None else None
                )

                state, last_ver = _sim_state()
                cur_ver = last_ver if state == "visible" else 0
                # 版本沿键的历史单调递增：过期后从 1 重新开始；显式删除后延续。
                new_ver = (last_ver + 1) if state != REASON_EXPIRED else 1

                if expected is not None and expected != cur_ver:
                    msg = (
                        f"ops[{index}] key is {state} (version 0)"
                        if state != "visible"
                        else f"ops[{index}] expected version {expected} "
                        f"but current is {cur_ver}"
                    )
                    raise KVError(
                        REASON_VERSION_MISMATCH, msg, http_status=409,
                        details={"index": index, "expected_version": expected,
                                 "current_version": cur_ver, "state": state},
                    )

                sim_entries[full] = Entry(value=value, version=new_ver,
                                          expires_at_ns=expires_at)
                sim_deleted.discard(full)
                sim_deleted_ver.pop(full, None)
                frames.append((FrameKind.PUT, {
                    "ns": ns_name, "k": key, "v": value,
                    "ver": new_ver, "exp": expires_at,
                }))
                results.append({"index": index, "version": new_ver})

            elif opname == "delete":
                state, last_ver = _sim_state()
                if state != "visible":
                    raise KVError(
                        state, f"ops[{index}] cannot delete {state} key",
                        http_status=404,
                        details={"index": index, "state": state},
                    )
                if expected is not None and expected != last_ver:
                    raise KVError(
                        REASON_VERSION_MISMATCH,
                        f"ops[{index}] expected version {expected} but current "
                        f"is {last_ver}",
                        http_status=409,
                        details={"index": index, "expected_version": expected,
                                 "current_version": last_ver,
                                 "state": "visible"},
                    )
                sim_deleted.add(full)
                sim_entries.pop(full, None)
                sim_deleted_ver[full] = last_ver + 1
                frames.append((FrameKind.DELETE, {
                    "ns": ns_name, "k": key, "ver": last_ver + 1,
                }))
                results.append({"index": index, "version": last_ver + 1})

        # 容量检查：直接构造“提交后”每个被触碰命名空间的存活键集合。
        # 真实 entries 不预先搬移，按 now_ns 过滤存活；再叠加本批的
        # 覆盖/删除。过期键不占容量。
        touched_ns = {raw["ns"] for raw in ops}
        projected_count: dict[str, int] = {}
        for ns_name in touched_ns:
            target = self.ns(ns_name)
            live = {
                k for k, e in target.entries.items()
                if e.expires_at_ns is None or now_ns < e.expires_at_ns
            }
            for (ns2, key), e in sim_entries.items():
                if ns2 != ns_name:
                    continue
                if e.expires_at_ns is None or now_ns < e.expires_at_ns:
                    live.add(key)
                else:
                    live.discard(key)
            for ns2, key in sim_deleted:
                if ns2 == ns_name:
                    live.discard(key)
            projected_count[ns_name] = len(live)

        for ns_name, projected in projected_count.items():
            if projected > self.max_entries_per_namespace:
                raise KVError(
                    REASON_CAPACITY_EXCEEDED,
                    f"namespace {ns_name!r} would have {projected} live entries "
                    f"(limit {self.max_entries_per_namespace}); "
                    f"rejecting write, no data evicted",
                    http_status=507,
                    details={"namespace": ns_name, "projected": projected,
                             "limit": self.max_entries_per_namespace},
                )

        return _Prepared(frames=frames, results=results)


# ---------------------------------------------------------------------------
# 持久化引擎
# ---------------------------------------------------------------------------

import contextlib
import fcntl
import logging
import os
import threading
import time

from kvapp.wal import (
    LOCK_FILE,
    SNAPSHOT_FILE,
    SNAPSHOT_TMP,
    WAL_FILE,
    WAL_OLD_FILE,
    CorruptLogError,
    Frame,
    FrameKind,
    RecoveryReport,
    decode_frames,
    encode_frame,
)

log = logging.getLogger("kvapp.engine")


class EngineInUseError(Exception):
    """数据目录已被另一个引擎实例（进程）占用。"""


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Engine:
    """单进程异步引擎。提交语义：prepare 全部通过 → 写 BEGIN/数据/COMMIT
    并 flush+fsync → apply 内存状态 → 返回。未 fsync 完成的提交在崩溃后
    依据 torn-tail / 未闭合事务规则丢弃。
    """

    def __init__(
        self,
        data_dir: str,
        *,
        max_entries_per_namespace: int = 10_000,
        max_value_bytes: int = 1 << 20,
        compaction_threshold_bytes: int = 2 << 20,
        clock_ns: Callable[[], int] | None = None,
    ) -> None:
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.compaction_threshold = compaction_threshold_bytes
        self.clock_ns = clock_ns or time.monotonic_ns

        # 单进程互斥（同机多进程双开会立刻失败，而不是静默损坏 WAL）
        self._lock_fd = os.open(
            os.path.join(data_dir, LOCK_FILE), os.O_RDWR | os.O_CREAT, 0o600
        )
        try:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self._lock_fd)
            self._lock_fd = -1
            raise EngineInUseError(
                f"data dir {data_dir!r} is locked by another process"
            ) from exc
        os.ftruncate(self._lock_fd, 0)
        os.write(self._lock_fd, f"pid={os.getpid()}\n".encode())

        self.state = KVState(
            max_entries_per_namespace=max_entries_per_namespace,
            max_value_bytes=max_value_bytes,
        )
        self._seq = 0
        self._snapshot_seq: int | None = None
        self._write_lock = threading.RLock()
        self._wal_path = os.path.join(data_dir, WAL_FILE)
        self._wal_file = open(self._wal_path, "ab", buffering=0)
        try:
            self.report = self._recover()
            # 仅当恢复出非空 WAL 时做一次收敛快照，避免空目录无谓轮转。
            if os.path.exists(self._wal_path) and os.path.getsize(self._wal_path) > 0:
                self._compact_locked()
        except BaseException:
            # 启动失败（如日志损坏）也要释放文件句柄与目录锁
            self._wal_file.close()
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            os.close(self._lock_fd)
            self._lock_fd = -1
            raise

    # ---- 恢复 ------------------------------------------------------------

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _read_file(self, path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    def _recover(self) -> RecoveryReport:
        report = RecoveryReport()

        # 1) 快照（若存在，损坏必须报错；半截快照绝不静默使用）
        snap_path = os.path.join(self.data_dir, SNAPSHOT_FILE)
        have_snapshot = False
        if os.path.exists(snap_path):
            data = self._read_file(snap_path)
            frames, torn = decode_frames(data, source=SNAPSHOT_FILE)
            if torn:
                raise CorruptLogError(
                    f"{SNAPSHOT_FILE}: trailing {torn} torn bytes; snapshots "
                    f"are atomically replaced and must never be truncated"
                )
            self._apply_snapshot(frames)
            report.snapshot_frames = len(frames)
            report.source_opened = SNAPSHOT_FILE
            have_snapshot = True

        # 2) WAL 选择：
        #  - 有快照：只回放快照之后产生的新 wal.log；wal.old 已被快照覆盖，
        #    绝不能再叠加（否则会重放旧状态）。
        #  - 无快照：正常 wal.log；若只存在 wal.old（轮转瞬间崩溃），用它。
        old_path = os.path.join(self.data_dir, WAL_OLD_FILE)
        wal_candidates: list[str] = []
        if os.path.exists(self._wal_path):
            wal_candidates.append(self._wal_path)
        if not have_snapshot and not wal_candidates and os.path.exists(old_path):
            wal_candidates.append(old_path)
        elif have_snapshot and os.path.exists(old_path):
            report.warnings.append(
                f"{WAL_OLD_FILE}: superseded by {SNAPSHOT_FILE}, ignored"
            )

        open_txns: set[int] = set()
        pending_by_txn: dict[int, list[Frame]] = {}
        # 快照帧自身占一个 seq，后续提交必须严格大于它，避免 seq 回卷。
        max_seq = self._snapshot_seq or 0

        for wi, path in enumerate(wal_candidates):
            tag = os.path.basename(path)
            data = self._read_file(path)
            frames, torn = decode_frames(data, source=tag)
            if torn:
                report.torn_tail_bytes += torn
                msg = (f"{tag}: discarded {torn} trailing torn bytes "
                       f"(incomplete crash-time write)")
                report.warnings.append(msg)
                log.warning(msg)
            if wi == 0 and not report.source_opened:
                report.source_opened = tag

            for fr in frames:
                max_seq = max(max_seq, fr.seq)
                txn_val = fr.payload.get("txn")
                if fr.kind is FrameKind.BEGIN:
                    open_txns.add(int(txn_val))
                    pending_by_txn.setdefault(int(txn_val), [])
                elif fr.kind is FrameKind.COMMIT:
                    txn = int(txn_val)
                    group = pending_by_txn.pop(txn, [])
                    if txn in open_txns:
                        for pf in group:
                            self.state.apply_frame(
                                pf.kind, pf.payload, self.clock_ns()
                            )
                        open_txns.discard(txn)
                        report.committed_txns += 1
                elif txn_val is not None:
                    # 事务数据帧：等 COMMIT 后才应用
                    pending_by_txn.setdefault(int(txn_val), []).append(fr)
                else:
                    # 独立帧（前向兼容）：立即应用
                    self.state.apply_frame(fr.kind, fr.payload, self.clock_ns())
            report.wal_frames += len(frames)

        # 3) 未闭合事务：崩溃时未落 COMMIT，整体丢弃并报告
        if open_txns:
            report.discarded_incomplete_txns = len(open_txns)
            msg = (f"discarded {len(open_txns)} uncommitted transaction(s) "
                   f"after crash: {sorted(open_txns)}")
            report.warnings.append(msg)
            log.warning(msg)

        self._seq = max_seq
        report.last_seq = max_seq
        return report

    def _apply_snapshot(self, frames: list[Frame]) -> None:
        snap = frames[-1] if frames and frames[-1].kind is FrameKind.SNAPSHOT else None
        if snap is None:
            raise CorruptLogError(
                f"{SNAPSHOT_FILE}: does not end with a snapshot frame"
            )
        data = snap.payload
        if data.get("fmt") != "kvsnap/1":
            raise CorruptLogError(
                f"{SNAPSHOT_FILE}: unknown snapshot format {data.get('fmt')!r}"
            )
        self._snapshot_seq = snap.seq
        for ns_name, nsdata in data["namespaces"].items():
            ns = self.state.ns(ns_name)
            for k, ed in nsdata.get("entries", {}).items():
                ns.entries[k] = Entry(
                    value=ed["v"], version=int(ed["ver"]),
                    expires_at_ns=ed.get("exp"),
                )
            ns.expired.update({k: int(v) for k, v in nsdata.get("expired", {}).items()})
            ns.tombstones.update({k: int(v) for k, v in nsdata.get("tombstones", {}).items()})

    # ---- 提交/压缩 ------------------------------------------------------
    # 单一写者模型：所有提交都写同一 WAL 并 fsync，本就必须严格串行。
    # 用 threading.RLock（可重入）与具体事件循环解耦；读路径不加锁，
    # 写者在锁内以“prepare → fsync → apply”为一个不可分割的临界区。

    async def _commit(self, ops: list[dict[str, Any]]) -> list[dict[str, Any]]:
        now_ns = self.clock_ns()
        with self._write_lock:
            prepared = self.state.prepare_commit(ops, now_ns)
            txn = self._next_seq()
            parts: list[bytes] = [
                encode_frame(txn, FrameKind.BEGIN,
                             {"txn": txn, "n": len(prepared.frames)})
            ]
            for kind, payload in prepared.frames:
                pl = dict(payload)
                pl["txn"] = txn
                parts.append(encode_frame(self._next_seq(), kind, pl))
            parts.append(encode_frame(self._next_seq(), FrameKind.COMMIT,
                                      {"txn": txn}))
            blob = b"".join(parts)
            self._wal_file.write(blob)
            self._wal_file.flush()
            os.fsync(self._wal_file.fileno())
            # 已落盘确认后才更新内存
            for kind, payload in prepared.frames:
                self.state.apply_frame(kind, payload, now_ns)
            log.info("commit txn=%d ops=%d seq<=%d", txn, len(ops), self._seq)
            needs_compact = (
                os.fstat(self._wal_file.fileno()).st_size
                >= self.compaction_threshold
            )
            if needs_compact:
                self._compact_locked()
        return prepared.results

    def _compact_locked(self) -> None:
        namespaces: dict[str, Any] = {}
        for name, ns in self.state.namespaces.items():
            namespaces[name] = {
                "entries": {
                    k: {"v": e.value, "ver": e.version, "exp": e.expires_at_ns}
                    for k, e in ns.entries.items()
                },
                "expired": dict(ns.expired),
                "tombstones": dict(ns.tombstones),
            }
        payload = {"fmt": "kvsnap/1", "seq": self._seq, "namespaces": namespaces}
        snap_blob = encode_frame(self._next_seq(), FrameKind.SNAPSHOT, payload)
        tmp_path = os.path.join(self.data_dir, SNAPSHOT_TMP)
        with open(tmp_path, "wb", buffering=0) as f:
            f.write(snap_blob)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, os.path.join(self.data_dir, SNAPSHOT_FILE))
        _fsync_dir(self.data_dir)
        # 轮转 WAL：旧文件改名 wal.old，新建空 wal.log；下一次提交成功后删除
        # wal.old（fsync 目录），崩溃在中间任一步都能恢复。
        self._wal_file.close()
        if os.path.exists(self._wal_path):
            os.replace(self._wal_path, os.path.join(self.data_dir, WAL_OLD_FILE))
        self._wal_file = open(self._wal_path, "ab", buffering=0)
        _fsync_dir(self.data_dir)
        log.info("compaction complete: snapshot seq<=%d, %d namespaces",
                 self._seq, len(namespaces))

    async def close(self) -> None:
        with self._write_lock:
            with contextlib.suppress(Exception):
                self._wal_file.flush()
                os.fsync(self._wal_file.fileno())
            self._wal_file.close()
            old_path = os.path.join(self.data_dir, WAL_OLD_FILE)
            if os.path.exists(old_path):
                os.remove(old_path)
                _fsync_dir(self.data_dir)
        if self._lock_fd >= 0:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            os.close(self._lock_fd)
            self._lock_fd = -1

    # ---- 公共 API --------------------------------------------------------

    async def get(self, namespace: str, key: str) -> tuple[Any, Entry]:
        now_ns = self.clock_ns()
        ns = self.state.ns(namespace)
        state = self.state.classify(namespace, key, now_ns)
        if state != "visible":
            raise KVError(state, f"key {key!r} is {state}", http_status=404,
                          details={"state": state})
        e = ns.entries[key]
        return e.value, e

    async def put(self, namespace: str, key: str, value: Any, *,
                  ttl_ms: int | None = None,
                  expected_version: int | None = None) -> int:
        op = {"op": "put", "ns": namespace, "key": key, "value": value}
        if ttl_ms is not None:
            op["ttl_ms"] = ttl_ms
        if expected_version is not None:
            op["expected_version"] = expected_version
        results = await self._commit([op])
        return int(results[0]["version"])

    async def delete(self, namespace: str, key: str, *,
                     expected_version: int | None = None) -> int:
        op = {"op": "delete", "ns": namespace, "key": key}
        if expected_version is not None:
            op["expected_version"] = expected_version
        results = await self._commit([op])
        return int(results[0]["version"])

    async def batch(self, ops: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return await self._commit(ops)

    async def stats(self, namespace: str) -> dict[str, Any]:
        now_ns = self.clock_ns()
        ns = self.state.ns(namespace)
        live = 0
        expired_live_entries = 0
        for e in ns.entries.values():
            if e.expires_at_ns is None or now_ns < e.expires_at_ns:
                live += 1
            else:
                expired_live_entries += 1
        return {
            "namespace": namespace,
            "live_entries": live,
            "expired_entries_pending_sweep": expired_live_entries + len(ns.expired),
            "tombstones": len(ns.tombstones),
            "limit": self.state.max_entries_per_namespace,
            "wal_bytes": os.fstat(self._wal_file.fileno()).st_size,
        }

    def recovery_report(self) -> RecoveryReport:
        return self.report
