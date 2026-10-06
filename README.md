# 本地可持久化键值存储与过期缓存（FastAPI）

仅依赖本地磁盘与仓库已有依赖（FastAPI 标准库，无外部数据库 / 消息队列）。
所有已确认的写入以 **fsync 的追加式 WAL（write-ahead log）** 落盘；服务在
启动时重放 WAL 恢复内存索引，因此进程重启、甚至进程被 kill -9 后，数据状态
仍然可预期。

- 包代码：`kvstore/`（`records.py` 帧格式、`wal.py` 日志、`store.py` 引擎、
  `dirlock.py` 单写进程锁、`api.py` HTTP 层、`errors.py` 错误码）
- 入口：`main.py`
- 测试：`tests/`，覆盖并发争用、TTL 到期、重启恢复、损坏数据、失败写入、
  多实例互斥、日志整理等场景，使用 `-s` 打印操作序列、输入与断言依据

## 1. 快速开始 / 本地验证

```bash
uv sync                         # 安装 fastapi + pytest(dev)
uv run pytest -v                # 运行全部测试并打印操作日志
uv run python main.py           # 启动服务（KV_DATA_DIR 默认 ./.kvdata）
KV_DATA_DIR=/tmp/kv KV_CAPACITY_PER_NAMESPACE=100 uv run python main.py
KV_COMPACT_ON_START=1 uv run python main.py   # 启动时自动整理日志
KV_READ_ONLY=1 uv run python main.py          # 只读打开（不拿锁、不写盘）
```

运行时数据文件：`$KV_DATA_DIR/wal.log`（日志）、`$KV_DATA_DIR/kvstore.lock`
（单写进程锁文件）。整理期间会短暂出现 `wal.log.compact` 临时文件。

## 2. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康检查 |
| GET | `/v1/namespaces/{ns}/keys/{key}` | 读键（key 可含 `/`） |
| PUT | `/v1/namespaces/{ns}/keys/{key}` | 写键（支持 TTL 与条件） |
| DELETE | `/v1/namespaces/{ns}/keys/{key}` | 显式删除（写墓碑） |
| POST | `/v1/batch` | 原子批量写 |
| GET | `/v1/namespaces` | 各命名空间 live 键 / 墓碑 / 容量 |
| GET | `/v1/admin/recovery-report` | 本次启动的恢复报告 |
| POST | `/v1/admin/compact` | 手动触发日志整理 |
| GET | `/v1/admin/compaction-report` | 最近一次整理的结果 |

PUT 请求体：

```json
{
  "value": {"any": "json"},
  "ttl_ms": 5000,
  "expected_version": 3,
  "if_not_exists": false
}
```

批量请求体：`{"mutations": [{"op": "put|delete", "namespace": "...",
"key": "...", ...PUT 字段}]}`。响应：`{"applied": true, "atomic": true,
"count": N, "results": [...]}`。

### 错误响应（稳定、可区分的 error.code）

| code | HTTP | 触发条件 |
| --- | --- | --- |
| `not_found` | 404 | 键从未写入 |
| `deleted` | 404 | 键被显式删除（保留版本化墓碑） |
| `expired` | 404 | 键存在但 TTL 已自然到期 |
| `version_mismatch` | 409 | live 键的 `expected_version` 不匹配 |
| `key_exists` | 409 | `if_not_exists=true` 但键存在且未过期 |
| `capacity_exceeded` | 507 | 写入会使命名空间 live 键数超过容量，被拒绝 |
| `lock_held` | 409 | 同一数据目录已被另一个写进程打开（见第 6 节） |
| `read_only` | 409 | 以只读模式打开的存储收到了写请求 |
| `corrupt_log` | 500 | WAL 存在无法忽略的损坏（详见第 4 节） |
| `invalid_request` | 422 | 参数校验失败 |

对 **已过期或已删除** 的键携带不匹配的 `expected_version` 时，返回的是
`expired` / `deleted`（而不是笼统的版本冲突）；当且仅当版本号精确匹配时，
允许通过 CAS 重新写入该键（版本在原版本上 +1）。

示例：

```bash
curl -XPUT localhost:PORT/v1/namespaces/users/keys/u1 \
  -H 'content-type: application/json' \
  -d '{"value":{"name":"a"},"ttl_ms":60000}'
curl localhost:PORT/v1/namespaces/users/keys/u1
curl -XDELETE localhost:PORT/v1/namespaces/users/keys/u1
curl -XPOST localhost:PORT/v1/batch \
  -H 'content-type: application/json' \
  -d '{"mutations":[{"op":"put","namespace":"orders","key":"o1","value":1},
                    {"op":"put","namespace":"orders","key":"o2","value":2}]}'
```

## 3. 存储与过期/容量语义

### 3.1 持久化模型（已确认 = 已 fsync）

1. WAL 仅追加。一次写入在**同一把全局锁内**完成「校验 → 追加帧 → flush →
   `fsync` → 更新内存索引」；`put/delete/batch` 成功返回时帧已落盘。
2. 帧格式（小端）：`magic("PKV1") | payload_len u32 | crc32 u32 |
   seq u64 | op u8 | JSON payload`。`op ∈ PUT/DELETE/BEGIN/COMMIT/META`
   （META 只会作为首帧出现、seq 为 0，由整理写入，记录本代日志起始 seq）。
3. 每个帧有单调递增的 `seq`；重启后 seq 从「磁盘上最后一个完整帧 + 1」
   继续，绝不复用。
4. **批量即事务**：多条变更先在私有覆盖层完成全部前置条件与容量校验，
   再写入 `BEGIN … COMMIT`。任一前置条件不满足 → 一帧都不写；COMMIT 之前
   崩溃 → 整笔事务视为未确认，重启时整体回滚（见第 4 节）。

### 3.2 可见性、TTL 与删除的可区分原因

- 内存索引保留三种历史：无记录、值记录（带绝对 `expires_at_ms`）、墓碑。
- 自然过期 **不产生墓碑**：读取时依据 `now >= expires_at_ms` 判定，返回
  `expired`；显式删除写入墓碑，返回 `deleted`；从未写入返回 `not_found`。
  三种原因互不混淆。
- 过期判定是惰性的（读 / 写 / 容量检查时判断），因此 TTL 时间戳是
  **绝对时间**，重启后含义不变。边界规则为包含：`now == expires_at_ms`
  即视为过期。
- 覆写一个键会同时替换值、版本与过期时间（新 TTL 从写入时刻重新计算）。

### 3.3 容量规则（明确拒绝，不静默淘汰）

- 容量按命名空间统计 **live 键**（存在、未删除、未过期）。过期键与墓碑
  不占名额；覆写已存在的 live 键不增长名额。
- 任何会使 live 键数超过 `KV_CAPACITY_PER_NAMESPACE` 的写入，在持久化
  **之前** 被拒绝（`capacity_exceeded`）。系统不做 LRU / 静默淘汰，已确认
  的数据不会因为新写入而消失。
- 过期键释放名额（自然过期后该名额可被新键使用，旧键只是变为不可见）。

## 4. 恢复与损坏处理

启动时逐帧严格校验：magic、seq 连续性、payload 长度、CRC-32、JSON 结构与
字段类型。分两类结果：

1. **可恢复的未确认后缀（自动修复 + 显式报告，不静默跳过）**

   仅当破坏**确实落在日志最末尾**、能确认是崩溃时没写完的残留时才自动清理：
   - 日志末尾是不完整帧（崩溃发生在一次 write 中途）→ `truncated_frame`；
   - 日志完整但末尾事务缺少 COMMIT（崩溃发生在批量写中途）→ 报告
     `transaction without COMMIT`。
   - 处理：保留此前全部完整/已提交帧，将未确认后缀 **截断并 fsync**，
     在 `/v1/admin/recovery-report` 与 `store.last_recovery` 中报告
     `{reason, offset, detail}`。再次启动即干净。
2. **真正的损坏（拒绝启动，绝不静默跳过）**
   - 完整帧 CRC 不匹配（`bad_crc`）、magic 错（`bad_magic`）、seq 断裂
     （`bad_sequence`）、长度越界（`bad_length`）、JSON/字段非法或 op 未知
     （`bad_payload`）→ 抛出 `CorruptLogError`（带字节偏移 `position` 与
     `reason`），文件与索引都不被修改，需要人工介入。
   - **中段不完整帧**：如果某个"不完整帧"后面还扫描得到 magic/CRC/seq 均
     合法的已确认帧，说明破坏在中段而非末尾，报 `truncated_mid_log` 并拒绝
     启动——绝不把后面的已确认数据随"截断"一起丢掉。

另外，一次写入/批量写因值无法序列化等原因失败时，未确认帧被截断后
**写入编号 seq 会从磁盘上最后一个幸存帧续接**，不复用已确认帧的编号，
后续写入与重启恢复不受影响。

恢复报告示例：

```json
{"frames_replayed": 7, "truncated": null, "namespaces": 2, "entries": 5}
```

## 5. 日志整理（compaction）

长期写入后 WAL 会无限变长、重启重放越来越慢。整理把当前每个键的最终状态
（live 值 / 墓碑）重写成一条新日志，磁盘占用与启动恢复耗时随之收敛。

- **触发方式**
  - 手动：`POST /v1/admin/compact`，或在进程内调用 `store.compact()`；
  - 启动自动：`KV_COMPACT_ON_START=1`（或 `KVStore(..., compact_on_open=True)`）。
- **语义完全保持**：值、版本号、绝对过期时间（TTL 剩余时间不变）、墓碑，
  以及"从没写过 / 被显式删除 / 自然过期"的区分在整理前后完全一致；按
  整理前版本号的 `expected_version` CAS 仍然对得上；容量统计不多算不少算。
- **兼容范围**：老数据目录（无 META 帧、seq 从 1 开始的老日志）**直接启动
  即可，无需手工迁移**；整理后的新格式日志首帧为 META 帧（记录本代日志的
  起始 seq），恢复逻辑两种格式都认。整理前后写入编号 seq 全局单调递增、
  不复用。
- **并发与崩溃安全**：整理先在锁内做内存快照，再在锁外写
  `wal.log.compact`；快照期间确认的新写入在收尾阶段被逐帧复制到新日志
  （重新编号、绝不丢失），随后 `os.replace` 原子替换并 fsync 目录。读写只
  在快照与最终交换的短时间内被阻塞。整理中途崩溃：要么旧日志完整生效
  （残留的 `.compact` 临时文件下次启动时被丢弃），要么新日志完整生效，
  不会出现一半新一半旧。

整理报告示例（`store.last_compaction` / `POST /v1/admin/compact` 响应）：

```json
{"compacted": true, "entries": 6, "delta_frames": 2, "base_seq": 23,
 "next_seq": 31, "bytes_before": 2028, "bytes_after": 640}
```

## 6. 多实例约束（单写进程）

- 同一份数据目录**同一时刻只允许一个写进程**：写模式打开时对
  `$KV_DATA_DIR/kvstore.lock` 取 `flock(LOCK_EX|LOCK_NB)`。第二个进程以写
  模式打开同一目录会立即失败，抛出可识别的 `LockHeld`（错误码
  `lock_held`，HTTP 409），而不是两个进程各写各的。
- 锁由 OS 在内核侧持有：前一个进程被 `kill -9`、来不及清理时，锁随进程
  消亡自动释放，新进程**无需任何手工清理**即可拿到写权限。
- **只读访问不受影响**：`KV_READ_ONLY=1`（或 `KVStore(..., read_only=True)`）
  不取锁、不改文件，可在写进程运行时并发打开；只读模式下写操作返回
  `read_only` 错误。`/healthz` 健康检查同样不需要锁。
- 互斥粒度是**进程**：同一进程内重复打开自己已持有的目录是允许的
  （进程内重启场景），跨进程才被拒绝。

## 7. 并发约束

- 引擎内一把可重入锁同时保护内存索引与 WAL 追加，所有写操作在锁内完成
  校验与 fsync，因此单键写入是**线性一致**的，不会出现两个并发写互相覆盖
  或丢失更新。
- 乐观并发控制：
  - `expected_version=N`：仅当当前版本为 N 时写入；`expected_version=0`
    表示“仅当键不存在（无墓碑）时创建”。不满足返回 `version_mismatch`
    （live 键）或 `expired` / `deleted` / `not_found` 语义码。
  - `if_not_exists=true`：仅当键不存在/已过期/已删除时创建，否则
    `key_exists`。
  - 版本号从 1 开始，每次成功 put / delete 都 +1（删除也产生新版本墓碑）。
- 批量写具有一致的 all-or-nothing 语义：先统一校验（批量内后一条可以观察
  到前一条的结果），再按最终叠加状态检查容量；成功则整批可见，失败则零
  副作用。并发的单键更新与批量写通过同一把锁串行化。

## 8. 测试场景与可复现性

测试使用**注入式时钟**（`clock_ms`）与独立临时目录，时间与磁盘状态完全
确定，可复现：

- `tests/test_01_concurrency.py`：12 线程 CAS 自增（最终值与版本
  1..N+1 各恰好一次）、并发 create-only 唯一赢家、容量竞争恰好接受 C 个、
  与单键更新竞争时批量的全有/全无。
- `tests/test_02_ttl.py`：过期不可见且原因三分、`now == expires_at_ms`
  边界、过期释放名额、覆写刷新绝对过期时间、对过期键 CAS 的原因码。
- `tests/test_03_recovery.py`：重启后值/版本/TTL/墓碑一致且 seq 续接；
  撕裂尾部帧与缺 COMMIT 事务被报告并截断修复。
- `tests/test_04_corruption.py`：坏 CRC / 坏 magic / seq 断裂 / 非法
  payload / 未知 op 全部抛出带偏移的 `CorruptLogError` 且不改动文件；
  末尾不完整帧是唯一自动恢复情形。
- `tests/test_05_api.py`：HTTP 状态码/错误码、容量 507、批量原子性、
  重启后的恢复报告。
- `tests/test_06_failed_writes.py`：单笔/批量写因值无法序列化失败后，
  继续写入与重启恢复正常，seq 单调不复用。
- `tests/test_07_corruption_bounds.py`：末尾残留自动截断并报告；中段
  "不完整帧 + 后面仍有合法已确认帧"按 `truncated_mid_log` 损坏报出且
  文件不被改动。
- `tests/test_08_multinstance.py`：子进程持锁时第二个写进程被 `lock_held`
  拒绝；持有者被 kill -9 后新进程无需清理即可启动；只读打开不受锁影响。
- `tests/test_09_compaction.py`：整理前后语义逐项一致（值/版本/TTL/三种
  原因区分/CAS/容量统计）、整理期间并发读写不丢已确认写入、整理中途崩溃
  后重启落到一致状态、启动自动整理、HTTP 整理接口。

## 9. 设计边界

- 单 WAL 文件 + 整体重写式整理（无分段合并；本服务规模足够）。
- 索引驻留内存，容量是主要的内存上限。
- 单写进程互斥基于 `flock`，仅覆盖本机进程；跨主机共享同一目录不在
  保证范围内。
- 依赖 `fsync` 的崩溃语义由本地文件系统提供；网络盘 / 不带写屏障的介质不
  在保证范围内。
