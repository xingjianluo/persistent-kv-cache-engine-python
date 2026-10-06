# 本地可持久化键值存储与过期缓存（FastAPI）

仅依赖本地磁盘与仓库已有依赖（FastAPI 标准库，无外部数据库 / 消息队列）。
所有已确认的写入以 **fsync 的追加式 WAL（write-ahead log）** 落盘；服务在
启动时重放 WAL 恢复内存索引，因此进程重启、甚至进程被 kill -9 后，数据状态
仍然可预期。

- 包代码：`kvstore/`（`records.py` 帧格式、`wal.py` 日志、`store.py` 引擎、
  `compact.py` 日志整理、`lockfile.py` 单写进程锁、`api.py` HTTP 层、
  `errors.py` 错误码）
- 入口：`main.py`
- 测试：`tests/`，覆盖并发争用、TTL 到期、重启恢复、损坏数据、日志整理、
  多实例互斥、失败写入边界等场景，使用 `-s` 打印操作序列、输入与断言依据

## 1. 快速开始 / 本地验证

```bash
uv sync                         # 安装 fastapi + pytest(dev)
uv run pytest -v                # 运行全部测试并打印操作日志
uv run python main.py           # 启动服务（KV_DATA_DIR 默认 ./.kvdata）
KV_DATA_DIR=/tmp/kv KV_CAPACITY_PER_NAMESPACE=100 uv run python main.py
KV_COMPACT_ON_STARTUP=1 uv run python main.py   # 启动恢复后自动整理一次日志
KV_READ_ONLY=1 uv run python main.py            # 只读打开（不抢写锁、拒绝写）
```

运行时数据只有两个文件：`$KV_DATA_DIR/wal.log` 与 `$KV_DATA_DIR/writer.lock`
（整理过程中会短暂出现 `wal.log.compact`，见第 5 节）。

手动快速验证新能力（也可以直接跑 `tests/test_06~10` 看带日志的完整判定过程）：

```bash
# 1) 整理：写一批数据后调用整理端点，观察 wal.log 变小、数据不变
curl -XPOST localhost:PORT/v1/admin/compact
curl localhost:PORT/v1/admin/compaction-report
# 2) 多实例：同一 KV_DATA_DIR 再启动一个写进程 -> 立即以 data_dir_locked 拒绝；
#    kill -9 第一个进程后再启动 -> 正常获得写权限
# 3) 只读：KV_READ_ONLY=1 启动可与写进程并存，/healthz 与 GET 正常，
#    任何写操作 -> 403 read_only
```

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
| POST | `/v1/admin/compact` | 手动触发一次日志整理 |
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
| `corrupt_log` | 500 | WAL 存在无法忽略的损坏（详见第 4 节） |
| `data_dir_locked` | 409 | 另一进程已持有该数据目录的写锁（详见第 6 节） |
| `read_only` | 403 | 以只读模式打开，拒绝一切写操作与整理 |
| `invalid_request` | 422 | 参数校验失败（含值无法 JSON 序列化） |

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
   seq u64 | op u8 | JSON payload`。`op ∈ PUT/DELETE/BEGIN/COMMIT`。
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
字段类型。判定边界只有一条：**只有能确认是“崩溃时没写完”的日志最末尾残留
才可自动清理**；其余一律按损坏处理。分两类结果：

1. **可恢复的未确认后缀（自动修复 + 显式报告，不静默跳过）**
   - 日志末尾是不完整帧（崩溃发生在一次 write 中途）→ `truncated_frame`；
   - 日志完整但末尾事务缺少 COMMIT（崩溃发生在批量写中途）→ 报告
     `transaction without COMMIT`。
   - 处理：保留此前全部完整/已提交帧，将未确认后缀 **截断并 fsync**，
     在 `/v1/admin/recovery-report` 与 `store.last_recovery` 中报告
     `{reason, offset, detail, repaired}`。再次启动即干净。
   - 只读打开（`KV_READ_ONLY=1`）时**只报告、不修复**（`repaired: false`），
     文件一个字节都不会被改动；修复留给持有写锁的进程。
2. **真正的损坏（拒绝启动，绝不静默跳过）**
   - 完整帧 CRC 不匹配（`bad_crc`）、magic 错（`bad_magic`）、seq 断裂
     （`bad_sequence`）、长度越界（`bad_length`）、JSON/字段非法或 op 未知
     （`bad_payload`）→ 抛出 `CorruptLogError`（带字节偏移 `position` 与
     `reason`），**文件与索引都不被修改**，需要人工介入。
   - 特别地：即使损坏位于日志末尾，只要它构成一个**长度完整**的帧（内容
     校验失败），也按损坏处理而不是截断——“没写完”和“写完了但坏了”是两种
     不同的情况；损坏点后面若还有完好的已确认记录，更不会被静默丢弃。

另外，一次写入/批量写因值无法序列化或 I/O 失败而失败时，**不会留下任何
痕迹**：可序列化性在写任何帧之前校验（`invalid_request`）；批量写中途失败
会立即回滚未提交的事务帧。帧序号始终从“磁盘上最后一个完整帧 + 1”继续，
单调递增、不复用，服务不会因此拒绝再次启动。

恢复报告示例：

```json
{"frames_replayed": 7, "truncated": null, "namespaces": 2, "entries": 5}
```

## 5. 日志整理（compaction）

长期写入后 WAL 会无限变长，拖累磁盘与启动恢复耗时。整理把当前内存索引
重写成一份**最小快照日志**并原子替换旧日志，使磁盘占用与恢复耗时收敛到
与“当前键数量”成正比，而不是与“历史写入次数”成正比。

**触发方式**

- 手动：`POST /v1/admin/compact`，或进程内调用 `store.compact()`；
- 自动：`KV_COMPACT_ON_STARTUP=1` 时，启动恢复完成后自动整理一次。

**语义保证（整理前后完全一致）**

- 每个键的值、版本号、**绝对过期时间**（TTL 剩余时间不变）、删除标记；
- “从未写入 / 显式删除 / 自然过期”三种原因的区分（墓碑与已过期未清理的
  条目都会原样带入新日志）；
- 按前置版本号的条件写（CAS）照常对齐；容量统计不多算也不少算。

**并发与崩溃安全**

- 快照序列化在不持有全局锁的情况下进行，读写请求不会被长时间卡住；
  整理期间确认的写入通过增量（delta）合并进新日志，不会丢失；
- 新日志先写为 `wal.log.compact` 并 fsync，再用一次原子 `rename` 替换
  `wal.log` 并 fsync 目录。整理中途崩溃：替换前 → 旧日志为准，残留的
  `.compact` 文件在下次启动时被丢弃；替换后 → 新日志本身是完整一致的。
  两种情况下重启都落在一个一致状态，不会出现“一半新一半旧”。

**兼容范围**：整理是在线操作，不需要停机，也不需要任何手工迁移——老版本
产生的数据目录可以直接用新版本启动（帧格式未变），是否整理完全可选。

## 6. 多实例约束（单写进程）

- 同一数据目录同一时刻只允许**一个写进程**。写模式打开时会对
  `<data_dir>/writer.lock` 取 `flock(LOCK_EX|LOCK_NB)` 排他锁；第二个进程
  以写模式打开同一目录时，立即以 `data_dir_locked`（HTTP 409）拒绝启动，
  而不是两个进程各写各的把日志搞坏。
- 锁由内核持有在文件描述符上：持有进程被 `kill -9` 后锁自动释放，残留的
  `writer.lock` **文件**不影响新进程启动（文件存在不代表锁被持有）。
- 只读访问（`KV_READ_ONLY=1`）与健康检查（`/healthz`）**不取锁**，可以与
  写进程并存；只读实例拒绝一切写操作与整理（`read_only`，HTTP 403），且
  不会创建/修改目录里的任何内容。
- 同一进程内再次以写模式打开同一目录（例如测试里“不退出进程模拟重启”）
  属于**接管**：新实例获得所有权，被放弃的旧实例再写入会立即报
  `data_dir_locked`，防止陈旧句柄写坏日志。

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
- `tests/test_06_compaction.py`：整理前后值/版本/TTL 剩余时间/墓碑/三种
  原因/CAS/容量统计完全一致；整理期间并发读写不丢已确认变更；崩溃在
  替换前（残留 `.compact`）与替换失败两种情形都落到一致状态。
- `tests/test_07_multiprocess.py`：真实子进程抢同一数据目录被拒
  （`data_dir_locked`）、`kill -9` 后新进程直接起得来、只读实例与写进程
  并存、同进程接管后旧实例写入被拒。
- `tests/test_08_corruption_boundary.py`：中段损坏（后面还有完好已确认
  帧）vs 末尾残留的不同处理；长度完整但校验失败的尾帧不按残留截断；
  只读恢复只报告不修改。
- `tests/test_09_failed_writes.py`：不可序列化的值在任何帧落盘前被拒；
  批量写中途 I/O 失败立即回滚；失败之后继续写再重启，版本与 seq 单调
  连续、服务正常启动。
- `tests/test_10_api_extensions.py`：`POST /v1/admin/compact`、整理报告、
  `KV_COMPACT_ON_STARTUP` / `KV_READ_ONLY` 环境变量与对应错误码。

## 9. 设计边界

- 单 WAL 文件 + 在线整理（无分段）；整理是 O（当前键数） 的重写。
- 索引驻留内存，容量是主要的内存上限。
- 多实例互斥依赖 `flock`，只在**本机**有效；NFS 等不支持 flock 语义的
  文件系统不在保证范围内。
- 依赖 `fsync` 的崩溃语义由本地文件系统提供；网络盘 / 不带写屏障的介质不
  在保证范围内。
