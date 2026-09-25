# 本地可持久化键值存储与过期缓存（FastAPI）

仅依赖本地磁盘与仓库已有依赖（FastAPI 标准库，无外部数据库 / 消息队列）。
所有已确认的写入以 **fsync 的追加式 WAL（write-ahead log）** 落盘；服务在
启动时重放 WAL 恢复内存索引，因此进程重启、甚至进程被 kill -9 后，数据状态
仍然可预期。

- 包代码：`kvstore/`（`records.py` 帧格式、`wal.py` 日志、`store.py` 引擎、
  `api.py` HTTP 层、`errors.py` 错误码）
- 入口：`main.py`
- 测试：`tests/`，覆盖并发争用、TTL 到期、重启恢复、损坏数据四类场景，
  使用 `-s` 打印操作序列、输入与断言依据

## 1. 快速开始 / 本地验证

```bash
uv sync                         # 安装 fastapi + pytest(dev)
uv run pytest -v                # 运行全部测试并打印操作日志
uv run python main.py           # 启动服务（KV_DATA_DIR 默认 ./.kvdata）
KV_DATA_DIR=/tmp/kv KV_CAPACITY_PER_NAMESPACE=100 uv run python main.py
```

运行时数据只有一个文件：`$KV_DATA_DIR/wal.log`。

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
字段类型。分两类结果：

1. **可恢复的未确认后缀（自动修复 + 显式报告，不静默跳过）**
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

恢复报告示例：

```json
{"frames_replayed": 7, "truncated": null, "namespaces": 2, "entries": 5}
```

## 5. 并发约束

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

## 6. 测试场景与可复现性

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

## 7. 设计边界

- 单 WAL 文件、无压缩/分段（本服务规模足够；日志随每次变更线性增长）。
- 索引驻留内存，容量是主要的内存上限。
- 依赖 `fsync` 的崩溃语义由本地文件系统提供；网络盘 / 不带写屏障的介质不
  在保证范围内。
