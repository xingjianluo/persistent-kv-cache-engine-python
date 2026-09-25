# 存储与恢复语义说明

本服务在 FastAPI 内实现了一套**仅依赖本地磁盘**的可持久化键值存储与过期缓存。
除仓库已有依赖（FastAPI/Starlette/Pydantic）外不引入任何数据库或第三方存储。

- 代码：`kvapp/wal.py`（帧编解码/恢复）、`kvapp/engine.py`（状态机与持久化）、
  `kvapp/api.py`（HTTP 路由）
- 数据目录：默认 `./data`，可用环境变量 `KV_DATA_DIR` 覆盖。

---

## 1. 数据模型

数据按 **命名空间（namespace）** 隔离，键为非空字符串，值为任意 JSON。

每个存活键记录：

| 字段 | 含义 |
| --- | --- |
| `value` | JSON 值 |
| `version` | 单调递增的整数版本号，初始写入为 1 |
| `expires_at_ns` | 绝对过期时刻（单调时钟纳秒），`null` 表示不过期 |

对一个键，系统始终能区分四种对外状态：

- `visible`：存活可见；
- `expired`：TTL 自然到期，对外不可见（**不是** not_found）；
- `deleted`：被显式删除（墓碑），对外不可见（**不是** expired）；
- `not_found`：从未存在。

### 版本号规则

- 存活键每次成功写入，版本号 `+1`。
- TTL **自然过期后重建**：版本从 1 重新开始（上一段生命已结束）。
- **显式删除后重建**：版本沿该键历史继续（删除也产生一个墓碑版本），
  因此同一键的版本号不会因“删了再建”而重复，CAS 判定不会被“墓碑—重建”绕开。

---

## 2. TTL 过期规则

- 写入时传 `ttl_ms`（正整数），换算为绝对过期时刻 `expires_at_ns` 并随记录持久化。
- 过期是**惰性（lazy）**判定：读取/写入/容量计算时按当前时钟比较
  `now >= expires_at_ns`，到期键立即对外不可见。
- 过期键**不占命名空间容量**，到期后写入新键可复用其名额。
- 过期不产生墓碑，因此 `expired` 与 `deleted`/`not_found` 是不同的错误原因。
- TTL 以绝对时间戳落盘，**进程重启后过期语义保持一致**：重启不会延长也不会提前。

---

## 3. 容量与大小限制（不静默丢失数据）

- `max_entries_per_namespace`：每个命名空间的**存活键数量**上限
  （默认 10000，可用 `KV_MAX_ENTRIES_PER_NS` 配置）。
- `max_value_bytes`：单个值的 UTF-8 JSON 字节上限
  （默认 1 MiB，可用 `KV_MAX_VALUE_BYTES` 配置）。

超限时的规则是**明确拒绝，绝不静默淘汰**：

- 新增键会使存活数超过上限 → 整笔写入/批次被拒绝，错误码
  `capacity_exceeded`（HTTP 507），**已确认数据一条不丢**。
- 覆盖已有键不新增名额；同批“先删后建同一键”净增量为 0，不会被误拒。
- 值过大 → `value_too_large`（HTTP 413）。
- 系统**不做 LRU/自动淘汰**：容量是硬约束，把“保留什么”留给调用方决定，
  从而不会出现“写成功但旧数据被悄悄换掉”的情况。

---

## 4. 并发约束与原子更新

- 服务为**单进程、多协程**设计；同一数据目录以 `lock` 文件
  （`flock` 排他锁）互斥，第二个进程会立即得到 `EngineInUseError`，
  避免双开写坏 WAL。
- 所有写操作走**单一写者锁**：每个提交是一个不可分割的临界区
  `prepare(完整 dry-run) → 写 WAL 并 fsync → 更新内存`。
- 条件写（CAS）：请求可带 `expected_version`。
  - 键可见时，它必须等于当前版本；
  - 键不可见时版本视为 0，可显式传 `expected_version=0` 做“仅当不存在/已失效时创建”；
  - 不满足则拒绝，错误码 `version_mismatch`（HTTP 409），响应体
    `details` 给出 `expected_version / current_version / state`，
    其中 `state` 为 `visible|expired|deleted|not_found`。
- 并发写同一键：后到者若持过期版本会被明确拒绝（可由调用方读取新版本后重试），
  **不会互相覆盖、不会丢失更新**。
- 批量写 `POST /v1/batch` 为**全有或全无**：先对整批做 dry-run，任何一个操作
  的前置条件、容量或大小不满足，则整批拒绝、不产生任何帧；成功时每步结果
  （按 `index` 的版本号）在一个事务里一次落盘。

---

## 5. 持久化格式与提交协议

数据目录文件：

| 文件 | 作用 |
| --- | --- |
| `wal.log` | 追加写预写日志，每个提交 fsync |
| `snapshot.kvs` | 周期性压缩快照（原子 rename 生成） |
| `wal.old` | 快照轮转瞬间的旧 WAL，下一次提交成功后删除 |
| `lock` | 单进程排他锁 |

帧格式（小端序）：

```
magic(4)=KVRW | seq(8) | payload_len(4) | kind(1) | payload(JSON, UTF-8) | crc32(4)
# crc32 覆盖 seq + payload_len + kind + payload
```

一个提交在 WAL 中是一个事务：

```
BEGIN{txn,n}  PUT/DELETE...(携带 txn)  COMMIT{txn}
```

确认点是 **COMMIT 帧 flush + fsync 完成**。在这之前崩溃，恢复时该事务
（BEGIN 后没有配对 COMMIT）整体丢弃。

### 快照与轮转（崩溃安全）

1. 写临时 `snapshot.kvs.tmp` → fsync；
2. `os.replace` 原子替换 `snapshot.kvs` → fsync 目录；
3. 关闭并把 `wal.log` 改名为 `wal.old`，新建空 `wal.log` → fsync 目录；
4. 之后第一次成功提交后删除 `wal.old`。

崩溃发生在任意一步都可恢复：有快照时只把**快照之后**的新 `wal.log`
叠加到快照之上；`wal.old` 已被快照涵盖，绝不再重放（否则会重复旧状态）。

---

## 6. 崩溃恢复与损坏处理

启动时依次加载快照与 WAL：

- **已确认写入**（含值、绝对过期时间、版本号、墓碑）完整恢复；
- 帧流末尾不完整/CRC 不符的帧视为崩溃时的半截写（torn tail），
  丢弃其尾部字节并在恢复报告 `torn_tail_bytes` / `warnings` 中体现；
- 没有 COMMIT 的未闭合事务整体丢弃，计入 `discarded_incomplete_txns`；
- **中间损坏绝不静默跳过**：若损坏位置之后还能重同步出一串完好到 EOF 的帧
  （用 magic + CRC 重同步探测判定），抛 `CorruptLogError`，拒绝启动并给出
  可识别错误，而不是截断后继续运行；
- 快照要求严格完整：任何截断/CRC 错误都抛 `CorruptLogError`
  （快照是原子替换生成的，截断不可能来自正常崩溃，只能是介质/人为损坏）。

可通过 `GET /v1/recovery` 查看本次启动的恢复报告（帧数、已提交事务数、
丢弃的未闭合事务数、torn 字节数、告警等）。

---

## 7. HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `GET /health` | 健康检查 |
| `GET /v1/namespaces/{ns}/keys/{key}` | 读取（返回 value/version/expires_at_ns） |
| `PUT /v1/namespaces/{ns}/keys/{key}` | 写入，body：`value`、`ttl_ms?`、`expected_version?` |
| `DELETE /v1/namespaces/{ns}/keys/{key}` | 删除，body 可选 `expected_version` |
| `POST /v1/batch` | 批量，body：`{"ops":[{op,ns,key,value?,ttl_ms?,expected_version?}]}` |
| `GET /v1/namespaces/{ns}/stats` | 存活数/墓碑数/容量/WAL 字节等 |
| `GET /v1/recovery` | 本次启动恢复报告 |

错误统一形如：

```json
{"error": {"code": "version_mismatch",
           "message": "ops[0] expected version 9 but current is 1",
           "details": {"index": 0, "expected_version": 9,
                       "current_version": 1, "state": "visible"}}}
```

错误码：`not_found` / `expired` / `deleted` / `version_mismatch` /
`capacity_exceeded` / `value_too_large` / `bad_request` / `corrupt_log`。

---

## 8. 本地验证

```bash
# 安装依赖（仓库自带 uv.lock）
uv sync

# 运行全部单测（含并发争用、TTL 到期、重启恢复、损坏数据四类）
uv run python -m unittest discover -s tests -v

# 启动服务
KV_DATA_DIR=./data uv run python main.py
# 或 uvicorn main:app --reload
```

快速手工验证：

```bash
curl -s -XPUT localhost:8000/v1/namespaces/demo/keys/a \
  -H 'content-type: application/json' \
  -d '{"value":{"x":1},"ttl_ms":2000}'
curl -s localhost:8000/v1/namespaces/demo/keys/a
sleep 2
curl -i localhost:8000/v1/namespaces/demo/keys/a   # 404, error.code=expired
curl -s localhost:8000/v1/recovery
```

测试覆盖对应关系：

| 场景 | 测试文件 |
| --- | --- |
| 并发争用 / CAS 无丢失更新 / 并发容量 | `tests/test_concurrency.py` |
| TTL 到期、过期/删除/不存在区分、HTTP 语义 | `tests/test_state.py`, `tests/test_api.py` |
| 重启恢复（值/TTL/版本/墓碑）、快照增量合并 | `tests/test_persistence.py`, `tests/test_api.py` |
| torn tail、未闭合事务、WAL 中部损坏、快照截断 | `tests/test_persistence.py`, `tests/test_api.py` |

测试在 INFO 级别打印每个操作序列、输入与判定依据，
时间相关用例使用固定基准时钟或显式 sleep，断言可复现。
