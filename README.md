# Persistent KV Cache Engine (Python / FastAPI)

本地可持久化的命名空间键值存储与过期缓存：支持 TTL、显式删除/自然过期的
可区分原因、版本号 CAS 条件写、原子批量、容量硬上限（不静默丢数据），
以及基于 WAL + CRC + 快照的崩溃恢复与损坏检测。仅依赖本地磁盘与仓库已有依赖。

## 快速开始

```bash
uv sync
uv run python main.py            # 默认数据目录 ./data
# 自定义：KV_DATA_DIR=/tmp/kv KV_MAX_ENTRIES_PER_NS=100 uv run python main.py
```

## 运行测试

```bash
uv run python -m unittest discover -s tests -v
```

覆盖四类场景：并发争用（CAS 无丢失更新）、TTL 到期、进程重启恢复、损坏/截断数据。

## 文档

详见 [docs/STORAGE.md](docs/STORAGE.md)：存储与恢复语义、过期与容量规则、
并发约束、WAL/快照格式、错误码与本地验证方法。

## 接口速览

- `GET    /v1/namespaces/{ns}/keys/{key}`
- `PUT    /v1/namespaces/{ns}/keys/{key}`（body: `value`, `ttl_ms?`, `expected_version?`）
- `DELETE /v1/namespaces/{ns}/keys/{key}`（body 可选 `expected_version`）
- `POST   /v1/batch`（原子批量，全有或全无）
- `GET    /v1/namespaces/{ns}/stats`
- `GET    /v1/recovery`
