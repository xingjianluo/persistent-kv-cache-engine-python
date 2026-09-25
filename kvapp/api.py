"""FastAPI 路由：键值读写、条件写、批量写与恢复状态查询。

错误原因（``error.code``）可区分：

``not_found`` / ``expired`` / ``deleted`` / ``version_mismatch`` /
``capacity_exceeded`` / ``value_too_large`` / ``bad_request`` /
``corrupt_log``。
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from kvapp.engine import Engine, KVError
from kvapp.wal import CorruptLogError

log = logging.getLogger("kvapp.api")


class PutBody(BaseModel):
    value: Any = None
    ttl_ms: int | None = Field(default=None, gt=0)
    expected_version: int | None = Field(default=None, ge=0)


class DeleteBody(BaseModel):
    expected_version: int | None = Field(default=None, ge=0)


class BatchOp(BaseModel):
    op: str
    ns: str = Field(min_length=1)
    key: str = Field(min_length=1)
    value: Any = None
    ttl_ms: int | None = Field(default=None, gt=0)
    expected_version: int | None = Field(default=None, ge=0)


class BatchBody(BaseModel):
    ops: list[BatchOp] = Field(min_length=1)


def create_app(
    data_dir: str | None = None,
    *,
    max_entries_per_namespace: int | None = None,
    max_value_bytes: int | None = None,
    compaction_threshold_bytes: int | None = None,
) -> FastAPI:
    data_dir = data_dir or os.environ.get("KV_DATA_DIR", "data")
    max_entries_per_namespace = (
        max_entries_per_namespace
        if max_entries_per_namespace is not None
        else int(os.environ.get("KV_MAX_ENTRIES_PER_NS", "10000"))
    )
    max_value_bytes = (
        max_value_bytes
        if max_value_bytes is not None
        else int(os.environ.get("KV_MAX_VALUE_BYTES", str(1 << 20)))
    )
    compaction_threshold_bytes = (
        compaction_threshold_bytes
        if compaction_threshold_bytes is not None
        else int(os.environ.get("KV_COMPACTION_BYTES", str(2 << 20)))
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.engine = Engine(
            data_dir,
            max_entries_per_namespace=max_entries_per_namespace,
            max_value_bytes=max_value_bytes,
            compaction_threshold_bytes=compaction_threshold_bytes,
        )
        log.info("engine opened at %s: %s", data_dir,
                 app.state.engine.recovery_report().as_dict())
        try:
            yield
        finally:
            await app.state.engine.close()

    app = FastAPI(title="Persistent KV Cache Engine", lifespan=lifespan)

    @app.exception_handler(KVError)
    async def kv_error_handler(request, exc: KVError):
        log.info("reject %s %s code=%s details=%r",
                 request.method, request.url.path, exc.code, exc.details)
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": {"code": exc.code, "message": str(exc),
                               "details": exc.details}},
        )

    @app.exception_handler(CorruptLogError)
    async def corrupt_handler(request, exc: CorruptLogError):
        log.error("corrupt log on %s %s: %s",
                  request.method, request.url.path, exc)
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "corrupt_log", "message": str(exc)}},
        )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/namespaces/{ns}/keys/{key}")
    async def get_key(ns: str, key: str):
        engine: Engine = app.state.engine
        value, entry = await engine.get(ns, key)
        return {"namespace": ns, "key": key, "value": value,
                "version": entry.version, "expires_at_ns": entry.expires_at_ns}

    @app.put("/v1/namespaces/{ns}/keys/{key}")
    async def put_key(ns: str, key: str, body: PutBody):
        engine: Engine = app.state.engine
        log.info("PUT ns=%s key=%s ttl_ms=%r expected_version=%r",
                 ns, key, body.ttl_ms, body.expected_version)
        version = await engine.put(
            ns, key, body.value,
            ttl_ms=body.ttl_ms, expected_version=body.expected_version,
        )
        return {"namespace": ns, "key": key, "version": version,
                "applied": True}

    @app.delete("/v1/namespaces/{ns}/keys/{key}")
    async def delete_key(ns: str, key: str, body: DeleteBody | None = None):
        engine: Engine = app.state.engine
        expected = body.expected_version if body is not None else None
        log.info("DELETE ns=%s key=%s expected_version=%r", ns, key, expected)
        version = await engine.delete(ns, key, expected_version=expected)
        return {"namespace": ns, "key": key, "version": version,
                "applied": True, "reason": "deleted"}

    @app.post("/v1/batch")
    async def batch(body: BatchBody):
        engine: Engine = app.state.engine
        ops = [m.model_dump(exclude_none=True) for m in body.ops]
        log.info("BATCH ops=%r", ops)
        results = await engine.batch(ops)
        return {"applied": True, "results": results}

    @app.get("/v1/namespaces/{ns}/stats")
    async def ns_stats(ns: str):
        return await app.state.engine.stats(ns)

    @app.get("/v1/recovery")
    async def recovery():
        return app.state.engine.recovery_report().as_dict()

    return app
