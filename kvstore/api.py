"""FastAPI HTTP surface for :class:`KVStore`.

Routes
------
GET    /healthz
GET    /v1/namespaces/{ns}/keys/{key}
PUT    /v1/namespaces/{ns}/keys/{key}
DELETE /v1/namespaces/{ns}/keys/{key}
POST   /v1/batch
GET    /v1/namespaces
GET    /v1/admin/recovery-report

Domain failures never return 500; they carry a stable ``error.code`` so
clients can branch on ``not_found`` / ``deleted`` / ``expired`` /
``version_mismatch`` / ``key_exists`` / ``capacity_exceeded`` /
``corrupt_log``.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import CorruptLogError, KVError
from .store import KVStore

_STATUS_BY_CODE = {
    "not_found": 404,
    "deleted": 404,
    "expired": 404,
    "version_mismatch": 409,
    "key_exists": 409,
    "capacity_exceeded": 507,
    "corrupt_log": 500,
    "truncated_frame": 500,
}


class PutBody(BaseModel):
    value: object = None
    ttl_ms: int | None = Field(default=None, ge=1)
    expected_version: int | None = Field(default=None, ge=0)
    if_not_exists: bool = False


class Mutation(BaseModel):
    op: str
    namespace: str = Field(min_length=1, max_length=4096)
    key: str = Field(min_length=1, max_length=4096)
    value: object = None
    ttl_ms: int | None = Field(default=None, ge=1)
    expected_version: int | None = Field(default=None, ge=0)
    if_not_exists: bool = False


class BatchBody(BaseModel):
    mutations: list[Mutation] = Field(min_length=1)


def create_app(store: KVStore, lifespan=None) -> FastAPI:
    app = FastAPI(title="Persistent KV Cache", version="0.1.0", lifespan=lifespan)
    app.state.store = store

    @app.exception_handler(KVError)
    async def _kv_error_handler(request: Request, exc: KVError):
        status = _STATUS_BY_CODE.get(exc.code, 400)
        body: dict = {"error": {"code": exc.code, "message": str(exc)}}
        if isinstance(exc, CorruptLogError):
            body["error"]["detail"] = {
                "reason": exc.reason,
                "position": exc.position,
            }
        return JSONResponse(status_code=status, content=body)

    @app.exception_handler(ValueError)
    async def _value_error_handler(request: Request, exc: ValueError):
        return JSONResponse(
            status_code=422,
            content={"error": {"code": "invalid_request", "message": str(exc)}},
        )

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/v1/namespaces/{namespace}/keys/{key:path}")
    def get_key(namespace: str, key: str):
        return store.get(namespace, key)

    @app.put("/v1/namespaces/{namespace}/keys/{key:path}")
    def put_key(namespace: str, key: str, body: PutBody):
        return store.put(
            namespace,
            key,
            body.value,
            ttl_ms=body.ttl_ms,
            expected_version=body.expected_version,
            if_not_exists=body.if_not_exists,
        )

    @app.delete("/v1/namespaces/{namespace}/keys/{key:path}")
    def delete_key(namespace: str, key: str):
        return store.delete(namespace, key)

    @app.post("/v1/batch")
    def post_batch(body: BatchBody):
        return store.batch([m.model_dump() for m in body.mutations])

    @app.get("/v1/namespaces")
    def list_namespaces():
        # Snapshot under the store lock for a consistent view.
        with store._lock:
            now = store.clock_ms()
            out = []
            for ns, table in store._tables.items():
                live = sum(1 for st in table.values() if store._is_live(st, now))
                out.append({
                    "namespace": ns,
                    "live_keys": live,
                    "tombstones": sum(1 for st in table.values() if st.deleted),
                    "capacity": store.capacity_per_namespace,
                })
            return {"namespaces": out}

    @app.get("/v1/admin/recovery-report")
    def recovery_report():
        return store.last_recovery

    return app
