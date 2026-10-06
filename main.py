"""FastAPI entrypoint for the local persistent KV cache.

Environment variables
----------------------
KV_DATA_DIR                 directory holding ``wal.log`` (default ./.kvdata)
KV_CAPACITY_PER_NAMESPACE   max live keys per namespace (default 10000)
KV_COMPACT_ON_STARTUP       "1"/"true" compacts the WAL right after recovery
KV_READ_ONLY                "1"/"true" opens without the writer lock and
                            rejects all mutations (health checks unaffected)
"""

from __future__ import annotations

import contextlib
import os

import uvicorn
from fastapi import FastAPI

from kvstore.api import create_app
from kvstore.store import KVStore


def _flag(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes", "on")


def build_app() -> FastAPI:
    data_dir = os.environ.get("KV_DATA_DIR", ".kvdata")
    capacity = int(os.environ.get("KV_CAPACITY_PER_NAMESPACE", "10000"))
    store = KVStore(
        data_dir=data_dir,
        capacity_per_namespace=capacity,
        read_only=_flag("KV_READ_ONLY"),
        compact_on_startup=_flag("KV_COMPACT_ON_STARTUP"),
    )

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            yield
        finally:
            store.close()

    return create_app(store, lifespan=lifespan)


def create_default_app() -> FastAPI:
    return build_app()


app = build_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
