"""服务入口：uvicorn main:app。

可通过环境变量配置：
KV_DATA_DIR / KV_MAX_ENTRIES_PER_NS / KV_MAX_VALUE_BYTES
"""

import os

import uvicorn

from kvapp.api import create_app

app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        app,
        host=os.environ.get("KV_HOST", "127.0.0.1"),
        port=int(os.environ.get("KV_PORT", "8000")),
    )
