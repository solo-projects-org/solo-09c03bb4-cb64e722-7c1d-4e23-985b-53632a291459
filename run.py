"""统一启动入口：ROLE=hospital 启动院端，ROLE=coordinator 启动协调端。

注意：环境变量必须在 import app.* 之前就绪，因为 app.config 在导入时即读取配置。
"""
from __future__ import annotations

import os


def main() -> None:
    # 优先 SERVICE_ROLE；兼容 ROLE（部分环境 ROLE 可能被预置为空串）
    role = (os.environ.get("SERVICE_ROLE") or os.environ.get("ROLE") or "hospital").lower()
    port = int(os.environ.get("PORT", "8000"))
    if role == "hospital":
        from app.hospital.app import app  # noqa: F401
    elif role == "coordinator":
        from app.coordinator.app import app  # noqa: F401
    else:
        raise SystemExit(f"未知 ROLE={role}，应为 hospital 或 coordinator")

    import uvicorn

    uvicorn.run(
        app,
        host=os.environ.get("HOST", "0.0.0.0"),
        port=port,
        log_level=os.environ.get("LOG_LEVEL", "info"),
    )


if __name__ == "__main__":
    main()
