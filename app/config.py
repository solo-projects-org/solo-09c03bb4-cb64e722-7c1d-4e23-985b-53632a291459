"""全局常量与运行配置（环境变量可覆盖）。

血糖值约定：CSV 中的 value 为整数 1..1000，单位 0.1 mmol/L，
即 value=55 表示 5.5 mmol/L。
"""
from __future__ import annotations

import os
from dataclasses import dataclass

# 血糖值取值域（含端点），单位 0.1 mmol/L
VALUE_MIN = 1
VALUE_MAX = 1000
UNIT_MMOL_PER_L = 0.1


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


@dataclass(frozen=True)
class HospitalConfig:
    """院端服务配置。"""

    hospital_id: str
    db_path: str
    snapshot_ttl_seconds: int
    # 模拟故障默认延迟上限（秒），0 表示不模拟
    sim_default_delay: float


@dataclass(frozen=True)
class CoordinatorConfig:
    """协调端配置。"""

    db_path: str
    hospital_a_base_url: str
    hospital_b_base_url: str
    request_timeout: float
    max_attempts: int
    poll_interval: float
    busy_wait: float
    # 协调端退出时，对未结束批次最多等待多少秒再做本地兜底
    shutdown_grace: float


def hospital_config() -> HospitalConfig:
    return HospitalConfig(
        hospital_id=os.environ.get("HOSPITAL_ID", "unknown-hospital"),
        db_path=os.environ.get("HOSPITAL_DB", "data/runtime/hospital.sqlite3"),
        snapshot_ttl_seconds=_int_env("SNAPSHOT_TTL_SECONDS", 3600),
        sim_default_delay=_float_env("SIM_DEFAULT_DELAY", 0.0),
    )


def coordinator_config() -> CoordinatorConfig:
    return CoordinatorConfig(
        db_path=os.environ.get("COORDINATOR_DB", "data/runtime/coordinator.sqlite3"),
        hospital_a_base_url=os.environ.get("HOSPITAL_A_URL", "http://localhost:8101"),
        hospital_b_base_url=os.environ.get("HOSPITAL_B_URL", "http://localhost:8102"),
        request_timeout=_float_env("COORDINATOR_TIMEOUT", 10.0),
        max_attempts=_int_env("COORDINATOR_MAX_ATTEMPTS", 3),
        poll_interval=_float_env("COORDINATOR_POLL_INTERVAL", 0.2),
        busy_wait=_float_env("COORDINATOR_BUSY_WAIT", 0.5),
        shutdown_grace=_float_env("COORDINATOR_SHUTDOWN_GRACE", 5.0),
    )
