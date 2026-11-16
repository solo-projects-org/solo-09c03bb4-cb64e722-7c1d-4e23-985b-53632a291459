"""Pydantic 模型：院端与协调端的请求/响应结构。"""
from __future__ import annotations

from datetime import date
from typing import Literal, Optional

from pydantic import BaseModel, Field

from .config import VALUE_MAX, VALUE_MIN


# ----------------------------- 院端模型 -----------------------------

class AcquireRequest(BaseModel):
    batch_id: str = Field(..., min_length=1, max_length=64, description="研究批次 ID")
    date_from: date = Field(..., description="纳入起始日期（含）")
    date_to: date = Field(..., description="纳入结束日期（含）")


class SnapshotInfo(BaseModel):
    batch_id: str
    snapshot_id: int
    generation: int = Field(..., description="快照名额代次，释放时用于防止旧批迟到释放")
    version_id: int
    version_tag: str
    sample_size: int
    date_from: date
    date_to: date
    expires_at: str
    status: Literal["active", "expired", "released"]


class CountResponse(BaseModel):
    batch_id: str
    snapshot_id: int
    generation: int
    sample_size: int
    value: int = Field(..., ge=VALUE_MIN, le=VALUE_MAX)
    count_lte: int


class ReleaseRequest(BaseModel):
    batch_id: str
    generation: int = Field(..., description="获取名额时返回的代次；旧批次迟到释放会被拒绝")


class ReleaseResponse(BaseModel):
    released: bool
    reason: Optional[str] = None


class VersionInfo(BaseModel):
    version_id: int
    version_tag: str
    imported_at: str
    row_count: int


class SimRule(BaseModel):
    """针对某批次的院端故障注入规则（Swagger 可直接操作）。

    - phase：规则生效阶段（acquire/count/release/all）
    - delay_seconds：处理该批次请求前先 sleep（模拟网络/处理延迟）
    - fail_status：非空时直接返回该 HTTP 状态码（如 500/503）
    - expire_now：对该批次的快照立即标记过期（模拟样本过期）
    - persistent=False（默认）：命中一次后自动失效（用于"回执丢失后重试恢复"）；
      persistent=True：每次该阶段请求都生效（用于迟到响应场景）
    """

    batch_id: str
    phase: Literal["acquire", "count", "release", "all"] = "all"
    delay_seconds: float = 0.0
    fail_status: Optional[int] = None
    expire_now: bool = False
    persistent: bool = False


# ----------------------------- 协调端模型 -----------------------------

class EmbeddedSim(BaseModel):
    hospital: Literal["A", "B", "both"] = "both"
    phase: Literal["acquire", "count", "release", "all"] = "all"
    delay_seconds: float = 0.0
    fail_status: Optional[int] = None
    lose_receipt_once: bool = False
    expire_now: bool = False
    persistent: bool = False


class BatchCreateRequest(BaseModel):
    date_from: date
    date_to: date
    simulation: Optional[EmbeddedSim] = Field(
        default=None,
        description="可选：发起时内嵌故障注入，保证在首次调度前完成下发，避免竞态",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [{"date_from": "2026-01-01", "date_to": "2026-03-31"}]
        }
    }


class HospitalSnapshot(BaseModel):
    hospital_id: str
    version_tag: str
    sample_size: int


class BatchResponse(BaseModel):
    batch_id: str
    date_from: date
    date_to: date
    status: str
    failure_reason: Optional[str] = None
    median_value: Optional[float] = Field(
        default=None,
        description="合并中位数（0.1 mmol/L 整数单位）；偶数样本为中间两值平均，可能带 .5",
    )
    median_mmol_l: Optional[float] = Field(default=None, description="中位数，单位 mmol/L")
    pooled_sample_size: Optional[int] = None
    snapshots: list[HospitalSnapshot] = Field(default_factory=list)
    created_at: str
    updated_at: str


class BatchListResponse(BaseModel):
    batches: list[BatchResponse]


class SimConfigRequest(BaseModel):
    """协调端维度的故障注入：把规则下发到两院（持久化，重启后仍在）。"""

    batch_id: str
    hospital: Literal["A", "B", "both"] = "both"
    phase: Literal["acquire", "count", "release", "all"] = "all"
    delay_seconds: float = 0.0
    fail_status: Optional[int] = None
    lose_receipt_once: bool = Field(
        default=False,
        description="True 时让该批次首次 acquire 的响应在院端处理成功后被丢弃（模拟回执丢失）",
    )
    expire_now: bool = False
    persistent: bool = False
