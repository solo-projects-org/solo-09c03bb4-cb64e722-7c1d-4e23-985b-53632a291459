"""协调端 FastAPI 应用：发起研究、查结果/报告、故障模拟。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException

from ..config import UNIT_MMOL_PER_L, coordinator_config
from ..models import (
    BatchCreateRequest,
    BatchListResponse,
    BatchResponse,
    HospitalSnapshot,
    SimConfigRequest,
)
from . import db as dbmod
from .worker import CoordinatorWorker, new_batch_id

cfg = coordinator_config()
database: Optional[dbmod.CoordinatorDB] = None
worker: Optional[CoordinatorWorker] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global database, worker
    database = dbmod.CoordinatorDB(cfg.db_path)
    worker = CoordinatorWorker(database)
    await worker.start()
    yield
    # 协调端退出：在飞批次的院端名额自动回收
    await worker.stop()
    database.close()


app = FastAPI(
    title="多中心空腹血糖研究 - 协调端",
    version="1.0.0",
    description=(
        "按日期范围发起两院研究：两院各固定样本与版号，协调端仅取样本数与 "
        "<= 指定值计数，二分求合并中位数。每院仅一个快照名额，批次结束或协调端退出自动回收。"
    ),
    lifespan=lifespan,
)


def _batch_response(row) -> BatchResponse:
    snapshots = []
    for h in database.hospital_rows(row["batch_id"]):
        if h["acquired"]:
            snapshots.append(
                HospitalSnapshot(
                    hospital_id=h["hospital_id"],
                    version_tag=h["version_tag"],
                    sample_size=h["sample_size"],
                )
            )
    median_value = row["median_value"]
    median_mmol = row["median_mmol_l"]
    if median_value is not None and median_mmol is None:
        median_mmol = round(median_value * UNIT_MMOL_PER_L, 2)
    return BatchResponse(
        batch_id=row["batch_id"],
        date_from=row["date_from"],
        date_to=row["date_to"],
        status=row["status"],
        failure_reason=row["failure_reason"],
        median_value=median_value,
        median_mmol_l=median_mmol,
        pooled_sample_size=row["pooled_sample_size"],
        snapshots=snapshots,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@app.get("/health", tags=["运维"])
async def health():
    return {"status": "ok", "service": "coordinator"}


@app.post("/batches", response_model=BatchResponse, status_code=201, tags=["研究批次"])
async def create_batch(req: BatchCreateRequest):
    if req.date_from > req.date_to:
        raise HTTPException(status_code=422, detail="date_from 不得晚于 date_to")
    batch_id = new_batch_id()
    database.create_batch(
        batch_id,
        req.date_from.isoformat(),
        req.date_to.isoformat(),
        cfg.hospital_a_base_url,
        cfg.hospital_b_base_url,
    )
    # 内嵌模拟：在批次入队前落库，worker 首次调度即下发，杜绝竞态
    if req.simulation is not None:
        sim = req.simulation
        database.upsert_sim(
            batch_id,
            sim.hospital,
            sim.phase,
            sim.delay_seconds,
            sim.fail_status,
            int(sim.lose_receipt_once),
            int(sim.expire_now),
            int(sim.persistent),
        )
    row = database.get_batch(batch_id)
    return _batch_response(row)


@app.get("/batches", response_model=BatchListResponse, tags=["研究批次"])
async def list_batches():
    return BatchListResponse(
        batches=[_batch_response(r) for r in database.list_batches()]
    )


@app.get("/batches/{batch_id}", response_model=BatchResponse, tags=["研究批次"])
async def get_batch(batch_id: str):
    row = database.get_batch(batch_id)
    if row is None:
        raise HTTPException(status_code=404, detail="批次不存在")
    return _batch_response(row)


@app.get("/batches/{batch_id}/rounds", tags=["研究批次"])
async def get_rounds(batch_id: str):
    if database.get_batch(batch_id) is None:
        raise HTTPException(status_code=404, detail="批次不存在")
    return [dict(r) for r in database.list_rounds(batch_id)]


@app.get("/reports", tags=["报告"])
async def list_reports():
    return [dict(r) for r in database.list_reports()]


@app.get("/reports/{batch_id}", tags=["报告"])
async def get_report(batch_id: str):
    row = database.get_report(batch_id)
    if row is None:
        raise HTTPException(status_code=404, detail="该批次暂无成功报告")
    return dict(row)


@app.put("/sim/config", tags=["故障模拟"])
async def configure_simulation(req: SimConfigRequest):
    """在 Swagger 中模拟：院端延迟、明确失败、回执丢失、样本过期。

    规则在批次被调度前下发到两院；一次性规则命中后自动失效，
    因此"回执丢失后原批重试"可找回原样本继续完成。
    """
    if database.get_batch(req.batch_id) is None:
        raise HTTPException(status_code=404, detail="批次不存在，请先 POST /batches")
    database.upsert_sim(
        req.batch_id,
        req.hospital,
        req.phase,
        req.delay_seconds,
        req.fail_status,
        int(req.lose_receipt_once),
        int(req.expire_now),
        int(req.persistent),
    )
    return {
        "message": "模拟配置已保存，将在批次下一次调度时下发",
        "config": req.model_dump(),
    }


@app.get("/sim/config", tags=["故障模拟"])
async def list_sim_config():
    return [dict(r) for r in database.list_sim()]
