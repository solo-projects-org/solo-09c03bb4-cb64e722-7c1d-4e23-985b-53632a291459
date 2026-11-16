"""院端 FastAPI 应用：CSV 整版导入、快照名额、只暴露样本数与计数。"""
from __future__ import annotations

import asyncio
import sqlite3
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse

from ..config import hospital_config
from ..models import (
    AcquireRequest,
    CountResponse,
    ReleaseRequest,
    ReleaseResponse,
    SimRule,
    SnapshotInfo,
    VersionInfo,
)
from .db import ERR_SLOT_BUSY, HospitalDB, HospitalError
from .importer import CsvImportError, parse_csv

cfg = hospital_config()
db: Optional[HospitalDB] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    db = HospitalDB(cfg)
    yield
    db.close()


app = FastAPI(
    title=f"院端服务 - {cfg.hospital_id}",
    version="1.0.0",
    description=(
        "多中心空腹血糖研究院端。整版导入 CSV（检验号/日期/0.1mmol/L 整数血糖值），"
        "为研究批次固定样本与版号；协调端只能取样本数与 <= 指定值计数。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(HospitalError)
async def hospital_error_handler(_: Request, exc: HospitalError):
    return JSONResponse(
        status_code=exc.http_status,
        content={"error": exc.code, "message": exc.message},
        headers={"X-Hospital-Id": cfg.hospital_id},
    )


def _run(blocking, *args):
    """把阻塞式 SQLite 调用交给线程池，避免卡住事件循环。"""
    return asyncio.to_thread(blocking, *args)


async def _apply_sim(batch_id: str, phase: str) -> None:
    """应用故障注入规则（延迟 / 显式失败 / 样本过期），按阶段匹配。"""
    rule = await _run(db.pop_sim_rule, batch_id, phase)
    if rule is None:
        return
    if rule["delay_seconds"] > 0:
        await asyncio.sleep(rule["delay_seconds"])
    if rule["expire_now"]:
        await _run(db.expire_snapshot, batch_id)
    if rule["fail_status"]:
        raise HTTPException(
            status_code=int(rule["fail_status"]),
            detail={
                "error": "SIMULATED_FAILURE",
                "message": f"模拟院端明确失败 HTTP {rule['fail_status']}（phase={phase}）",
            },
        )


def _snapshot_info(row: sqlite3.Row) -> SnapshotInfo:
    return SnapshotInfo(
        batch_id=row["batch_id"],
        snapshot_id=row["snapshot_id"],
        generation=row["generation"],
        version_id=row["version_id"],
        version_tag=db.version_tag_of(row["version_id"]),
        sample_size=row["sample_size"],
        date_from=row["date_from"],
        date_to=row["date_to"],
        expires_at=row["expires_at"],
        status=row["status"],
    )


# ----------------------------- 导入与版本 -----------------------------

@app.post("/imports", status_code=201, tags=["数据导入"])
async def import_csv(file: UploadFile = File(..., description="整版 CSV，表头 test_id,date,value")):
    content = await file.read()
    try:
        rows = parse_csv(content)
    except CsvImportError as exc:
        raise HTTPException(status_code=422, detail={"error": "BAD_CSV", "message": str(exc)})
    latest = await _run(db.latest_version)
    next_version = (latest["version_id"] + 1) if latest else 1
    version_tag = f"v{next_version}"
    try:
        version_id = await _run(db.import_version, version_tag, rows)
    except HospitalError as exc:
        raise HTTPException(status_code=exc.http_status, detail={"error": exc.code, "message": exc.message})
    return {
        "hospital_id": cfg.hospital_id,
        "version_id": version_id,
        "version_tag": version_tag,
        "row_count": len(rows),
        "message": "整版导入成功；在研批次使用各自固定快照，不受新版本影响",
    }


@app.get("/versions", response_model=list[VersionInfo], tags=["数据导入"])
async def get_versions():
    rows = await _run(db.list_versions)
    return [
        VersionInfo(
            version_id=r["version_id"],
            version_tag=r["version_tag"],
            imported_at=r["imported_at"],
            row_count=r["row_count"],
        )
        for r in rows
    ]


# ----------------------------- 研究批次接口（协调端调用） -----------------------------

@app.post("/snapshots", response_model=SnapshotInfo, tags=["研究批次"])
async def acquire_snapshot(req: AcquireRequest):
    await _apply_sim(req.batch_id, "acquire")
    try:
        row = await _run(
            db.acquire_snapshot, req.batch_id, req.date_from.isoformat(), req.date_to.isoformat()
        )
    except HospitalError as exc:
        # 名额占用语义化，方便协调端区分"忙等待"与"明确失败"
        status = 409 if exc.code == ERR_SLOT_BUSY else exc.http_status
        raise HTTPException(
            status_code=status, detail={"error": exc.code, "message": exc.message}
        )
    return _snapshot_info(row)


@app.get("/snapshots/{batch_id}/counts", response_model=CountResponse, tags=["研究批次"])
async def get_counts(batch_id: str, value: int):
    await _apply_sim(batch_id, "count")
    try:
        row = await _run(db.count_lte, batch_id, value)
    except HospitalError as exc:
        raise HTTPException(
            status_code=exc.http_status, detail={"error": exc.code, "message": exc.message}
        )
    return CountResponse(
        batch_id=batch_id,
        snapshot_id=row["snapshot_id"],
        generation=row["generation"],
        sample_size=row["sample_size"],
        value=value,
        count_lte=row["count_lte"],
    )


@app.post("/snapshots/{batch_id}/release", response_model=ReleaseResponse, tags=["研究批次"])
async def release_snapshot(batch_id: str, req: ReleaseRequest):
    if req.batch_id != batch_id:
        raise HTTPException(status_code=400, detail="路径与请求体 batch_id 不一致")
    await _apply_sim(batch_id, "release")
    released, reason = await _run(db.release_snapshot, batch_id, req.generation)
    return ReleaseResponse(released=released, reason=reason)


@app.get("/snapshots/{batch_id}", response_model=SnapshotInfo, tags=["研究批次"])
async def get_snapshot(batch_id: str):
    row = await _run(db.snapshot_row, batch_id)
    if row is None:
        raise HTTPException(status_code=404, detail={"error": "SNAPSHOT_NOT_FOUND", "message": "无此批次快照"})
    return _snapshot_info(row)


# ----------------------------- 运维 / 模拟故障 -----------------------------

@app.get("/health", tags=["运维"])
async def health():
    active = await _run(db.active_snapshot)
    return {
        "hospital_id": cfg.hospital_id,
        "status": "ok",
        "snapshot_slot": "busy" if active else "free",
        "active_batch": active["batch_id"] if active else None,
    }


@app.get("/sim/rules", tags=["故障模拟"])
async def list_sim_rules():
    rows = await _run(db.list_sim_rules)
    return [dict(r) for r in rows]


@app.put("/sim/rules", tags=["故障模拟"])
async def put_sim_rule(rule: SimRule):
    await _run(
        db.upsert_sim_rule,
        rule.batch_id,
        rule.delay_seconds,
        rule.fail_status,
        rule.expire_now,
        rule.phase,
        rule.persistent,
    )
    return {"hospital_id": cfg.hospital_id, "applied": rule.model_dump()}


@app.delete("/sim/rules/{batch_id}", tags=["故障模拟"])
async def delete_sim_rule(batch_id: str):
    await _run(db.delete_sim_rule, batch_id)
    return {"deleted": batch_id}
