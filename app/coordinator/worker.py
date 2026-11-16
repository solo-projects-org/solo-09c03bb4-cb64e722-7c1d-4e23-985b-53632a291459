"""协调端工作引擎：批次调度、两院固定样本、二分计数求合并中位数、迟到响应治理。

协议要点：
- 协调端只向院端取「样本数」和「<= 指定值的计数」，拿不到任何明细。
- 多轮计数复用同一批次快照（院端按 batch_id 固定），多轮不得换样本。
- 任一院明确失败（4xx/5xx/模拟失败）或样本过期（410）=> 整批失败。
- 院端名额忙（409 SLOT_BUSY）=> 回队列等待，不算失败。
- 批次终态后释放两院名额；代次令牌防止旧批迟到回收释放新批名额。
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Optional

from ..config import VALUE_MAX, VALUE_MIN, UNIT_MMOL_PER_L, coordinator_config
from . import db as dbmod
from .hospital_client import (
    HospitalClient,
    HospitalClientError,
    SlotBusyError,
    SNAPSHOT_EXPIRED,
    SNAPSHOT_NOT_FOUND,
)

cfg = coordinator_config()


class CoordinatorWorker:
    def __init__(self, database: dbmod.CoordinatorDB):
        self.db = database
        self.client_a = HospitalClient(
            cfg.hospital_a_base_url, "A", cfg.request_timeout, cfg.max_attempts
        )
        self.client_b = HospitalClient(
            cfg.hospital_b_base_url, "B", cfg.request_timeout, cfg.max_attempts
        )
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        # 正在处理的批次：协调端退出时据此释放院端名额
        self._inflight: set[str] = set()

    async def start(self) -> None:
        await self.client_a.start()
        await self.client_b.start()
        self._stop.clear()
        # 启动恢复：上次异常退出残留的 RUNNING 批次，回收院端名额后置失败
        await self._recover_interrupted()
        self._task = asyncio.create_task(self._loop(), name="coordinator-worker")

    async def stop(self) -> None:
        """优雅停机：取消调度循环，再同步完成在飞批次的名额回收。"""
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await asyncio.wait_for(self._task, timeout=cfg.shutdown_grace)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        # 恢复/回收在飞批次：失败落库 + 带代次释放两院名额
        await self._recover_interrupted()
        await self.client_a.close()
        await self.client_b.close()

    async def _recover_interrupted(self) -> None:
        """重启恢复或优雅停机：对所有非终态批次（RUNNING/QUEUED 之外的在飞批次），
        回收其已持有的院端名额；RUNNING 批次置失败并记录原因。"""
        for row in self.db.recover_interrupted():
            batch_id = row["batch_id"]
            for h in self.db.hospital_rows(batch_id):
                if h["acquired"] and h["generation"] is not None:
                    client = self.client_a if h["hospital_id"] == "A" else self.client_b
                    try:
                        await client.release(batch_id, h["generation"])
                    except Exception:
                        pass
            self.db.fail_batch(batch_id, "协调端重启中断，批次终止并回收名额")

    # ----------------------------- 主循环 -----------------------------

    async def _loop(self) -> None:
        while not self._stop.is_set():
            row = self.db.pop_next_batch()
            if row is None:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=cfg.poll_interval)
                except asyncio.TimeoutError:
                    pass
                continue
            batch_id = row["batch_id"]
            self._inflight.add(batch_id)
            try:
                await self._process_batch(row)
            except asyncio.CancelledError:
                # 优雅停机：状态与名额回收统一交给 stop() -> _recover_interrupted()
                raise
            except Exception as exc:  # 防御性兜底：任何意外都要让批次到终态
                self.db.fail_batch(batch_id, f"协调端内部错误: {exc!r}")
                await self._safe_release(batch_id)
            finally:
                self._inflight.discard(batch_id)

    # ----------------------------- 单批次处理 -----------------------------

    async def _process_batch(self, row) -> None:
        batch_id = row["batch_id"]
        date_from, date_to = row["date_from"], row["date_to"]
        await self._push_sim_config(batch_id)

        clients = {"A": self.client_a, "B": self.client_b}

        # 1) 两院固定样本与版号（幂等：回执丢失重试会找回原快照）
        for hid in ("A", "B"):
            try:
                snap = await clients[hid].acquire(batch_id, date_from, date_to)
            except SlotBusyError:
                # 忙时新批等待：已拿到的名额保留，整批回队列稍后重试
                self.db.requeue(batch_id, f"医院 {hid} 快照名额忙，排队等待")
                await asyncio.sleep(cfg.busy_wait)
                return
            except HospitalClientError as exc:
                self.db.update_error(batch_id, str(exc))
                self.db.fail_batch(batch_id, self._fail_reason(exc))
                await self._safe_release(batch_id)
                return
            self.db.mark_acquired(
                batch_id, hid, snap.snapshot_id, snap.generation,
                snap.version_tag, snap.sample_size,
            )

        # 2) 多轮计数 + 二分求合并中位数（样本始终是刚才固定的快照）
        try:
            result = await self._compute_median(batch_id, clients)
        except HospitalClientError as exc:
            self.db.update_error(batch_id, str(exc))
            self.db.fail_batch(batch_id, self._fail_reason(exc))
            await self._safe_release(batch_id)
            return

        # 3) 留存报告并释放两院名额
        hosp = {h["hospital_id"]: h for h in self.db.hospital_rows(batch_id)}
        if result is None:
            # 合并为空：不出中位数，但批次成功结束
            self.db.succeed_batch(batch_id, None, None, 0)
            self.db.save_report(
                batch_id,
                hosp["A"]["version_tag"], hosp["B"]["version_tag"],
                hosp["A"]["sample_size"], hosp["B"]["sample_size"], 0,
                None, None, "两院合并样本为空，不出中位数",
            )
        else:
            median_value, median_mmol, pooled = result
            self.db.succeed_batch(batch_id, median_value, median_mmol, pooled)
            self.db.save_report(
                batch_id,
                hosp["A"]["version_tag"], hosp["B"]["version_tag"],
                hosp["A"]["sample_size"], hosp["B"]["sample_size"], pooled,
                median_value, median_mmol,
                "偶数样本取中间两值平均" if float(median_value) != int(median_value) else None,
            )
        await self._safe_release(batch_id)

    @staticmethod
    def _fail_reason(exc: HospitalClientError) -> str:
        if exc.code in (SNAPSHOT_EXPIRED,):
            return f"医院 {exc.hospital_id} 固定样本过期，整批失败"
        if exc.code == SNAPSHOT_NOT_FOUND:
            return f"医院 {exc.hospital_id} 快照不存在或已回收，整批失败"
        status = f"（HTTP {exc.status_code}）" if exc.status_code else ""
        return f"医院 {exc.hospital_id} 明确失败{status}：{exc.message}"

    async def _safe_release(self, batch_id: str) -> None:
        """带代次释放两院名额；代次不符（旧批迟到）由院端拒绝，不会误放新批。"""
        rows = self.db.hospital_rows(batch_id)
        for h in rows:
            if not h["acquired"] or h["generation"] is None:
                continue
            client = self.client_a if h["hospital_id"] == "A" else self.client_b
            try:
                await client.release(batch_id, h["generation"])
            except Exception:
                pass

    # ----------------------------- 中位数（仅靠计数查询） -----------------------------

    async def _count_both(self, clients: dict, batch_id: str,
                          value: int, round_no: int) -> tuple[int, int, int]:
        (n_a, c_a), (n_b, c_b) = await asyncio.gather(
            clients["A"].count_lte(batch_id, value),
            clients["B"].count_lte(batch_id, value),
        )
        self.db.add_round(batch_id, round_no, value, c_a, c_b)
        return n_a + n_b, c_a, c_b

    async def _compute_median(self, batch_id: str, clients: dict):
        """返回 (median_value, median_mmol, pooled) 或 None（合并为空）。"""
        # 样本数也是通过计数接口之外的 acquire 记录取得；这里再取一次确认快照有效
        n_a, c_a = await clients["A"].count_lte(batch_id, VALUE_MAX)
        n_b, c_b = await clients["B"].count_lte(batch_id, VALUE_MAX)
        self.db.add_round(batch_id, 1, VALUE_MAX, c_a, c_b)
        pooled = n_a + n_b
        if pooled == 0:
            return None

        async def kth_smallest(k: int, start_round: int) -> float:
            """在 1..1000 整数域上二分第 k 小值，全部信息只来自 count<=x。"""
            lo, hi = VALUE_MIN, VALUE_MAX
            round_no = start_round
            while lo < hi:
                mid = (lo + hi) // 2
                _, ca, cb = await self._count_both(clients, batch_id, mid, round_no)
                round_no += 1
                if ca + cb >= k:
                    hi = mid
                else:
                    lo = mid + 1
            return float(lo)

        if pooled % 2 == 1:
            k = (pooled + 1) // 2
            median_value = await kth_smallest(k, 2)
        else:
            k1, k2 = pooled // 2, pooled // 2 + 1
            v1 = await kth_smallest(k1, 2)
            # 第二轮接着编号，保证轮次可审计
            rounds = self.db.list_rounds(batch_id)
            v2 = await kth_smallest(k2, len(rounds) + 1)
            median_value = (v1 + v2) / 2.0  # 中间两值平均，可能为 x.5
        return median_value, round(median_value * UNIT_MMOL_PER_L, 2), pooled

    # ----------------------------- 故障注入下发 -----------------------------

    async def _push_sim_config(self, batch_id: str) -> None:
        cfg_row = self.db.get_sim(batch_id)
        if cfg_row is None or cfg_row["pushed"]:
            return
        target = cfg_row["hospital_target"]
        phase = cfg_row["phase"]
        base_rule = {
            "batch_id": batch_id,
            "phase": phase,
            "delay_seconds": cfg_row["delay_seconds"],
            "fail_status": cfg_row["fail_status"],
            "expire_now": bool(cfg_row["expire_now"]),
            "persistent": bool(cfg_row["persistent"]),
        }
        targets = ["A", "B"] if target == "both" else [target]
        for hid in targets:
            client = self.client_a if hid == "A" else self.client_b
            # 回执丢失：院端已成功固定快照，但协调端收不到响应。
            # 仅在显式开启 lose_receipt_once 时，对第一个目标院的首次 acquire
            # 注入超长延迟使协调端超时；规则一次性，重试命中幂等快照找回原样本。
            if cfg_row["lose_receipt_once"] and hid == targets[0]:
                rule = dict(base_rule)
                rule["phase"] = "acquire"
                rule["persistent"] = False
                rule["fail_status"] = None
                rule["expire_now"] = False
                rule["delay_seconds"] = max(
                    cfg_row["delay_seconds"], cfg.request_timeout + 2
                )
                await client.push_sim_rule(rule)
            else:
                await client.push_sim_rule(base_rule)
        self.db.mark_sim_pushed(batch_id)


def new_batch_id() -> str:
    return f"batch-{uuid.uuid4().hex[:12]}"
