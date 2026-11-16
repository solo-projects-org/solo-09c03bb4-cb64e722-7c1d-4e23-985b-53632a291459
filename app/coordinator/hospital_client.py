"""协调端 -> 院端 HTTP 客户端：重试、忙等待、迟到响应治理。"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Optional

import httpx

# 院端语义化错误码 -> 协调端处理策略
SLOT_BUSY = "SLOT_BUSY"
SNAPSHOT_EXPIRED = "SNAPSHOT_EXPIRED"
SNAPSHOT_NOT_FOUND = "SNAPSHOT_NOT_FOUND"
STALE_GENERATION = "STALE_GENERATION"
SIMULATED_FAILURE = "SIMULATED_FAILURE"


class HospitalClientError(Exception):
    """院端明确失败（含模拟失败、样本过期），整批失败。"""

    def __init__(self, hospital_id: str, phase: str, message: str,
                 status_code: Optional[int] = None, code: Optional[str] = None):
        super().__init__(f"[{hospital_id}] {phase}: {message}")
        self.hospital_id = hospital_id
        self.phase = phase
        self.message = message
        self.status_code = status_code
        self.code = code


class SlotBusyError(Exception):
    """院端唯一名额被占：批次回队列等待，非失败。"""

    def __init__(self, hospital_id: str):
        super().__init__(f"医院 {hospital_id} 快照名额忙")
        self.hospital_id = hospital_id


@dataclass
class SnapshotResult:
    snapshot_id: int
    generation: int
    version_tag: str
    sample_size: int
    expires_at: str


class HospitalClient:
    def __init__(self, base_url: str, hospital_id: str, timeout: float,
                 max_attempts: int):
        self.base_url = base_url.rstrip("/")
        self.hospital_id = hospital_id
        self.timeout = timeout
        self.max_attempts = max_attempts
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(self.timeout),
            headers={"X-Coordinator": "mcr-coordinator"},
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _request(self, method: str, path: str, phase: str,
                       json_body=None, params=None) -> httpx.Response:
        """带重试的请求。

        - 连接错误/超时：可重试（模拟瞬时故障、回执丢失后的原批重试）
        - 任何 HTTP 4xx/5xx：院端"明确失败"，不重试，立即抛错 => 整批失败
          - 409 SLOT_BUSY：抛 SlotBusyError（等待，不算失败）
          - 410 样本过期 / 404 快照缺失：语义化错误，整批失败
        """
        last_err: Optional[Exception] = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                resp = await self._client.request(
                    method, path, json=json_body, params=params
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_err = exc
                if attempt < self.max_attempts:
                    # 指数退避：回执丢失等瞬时网络故障下的原批重试
                    await asyncio.sleep(min(0.5 * attempt, 2.0))
                continue
            if resp.status_code == 409:
                detail = {}
                try:
                    detail = resp.json().get("detail", {})
                except Exception:
                    pass
                if isinstance(detail, dict) and detail.get("error") == SLOT_BUSY:
                    raise SlotBusyError(self.hospital_id)
                raise HospitalClientError(
                    self.hospital_id, phase,
                    f"409 冲突: {resp.text[:200]}", 409,
                    code=detail.get("error") if isinstance(detail, dict) else None,
                )
            if resp.status_code >= 400:
                detail = {}
                try:
                    detail = resp.json().get("detail", {})
                except Exception:
                    pass
                code = detail.get("error") if isinstance(detail, dict) else None
                raise HospitalClientError(
                    self.hospital_id, phase,
                    f"HTTP {resp.status_code}: {resp.text[:200]}",
                    status_code=resp.status_code, code=code,
                )
            return resp
        raise HospitalClientError(
            self.hospital_id, phase,
            f"重试 {self.max_attempts} 次仍失败: {last_err!r}",
            code="RETRY_EXHAUSTED",
        )

    async def acquire(self, batch_id: str, date_from: str,
                      date_to: str) -> SnapshotResult:
        resp = await self._request(
            "POST", "/snapshots", "acquire",
            json_body={
                "batch_id": batch_id,
                "date_from": date_from,
                "date_to": date_to,
            },
        )
        data = resp.json()
        return SnapshotResult(
            snapshot_id=data["snapshot_id"],
            generation=data["generation"],
            version_tag=data["version_tag"],
            sample_size=data["sample_size"],
            expires_at=data["expires_at"],
        )

    async def count_lte(self, batch_id: str, value: int) -> tuple[int, int]:
        """返回 (样本数, <= value 的计数)。协调端只取这两个量。"""
        resp = await self._request(
            "GET", f"/snapshots/{batch_id}/counts", "count",
            params={"value": value},
        )
        data = resp.json()
        return data["sample_size"], data["count_lte"]

    async def release(self, batch_id: str, generation: int) -> None:
        try:
            await self._request(
                "POST", f"/snapshots/{batch_id}/release", "release",
                json_body={"batch_id": batch_id, "generation": generation},
            )
        except HospitalClientError as exc:
            # 旧批迟到回收被拒（代次不符）是预期保护，不影响主流程
            if exc.code == STALE_GENERATION:
                return
            raise

    async def push_sim_rule(self, rule: dict) -> None:
        await self._client.put("/sim/rules", json=rule)
