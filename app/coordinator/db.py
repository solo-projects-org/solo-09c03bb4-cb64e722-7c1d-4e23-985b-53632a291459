"""协调端 SQLite 存储层：批次、两院快照记录、二分轮次、成功报告、模拟配置。"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# 批次状态机
QUEUED = "QUEUED"        # 已发起，等待调度
RUNNING = "RUNNING"      # 已在两院固定样本，正在多轮计数
SUCCEEDED = "SUCCEEDED"  # 完成（含合并为空、无中位数）
FAILED = "FAILED"        # 任一院明确失败 / 样本过期 / 重试耗尽

TERMINAL_STATUSES = (SUCCEEDED, FAILED)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id           TEXT PRIMARY KEY,
    date_from          TEXT NOT NULL,
    date_to            TEXT NOT NULL,
    status             TEXT NOT NULL,
    failure_reason     TEXT,
    median_value       REAL,
    median_mmol_l      REAL,
    pooled_sample_size INTEGER,
    attempts           INTEGER NOT NULL DEFAULT 0,
    last_error         TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_status_created
    ON batches(status, created_at);

CREATE TABLE IF NOT EXISTS batch_hospitals (
    batch_id     TEXT NOT NULL REFERENCES batches(batch_id),
    hospital_id  TEXT NOT NULL CHECK (hospital_id IN ('A','B')),
    base_url     TEXT NOT NULL,
    snapshot_id  INTEGER,
    generation   INTEGER,
    version_tag  TEXT,
    sample_size  INTEGER,
    acquired     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (batch_id, hospital_id)
);

CREATE TABLE IF NOT EXISTS rounds (
    round_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    TEXT NOT NULL REFERENCES batches(batch_id),
    round_no    INTEGER NOT NULL,
    value       INTEGER NOT NULL,
    count_a     INTEGER NOT NULL,
    count_b     INTEGER NOT NULL,
    count_total INTEGER NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reports (
    batch_id           TEXT PRIMARY KEY REFERENCES batches(batch_id),
    version_tag_a      TEXT NOT NULL,
    version_tag_b      TEXT NOT NULL,
    sample_size_a      INTEGER NOT NULL,
    sample_size_b      INTEGER NOT NULL,
    pooled_sample_size INTEGER NOT NULL,
    median_value       REAL,
    median_mmol_l      REAL,
    note               TEXT,
    created_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sim_config (
    batch_id          TEXT PRIMARY KEY,
    hospital_target   TEXT NOT NULL,
    phase             TEXT NOT NULL DEFAULT 'all',
    delay_seconds     REAL NOT NULL DEFAULT 0,
    fail_status       INTEGER,
    lose_receipt_once INTEGER NOT NULL DEFAULT 0,
    expire_now        INTEGER NOT NULL DEFAULT 0,
    persistent        INTEGER NOT NULL DEFAULT 0,
    pushed            INTEGER NOT NULL DEFAULT 0,
    updated_at        TEXT NOT NULL
);
"""


class CoordinatorDB:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ----------------------------- 批次 -----------------------------

    def create_batch(self, batch_id: str, date_from: str, date_to: str,
                     url_a: str, url_b: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO batches(batch_id, date_from, date_to, status, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?)",
                (batch_id, date_from, date_to, QUEUED, _utcnow(), _utcnow()),
            )
            self._conn.executemany(
                "INSERT INTO batch_hospitals(batch_id, hospital_id, base_url) VALUES (?,?,?)",
                [(batch_id, "A", url_a), (batch_id, "B", url_b)],
            )

    def get_batch(self, batch_id: str) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone()

    def list_batches(self) -> list[sqlite3.Row]:
        return list(
            self._conn.execute("SELECT * FROM batches ORDER BY created_at DESC, batch_id")
        )

    def pop_next_batch(self) -> Optional[sqlite3.Row]:
        """FIFO：取最早创建的 QUEUED 批次并置为 RUNNING。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM batches WHERE status=? ORDER BY created_at, batch_id LIMIT 1",
                (QUEUED,),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE batches SET status=?, updated_at=? WHERE batch_id=? AND status=?",
                (RUNNING, _utcnow(), row["batch_id"], QUEUED),
            )
            return row

    def requeue(self, batch_id: str, last_error: str) -> None:
        """院端名额忙：回到队尾等待，不算失败。"""
        with self._lock:
            self._conn.execute(
                "UPDATE batches SET status=?, last_error=?, updated_at=? WHERE batch_id=?",
                (QUEUED, last_error, _utcnow(), batch_id),
            )

    def update_error(self, batch_id: str, last_error: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE batches SET attempts=attempts+1, last_error=?, updated_at=? WHERE batch_id=?",
                (last_error, _utcnow(), batch_id),
            )

    def fail_batch(self, batch_id: str, reason: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE batches SET status=?, failure_reason=?, updated_at=? WHERE batch_id=?",
                (FAILED, reason, _utcnow(), batch_id),
            )

    def succeed_batch(self, batch_id: str, median_value, median_mmol_l,
                      pooled: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE batches SET status=?, median_value=?, median_mmol_l=?, "
                "pooled_sample_size=?, updated_at=? WHERE batch_id=?",
                (SUCCEEDED, median_value, median_mmol_l, pooled, _utcnow(), batch_id),
            )

    def hospital_rows(self, batch_id: str) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                "SELECT * FROM batch_hospitals WHERE batch_id=? ORDER BY hospital_id",
                (batch_id,),
            )
        )

    def mark_acquired(self, batch_id: str, hospital_id: str, snapshot_id: int,
                      generation: int, version_tag: str, sample_size: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE batch_hospitals SET snapshot_id=?, generation=?, version_tag=?, "
                "sample_size=?, acquired=1 WHERE batch_id=? AND hospital_id=?",
                (snapshot_id, generation, version_tag, sample_size, batch_id, hospital_id),
            )

    def add_round(self, batch_id: str, round_no: int, value: int,
                  count_a: int, count_b: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO rounds(batch_id, round_no, value, count_a, count_b, count_total, created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (batch_id, round_no, value, count_a, count_b, count_a + count_b, _utcnow()),
            )

    def list_rounds(self, batch_id: str) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                "SELECT * FROM rounds WHERE batch_id=? ORDER BY round_id", (batch_id,)
            )
        )

    def save_report(self, batch_id: str, ver_a: str, ver_b: str,
                    n_a: int, n_b: int, pooled: int, median_value,
                    median_mmol_l, note: Optional[str]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO reports(batch_id, version_tag_a, version_tag_b, "
                "sample_size_a, sample_size_b, pooled_sample_size, median_value, "
                "median_mmol_l, note, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (batch_id, ver_a, ver_b, n_a, n_b, pooled, median_value,
                 median_mmol_l, note, _utcnow()),
            )

    def get_report(self, batch_id: str) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM reports WHERE batch_id=?", (batch_id,)
        ).fetchone()

    def list_reports(self) -> list[sqlite3.Row]:
        return list(self._conn.execute("SELECT * FROM reports ORDER BY created_at DESC"))

    def recover_interrupted(self) -> list[sqlite3.Row]:
        """重启恢复：上次退出时残留的 RUNNING 批次（可能持有院端名额）。"""
        return list(self._conn.execute("SELECT * FROM batches WHERE status=?", (RUNNING,)))

    # ----------------------------- 模拟配置 -----------------------------

    def upsert_sim(self, batch_id: str, target: str, phase: str, delay: float,
                   fail_status, lose_once: int, expire_now: int,
                   persistent: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO sim_config(batch_id, hospital_target, phase, delay_seconds, "
                "fail_status, lose_receipt_once, expire_now, persistent, pushed, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,0,?) "
                "ON CONFLICT(batch_id) DO UPDATE SET hospital_target=excluded.hospital_target, "
                "phase=excluded.phase, delay_seconds=excluded.delay_seconds, "
                "fail_status=excluded.fail_status, lose_receipt_once=excluded.lose_receipt_once, "
                "expire_now=excluded.expire_now, persistent=excluded.persistent, "
                "pushed=0, updated_at=excluded.updated_at",
                (batch_id, target, phase, delay, fail_status, lose_once,
                 expire_now, persistent, _utcnow()),
            )

    def get_sim(self, batch_id: str) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM sim_config WHERE batch_id=?", (batch_id,)
        ).fetchone()

    def mark_sim_pushed(self, batch_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE sim_config SET pushed=1, updated_at=? WHERE batch_id=?",
                (_utcnow(), batch_id),
            )

    def list_sim(self) -> list[sqlite3.Row]:
        return list(self._conn.execute("SELECT * FROM sim_config ORDER BY batch_id"))
