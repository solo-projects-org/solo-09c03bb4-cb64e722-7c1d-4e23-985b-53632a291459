"""院端 SQLite 存储层。

关键设计：
1. versions 为整版导入：每次导入生成新版本，旧版本保留（更正可再次提交）。
2. snapshots 物化快照：acquire 时把当前版本、日期范围内的数据复制到
   snapshot_observations，之后新导入不影响在研批次，满足"多轮不得换样本"。
3. 每院仅一个 active 快照名额：部分唯一索引保证；generation 代次令牌保证
   "旧批迟到回收不能释放新批名额"。
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ..config import VALUE_MAX, VALUE_MIN, HospitalConfig

# 院端明确失败的错误码
ERR_SLOT_BUSY = "SLOT_BUSY"            # 快照名额被占（新批等待）
ERR_SNAPSHOT_NOT_FOUND = "SNAPSHOT_NOT_FOUND"
ERR_SNAPSHOT_EXPIRED = "SNAPSHOT_EXPIRED"
ERR_STALE_GENERATION = "STALE_GENERATION"
ERR_BAD_VALUE = "BAD_VALUE"
ERR_BAD_DATE_RANGE = "BAD_DATE_RANGE"
ERR_DUPLICATE_TEST_ID = "DUPLICATE_TEST_ID"


class HospitalError(Exception):
    def __init__(self, code: str, message: str, http_status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


SCHEMA = """
CREATE TABLE IF NOT EXISTS versions (
    version_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    version_tag  TEXT NOT NULL UNIQUE,
    imported_at  TEXT NOT NULL,
    row_count    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS observations (
    version_id   INTEGER NOT NULL REFERENCES versions(version_id),
    test_id      TEXT NOT NULL,
    obs_date     TEXT NOT NULL,
    value        INTEGER NOT NULL CHECK (value BETWEEN {vmin} AND {vmax}),
    PRIMARY KEY (version_id, test_id)
);
CREATE INDEX IF NOT EXISTS idx_obs_version_date
    ON observations(version_id, obs_date);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id     TEXT NOT NULL UNIQUE,
    generation   INTEGER NOT NULL,
    version_id   INTEGER NOT NULL REFERENCES versions(version_id),
    date_from    TEXT NOT NULL,
    date_to      TEXT NOT NULL,
    sample_size  INTEGER NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('active', 'expired', 'released')),
    created_at   TEXT NOT NULL,
    expires_at   TEXT NOT NULL,
    released_at  TEXT
);
-- 每院至多一个 active 快照名额
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_snapshot
    ON snapshots(1) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS snapshot_observations (
    snapshot_id  INTEGER NOT NULL REFERENCES snapshots(snapshot_id),
    test_id      TEXT NOT NULL,
    obs_date     TEXT NOT NULL,
    value        INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, test_id)
);
CREATE INDEX IF NOT EXISTS idx_snapobs_snap_value
    ON snapshot_observations(snapshot_id, value);

CREATE TABLE IF NOT EXISTS sim_rules (
    batch_id      TEXT PRIMARY KEY,
    phase         TEXT NOT NULL DEFAULT 'all',
    delay_seconds REAL NOT NULL DEFAULT 0,
    fail_status   INTEGER,
    expire_now    INTEGER NOT NULL DEFAULT 0,
    persistent    INTEGER NOT NULL DEFAULT 0,
    used          INTEGER NOT NULL DEFAULT 0,
    updated_at    TEXT NOT NULL
);
""".format(vmin=VALUE_MIN, vmax=VALUE_MAX)


class HospitalDB:
    """线程安全封装：所有写操作串行化，读用独立连接（WAL 模式）。"""

    def __init__(self, config: HospitalConfig):
        self.config = config
        Path(config.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            config.db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ----------------------------- 整版导入 -----------------------------

    def import_version(
        self, version_tag: str, rows: list[tuple[str, str, int]]
    ) -> int:
        """整版导入。rows: (test_id, obs_date, value)，test_id 在版内唯一。"""
        if not rows:
            raise HospitalError("EMPTY_CSV", "CSV 没有任何数据行", 422)
        seen: set[str] = set()
        for test_id, obs_date, value in rows:
            if test_id in seen:
                raise HospitalError(
                    ERR_DUPLICATE_TEST_ID,
                    f"检验号重复: {test_id}",
                    422,
                )
            seen.add(test_id)
            if not (VALUE_MIN <= value <= VALUE_MAX):
                raise HospitalError(
                    ERR_BAD_VALUE,
                    f"检验号 {test_id} 的值 {value} 超出 [{VALUE_MIN},{VALUE_MAX}]",
                    422,
                )
        with self._lock:
            try:
                cur = self._conn.execute(
                    "INSERT INTO versions(version_tag, imported_at, row_count) "
                    "VALUES (?,?,?)",
                    (version_tag, _utcnow(), len(rows)),
                )
            except sqlite3.IntegrityError as exc:
                raise HospitalError(
                    "DUP_VERSION", f"版号已存在: {version_tag}", 409
                ) from exc
            version_id = cur.lastrowid
            self._conn.executemany(
                "INSERT INTO observations(version_id, test_id, obs_date, value) "
                "VALUES (?,?,?,?)",
                [(version_id, *r) for r in rows],
            )
            return version_id

    def latest_version(self) -> Optional[sqlite3.Row]:
        row = self._conn.execute(
            "SELECT * FROM versions ORDER BY version_id DESC LIMIT 1"
        ).fetchone()
        return row

    def list_versions(self) -> list[sqlite3.Row]:
        return list(
            self._conn.execute("SELECT * FROM versions ORDER BY version_id")
        )

    # ----------------------------- 快照名额 -----------------------------

    def acquire_snapshot(
        self, batch_id: str, date_from: str, date_to: str
    ) -> sqlite3.Row:
        """为批次固定样本与版号；幂等：同批次重试返回原快照。"""
        if date_from > date_to:
            raise HospitalError(
                ERR_BAD_DATE_RANGE, "date_from 不得晚于 date_to", 422
            )
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM snapshots WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if existing is not None:
                # 回执丢失后的原批重试：找回原样本（含已过期/已释放的真实状态）
                if existing["status"] == "released":
                    raise HospitalError(
                        ERR_SNAPSHOT_NOT_FOUND,
                        f"批次 {batch_id} 的快照名额已被回收",
                        404,
                    )
                if existing["status"] == "expired" or self._is_expired(existing):
                    if existing["status"] == "active":
                        self._conn.execute(
                            "UPDATE snapshots SET status='expired' WHERE snapshot_id=?",
                            (existing["snapshot_id"],),
                        )
                        existing = self._conn.execute(
                            "SELECT * FROM snapshots WHERE snapshot_id=?",
                            (existing["snapshot_id"],),
                        ).fetchone()
                    raise HospitalError(
                        ERR_SNAPSHOT_EXPIRED,
                        f"批次 {batch_id} 的固定样本已过期",
                        410,
                    )
                return existing

            version = self.latest_version()
            if version is None:
                raise HospitalError(
                    "NO_DATA", "本院尚未导入任何 CSV 版本", 409
                )
            active = self._conn.execute(
                "SELECT 1 FROM snapshots WHERE status='active' LIMIT 1"
            ).fetchone()
            if active is not None:
                raise HospitalError(
                    ERR_SLOT_BUSY, "本院快照名额已被其他批次占用，请等待", 409
                )

            version_id = version["version_id"]
            now = datetime.now(timezone.utc)
            created_at = now.isoformat(timespec="seconds")
            expires_at = (
                now.timestamp() + self.config.snapshot_ttl_seconds
            )
            expires_iso = datetime.fromtimestamp(expires_at, timezone.utc).isoformat(
                timespec="seconds"
            )
            cur = self._conn.execute(
                "INSERT INTO snapshots(batch_id, generation, version_id, date_from, "
                "date_to, sample_size, status, created_at, expires_at) "
                "VALUES (?,?,?,?,?,?,'active',?,?)",
                (
                    batch_id,
                    1,
                    version_id,
                    date_from,
                    date_to,
                    0,
                    created_at,
                    expires_iso,
                ),
            )
            snapshot_id = cur.lastrowid
            self._conn.execute(
                "INSERT INTO snapshot_observations(snapshot_id, test_id, obs_date, value) "
                "SELECT ?, test_id, obs_date, value FROM observations "
                "WHERE version_id=? AND obs_date BETWEEN ? AND ?",
                (snapshot_id, version_id, date_from, date_to),
            )
            sample_size = self._conn.execute(
                "SELECT COUNT(*) AS c FROM snapshot_observations WHERE snapshot_id=?",
                (snapshot_id,),
            ).fetchone()["c"]
            self._conn.execute(
                "UPDATE snapshots SET sample_size=? WHERE snapshot_id=?",
                (sample_size, snapshot_id),
            )
            return self._conn.execute(
                "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,)
            ).fetchone()

    def _is_expired(self, snap: sqlite3.Row) -> bool:
        return snap["expires_at"] <= _utcnow()

    def _load_active_snapshot(self, batch_id: str) -> sqlite3.Row:
        snap = self._conn.execute(
            "SELECT * FROM snapshots WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if snap is None:
            raise HospitalError(
                ERR_SNAPSHOT_NOT_FOUND, f"批次 {batch_id} 无固定快照", 404
            )
        if snap["status"] == "released":
            raise HospitalError(
                ERR_SNAPSHOT_NOT_FOUND,
                f"批次 {batch_id} 的快照名额已回收",
                404,
            )
        if snap["status"] == "expired" or self._is_expired(snap):
            if snap["status"] == "active":
                self._conn.execute(
                    "UPDATE snapshots SET status='expired' WHERE snapshot_id=?",
                    (snap["snapshot_id"],),
                )
            raise HospitalError(
                ERR_SNAPSHOT_EXPIRED, f"批次 {batch_id} 的固定样本已过期", 410
            )
        return snap

    def count_lte(self, batch_id: str, value: int) -> sqlite3.Row:
        """协调端只被允许取：样本数 + 不大于指定值的计数。"""
        if not (VALUE_MIN <= value <= VALUE_MAX):
            raise HospitalError(
                ERR_BAD_VALUE,
                f"value 须在 [{VALUE_MIN},{VALUE_MAX}]",
                422,
            )
        with self._lock:
            snap = self._load_active_snapshot(batch_id)
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM snapshot_observations "
                "WHERE snapshot_id=? AND value<=?",
                (snap["snapshot_id"], value),
            ).fetchone()
            result = dict(snap)
            result["count_lte"] = row["c"]
            return result

    def release_snapshot(self, batch_id: str, generation: int) -> tuple[bool, str]:
        """批次结束回收名额；代次不符（旧批迟到回收）则拒绝。"""
        with self._lock:
            snap = self._conn.execute(
                "SELECT * FROM snapshots WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if snap is None:
                return False, ERR_SNAPSHOT_NOT_FOUND
            if snap["generation"] != generation:
                return False, ERR_STALE_GENERATION
            if snap["status"] == "released":
                return True, "already_released"
            self._conn.execute(
                "UPDATE snapshots SET status='released', released_at=? "
                "WHERE snapshot_id=?",
                (_utcnow(), snap["snapshot_id"]),
            )
            return True, "released"

    def snapshot_row(self, batch_id: str) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM snapshots WHERE batch_id=?", (batch_id,)
        ).fetchone()

    def version_tag_of(self, version_id: int) -> str:
        row = self._conn.execute(
            "SELECT version_tag FROM versions WHERE version_id=?", (version_id,)
        ).fetchone()
        return row["version_tag"] if row else "unknown"

    def active_snapshot(self) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM snapshots WHERE status='active' LIMIT 1"
        ).fetchone()

    def expire_snapshot(self, batch_id: str) -> None:
        """故障注入：立即把某批次快照标记过期。"""
        with self._lock:
            self._conn.execute(
                "UPDATE snapshots SET status='expired' WHERE batch_id=? AND status='active'",
                (batch_id,),
            )

    # ----------------------------- 模拟故障 -----------------------------

    def upsert_sim_rule(
        self,
        batch_id: str,
        delay_seconds: float,
        fail_status: Optional[int],
        expire_now: bool,
        phase: str = "all",
        persistent: bool = False,
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO sim_rules(batch_id, phase, delay_seconds, fail_status, expire_now, "
                "persistent, used, updated_at) VALUES (?,?,?,?,?,?,0,?) "
                "ON CONFLICT(batch_id) DO UPDATE SET phase=excluded.phase, "
                "delay_seconds=excluded.delay_seconds, fail_status=excluded.fail_status, "
                "expire_now=excluded.expire_now, persistent=excluded.persistent, "
                "used=0, updated_at=excluded.updated_at",
                (batch_id, phase, delay_seconds, fail_status, int(expire_now),
                 int(persistent), _utcnow()),
            )

    def pop_sim_rule(self, batch_id: str, phase: str) -> Optional[sqlite3.Row]:
        """读取匹配阶段的规则；persistent 规则持续生效（用于迟到响应场景）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM sim_rules WHERE batch_id=? AND used=0 "
                "AND phase IN ('all', ?) LIMIT 1",
                (batch_id, phase),
            ).fetchone()
            if row is None:
                return None
            if not row["persistent"]:
                self._conn.execute(
                    "UPDATE sim_rules SET used=1, updated_at=? WHERE batch_id=?",
                    (_utcnow(), batch_id),
                )
            return row

    def delete_sim_rule(self, batch_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM sim_rules WHERE batch_id=?", (batch_id,))

    def list_sim_rules(self) -> list[sqlite3.Row]:
        return list(self._conn.execute("SELECT * FROM sim_rules"))
