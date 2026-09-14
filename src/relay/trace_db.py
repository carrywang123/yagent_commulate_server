"""跨端任务追踪的 SQLite 持久层。

## 为什么是 SQLite 而不是文件

追踪数据要**长期留存且能按任意条件筛选**（按人、按状态、按时间段翻几个月前的账）。
按天 NDJSON 文件做这件事必须每次查询都重读+重放整段历史，查「近三个月」会慢到不可用；
而 `sqlite3` 是 Python 标准库，带索引、事务、分页，零新增依赖，单进程写入毫无压力。

## 两张表的分工

- `events`：**append-only 原始事件**。每条流经 Relay 的跨端信封都在这里，
  包含每一轮 probe 心跳。这是「不丢数据」的底线，任何聚合口径以后要改，
  都能从这张表重算出来。
- `tasks`：**聚合快照**，一行一个任务。看板列表/统计全查这张表，
  所以列表查询是一次带索引的 SELECT，与历史总量无关。

`tasks` 是可以从 `events` 完整重建的派生数据；`events` 才是真相。

## 写入策略

每条事件一个事务：`INSERT INTO events` + `UPSERT INTO tasks` 一起提交。
不做批量缓冲——跨端消息频率很低（每秒最多几条），换来的是
**进程被强杀也不会丢任何已转发的消息记录**。

## 保留策略

默认 `prune_days=0`，即**永不删除**。要清理时显式设置天数，
清理只删 `events` 里的老明细与 `tasks` 里的老任务，不会动索引结构。
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

#: tasks 表的列顺序（UPSERT 与 row→dict 都以此为准）
TASK_COLUMNS = (
    "task_id",
    "conv_id",
    "kind",
    "requester",
    "requester_session",
    "handler",
    "handler_session",
    "summary",
    "created_at",
    "first_seen",
    "delivered_at",
    "delivered",
    "state",
    "state_at",
    "started_at",
    "queued_at",
    "finished_at",
    "error_code",
    "error_message",
    "probe_count",
    "probe_ack_count",
    "last_ack_at",
    "last_activity",
    "bytes_out",
    "bytes_in",
    "hop",
    "ttl",
    "partial",
    "terminal",
    "chain",
    "timeline",
)

EVENT_COLUMNS = (
    "ts",
    "ev",
    "task_id",
    "msg_id",
    "conv_id",
    "reply_to",
    "from_client",
    "from_session",
    "to_client",
    "to_session",
    "delivered",
    "bytes",
    "peer_state",
    "code",
    "message",
    "error_code",
    "error_message",
    "origin",
    "summary",
    "hop",
    "ttl",
    "created_at",
)

_DDL = f"""
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id           TEXT PRIMARY KEY,
    conv_id           TEXT    DEFAULT '',
    kind              TEXT    DEFAULT 'request',
    requester         TEXT    DEFAULT '',
    requester_session TEXT    DEFAULT '',
    handler           TEXT    DEFAULT '',
    handler_session   TEXT    DEFAULT '',
    summary           TEXT    DEFAULT '',
    created_at        REAL    DEFAULT 0,
    first_seen        REAL    DEFAULT 0,
    delivered_at      REAL,
    delivered         INTEGER DEFAULT 0,
    state             TEXT    DEFAULT 'sent',
    state_at          REAL    DEFAULT 0,
    started_at        REAL,
    queued_at         REAL,
    finished_at       REAL,
    error_code        TEXT,
    error_message     TEXT,
    probe_count       INTEGER DEFAULT 0,
    probe_ack_count   INTEGER DEFAULT 0,
    last_ack_at       REAL,
    last_activity     REAL    DEFAULT 0,
    bytes_out         INTEGER DEFAULT 0,
    bytes_in          INTEGER DEFAULT 0,
    hop               INTEGER DEFAULT 1,
    ttl               INTEGER,
    partial           INTEGER DEFAULT 0,
    terminal          INTEGER DEFAULT 0,
    chain             TEXT    DEFAULT '[]',
    timeline          TEXT    DEFAULT '[]'
);

-- 列表默认按「未终结优先 + 最近活动」排，再叠加人/状态/时间过滤
CREATE INDEX IF NOT EXISTS idx_tasks_created   ON tasks(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_activity  ON tasks(terminal, last_activity DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_requester ON tasks(requester, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_handler   ON tasks(handler, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_state     ON tasks(state, created_at DESC);

CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts            REAL    NOT NULL,
    ev            TEXT    NOT NULL,
    task_id       TEXT,
    msg_id        TEXT,
    conv_id       TEXT,
    reply_to      TEXT,
    from_client   TEXT,
    from_session  TEXT,
    to_client     TEXT,
    to_session    TEXT,
    delivered     INTEGER,
    bytes         INTEGER,
    peer_state    TEXT,
    code          TEXT,
    message       TEXT,
    error_code    TEXT,
    error_message TEXT,
    origin        TEXT,
    summary       TEXT,
    hop           INTEGER,
    ttl           INTEGER,
    created_at    REAL
);

CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id, ts);
CREATE INDEX IF NOT EXISTS idx_events_ts   ON events(ts DESC);
"""


def _dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return "[]"


def _loads(text: Any, fallback: Any) -> Any:
    if not text:
        return fallback
    try:
        return json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return fallback


class TraceDB:
    """线程安全的 SQLite 封装。

    埋点在事件循环线程里同步调用，查询可能来自 `asyncio.to_thread`，
    所以连接用 `check_same_thread=False` 并由一把 RLock 串行化。
    单写者场景下这比连接池简单且不会有 `database is locked`。
    """

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False, timeout=15.0)
        self.conn.row_factory = sqlite3.Row
        with self.lock:
            # WAL：读不阻塞写，且断电最多丢未提交事务
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.executescript(_DDL)
            self.conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            self.conn.commit()

    def close(self) -> None:
        with self.lock:
            try:
                self.conn.commit()
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.Error:
                pass
            self.conn.close()

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------

    def write(self, event: dict[str, Any], task_row: Optional[dict[str, Any]]) -> None:
        """一条事件 + 它影响的任务快照，一个事务写完。"""
        with self.lock:
            try:
                self.conn.execute(
                    f"INSERT INTO events ({','.join(EVENT_COLUMNS)}) "
                    f"VALUES ({','.join('?' * len(EVENT_COLUMNS))})",
                    tuple(event.get(col) for col in EVENT_COLUMNS),
                )
                if task_row is not None:
                    self._upsert_task_locked(task_row)
                self.conn.commit()
            except sqlite3.Error:
                self.conn.rollback()
                raise

    def upsert_task(self, task_row: dict[str, Any]) -> None:
        with self.lock:
            try:
                self._upsert_task_locked(task_row)
                self.conn.commit()
            except sqlite3.Error:
                self.conn.rollback()
                raise

    def _upsert_task_locked(self, row: dict[str, Any]) -> None:
        columns = ",".join(TASK_COLUMNS)
        holders = ",".join("?" * len(TASK_COLUMNS))
        updates = ",".join(f"{c}=excluded.{c}" for c in TASK_COLUMNS if c != "task_id")
        values = []
        for col in TASK_COLUMNS:
            value = row.get(col)
            if col in ("chain", "timeline") and not isinstance(value, str):
                value = _dumps(value if value is not None else [])
            values.append(value)
        self.conn.execute(
            f"INSERT INTO tasks ({columns}) VALUES ({holders}) "
            f"ON CONFLICT(task_id) DO UPDATE SET {updates}",
            tuple(values),
        )

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["chain"] = _loads(data.get("chain"), [])
        data["timeline"] = _loads(data.get("timeline"), [])
        return data

    def _filters(
        self,
        *,
        since: Optional[float],
        until: Optional[float],
        user: str,
        role: str,
        state: str,
        keyword: str,
        silent_before: Optional[float] = None,
    ) -> tuple[str, list[Any]]:
        where: list[str] = []
        args: list[Any] = []

        # 时间维度用 created_at（任务发起时刻）：用户的直觉是「这天派出去的任务」
        if since is not None:
            where.append("COALESCE(NULLIF(created_at, 0), first_seen) >= ?")
            args.append(since)
        if until is not None:
            where.append("COALESCE(NULLIF(created_at, 0), first_seen) <= ?")
            args.append(until)

        if user:
            if role == "sent":
                where.append("requester = ?")
                args.append(user)
            elif role == "handled":
                where.append("handler = ?")
                args.append(user)
            else:
                where.append("(requester = ? OR handler = ?)")
                args.extend([user, user])

        if state and state != "all":
            if state == "active":
                where.append("terminal = 0")
            elif state == "silent":
                where.append("terminal = 0 AND last_activity < ?")
                args.append(silent_before if silent_before is not None else time.time() - 200)
            else:
                where.append("state = ?")
                args.append(state)

        if keyword:
            like = f"%{keyword}%"
            where.append(
                "(summary LIKE ? OR task_id LIKE ? OR conv_id LIKE ? "
                "OR requester LIKE ? OR handler LIKE ? "
                "OR requester_session LIKE ? OR handler_session LIKE ?)"
            )
            args.extend([like] * 7)

        clause = (" WHERE " + " AND ".join(where)) if where else ""
        return clause, args

    def query_tasks(
        self,
        *,
        since: Optional[float] = None,
        until: Optional[float] = None,
        user: str = "",
        role: str = "all",
        state: str = "all",
        keyword: str = "",
        limit: int = 100,
        offset: int = 0,
        silent_before: Optional[float] = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        clause, args = self._filters(
            since=since, until=until, user=user, role=role,
            state=state, keyword=keyword, silent_before=silent_before,
        )
        with self.lock:
            total = self.conn.execute(f"SELECT COUNT(*) FROM tasks{clause}", args).fetchone()[0]
            rows = self.conn.execute(
                f"SELECT * FROM tasks{clause} "
                # 未终结的排前面（正在跑的最该被看到），其余按最后活动倒序
                f"ORDER BY terminal ASC, last_activity DESC LIMIT ? OFFSET ?",
                (*args, limit, offset),
            ).fetchall()
        return total, [self._row_to_dict(r) for r in rows]

    def get_task(self, task_id: str) -> Optional[dict[str, Any]]:
        with self.lock:
            row = self.conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        return self._row_to_dict(row) if row is not None else None

    def task_events(self, task_id: str, limit: int = 2000) -> list[dict[str, Any]]:
        """某个任务的原始事件明细（含每一轮 probe），排查时用。"""
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM events WHERE task_id = ? ORDER BY ts ASC, id ASC LIMIT ?",
                (task_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def load_hot_tasks(self, *, active_days: int = 30, recent: int = 500) -> list[dict[str, Any]]:
        """启动时载入「热任务」：所有未终结的 + 最近若干条。

        未终结的必须全载：它们还会收到 probe_ack / response，
        少载一条就会在收到回复时被当成孤儿任务（partial）。
        """
        cutoff = time.time() - 86400 * max(1, active_days)
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM tasks WHERE terminal = 0 AND last_activity >= ? "
                "UNION "
                "SELECT * FROM tasks WHERE task_id IN "
                "(SELECT task_id FROM tasks ORDER BY last_activity DESC LIMIT ?)",
                (cutoff, recent),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # 统计
    # ------------------------------------------------------------------

    def overview(
        self,
        *,
        since: Optional[float] = None,
        until: Optional[float] = None,
        silent_before: Optional[float] = None,
    ) -> dict[str, Any]:
        clause, args = self._filters(
            since=since, until=until, user="", role="all", state="all", keyword="",
        )
        silent_cut = silent_before if silent_before is not None else time.time() - 200

        with self.lock:
            kpi_row = self.conn.execute(
                f"""
                SELECT
                    COUNT(*)                                                    AS total,
                    SUM(CASE WHEN terminal = 0 THEN 1 ELSE 0 END)               AS active,
                    SUM(CASE WHEN terminal = 0 AND last_activity < ? THEN 1 ELSE 0 END) AS silent,
                    SUM(CASE WHEN state = 'done' THEN 1 ELSE 0 END)             AS done,
                    SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END)           AS failed,
                    SUM(CASE WHEN state = 'cancelled' THEN 1 ELSE 0 END)        AS cancelled,
                    SUM(CASE WHEN state = 'notified' THEN 1 ELSE 0 END)         AS notified,
                    AVG(CASE WHEN state = 'done'
                             THEN finished_at - COALESCE(NULLIF(created_at, 0), first_seen)
                        END)                                                    AS avg_duration
                FROM tasks{clause}
                """,
                (silent_cut, *args),
            ).fetchone()

            done_clause = clause + (" AND " if clause else " WHERE ") + "state = 'done'"
            done_total = self.conn.execute(
                f"SELECT COUNT(*) FROM tasks{done_clause}", args
            ).fetchone()[0]
            p95 = None
            if done_total:
                # 取第 95 百分位那一行，避免把所有耗时读进内存
                offset = min(done_total - 1, int(round(0.95 * (done_total - 1))))
                row = self.conn.execute(
                    f"SELECT finished_at - COALESCE(NULLIF(created_at, 0), first_seen) AS d "
                    f"FROM tasks{done_clause} ORDER BY d ASC LIMIT 1 OFFSET ?",
                    (*args, offset),
                ).fetchone()
                p95 = round(row["d"], 3) if row and row["d"] is not None else None

            sent_rows = self.conn.execute(
                f"""
                SELECT requester AS who, COUNT(*) AS total,
                       SUM(CASE WHEN terminal = 0 THEN 1 ELSE 0 END) AS active,
                       SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) AS failed,
                       MAX(last_activity) AS last_activity
                FROM tasks{clause}{' AND ' if clause else ' WHERE '}requester <> ''
                GROUP BY requester
                """,
                args,
            ).fetchall()
            handled_rows = self.conn.execute(
                f"""
                SELECT handler AS who, COUNT(*) AS total,
                       SUM(CASE WHEN terminal = 0 THEN 1 ELSE 0 END) AS active,
                       SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) AS failed,
                       MAX(last_activity) AS last_activity
                FROM tasks{clause}{' AND ' if clause else ' WHERE '}handler <> ''
                GROUP BY handler
                """,
                args,
            ).fetchall()

        users: dict[str, dict[str, Any]] = {}

        def slot(name: str) -> dict[str, Any]:
            return users.setdefault(
                name,
                {
                    "client_id": name,
                    "sent_total": 0, "sent_active": 0, "sent_failed": 0,
                    "handled_total": 0, "handled_active": 0, "handled_failed": 0,
                    "last_activity": 0.0,
                },
            )

        for row in sent_rows:
            item = slot(row["who"])
            item["sent_total"] = row["total"] or 0
            item["sent_active"] = row["active"] or 0
            item["sent_failed"] = row["failed"] or 0
            item["last_activity"] = max(item["last_activity"], row["last_activity"] or 0)
        for row in handled_rows:
            item = slot(row["who"])
            item["handled_total"] = row["total"] or 0
            item["handled_active"] = row["active"] or 0
            item["handled_failed"] = row["failed"] or 0
            item["last_activity"] = max(item["last_activity"], row["last_activity"] or 0)

        kpi = {
            "tasks_total": kpi_row["total"] or 0,
            "active": kpi_row["active"] or 0,
            "silent": kpi_row["silent"] or 0,
            "done": kpi_row["done"] or 0,
            "failed": kpi_row["failed"] or 0,
            "cancelled": kpi_row["cancelled"] or 0,
            "notified": kpi_row["notified"] or 0,
            "avg_duration": round(kpi_row["avg_duration"], 3) if kpi_row["avg_duration"] else None,
            "p95_duration": p95,
        }
        return {"kpi": kpi, "users": users}

    def known_clients(self) -> list[str]:
        """出现在追踪数据里的所有用户名（含已离线的历史用户）。"""
        with self.lock:
            rows = self.conn.execute(
                "SELECT DISTINCT requester AS who FROM tasks WHERE requester <> '' "
                "UNION SELECT DISTINCT handler FROM tasks WHERE handler <> ''"
            ).fetchall()
        return sorted(r["who"] for r in rows)

    def data_span(self) -> tuple[Optional[float], Optional[float]]:
        """已存数据的时间跨度，前端用来提示「全部」到底是多久。"""
        with self.lock:
            row = self.conn.execute("SELECT MIN(ts) AS lo, MAX(ts) AS hi FROM events").fetchone()
        return (row["lo"], row["hi"]) if row else (None, None)

    def stats(self) -> dict[str, Any]:
        with self.lock:
            tasks = self.conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            events = self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

        def size_of(suffix: str) -> int:
            candidate = Path(str(self.path) + suffix)
            try:
                return candidate.stat().st_size if candidate.exists() else 0
            except OSError:
                return 0

        # 分开报：WAL 会预分配增长，混在一起看会让人误以为数据量很大
        db_bytes = size_of("")
        wal_bytes = size_of("-wal")
        shm_bytes = size_of("-shm")
        lo, hi = self.data_span()
        return {
            "db_path": str(self.path),
            "db_bytes": db_bytes,
            "wal_bytes": wal_bytes,
            "total_bytes": db_bytes + wal_bytes + shm_bytes,
            "tasks": tasks,
            "events": events,
            "oldest_event": lo,
            "newest_event": hi,
        }

    # ------------------------------------------------------------------
    # 维护
    # ------------------------------------------------------------------

    def prune(self, before_ts: float) -> dict[str, int]:
        """删除该时刻之前的历史。默认配置不会调用它（永久保留）。"""
        with self.lock:
            try:
                cur = self.conn.execute("DELETE FROM events WHERE ts < ?", (before_ts,))
                events = cur.rowcount or 0
                cur = self.conn.execute(
                    "DELETE FROM tasks WHERE terminal = 1 AND last_activity < ?", (before_ts,)
                )
                tasks = cur.rowcount or 0
                self.conn.commit()
            except sqlite3.Error:
                self.conn.rollback()
                raise
        if events or tasks:
            logger.info("trace: 清理历史 events=%d tasks=%d", events, tasks)
        return {"events": events, "tasks": tasks}

    def checkpoint(self) -> None:
        """把 WAL 合并回主库，避免 -wal 文件无限增长。"""
        with self.lock:
            try:
                self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            except sqlite3.Error:
                logger.debug("trace: wal checkpoint 失败", exc_info=True)

    def rebuild_tasks(self, apply_events: Any) -> int:
        """从 events 全量重建 tasks（聚合口径变更或 tasks 损坏时用）。

        `apply_events(iterable_of_events) -> iterable_of_task_rows` 由上层提供，
        本层不理解聚合语义。
        """
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM events ORDER BY ts ASC, id ASC"
            ).fetchall()
        events: Iterable[dict[str, Any]] = [dict(r) for r in rows]
        task_rows = list(apply_events(events))
        with self.lock:
            try:
                self.conn.execute("DELETE FROM tasks")
                for row in task_rows:
                    self._upsert_task_locked(row)
                self.conn.commit()
            except sqlite3.Error:
                self.conn.rollback()
                raise
        logger.info("trace: 已从 %d 条事件重建 %d 个任务", len(list(events)), len(task_rows))
        return len(task_rows)
