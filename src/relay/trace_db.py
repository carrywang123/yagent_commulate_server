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

SCHEMA_VERSION = 3

#: v2 新增的 tasks 列。新库由 _DDL 直接建出来，老库靠 _migrate_locked 补。
#: 两处必须保持一致，所以列名与类型集中在这里定义。
TASK_COLUMNS_V2 = (
    ("link_key", "TEXT DEFAULT ''"),
    ("parent_task_id", "TEXT DEFAULT ''"),
    ("handler_session_real", "TEXT DEFAULT ''"),
)

#: v3 新增的 events 列。
#: 必须跟 tasks 一起补：events 是真相、tasks 是可重建的派生数据，
#: 只写 tasks 不写 events 的话，一旦执行 rebuild_tasks 重放，这些字段会静默丢失。
#:
#: 注：早期版本还补过一列 task_status（agent 上报的业务状态）。
#: 该设计已废弃：任务成败完全由传输层 state 表达（发出去、对方返回即成功）。
#: 已存在的库里那两列不再读写，也不主动删除（SQLite 删列会连带索引，得不偿失）。
EVENT_COLUMNS_V3 = (
    ("parent_msg_id", "TEXT"),
)

DEFAULT_SILENT_SECONDS = 200.0

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
    "link_key",
    "parent_task_id",
    "handler_session_real",
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
    "parent_msg_id",
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
    link_key             TEXT DEFAULT '',
    parent_task_id       TEXT DEFAULT '',
    handler_session_real TEXT DEFAULT '',
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
    created_at    REAL,
    parent_msg_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id, ts);
CREATE INDEX IF NOT EXISTS idx_events_ts   ON events(ts DESC);
"""

#: 链路相关的索引必须等到补列之后再建 —— 老库里这些列压根不存在，
#: 跟 _DDL 一起 executescript 会直接报 "no such column"。
_DDL_LINK_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_tasks_link   ON tasks(link_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_task_id);
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
            #: 本次启动给老库补上的列（空 = 库已是最新结构）
            self.migrated_columns = self._migrate_locked()
            # 补完列才能建链路索引
            self.conn.executescript(_DDL_LINK_INDEXES)
            self.conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            self.conn.commit()

    def _migrate_locked(self) -> list[str]:
        """给老库补列。调用方已持锁。

        不能只靠 schema_version 判断：v1 的代码把版本号写进 meta 却从来没读过，
        也没有任何 ALTER 逻辑。所以这里直接问 PRAGMA 看列到底在不在，
        幂等且对任何历史版本都成立。

        ADD COLUMN 在 SQLite 里不重写表，库再大也是瞬时完成。
        """
        added: list[str] = []
        # 表名与列定义均来自模块常量，不含外部输入
        for table, columns in (
            ("tasks", TASK_COLUMNS_V2),
            ("events", EVENT_COLUMNS_V3),
        ):
            cols = {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})")}
            for name, decl in columns:
                if name in cols:
                    continue
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
                added.append(f"{table}.{name}")
        if added:
            logger.info("trace: 已为老库补上列 %s", ",".join(added))
        return added

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
    # 链路（会话对）
    # ------------------------------------------------------------------

    _NO_LINK = "(link_key IS NULL OR link_key = '')"

    def clear_stale_link_keys(self, compute_key: Any) -> int:
        """兼容旧方法名：直接改写不一致的键，不先清空；重复执行不再更新。"""
        with self.lock:
            rows = self.conn.execute(
                "SELECT task_id, requester_session, handler_session, "
                "handler_session_real, link_key FROM tasks"
            ).fetchall()
            updates = []
            for row in rows:
                key = compute_key(row["requester_session"],
                                  row["handler_session_real"] or row["handler_session"])
                if key and row["link_key"] and key != row["link_key"]:
                    updates.append((key, row["task_id"]))
            try:
                self.conn.executemany(
                    "UPDATE tasks SET link_key = ? WHERE task_id = ?", updates
                )
                self.conn.commit()
            except sqlite3.Error:
                self.conn.rollback()
                raise
        return len(updates)

    def backfill_links(self, compute_key: Any, batch: int = 2000) -> dict[str, int]:
        """给历史任务补 link_key 与 parent_task_id。

        为什么不直接调 rebuild()：它会 `DELETE FROM tasks` 再全量重插，
        与实时写入存在竞态 —— 重放期间新到的事件，其 tasks 行会被一并删掉。
        回填只做 UPDATE，不删任何行，可以安全地在服务跑着的时候执行。

        link_key 必须在 Python 里算（sha1 纯 SQL 做不了）；
        parent 的时间区间判定反而一条 SQL 就够，且与实时推断口径等价：
        「child 发起时 parent 还没结束」。
        """
        filled_links = 0
        while True:
            with self.lock:
                rows = self.conn.execute(
                    "SELECT task_id, requester_session, handler_session, handler_session_real "
                    "FROM tasks "
                    f"WHERE {self._NO_LINK} "
                    "AND requester_session <> '' AND handler_session <> '' LIMIT ?",
                    (batch,),
                ).fetchall()
            if not rows:
                break
            updates = []
            for row in rows:
                # 口径跟 Task.handler_target() 一致：优先真实执行会话，
                # 没学到才退而用入口 ID。两边不一致会出两套 link_key。
                target = row["handler_session_real"] or row["handler_session"]
                key = compute_key(row["requester_session"], target)
                if key:
                    updates.append((key, row["task_id"]))
            if not updates:
                break
            with self.lock:
                try:
                    self.conn.executemany(
                        "UPDATE tasks SET link_key = ? WHERE task_id = ?", updates
                    )
                    self.conn.commit()
                except sqlite3.Error:
                    self.conn.rollback()
                    raise
            filled_links += len(updates)
            if len(rows) < batch:
                break

        with self.lock:
            try:
                # 与实时推断口径一致（见 trace._infer_parent）：
                # 候选不唯一时直接放弃，所以用 CASE WHEN COUNT(*)=1 而不是 ORDER BY + LIMIT 1。
                self.conn.execute(
                    """
                    UPDATE tasks SET parent_task_id = COALESCE((
                        SELECT CASE WHEN COUNT(*) = 1 THEN MIN(p.task_id) END
                        FROM tasks p
                        WHERE p.task_id <> tasks.task_id
                          AND tasks.requester_session <> ''
                          AND (p.handler_session = tasks.requester_session
                               OR p.handler_session_real = tasks.requester_session)
                          -- 排除反向回合（打回给父任务的发起方），那是同一条链路的下一步
                          AND tasks.handler_session <> p.requester_session
                          AND COALESCE(NULLIF(tasks.created_at, 0), tasks.first_seen)
                              >= COALESCE(NULLIF(p.created_at, 0), p.first_seen, p.delivered_at) - 1
                          AND COALESCE(NULLIF(tasks.created_at, 0), tasks.first_seen)
                              <= COALESCE(p.finished_at, 9e18)
                    ), '')
                    WHERE parent_task_id IS NULL OR parent_task_id = ''
                    """
                )
                self.conn.commit()
                parents = self.conn.execute(
                    "SELECT COUNT(*) FROM tasks WHERE parent_task_id <> ''"
                ).fetchone()[0]
            except sqlite3.Error:
                self.conn.rollback()
                raise
        if filled_links or parents:
            logger.info("trace: 回填链路完成 link_key=%d parent=%d", filled_links, parents)
        return {"link_key": filled_links, "parent": parents}

    def _link_agg_sql(self, base: str, having: str) -> str:
        """按 link_key 聚合。

        turns_low_first 的小技巧：link_key = sha1(sorted(a,b))，而建键用的是
        requester_session 与「真实执行会话」（handler_session_real 优先），
        所以这里的比较必须用同一个口径，否则走协作入口的回合会把方向判反。
        一条 SQL 就能拆出双向回合数，不用把每条链路的全部回合拉回内存。
        """
        return f"""
            SELECT link_key,
                   COUNT(*)                                                      AS turns,
                   SUM(CASE WHEN terminal = 0 THEN 1 ELSE 0 END)                 AS active,
                   SUM(CASE WHEN terminal = 0 AND last_activity < ? THEN 1 ELSE 0 END) AS silent,
                   SUM(CASE WHEN state = 'failed'    THEN 1 ELSE 0 END)          AS failed,
                   SUM(CASE WHEN state = 'done'      THEN 1 ELSE 0 END)          AS done,
                   SUM(CASE WHEN state = 'notified'  THEN 1 ELSE 0 END)          AS notified,
                   SUM(CASE WHEN state = 'cancelled' THEN 1 ELSE 0 END)          AS cancelled,
                   SUM(CASE WHEN requester_session <
                                 COALESCE(NULLIF(handler_session_real, ''), handler_session)
                            THEN 1 ELSE 0 END)                                   AS turns_low_first,
                   MIN(COALESCE(NULLIF(created_at, 0), first_seen))              AS first_at,
                   MAX(last_activity)                                            AS last_at,
                   SUM(CASE WHEN state = 'done'
                            THEN finished_at - COALESCE(NULLIF(created_at, 0), first_seen)
                       END)                                                      AS work_total
            FROM tasks{base}
            GROUP BY link_key{having}
        """

    def query_links(
        self,
        *,
        since: Optional[float] = None,
        until: Optional[float] = None,
        user: str = "",
        role: str = "all",
        link_state: str = "all",
        keyword: str = "",
        limit: int = 50,
        offset: int = 0,
        silent_before: Optional[float] = None,
    ) -> dict[str, Any]:
        """链路列表。过滤先作用在回合行上，再按会话对聚合。

        注意 state 在链路层语义不同：“进行中”指「这条链路还有未终结的回合」，
        而不是“只看未终结的回合”。所以给 _filters 传 state=all，自己用 HAVING 筛。
        """
        clause, args = self._filters(
            since=since, until=until, user=user, role=role, state="all", keyword=keyword,
        )
        joiner = " AND " if clause else " WHERE "
        base = f"{clause}{joiner}link_key <> ''"
        cut = silent_before if silent_before is not None else time.time() - DEFAULT_SILENT_SECONDS

        having = ""
        if link_state == "active":
            having = " HAVING active > 0"
        elif link_state == "silent":
            having = " HAVING silent > 0"
        elif link_state == "failed":
            having = " HAVING failed > 0"
        elif link_state == "done":
            having = " HAVING active = 0 AND failed = 0"
        agg = self._link_agg_sql(base, having)

        with self.lock:
            total = self.conn.execute(
                f"SELECT COUNT(*) FROM ({agg})", (cut, *args)
            ).fetchone()[0]
            # 进行中链路数必须走全量统计，不能只数当前页
            active_total = self.conn.execute(
                f"SELECT COUNT(*) FROM ({self._link_agg_sql(base, ' HAVING active > 0')})",
                (cut, *args),
            ).fetchone()[0]
            rows = self.conn.execute(
                f"{agg} ORDER BY (active > 0) DESC, last_at DESC LIMIT ? OFFSET ?",
                (cut, *args, limit, offset),
            ).fetchall()
            # 会话 ID 缺失而无法归入任何链路的任务，必须诚实告知数量
            orphans = self.conn.execute(
                f"SELECT COUNT(*) FROM tasks{clause}{joiner}{self._NO_LINK}", args
            ).fetchone()[0]
        return {
            "total": total,
            "active_total": active_total,
            "rows": [dict(r) for r in rows],
            "orphan_tasks": orphans,
            "silent_cut": cut,
        }

    def tasks_of_links(self, link_keys: list[str], cap: int = 20000) -> list[dict[str, Any]]:
        """一次把若干条链路的回合全拉出来（避免 N+1），按发起时间升序。"""
        if not link_keys:
            return []
        holders = ",".join("?" * len(link_keys))
        with self.lock:
            rows = self.conn.execute(
                f"SELECT * FROM tasks WHERE link_key IN ({holders}) "
                "ORDER BY link_key, COALESCE(NULLIF(created_at, 0), first_seen) ASC LIMIT ?",
                (*link_keys, cap),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def session_graph_rows(self, *, link_key: str = "", task_id: str = "") -> list[dict[str, Any]]:
        """All records in the selected session-connected component, regardless of state.

        Client namespaces matter for named entry targets. No parent-task inference.
        """
        from collections import defaultdict, deque
        if not link_key and not task_id:
            return []
        with self.lock:
            rows = [self._row_to_dict(r) for r in self.conn.execute(
                "SELECT * FROM tasks ORDER BY COALESCE(NULLIF(created_at, 0), first_seen), task_id"
            ).fetchall()]
        sessions = defaultdict(list)
        endpoints = []
        seeds = []
        for i, r in enumerate(rows):
            pair = ((r['requester'], r['requester_session']),
                    (r['handler'], r['handler_session_real'] or r['handler_session']))
            pair = tuple(p for p in pair if p[1])
            endpoints.append(pair)
            for p in pair:
                sessions[p].append(i)
            if (r['task_id'] == task_id if task_id else r['link_key'] == link_key):
                seeds.append(i)
        queue = deque(seeds)
        found = set(seeds)
        seen_sessions = set()
        while queue:
            for p in endpoints[queue.popleft()]:
                if p in seen_sessions:
                    continue
                seen_sessions.add(p)
                for i in sessions[p]:
                    if i not in found:
                        found.add(i)
                        queue.append(i)
        return [r for i, r in enumerate(rows) if i in found]

    def execution_graph_rows(self, *, link_key: str = "", task_id: str = "",
                             cap: int = 500) -> tuple[list[dict[str, Any]], bool]:
        """单次任务的祖先、后代与同树分支；UNION 去重保证环不会无限递归。"""
        if not link_key and not task_id:
            return [], False
        seed = "task_id = ?" if task_id else "link_key = ?"
        with self.lock:
            rows = self.conn.execute(
                f"""
                WITH RECURSIVE family(task_id) AS (
                    SELECT task_id FROM tasks WHERE {seed}
                    UNION
                    SELECT p.task_id FROM tasks t JOIN family f ON t.task_id = f.task_id
                    JOIN tasks p ON p.task_id = t.parent_task_id
                    UNION
                    SELECT t.task_id FROM tasks t JOIN family f ON t.parent_task_id = f.task_id
                    LIMIT ?
                )
                SELECT tasks.* FROM family JOIN tasks ON tasks.task_id = family.task_id
                ORDER BY COALESCE(NULLIF(created_at, 0), first_seen), tasks.task_id
                """, (task_id or link_key, cap + 1),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows[:cap]], len(rows) > cap

    def related_session_tasks(self, sessions: list[str], exclude_link: str) -> list[dict[str, Any]]:
        """两端真实会话与第三方的全部通信；会话关联不代表因果关系。"""
        sessions = [s for s in sessions if s]
        if not sessions:
            return []
        holders = ",".join("?" for _ in sessions)
        with self.lock:
            rows = self.conn.execute(
                f"SELECT * FROM tasks WHERE link_key <> ? AND ("
                f"requester_session IN ({holders}) OR "
                f"COALESCE(NULLIF(handler_session_real, ''), handler_session) IN ({holders})) "
                "ORDER BY COALESCE(NULLIF(created_at, 0), first_seen), task_id",
                (exclude_link, *sessions, *sessions),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def link_edges(self, link_keys: list[str]) -> list[dict[str, Any]]:
        """链路之间的父子边（多级转发 A→B→C 就靠这个拼成树）。"""
        if not link_keys:
            return []
        holders = ",".join("?" * len(link_keys))
        with self.lock:
            rows = self.conn.execute(
                f"""
                SELECT p.link_key AS parent_link, c.link_key AS child_link,
                       COUNT(*) AS turns, MIN(c.task_id) AS sample_task
                FROM tasks c JOIN tasks p ON c.parent_task_id = p.task_id
                WHERE c.parent_task_id <> '' AND c.link_key <> '' AND p.link_key <> ''
                  AND c.link_key <> p.link_key
                  AND (p.link_key IN ({holders}) OR c.link_key IN ({holders}))
                GROUP BY parent_link, child_link
                """,
                (*link_keys, *link_keys),
            ).fetchall()
        return [dict(r) for r in rows]

    def link_summary(self, link_key: str, silent_before: Optional[float] = None) -> Optional[dict[str, Any]]:
        """单条链路的聚合行（详情页用，不受时间筛选影响）。"""
        cut = silent_before if silent_before is not None else time.time() - DEFAULT_SILENT_SECONDS
        agg = self._link_agg_sql(" WHERE link_key = ?", "")
        with self.lock:
            row = self.conn.execute(agg, (cut, link_key)).fetchone()
        if row is None or not row["link_key"]:
            return None
        return dict(row)

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
