"""跨端任务链路追踪。

## 这个模块存在的理由

Relay 本身只是「身份注册 + 转发」，不理解业务。但 YAgent 的跨端协作（Hub）
**刻意不用** Relay 的 expect_reply/reply，request / response / notify /
probe / probe_ack / cancel 六种信封全部走 `send`，都必须经过 Relay 这一跳
（原因见 Yagent_Desk/src/main/hub/relay-protocol.ts 顶部注释）。

于是 Relay 成了整条跨端链路上**唯一的全局汇聚点**：不加任何客户端改动，
仅靠流经的帧就能还原出「谁把什么任务派给了谁、对方处理到哪一步、花了多久」。
本模块干的就是这件事——从帧里抽元数据、聚合成任务，写入 SQLite 供看板查询。

## 关联规则（全部来自 Hub 的既有约定，不是这里发明的）

- 任务 = 一条 request，`task_id` 取该 request 的 `envelope.msg_id`
- response / error / cancel / probe / probe_ack 一律用 `reply_to` 指回 request 的 msg_id
- probe 的 msg_id 形如 `PB:<task_id>:<seq>`，所以 Relay 回报 PEER_OFFLINE 时
  能从 `error.ref` 反推出是哪个任务的探针没送到（见 hub.ts PROBE_ID_PREFIX）
- probe_ack 的 payload 是 `{"state": "working|queued|deferred|pending_retry|unknown"}`

## 精度声明（重要，别对着看板得出错误结论）

- 发起时间、投递结果、完成时间、总耗时：**精确**
- 「何时真正开始处理」：只能由 probe_ack 推断，而 probe 每 60s 一轮，
  所以开始时间与排队/处理时长的**误差可达 ±60s**。
  想要秒级阶段进度，得让 Desk / Server 侧主动上报，Relay 无法凭空得知。

## 存储

事件与任务全部落在 SQLite（见 `trace_db.py`），**默认永久保留**；
看板默认只展示最近 14 天，但可以筛选到任意区间。
写入同步提交，进程被强杀也不丢已转发的记录。

## 隐私

payload 默认会保留**完整正文**（`trace_payload_chars`，默认 20000 字符），
以便在看板上直接查看请求内容。这意味着**经本中继转发的消息正文
会落到本机数据库**，包括其他客户端发来的内容。

把 `trace_payload_chars` 设为 0 可以回到旧行为：只留 200 字摘要与字节数，
不留全文。
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

from .trace_graph import build_execution_graph, task_timing

logger = logging.getLogger(__name__)

#: crossnode payload 的包装标记，与 relay-protocol.ts 的 CROSSNODE_PAYLOAD_KEY 一致
CROSSNODE_PAYLOAD_KEY = "__crossnode__"
#: probe 信封 msg_id 前缀，与 hub.ts 的 PROBE_ID_PREFIX 一致
PROBE_ID_PREFIX = "PB:"

#: 超过这个时间没有任何事件且未终结，认为链路「静默」（≈3 轮 probe，与 Hub 判死阈值对齐）
SILENT_AFTER_SECONDS = 200.0

#: 摘要优先取这些字段。Hub 侧 extractSummary 取的是 payload.message，其余是兜底
_SUMMARY_KEYS = ("message", "text", "prompt", "content", "summary", "result", "detail")

_WS_RE = re.compile(r"\s+")

# 任务状态（state）取值
STATE_SENT = "sent"                    # 已发出，尚未确认投递
STATE_DELIVERED = "delivered"          # Relay 已投递给对端，还没有任何进度反馈
STATE_QUEUED = "queued"                # 对端会话忙，在 inbox 排队
STATE_WORKING = "working"              # 对端正在跑
STATE_DEFERRED = "deferred"            # 对端主动推迟
STATE_PENDING_RETRY = "pending_retry"  # 对端已收下但当前没在跑，等催办重投
STATE_DONE = "done"                    # 收到 response
STATE_FAILED = "failed"                # 收到 error / 投递失败 / 对端不认识
STATE_CANCELLED = "cancelled"          # 发起方撤回
STATE_NOTIFIED = "notified"            # 单向 notify，按协议无回执

TERMINAL_STATES = frozenset({STATE_DONE, STATE_FAILED, STATE_CANCELLED, STATE_NOTIFIED})

#: probe_ack 的 state → 任务状态
_PROBE_STATE_MAP = {
    "working": STATE_WORKING,
    "queued": STATE_QUEUED,
    "deferred": STATE_DEFERRED,
    "pending_retry": STATE_PENDING_RETRY,
}


# ----------------------------------------------------------------------
# 帧 → 事件
# ----------------------------------------------------------------------


def summarize_payload(payload: Any, limit: int) -> str:
    """从业务 payload 里取一段人能看懂的摘要。只用于展示，不参与任何判定。"""
    if payload is None:
        return ""
    text: Optional[str] = None
    if isinstance(payload, str):
        text = payload
    elif isinstance(payload, dict):
        for key in _SUMMARY_KEYS:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                text = value
                break
        if text is None:
            try:
                text = json.dumps(payload, ensure_ascii=False)
            except (TypeError, ValueError):
                text = str(payload)
    else:
        text = str(payload)
    text = _WS_RE.sub(" ", text or "").strip()
    return text[:limit]


def payload_text(payload: Any, limit: int) -> str:
    """取业务 payload 的**完整正文**，用于看板详情展示。

    和 summarize_payload 的取值口径一致（同一组 _SUMMARY_KEYS），但：
      - 不把换行压成空格，保留原排版；
      - 不按摘要长度截断。

    limit 只是防弹保护：异常巨大的 payload（比如误传文件）不应该把
    中继的库撑爆，超过就截断并在末尾标明。
    """
    if payload is None:
        return ""
    text: Optional[str] = None
    if isinstance(payload, str):
        text = payload
    elif isinstance(payload, dict):
        for key in _SUMMARY_KEYS:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                text = value
                break
        if text is None:
            try:
                text = json.dumps(payload, ensure_ascii=False, indent=2)
            except (TypeError, ValueError):
                text = str(payload)
    else:
        text = str(payload)
    text = (text or "").strip()
    if limit > 0 and len(text) > limit:
        return text[:limit] + f"\n…已截断，原文共 {len(text)} 字符"
    return text


def payload_bytes(payload: Any) -> int:
    if payload is None:
        return 0
    try:
        return len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    except (TypeError, ValueError):
        return len(str(payload).encode("utf-8"))


def unwrap_crossnode(payload: Any) -> Optional[dict[str, Any]]:
    """从 Relay payload 里取出 crossnode 信封；不是跨端消息则返回 None。

    同一个 Relay 上还挂着别人的 ui / py_worker 客户端，它们的业务消息与追踪无关，
    静默忽略即可（这与 Hub 侧 unwrapEnvelopeResult 的 not_crossnode 分支同义）。
    """
    if not isinstance(payload, dict) or CROSSNODE_PAYLOAD_KEY not in payload:
        return None
    envelope = payload.get("envelope")
    if not isinstance(envelope, dict):
        return None
    if not isinstance(envelope.get("msg_id"), str) or not isinstance(envelope.get("type"), str):
        return None
    return envelope


def make_link_key(session_a: str, session_b: str) -> str:
    """两个会话之间的链路标识（**无序对**）。

    链路 = 一对会话之间的「边」，而不是某个会话本身。这点很关键：
    A 的同一个会话可能同时找 B 和 C 干活，那是两条链路；
    按单个会话分组会把它们糊在一起，按人分组更糟（A 用两个会话找 B
    做两件不相干的事也会被合并）。

    排序后再哈希，保证 A→B 与 B→A 落到同一条链路 —— 这正是把
    「A→B→A→B」多轮往返串成一条线的地基。

    取 sha1 前 16 位而不是直接拼接：会话 ID 是 UUID，拼出来 70+ 字符，
    既撑大索引又没法安全放进 URL 路径。原始两个会话 ID 仍在
    requester_session / handler_session 里，随时可还原。
    """
    a = (session_a or "").strip()
    b = (session_b or "").strip()
    if not a or not b:
        return ""
    low, high = sorted((a, b))
    return hashlib.sha1(f"{low}|{high}".encode("utf-8")).hexdigest()[:16]


def parse_probe_task(msg_id: str) -> Optional[str]:
    """`PB:<task_id>:<seq>` → task_id。"""
    if not msg_id.startswith(PROBE_ID_PREFIX):
        return None
    rest = msg_id[len(PROBE_ID_PREFIX) :]
    index = rest.rfind(":")
    return rest[:index] if index > 0 else (rest or None)


def _addr(value: Any) -> tuple[str, str]:
    if isinstance(value, dict):
        return str(value.get("client") or ""), str(value.get("session") or "")
    return "", ""


def build_event(
    envelope: dict[str, Any],
    *,
    relay_from: str,
    relay_to: str,
    delivered: int,
    ts: float,
    summary_chars: int,
    payload_chars: int = 0,
    origin: str = "ws",
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> dict[str, Any]:
    """把一条 crossnode 信封压成可落盘的追踪事件（不含业务全文）。"""
    etype = str(envelope.get("type") or "other")
    from_client, from_session = _addr(envelope.get("from"))
    to_client, to_session = _addr(envelope.get("to"))
    payload = envelope.get("payload")

    event: dict[str, Any] = {
        "ts": round(ts, 3),
        "ev": etype,
        "msg_id": str(envelope.get("msg_id") or ""),
        "conv_id": str(envelope.get("conv_id") or ""),
        "reply_to": str(envelope.get("reply_to") or "") or None,
        # from/to 以信封里的会话级地址为准，Relay 层的 client_id 只在信封缺字段时兜底
        "from_client": from_client or relay_from,
        "from_session": from_session,
        "to_client": to_client or relay_to,
        "to_session": to_session,
        "hop": envelope.get("hop"),
        "ttl": envelope.get("ttl"),
        "created_at": envelope.get("created_at"),
        "delivered": delivered,
        "bytes": payload_bytes(payload),
        "origin": origin,
    }
    if etype in ("request", "notify", "response"):
        # agent 显式上报的摘要优先；没有就回落到截断正文（旧行为）。
        # 回落而不是置空，是为了老客户端与历史数据在看板上不至于突然变成空白。
        declared = envelope.get("summary")
        if isinstance(declared, str) and declared.strip():
            event["summary"] = _WS_RE.sub(" ", declared).strip()[:summary_chars]
        else:
            event["summary"] = summarize_payload(payload, summary_chars)
        # 完整正文：摘要只有 200 字，排查时不够用。
        # 注意：这会把转发过的消息正文落到中继的库里，已与使用方确认。
        if payload_chars:
            event["message"] = payload_text(payload, payload_chars)
        # 仅供看板关联使用，不参与任何投递判定
        parent_msg_id = str(envelope.get("parent_msg_id") or "")
        if parent_msg_id:
            event["parent_msg_id"] = parent_msg_id
    if etype == "probe_ack":
        state = payload.get("state") if isinstance(payload, dict) else None
        event["peer_state"] = str(state or "")
    if etype == "error":
        event["code"] = str(envelope.get("code") or "")
        event["message"] = summarize_payload(envelope.get("message"), 200)
    if error_code:
        event["error_code"] = error_code
        event["error_message"] = summarize_payload(error_message, 200)
    chain = envelope.get("chain")
    if isinstance(chain, list) and chain:
        event["chain"] = [
            {"client": c.get("client"), "session": c.get("session"), "ts": c.get("ts")}
            for c in chain
            if isinstance(c, dict)
        ][:16]
    return event


# ----------------------------------------------------------------------
# 任务
# ----------------------------------------------------------------------


@dataclass
class Task:
    task_id: str
    conv_id: str = ""
    kind: str = "request"           # request | notify
    requester: str = ""
    requester_session: str = ""
    handler: str = ""
    handler_session: str = ""
    summary: str = ""
    created_at: float = 0.0
    first_seen: float = 0.0
    delivered_at: Optional[float] = None
    delivered: int = 0
    state: str = STATE_SENT
    state_at: float = 0.0
    started_at: Optional[float] = None   # 首次得知对端 working（±60s）
    queued_at: Optional[float] = None
    finished_at: Optional[float] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    probe_count: int = 0
    probe_ack_count: int = 0
    last_ack_at: Optional[float] = None
    last_activity: float = 0.0
    bytes_out: int = 0               # request 的 payload 大小
    bytes_in: int = 0                # response 的 payload 大小
    hop: int = 1
    ttl: Optional[int] = None
    #: 会话对链路标识（无序），见 make_link_key
    link_key: str = ""
    #: 多级转发的父任务。优先用协议直接给的 parent_msg_id；
    #: 没有才回落到 _infer_parent 的推断（老客户端 / agent 漏填）
    parent_task_id: str = ""
    #: 对端**实际执行**的会话。走协作入口时 handler_session 是入口 ID（如
    #: desk-control），真实会话要等 response / probe_ack 回来才知道
    handler_session_real: str = ""
    chain: list[dict[str, Any]] = field(default_factory=list)
    #: 没见到发起帧就先见到回复（Relay 重启后半途接入），数据不完整
    partial: bool = False
    timeline: list[dict[str, Any]] = field(default_factory=list)

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def handler_target(self) -> str:
        """建链路时该用的处理方会话。

        优先真实执行会话：走协作入口时 handler_session 只是入口 ID，
        不是对端真正干活的那个会话。还没学到真实会话时先用入口 ID 兑着，
        等回帧回来再改绑（见 TaskAggregator._rebind_link）。

        这个口径必须与 trace_db 的回填 SQL 保持一致，否则实时写入与历史
        回填会算出两套不同的 link_key。
        """
        return self.handler_session_real or self.handler_session

    def duration(self, now: Optional[float] = None) -> float:
        end = self.finished_at or (now or time.time())
        start = self.created_at or self.first_seen
        return max(0.0, end - start)

    def silent(self, now: Optional[float] = None) -> bool:
        """未终结且长时间没有任何帧——对端可能已断电，Hub 也快判死了。"""
        if self.terminal:
            return False
        return (now or time.time()) - self.last_activity > SILENT_AFTER_SECONDS

    def brief(self, now: Optional[float] = None) -> dict[str, Any]:
        now = now or time.time()
        wait = None
        if self.started_at:
            wait = max(0.0, self.started_at - (self.created_at or self.first_seen))
        work = None
        if self.started_at:
            work = max(0.0, (self.finished_at or now) - self.started_at)
        return {
            **task_timing(self.to_row(), now),
            "task_id": self.task_id,
            "conv_id": self.conv_id,
            "kind": self.kind,
            "requester": self.requester,
            "requester_session": self.requester_session,
            "handler": self.handler,
            "handler_session": self.handler_session,
            "summary": self.summary,
            "created_at": self.created_at or self.first_seen,
            "delivered_at": self.delivered_at,
            "delivered": self.delivered,
            "state": self.state,
            "state_at": self.state_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration": round(self.duration(now), 3),
            "wait_seconds": round(wait, 3) if wait is not None else None,
            "work_seconds": round(work, 3) if work is not None else None,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "probe_count": self.probe_count,
            "probe_ack_count": self.probe_ack_count,
            "last_ack_at": self.last_ack_at,
            "last_activity": self.last_activity,
            "bytes_out": self.bytes_out,
            "bytes_in": self.bytes_in,
            "hop": self.hop,
            "ttl": self.ttl,
            "link_key": self.link_key,
            "parent_task_id": self.parent_task_id,
            "handler_session_real": self.handler_session_real,
            #: 走协作入口时入口 ID 与真实执行会话不一致，界面需要标出来
            "via_endpoint": bool(
                self.handler_session_real
                and self.handler_session
                and self.handler_session_real != self.handler_session
            ),
            "terminal": self.terminal,
            "silent": self.silent(now),
            "partial": self.partial,
        }

    def detail(self, now: Optional[float] = None) -> dict[str, Any]:
        data = self.brief(now)
        data["chain"] = self.chain
        data["timeline"] = self.timeline
        return data

    # ---------- 与 SQLite 行的互转 ----------

    def to_row(self) -> dict[str, Any]:
        """导出为 tasks 表的一行。terminal 写成冗余列，让 SQL 能直接筛“进行中”。"""
        return {
            "task_id": self.task_id,
            "conv_id": self.conv_id,
            "kind": self.kind,
            "requester": self.requester,
            "requester_session": self.requester_session,
            "handler": self.handler,
            "handler_session": self.handler_session,
            "summary": self.summary,
            "created_at": self.created_at,
            "first_seen": self.first_seen,
            "delivered_at": self.delivered_at,
            "delivered": self.delivered,
            "state": self.state,
            "state_at": self.state_at,
            "started_at": self.started_at,
            "queued_at": self.queued_at,
            "finished_at": self.finished_at,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "probe_count": self.probe_count,
            "probe_ack_count": self.probe_ack_count,
            "last_ack_at": self.last_ack_at,
            "last_activity": self.last_activity,
            "bytes_out": self.bytes_out,
            "bytes_in": self.bytes_in,
            "hop": self.hop,
            "ttl": self.ttl,
            "link_key": self.link_key,
            "parent_task_id": self.parent_task_id,
            "handler_session_real": self.handler_session_real,
            "partial": 1 if self.partial else 0,
            "terminal": 1 if self.terminal else 0,
            "chain": self.chain,
            "timeline": self.timeline,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Task":
        task = cls(task_id=str(row.get("task_id") or ""))
        task.conv_id = row.get("conv_id") or ""
        task.kind = row.get("kind") or "request"
        task.requester = row.get("requester") or ""
        task.requester_session = row.get("requester_session") or ""
        task.handler = row.get("handler") or ""
        task.handler_session = row.get("handler_session") or ""
        task.summary = row.get("summary") or ""
        task.created_at = float(row.get("created_at") or 0.0)
        task.first_seen = float(row.get("first_seen") or 0.0)
        task.delivered_at = row.get("delivered_at")
        task.delivered = int(row.get("delivered") or 0)
        task.state = row.get("state") or STATE_SENT
        task.state_at = float(row.get("state_at") or 0.0)
        task.started_at = row.get("started_at")
        task.queued_at = row.get("queued_at")
        task.finished_at = row.get("finished_at")
        task.error_code = row.get("error_code")
        task.error_message = row.get("error_message")
        task.probe_count = int(row.get("probe_count") or 0)
        task.probe_ack_count = int(row.get("probe_ack_count") or 0)
        task.last_ack_at = row.get("last_ack_at")
        task.last_activity = float(row.get("last_activity") or 0.0)
        task.bytes_out = int(row.get("bytes_out") or 0)
        task.bytes_in = int(row.get("bytes_in") or 0)
        task.hop = int(row.get("hop") or 1)
        task.ttl = row.get("ttl")
        task.link_key = row.get("link_key") or ""
        task.parent_task_id = row.get("parent_task_id") or ""
        task.handler_session_real = row.get("handler_session_real") or ""
        task.partial = bool(row.get("partial"))
        chain = row.get("chain")
        task.chain = chain if isinstance(chain, list) else []
        timeline = row.get("timeline")
        task.timeline = timeline if isinstance(timeline, list) else []
        return task


def event_task_id(event: dict[str, Any]) -> Optional[str]:
    """一条事件归属的任务 id。

    归因规则全部来自 Hub 的既有约定：request / notify 自己就是任务，
    其余靠 reply_to；probe 还额外支持从 `PB:<task_id>:<seq>` 反推。
    """
    etype = event.get("ev")
    if etype in ("request", "notify"):
        return str(event.get("msg_id") or "") or None
    reply_to = event.get("reply_to")
    if reply_to:
        return str(reply_to)
    if etype == "probe":
        return parse_probe_task(str(event.get("msg_id") or ""))
    return None


class TaskAggregator:
    """事件流 → 任务集合（纯内存推导，不碰存储）。

    它既用于实时埋点，也用于从 `events` 表重建 `tasks`，
    保证“三个月前的任务”与“刚刚发生的任务”在看板上口径完全一致。
    """

    def __init__(self, *, max_tasks: int = 5000, max_timeline: int = 60) -> None:
        self.max_tasks = max_tasks
        self.max_timeline = max_timeline
        self.tasks: "OrderedDict[str, Task]" = OrderedDict()
        self.ignored = 0  # 无法归因到任务的事件数（只做自检用）
        #: 会话 → 它正在处理的外来任务 id（仅未终结的）。
        #: 用于多级转发 A→B→C 的父子推断，见 _infer_parent。
        self.open_by_session: dict[str, list[str]] = {}

    # ---------- 内部 ----------

    def _index_open(self, task: Task) -> None:
        """把未终结任务挂到它的处理会话下。

        入口 ID 与真实执行会话两个键都挂：走协作入口时，对端 agent
        再转发出去的请求用的是**真实会话**，只挂入口 ID 就对不上了。
        """
        for sess in (task.handler_session, task.handler_session_real):
            if not sess:
                continue
            bucket = self.open_by_session.setdefault(sess, [])
            if task.task_id not in bucket:
                bucket.append(task.task_id)

    def _unindex(self, task: Task) -> None:
        for sess in (task.handler_session, task.handler_session_real):
            bucket = self.open_by_session.get(sess)
            if not bucket:
                continue
            if task.task_id in bucket:
                bucket.remove(task.task_id)
            if not bucket:
                self.open_by_session.pop(sess, None)

    def _ensure_link_key(self, task: Task) -> None:
        """算出会话对链路标识。

        链路建在**真实执行会话**上：走协作入口时 handler_session 只是入口 ID
        （desk-control / yaya-collab 这种人取的名字），对端真正干活的是入口
        派生出来的会话。绑到入口上会把「A 对这个入口派的所有活」糊成一条，
        而它们其实是一对多的独立对话。

        时序上有个坎：发起时还不知道真实会话，要等对端回帧才学得到。
        所以这里允许**改绑一次** —— 从入口键切到真实会话键。见 _rebind_link。
        """
        if task.link_key:
            return
        key = make_link_key(task.requester_session, task.handler_target())
        if key:
            task.link_key = key

    def _learn_real_session(self, task: Task, event: dict[str, Any]) -> None:
        """从对端回帧（response / error / probe_ack）学到真实执行会话。

        只能用回复类帧：cancel 是发起方发的，它的 from_session 是发起方。
        """
        sess = str(event.get("from_session") or "")
        if not sess or task.handler_session_real == sess:
            return
        self._unindex(task)
        task.handler_session_real = sess
        # 之前按入口 ID 建的链路要改绑到真实会话上
        self._rebind_link(task)
        if not task.terminal:
            self._index_open(task)

    def _rebind_link(self, task: Task) -> None:
        """真实执行会话到手后，把链路从入口键改绑到真实会话键。

        为什么允许改：发起帧里 handler_session 是入口 ID，那时候没法知道
        对端会派哪个会话来干活。等 response/probe_ack 回来才有真实会话，
        此时必须把这条任务挪到正确的链路上，否则同一次对话的来回两个方向
        会落在两条链路里（去程按入口、回程按真实会话）。

        可随新回帧校正；旧的会话索引在学习新会话前移除。
        """
        new_key = make_link_key(task.requester_session, task.handler_target())
        if new_key and new_key != task.link_key:
            task.link_key = new_key

    def _infer_parent(self, task: Task) -> None:
        """多级转发 A→B→C 的父子推断。

        协议层帮不上忙：hub.ts 的 sendRequest 里 chain 永远是 `[本机]`、
        hop 永远是 1（chain 只在 response **回传**方向累积），所以 B 转发给 C
        的请求与 A 的请求没有任何机器可读的关联。

        只能靠「B 在处理 A 的请求期间，从同一个会话发起了去 C 的请求」推断，
        这建立在「一个 agent 会话串行执行」这个事实上。所以一旦同一会话上同时
        挂着多个未终结的外来任务，前提就不成立了 —— 此时**宁缺毋滥**，
        直接放弃，不塞一个错的父亲给看板。推断结果在接口上以 parent_task_id
        暴露，界面上明确标为“推断”。

        协议已经给出 parent 时不走这里：那是 agent 亲口说的，比任何推断都准。
        """
        sess = task.requester_session
        if not sess or task.parent_task_id:
            return
        start = task.created_at or task.first_seen
        if not start:
            return
        matches: list[str] = []
        for tid in self.open_by_session.get(sess, ()):
            if tid == task.task_id:
                continue
            parent = self.tasks.get(tid)
            if parent is None or parent.terminal:
                continue
            # 排除反向回合：child 打回给 parent 的发起方，那是同一条链路的
            # 下一个回合（B 回头问 A），不是多级转发。真正的转发必须是找**第三方**。
            if task.handler_session and task.handler_session == parent.requester_session:
                continue
            # 下界用发起时刻而不是投递时刻：created_at 来自信封，
            # delivered_at 是埋点时刻，两者在补录/重放场景下会错位。
            # 语义上也更准：child 不可能在 parent 发起之前就是它的子任务。
            begin = parent.created_at or parent.first_seen or parent.delivered_at
            if not begin or start < begin - 1.0:
                continue
            matches.append(parent.task_id)
            if len(matches) > 1:
                return  # 同一会话同时挂着多个任务 → 无法归因，放弃
        if len(matches) == 1:
            task.parent_task_id = matches[0]

    def _touch(self, task: Task, ts: float) -> None:
        task.last_activity = max(task.last_activity, ts)

    def _push(self, task: Task, item: dict[str, Any]) -> None:
        """写时间线。

        probe / probe_ack 是每 60s 一轮的对账心跳，且**交替出现**，
        长任务能刷出几十行毫无信息量的记录。所以把它们合并成一条
        `heartbeat`：只要对端汇报的状态没变就累加计数；状态一变（queued→working）
        立刻新起一条，这才是真正值得看的进度拐点。
        投递失败的探针（带 error_code）不合并，那是要单独被看到的异常。
        """
        if item.get("ev") in ("probe", "probe_ack") and not item.get("error_code"):
            state = item.get("peer_state") or ""
            last = task.timeline[-1] if task.timeline else None
            mergeable = (
                last is not None
                and last.get("ev") == "heartbeat"
                and (not state or not last.get("peer_state") or last.get("peer_state") == state)
            )
            if mergeable:
                last["last_ts"] = item["ts"]
                if item["ev"] == "probe":
                    last["probe_count"] = int(last.get("probe_count", 0)) + 1
                else:
                    last["ack_count"] = int(last.get("ack_count", 0)) + 1
                    if state:
                        last["peer_state"] = state
                last["count"] = int(last.get("probe_count", 0)) + int(last.get("ack_count", 0))
                return
            entry = {
                "ev": "heartbeat",
                "ts": item["ts"],
                "last_ts": item["ts"],
                "peer_state": state or None,
                "probe_count": 1 if item["ev"] == "probe" else 0,
                "ack_count": 0 if item["ev"] == "probe" else 1,
                "count": 1,
            }
            task.timeline.append(entry)
        else:
            entry = dict(item)
            entry["count"] = 1
            entry["last_ts"] = item["ts"]
            task.timeline.append(entry)

        if len(task.timeline) > self.max_timeline:
            # 保留首帧（发起）与最近的若干条，中间掐掉
            head = task.timeline[0]
            task.timeline = [head, {"ev": "truncated", "ts": head["ts"]}] + task.timeline[
                -(self.max_timeline - 2) :
            ]

    def _evict(self) -> None:
        while len(self.tasks) > self.max_tasks:
            # 优先淘汰最老的已终结任务；全是未终结的才动最老的那条
            victim = None
            for task_id, task in self.tasks.items():
                if task.terminal:
                    victim = task_id
                    break
            if victim is None:
                victim = next(iter(self.tasks))
            dropped = self.tasks.pop(victim, None)
            if dropped is not None:
                self._unindex(dropped)

    def _get_or_create(self, task_id: str, ts: float) -> Task:
        task = self.tasks.get(task_id)
        if task is None:
            task = Task(task_id=task_id, first_seen=ts, state_at=ts, last_activity=ts, partial=True)
            self.tasks[task_id] = task
            self._evict()
        return task

    def _finish(self, task: Task, state: str, ts: float, code: str = "", message: str = "") -> None:
        task.state = state
        task.state_at = ts
        task.finished_at = ts
        if code:
            task.error_code = code
        if message:
            task.error_message = message
        self._touch(task, ts)
        # 终结了就不再能做别人的父任务
        self._unindex(task)

    # ---------- 对外 ----------

    def _apply_trace_fields(self, task: Task, event: dict[str, Any]) -> None:
        """消费 agent 上报的追踪字段。

        协议给的 parent 优先于 _infer_parent 的推断 —— 那是 agent 亲口说的，
        而推断在异步场景下本身就不成立（B 异步派发后 run 就结束了，
        欠 A 的回复还挂着，此时 inbound 表里会同时存在多条）。

        只消费 parent_msg_id：任务成败完全由传输层 state 表达（消息发出去、
        对方返回了就算成功），agent 不再上报业务状态。
        """
        parent = str(event.get("parent_msg_id") or "")
        # 防自环：agent 万一把自己的 msg_id 填进来，会把递归 CTE 拖进死循环
        if parent and parent != task.task_id:
            task.parent_task_id = parent

    def apply(self, event: dict[str, Any]) -> Optional[str]:
        """吃进一条事件，返回受影响的 task_id（无法归因则 None）。"""
        etype = event.get("ev")
        ts = float(event.get("ts") or time.time())
        msg_id = str(event.get("msg_id") or "")
        reply_to = event.get("reply_to") or None
        delivered = int(event.get("delivered") or 0)
        relay_err = event.get("error_code")

        if etype == "request":
            task = self.tasks.get(msg_id)
            if task is None:
                task = Task(task_id=msg_id, first_seen=ts)
                self.tasks[msg_id] = task
                self._evict()
            else:
                # 先见回复后见发起（回放边界），补齐字段但保留已有终态
                task.partial = False
            task.partial = False
            task.kind = "request"
            task.conv_id = event.get("conv_id") or task.conv_id
            task.requester = event.get("from_client") or task.requester
            task.requester_session = event.get("from_session") or task.requester_session
            task.handler = event.get("to_client") or task.handler
            task.handler_session = event.get("to_session") or task.handler_session
            task.summary = event.get("summary") or task.summary
            task.created_at = float(event.get("created_at") or ts)
            task.bytes_out = int(event.get("bytes") or 0)
            task.hop = int(event.get("hop") or 1)
            task.ttl = event.get("ttl")
            if event.get("chain"):
                task.chain = event["chain"]
            if not task.terminal:
                if relay_err:
                    self._finish(task, STATE_FAILED, ts, relay_err, event.get("error_message") or "")
                elif delivered > 0:
                    task.state = STATE_DELIVERED
                    task.state_at = ts
                    task.delivered_at = ts
                    task.delivered = delivered
                else:
                    task.state = STATE_SENT
                    task.state_at = ts
            self._ensure_link_key(task)
            self._apply_trace_fields(task, event)
            # 协议没给 parent 时才推断（此时自己还没进索引，不会自己当自己的父亲），再入索引
            self._infer_parent(task)
            if not task.terminal:
                self._index_open(task)
            self._touch(task, ts)
            self._push(task, event)
            return task.task_id

        if etype == "notify":
            # 单向通知，协议上不会有回执，收下即终态。
            task = self.tasks.get(msg_id) or Task(task_id=msg_id, first_seen=ts)
            self.tasks[msg_id] = task
            self._evict()
            task.partial = False
            task.kind = "notify"
            task.conv_id = event.get("conv_id") or task.conv_id
            task.requester = event.get("from_client") or ""
            task.requester_session = event.get("from_session") or ""
            task.handler = event.get("to_client") or ""
            task.handler_session = event.get("to_session") or ""
            task.summary = event.get("summary") or ""
            task.created_at = float(event.get("created_at") or ts)
            task.bytes_out = int(event.get("bytes") or 0)
            task.delivered = delivered
            task.delivered_at = ts if delivered else None
            self._apply_trace_fields(task, event)
            if relay_err:
                self._finish(task, STATE_FAILED, ts, relay_err, event.get("error_message") or "")
            else:
                self._finish(task, STATE_NOTIFIED, ts)
            self._ensure_link_key(task)
            self._infer_parent(task)
            self._touch(task, ts)
            self._push(task, event)
            return task.task_id

        # 以下都靠 reply_to 归因
        if etype in ("response", "error", "cancel", "probe_ack"):
            if not reply_to:
                self.ignored += 1
                return None
            task = self._get_or_create(str(reply_to), ts)
            if not task.handler and event.get("from_client"):
                task.handler = event["from_client"]
                task.handler_session = event.get("from_session") or ""
            if not task.requester and event.get("to_client"):
                task.requester = event["to_client"]
                task.requester_session = event.get("to_session") or ""
            task.conv_id = task.conv_id or (event.get("conv_id") or "")
            # 只有回复类帧的 from 才是处理方；cancel 是发起方发的，不能用
            if etype in ("response", "error", "probe_ack"):
                self._learn_real_session(task, event)
            self._ensure_link_key(task)

            if etype == "response":
                task.bytes_in = int(event.get("bytes") or 0)
                if not task.started_at:
                    task.started_at = task.delivered_at or task.first_seen
                # 对方回了话，这条链路就算走通了
                self._finish(task, STATE_DONE, ts)
            elif etype == "error":
                self._finish(
                    task,
                    STATE_FAILED,
                    ts,
                    event.get("code") or "error",
                    event.get("message") or "",
                )
            elif etype == "cancel":
                self._finish(task, STATE_CANCELLED, ts, "cancelled", "发起方撤回等待")
            else:  # probe_ack
                task.probe_ack_count += 1
                task.last_ack_at = ts
                peer_state = str(event.get("peer_state") or "")
                if peer_state == "unknown":
                    # 对端不认识这个请求：Hub 侧会据此判 no_reply
                    self._finish(task, STATE_FAILED, ts, "no_reply", "对端不认识该请求")
                elif not task.terminal:
                    mapped = _PROBE_STATE_MAP.get(peer_state)
                    if mapped:
                        task.state = mapped
                        task.state_at = ts
                        if mapped == STATE_WORKING and not task.started_at:
                            task.started_at = ts
                        if mapped == STATE_QUEUED and not task.queued_at:
                            task.queued_at = ts
            self._touch(task, ts)
            self._push(task, event)
            return task.task_id

        if etype == "probe":
            target = reply_to or parse_probe_task(msg_id)
            if not target:
                self.ignored += 1
                return None
            task = self._get_or_create(str(target), ts)
            task.probe_count += 1
            # probe 的投递失败不判任务死：对端可能只是在重启，Hub 自己会重试
            self._touch(task, ts)
            self._push(task, event)
            return task.task_id

        self.ignored += 1
        return None

    def apply_many(self, events: Iterable[dict[str, Any]]) -> int:
        count = 0
        for event in events:
            try:
                self.apply(event)
                count += 1
            except Exception:  # noqa: BLE001 —— 回放脏数据不能拖垮启动
                logger.debug("trace: 跳过无法解析的事件", exc_info=True)
        return count


# ----------------------------------------------------------------------
# 落盘 + 查询
# ----------------------------------------------------------------------


def _day_key(ts: float) -> str:
    """仅供日志/调试使用的日期键。"""
    return time.strftime("%Y%m%d", time.localtime(ts))


class TraceStore:
    """追踪存储：SQLite 永久落盘 + 活跃任务的内存聚合。

    数据流向：
        信封 → build_event → TaskAggregator（推导状态）
                            ↓
                    SQLite：INSERT events + UPSERT tasks（同一个事务）

    关键取舍：
    * **写入不做缓冲**。跨端消息频率很低，换来“进程被强杀也不丢记录”。
    * **查询全走 DB**，内存只是活跃任务的聚合缓存，避免两套口径。
    * 内存里被 LRU 淘汰的老任务，若稍后又收到它的 response，
      会先从 DB **回填**再推导（见 `_ensure_loaded`），
      否则会新建一个残缺任务把已有的完整记录覆盖掉。
    """

    def __init__(
        self,
        *,
        enabled: bool = True,
        db_path: Path | str = "logs/trace/trace.db",
        default_days: int = 14,
        prune_days: int = 0,
        max_tasks: int = 5000,
        max_timeline: int = 60,
        summary_chars: int = 200,
        payload_chars: int = 20000,
        store_probe_events: bool = True,
        hot_active_days: int = 30,
        hot_recent: int = 500,
    ) -> None:
        self.enabled = enabled
        self.db_path = Path(db_path)
        self.dir = self.db_path.parent
        #: 看板默认显示多少天（仅影响默认值，可以筛选到任意区间）
        self.default_days = max(1, default_days)
        #: 0 = 永不删除（默认）。大于 0 时才会清理那么多天之前的历史
        self.prune_days = max(0, prune_days)
        self.summary_chars = max(0, summary_chars)
        #: 消息完整正文上限；0 = 不存正文（回到只留摘要的旧行为）
        self.payload_chars = max(0, payload_chars)
        #: probe / probe_ack 心跳是否逐条入 events 表（关掉可省大量行，
        #: 任务上的轮数统计与时间线区间仍然保留）
        self.store_probe_events = store_probe_events
        self.agg = TaskAggregator(max_tasks=max_tasks, max_timeline=max_timeline)
        self.lock = threading.RLock()
        self.started_at = time.time()
        self.events_total = 0
        self.write_errors = 0
        self._subscribers: set[Any] = set()  # asyncio.Queue，由 trace_api 注册
        self.db: Any = None
        if self.enabled:
            from .trace_db import TraceDB  # 局部引入：关掉追踪时连 sqlite3 都不碰

            self.db = TraceDB(self.db_path)
            self._load_hot(hot_active_days, hot_recent)

    # ---------- 启动热加载 ----------

    def _load_hot(self, active_days: int, recent: int) -> int:
        """把未终结的任务与最近若干条载回内存，重启后能无缝接着算。"""
        try:
            rows = self.db.load_hot_tasks(active_days=active_days, recent=recent)
        except Exception:  # noqa: BLE001
            logger.warning("trace: 热任务加载失败，将从空内存开始", exc_info=True)
            return 0
        with self.lock:
            for row in rows:
                task = Task.from_row(row)
                if task.task_id:
                    self.agg.tasks[task.task_id] = task
        if rows:
            logger.info("trace: 载入热任务 %d 条（含所有未结束的）", len(rows))
        return len(rows)

    def _ensure_loaded(self, task_id: str) -> None:
        """事件归属的任务不在内存但在库里时，先回填，绍对不能覆盖历史。"""
        if not task_id or self.db is None:
            return
        with self.lock:
            if task_id in self.agg.tasks:
                return
        try:
            row = self.db.get_task(task_id)
        except Exception:  # noqa: BLE001
            logger.debug("trace: 回填任务失败 %s", task_id, exc_info=True)
            return
        if row is None:
            return
        with self.lock:
            self.agg.tasks[task_id] = Task.from_row(row)

    # ---------- 埋点入口 ----------

    def on_send(
        self,
        *,
        payload: Any,
        relay_from: str,
        relay_to: str,
        delivered: int,
        ts: Optional[float] = None,
        origin: str = "ws",
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """一条 send（或其投递失败）经过 Relay 时调用。

        不是 crossnode 消息就直接返回，开销只有一次 dict 取键。
        整个方法用 try/except 兜住：**追踪绝不允许影响转发**。
        """
        if not self.enabled:
            return
        try:
            envelope = unwrap_crossnode(payload)
            if envelope is None:
                return
            event = build_event(
                envelope,
                relay_from=relay_from,
                relay_to=relay_to,
                delivered=delivered,
                ts=ts or time.time(),
                summary_chars=self.summary_chars,
                payload_chars=self.payload_chars,
                origin=origin,
                error_code=error_code,
                error_message=error_message,
            )
            self.record(event)
        except Exception:  # noqa: BLE001
            logger.debug("trace: 埋点失败（已忽略，不影响转发）", exc_info=True)

    def record(self, event: dict[str, Any]) -> None:
        task_id = event_task_id(event)
        if task_id:
            # 先从 DB 回填，避免把已有历史覆盖成残缺任务
            self._ensure_loaded(task_id)
        with self.lock:
            self.events_total += 1
            hit = self.agg.apply(event)
            task = self.agg.tasks.get(hit) if hit else None
            row = task.to_row() if task else None
            brief = task.brief() if task else None

        # 归因结果回写进事件：events 表靠这一列才能按任务检索明细
        if hit and not event.get("task_id"):
            event = dict(event)
            event["task_id"] = hit

        if self.db is not None:
            skip_event = (
                not self.store_probe_events and event.get("ev") in ("probe", "probe_ack")
                and not event.get("error_code")
            )
            try:
                if skip_event:
                    if row is not None:
                        self.db.upsert_task(row)
                else:
                    self.db.write(event, row)
            except Exception:  # noqa: BLE001 —— 写库失败不能影响转发
                self.write_errors += 1
                logger.warning("trace: 写入 SQLite 失败（已忽略）", exc_info=True)

        if brief is not None:
            self._publish({"type": "task", "task": brief, "event": event})

    # ---------- 订阅（SSE） ----------

    def subscribe(self, queue: Any) -> None:
        self._subscribers.add(queue)

    def unsubscribe(self, queue: Any) -> None:
        self._subscribers.discard(queue)

    def _publish(self, message: dict[str, Any]) -> None:
        if not self._subscribers:
            return
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(message)
            except Exception:  # 队列满 = 页面卡住了，丢帧即可，看板会靠轮询自愈
                pass

    # ---------- 维护 ----------

    def flush(self) -> int:
        """兼容保留：写入已经是同步的，这里只把 WAL 合并回主库。"""
        if self.db is None:
            return 0
        self.db.checkpoint()
        return 0

    def maintain(self) -> dict[str, int]:
        """定期维护：checkpoint +（仅当配置了保留天数时）清理历史。

        默认 `prune_days=0` → **不删任何东西**。
        """
        if self.db is None:
            return {"events": 0, "tasks": 0}
        self.db.checkpoint()
        if self.prune_days <= 0:
            return {"events": 0, "tasks": 0}
        return self.db.prune(time.time() - 86400 * self.prune_days)

    def close(self) -> None:
        if self.db is not None:
            self.db.close()

    def rebuild(self) -> int:
        """从 events 表重建 tasks（聚合口径变更或 tasks 损坏时用）。"""
        if self.db is None:
            return 0

        def apply_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
            # 重建时不能淘汰，否则会丢任务
            agg = TaskAggregator(max_tasks=10**9, max_timeline=self.agg.max_timeline)
            agg.apply_many(events)
            return [task.to_row() for task in agg.tasks.values()]

        return self.db.rebuild_tasks(apply_events)

    # ---------- 查询（全部走 DB，与历史总量无关） ----------

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
    ) -> tuple[int, list[Task]]:
        if self.db is None:
            return 0, []
        total, rows = self.db.query_tasks(
            since=since, until=until, user=user, role=role, state=state,
            keyword=keyword, limit=limit, offset=offset,
            silent_before=time.time() - SILENT_AFTER_SECONDS,
        )
        return total, [self._merge_live(Task.from_row(row)) for row in rows]

    def _merge_live(self, task: Task) -> Task:
        """内存里更新的那份优先（两边同步写，这只是兼容写库失败的情况）。"""
        with self.lock:
            live = self.agg.tasks.get(task.task_id)
        if live is not None and live.last_activity >= task.last_activity:
            return live
        return task

    def get_task(self, task_id: str) -> Optional[Task]:
        with self.lock:
            live = self.agg.tasks.get(task_id)
        if live is not None:
            return live
        if self.db is None:
            return None
        row = self.db.get_task(task_id)
        return Task.from_row(row) if row is not None else None

    def task_events(self, task_id: str, limit: int = 2000) -> list[dict[str, Any]]:
        """原始事件明细（含每一轮 probe），用于深度排查。"""
        if self.db is None:
            return []
        return self.db.task_events(task_id, limit=limit)

    def overview(
        self, *, since: Optional[float] = None, until: Optional[float] = None
    ) -> dict[str, Any]:
        if self.db is None:
            return {"kpi": {}, "users": {}}
        return self.db.overview(
            since=since, until=until, silent_before=time.time() - SILENT_AFTER_SECONDS
        )

    def known_clients(self) -> list[str]:
        """出现在追踪数据里的所有用户名（含已离线的历史用户）。"""
        return self.db.known_clients() if self.db is not None else []

    # ---------- 链路视图 ----------

    @staticmethod
    def _endpoints_of(rows: list[dict[str, Any]]) -> tuple[dict[str, str], dict[str, str]]:
        """从回合行里解出链路两端的身份。

        处理方一律取**真实执行会话**，跟 Task.handler_target() 同口径：
        link_key 就是用它算的，这里若用入口 ID，详情页显示的双方会与
        链路自身的分组依据对不上（甚至解出第三个会话）。

        用 sorted 规范化，与 link_key 的排序口径一致，这样无论哪一回合是
        A→B 还是 B→A，算出来的 a / b 都稳定不变。
        """
        sessions: dict[str, str] = {}
        for row in rows:
            handler_sess = (row.get("handler_session_real") or "").strip() \
                or (row.get("handler_session") or "")
            for sess, client in (
                (row.get("requester_session"), row.get("requester")),
                (handler_sess, row.get("handler")),
            ):
                sess = (sess or "").strip()
                if sess and client and sess not in sessions:
                    sessions[sess] = client
        ordered = sorted(sessions.items())
        low = ordered[0] if ordered else ("", "")
        high = ordered[1] if len(ordered) > 1 else ("", "")
        return (
            {"session": low[0], "client": low[1]},
            {"session": high[0], "client": high[1]},
        )

    def _build_link(
        self,
        agg_row: dict[str, Any],
        turns: list[Task],
        now: float,
        recent: int = 3,
    ) -> dict[str, Any]:
        rows = [t.to_row() for t in turns]
        side_a, side_b = self._endpoints_of(rows)
        low_session = side_a["session"]

        # 球在谁手上：全部未终结回合的处理方都列出来。
        # A 连发两条没等回复时会有多个，不能只显示最新的那个。
        ball: list[dict[str, Any]] = []
        for task in turns:
            if task.terminal:
                continue
            ball.append(
                {
                    "client": task.handler,
                    "session": task.handler_session_real or task.handler_session,
                    "task_id": task.task_id,
                    "state": task.state,
                    "since": task.state_at or task.created_at,
                    "summary": task.summary,
                    # 需求：链路的每一段单独告警，所以静默标记打在回合上
                    "silent": task.silent(now),
                }
            )

        active = int(agg_row.get("active") or 0)
        failed = int(agg_row.get("failed") or 0)
        state = "active" if active else ("failed" if failed else "done")
        low_first = int(agg_row.get("turns_low_first") or 0)
        total_turns = int(agg_row.get("turns") or 0)

        brief_turns = [t.brief(now) for t in turns]
        for item, task in zip(brief_turns, turns):
            item["direction"] = (
                "a2b" if task.requester_session == low_session else "b2a"
            )
        return {
            "link_key": agg_row.get("link_key") or "",
            "a": side_a,
            "b": side_b,
            "turns": total_turns,
            "turns_a_to_b": low_first,
            "turns_b_to_a": max(0, total_turns - low_first),
            "state": state,
            "active_turns": active,
            "failed_turns": failed,
            "silent_turns": int(agg_row.get("silent") or 0),
            "done_turns": int(agg_row.get("done") or 0),
            "notified_turns": int(agg_row.get("notified") or 0),
            "cancelled_turns": int(agg_row.get("cancelled") or 0),
            "first_at": agg_row.get("first_at") or 0.0,
            "last_at": agg_row.get("last_at") or 0.0,
            "span": max(0.0, (agg_row.get("last_at") or 0.0) - (agg_row.get("first_at") or 0.0)),
            "work_total": agg_row.get("work_total"),
            "ball_at": ball,
            # 走协作入口时入口 ID ≠ 真实执行会话，链路可能不完整，必须标出
            "via_endpoint": any(item.get("via_endpoint") for item in brief_turns),
            "execution_unconfirmed_turns": sum(not t.handler_session_real for t in turns),
            "recent_turns": brief_turns[-recent:] if recent else brief_turns,
            "all_turns": brief_turns,
        }

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
        recent: int = 3,
    ) -> dict[str, Any]:
        """链路列表：一对会话一行，内含它们之间的全部回合。"""
        if self.db is None:
            return {"total": 0, "active_total": 0, "links": [], "orphan_tasks": 0, "edges": []}
        now = time.time()
        page = self.db.query_links(
            since=since, until=until, user=user, role=role, link_state=link_state,
            keyword=keyword, limit=limit, offset=offset,
            silent_before=now - SILENT_AFTER_SECONDS,
        )
        keys = [row["link_key"] for row in page["rows"]]
        grouped: dict[str, list[Task]] = {key: [] for key in keys}
        for row in self.db.tasks_of_links(keys):
            task = self._merge_live(Task.from_row(row))
            grouped.setdefault(row["link_key"], []).append(task)

        links = [
            self._build_link(row, grouped.get(row["link_key"], []), now, recent=recent)
            for row in page["rows"]
        ]
        return {
            "total": page["total"],
            "active_total": page.get("active_total", 0),
            "links": links,
            "orphan_tasks": page["orphan_tasks"],
            "edges": self.db.link_edges(keys),
        }

    def get_link(self, link_key: str) -> Optional[dict[str, Any]]:
        """单条链路的完整回合时间轴（不受时间筛选限制）。"""
        if self.db is None or not link_key:
            return None
        now = time.time()
        agg = self.db.link_summary(link_key, silent_before=now - SILENT_AFTER_SECONDS)
        if agg is None:
            return None
        turns = [
            self._merge_live(Task.from_row(row))
            for row in self.db.tasks_of_links([link_key])
        ]
        data = self._build_link(agg, turns, now, recent=0)
        data["edges"] = self.db.link_edges([link_key])
        data["related_tasks"] = [
            Task.from_row(row).brief(now)
            for row in self.db.related_session_tasks(
                [data["a"]["session"], data["b"]["session"]], link_key
            )
        ]
        data["session_graph"] = self.session_graph(link_key=link_key)
        data["execution_graph"] = self.execution_graph(link_key=link_key)
        return data

    def session_graph(self, *, link_key: str = "", task_id: str = "") -> dict[str, Any]:
        now = time.time()
        rows = self.db.session_graph_rows(link_key=link_key, task_id=task_id) if self.db else []
        nodes = [Task.from_row(r).brief(now) for r in rows]
        for n in nodes:
            n['selected'] = n['task_id'] == task_id if task_id else n['link_key'] == link_key
        return dict(mode='sessions', nodes=nodes, edges=[], roots=[], truncated=False,
                    warnings=[], relation_basis='按会话关联全部通信；不代表任务父子因果，不按完成状态过滤。',
                    timing_basis='显示首条至末条记录的时间跨度，不作为任务执行耗时。')

    def execution_graph(self, *, link_key: str = "", task_id: str = "") -> dict[str, Any]:
        if self.db is None:
            return build_execution_graph([], set(), time.time())
        rows, truncated = self.db.execution_graph_rows(link_key=link_key, task_id=task_id)
        now = time.time()
        selected = {r["task_id"] for r in rows if (
            r["task_id"] == task_id if task_id else r["link_key"] == link_key
        )}
        return build_execution_graph(
            [Task.from_row(row).brief(now) for row in rows], selected, now, truncated
        )

    def backfill_links(self) -> dict[str, int]:
        """给历史任务补链路字段（升级后跑一次）。

        原子改写旧 key，不先置空；重复运行不更新已正确的 key。
        与事件写入共用锁，并同步热缓存，避免旧缓存覆盖迁移结果。
        """
        if self.db is None:
            return {"link_key": 0, "parent": 0, "cleared": 0}
        with self.lock:
            cleared = self.db.clear_stale_link_keys(make_link_key)
            result = self.db.backfill_links(make_link_key)
            for task in self.agg.tasks.values():
                self.agg._rebind_link(task)
            result["cleared"] = cleared
            return result

    def stats(self) -> dict[str, Any]:
        with self.lock:
            data = {
                "enabled": self.enabled,
                "events_total": self.events_total,
                "tasks_in_memory": len(self.agg.tasks),
                "unattributed_events": self.agg.ignored,
                "write_errors": self.write_errors,
                "default_days": self.default_days,
                "prune_days": self.prune_days,
                "retention": "forever" if self.prune_days <= 0 else f"{self.prune_days}d",
                "store_probe_events": self.store_probe_events,
                "summary_chars": self.summary_chars,
                "payload_chars": self.payload_chars,
                "subscribers": len(self._subscribers),
                "since": self.started_at,
            }
        if self.db is not None:
            data.update(self.db.stats())
        return data


def build_store(settings: Any) -> TraceStore:
    """按 Settings 造一个 TraceStore。

    路径优先级：RELAY_TRACE_DB > <RELAY_TRACE_DIR>/trace.db > <日志目录>/trace/trace.db
    """
    db_path = getattr(settings, "trace_db", "") or ""
    if not db_path:
        directory = getattr(settings, "trace_dir", "") or ""
        if directory:
            base = Path(directory)
        else:
            log_dir = getattr(settings, "log_dir", "") or ""
            root = Path(log_dir) if log_dir else Path(__file__).resolve().parents[2] / "logs"
            base = root / "trace"
        db_path = base / "trace.db"
    return TraceStore(
        enabled=bool(getattr(settings, "trace_enabled", True)),
        db_path=db_path,
        default_days=int(getattr(settings, "trace_default_days", 14)),
        prune_days=int(getattr(settings, "trace_prune_days", 0)),
        max_tasks=int(getattr(settings, "trace_max_tasks", 5000)),
        max_timeline=int(getattr(settings, "trace_max_timeline", 60)),
        summary_chars=int(getattr(settings, "trace_summary_chars", 200)),
        payload_chars=int(getattr(settings, "trace_payload_chars", 20000)),
        store_probe_events=bool(getattr(settings, "trace_store_probe_events", True)),
    )
