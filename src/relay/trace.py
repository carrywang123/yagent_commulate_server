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

payload 只留摘要（默认 200 字符）与字节数，不留全文。
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

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
        event["summary"] = summarize_payload(payload, summary_chars)
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
    chain: list[dict[str, Any]] = field(default_factory=list)
    #: 没见到发起帧就先见到回复（Relay 重启后半途接入），数据不完整
    partial: bool = False
    timeline: list[dict[str, Any]] = field(default_factory=list)

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

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

    # ---------- 内部 ----------

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
            self.tasks.pop(victim, None)

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

    # ---------- 对外 ----------

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
            self._touch(task, ts)
            self._push(task, event)
            return task.task_id

        if etype == "notify":
            # 单向通知，协议上不会有回执，收下即终态
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
            if relay_err:
                self._finish(task, STATE_FAILED, ts, relay_err, event.get("error_message") or "")
            else:
                self._finish(task, STATE_NOTIFIED, ts)
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

            if etype == "response":
                task.bytes_in = int(event.get("bytes") or 0)
                if not task.started_at:
                    task.started_at = task.delivered_at or task.first_seen
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
        store_probe_events=bool(getattr(settings, "trace_store_probe_events", True)),
    )
