"""Task-level execution graph and reproducible boundary timings.

No duration is inferred from probe state. Existing persisted created_at/finished_at
are the source: send-envelope time (legacy fallback: Relay observation) to Relay
observing the response, not proof of requester receipt or business success.
"""
from __future__ import annotations

import math
from typing import Any


def timestamp(value: Any) -> float | None:
    try:
        value = float(value)
        return value if math.isfinite(value) and value > 0 else None
    except (TypeError, ValueError):
        return None


def task_timing(row: dict[str, Any], now: float) -> dict[str, Any]:
    start = timestamp(row.get('created_at')) if not row.get('partial') else None
    end = timestamp(row.get('finished_at'))
    state = row.get('state')
    total = elapsed = None
    if row.get('kind') == 'notify':
        status = 'notified' if state == 'notified' else state
    elif not start:
        status = 'missing_start'
    elif end is not None and end < start:
        status = 'invalid_timestamps'
    elif state == 'done':
        status = 'completed' if end is not None else 'missing_end'
        total = round(end - start, 3) if end is not None else None
    elif state in ('failed', 'cancelled'):
        status = state
    elif now < start:
        status = 'invalid_timestamps'
    else:
        status = 'running'
        elapsed = round(now - start, 3)
    return {'total_seconds': total, 'elapsed_seconds': elapsed, 'timing_status': status}


def build_execution_graph(rows: list[dict[str, Any]], selected: set[str], now: float,
                          truncated: bool = False) -> dict[str, Any]:
    nodes = []
    for row in rows:
        node = dict(row)
        node.update(task_timing(row, now))
        node['selected'] = row['task_id'] in selected
        nodes.append(node)
    nodes.sort(key=lambda n: (timestamp(n.get('created_at')) or 0, n['task_id']))
    by_id = {n['task_id']: n for n in nodes}
    warnings = []
    parents = {}
    for node in nodes:
        parent_id = node.get('parent_task_id')
        if not parent_id:
            continue
        if parent_id not in by_id:
            warnings.append('有上游任务不在当前数据中，未补造关联。')
            continue
        # Reject corrupt cycles rather than hiding the whole component in the UI.
        seen = {node['task_id']}
        ancestor = parent_id
        while ancestor and ancestor in by_id and ancestor not in seen:
            seen.add(ancestor)
            ancestor = by_id[ancestor].get('parent_task_id')
        if ancestor in seen:
            warnings.append('检测到循环父子关系，已移除循环连线。')
            continue
        parents[node['task_id']] = parent_id
    edges = []
    for child_id, parent_id in parents.items():
        child, parent = by_id[child_id], by_id[parent_id]
        ps = timestamp(parent.get('created_at')) if not parent.get('partial') else None
        cs = timestamp(child.get('created_at')) if not child.get('partial') else None
        pe = timestamp(parent.get('finished_at')) if parent['timing_status'] == 'completed' else None
        ce = timestamp(child.get('finished_at')) if child['timing_status'] == 'completed' else None
        # A child must lie within its parent's interval. Never silently clamp negatives.
        invalid = any(a is not None and b is not None and b < a
                      for a, b in ((ps, cs), (cs, ce), (ce, pe), (cs, pe)))
        segments = []
        for key, label, start, end in (
            ('before_dispatch', '上游发起 → 下游发起', ps, cs),
            ('downstream_roundtrip', '下游发起 → 下游回复', cs, ce),
            ('after_reply', '下游回复 → 上游回复', ce, pe),
        ):
            status = 'invalid_timestamps' if invalid else (
                'measured' if start is not None and end is not None else 'unavailable')
            seconds = round(end - start, 3) if status == 'measured' else None
            segments.append(dict(key=key, label=label, start_at=start, end_at=end,
                                 seconds=seconds, status=status))
        if invalid:
            warnings.append('部分时间顺序异常：可能存在时钟偏差或父子关系误判，未输出分段耗时。')
        edges.append(dict(parent_task_id=parent_id, child_task_id=child_id,
                          inferred=True, segments=segments))
    if any(n['timing_status'] == 'invalid_timestamps' for n in nodes):
        warnings.append('部分任务时间戳逆序，不能计算有效总耗时。')
    if truncated:
        warnings.append('关联任务超过展示上限，当前图不完整。')
    return dict(nodes=nodes, edges=edges,
                roots=[n['task_id'] for n in nodes if n['task_id'] not in parents],
                warnings=list(dict.fromkeys(warnings)), truncated=truncated,
                timing_basis='发起信封时间（老数据可能回退为 Relay 观测时间）到 Relay 观测回复；不含回复抵达发起端后的处理。',
                relation_basis='父子关系为会话与时间区间推断；并行分支耗时存在重叠，不可相加。')
