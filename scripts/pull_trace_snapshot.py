"""Read-only remote export with raw evidence, completeness report and local replay.
Never overwrites an existing export. No import of relay.main (avoids eager production DB).
"""
from __future__ import annotations
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pathlib import Path
import sys
import time
from urllib.parse import quote

import httpx
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from relay.trace import TraceStore


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--remote',default='http://10.12.5.212:8770')
    args=parser.parse_args()
    base=args.remote.rstrip('/')
    root=Path(__file__).resolve().parents[1]/'logs'/'remote_snapshots'/datetime.now().strftime('%Y%m%d-%H%M%S-%f')
    root.mkdir(parents=True,exist_ok=False)
    def save(name,data): (root/name).write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    with httpx.Client(timeout=30,trust_env=False) as client:
        def get(path,params=None):
            res=client.get(base+path,params=params);res.raise_for_status();return res.json()
        before=get('/api/trace/stats');save('remote_stats_before.json',before)
        tasks={};offset=0;totals=[]
        while True:
            page=get('/api/trace/tasks',{'days':0,'limit':500,'offset':offset})
            totals.append(page['total'])
            batch=page.get('tasks',[])
            for t in batch: tasks[t['task_id']]=t
            offset+=len(batch)
            if not batch or offset>=page['total']:break
        save('remote_tasks.json',list(tasks.values()))
        def events_for(tid):
            data=get('/api/trace/tasks/'+quote(tid,safe='')+'/events',{'limit':20000})
            events=data.get('events',[])
            if len(events)>=20000:raise RuntimeError('Event cap reached for '+tid+'; refusing incomplete replay')
            return events
        events={}
        with ThreadPoolExecutor(max_workers=6) as pool:
            for batch in pool.map(events_for,tasks):
                for e in batch:
                    if 'id' not in e:raise RuntimeError('Remote event missing unique id')
                    events[e['id']]=e
        after=get('/api/trace/stats');save('remote_stats_after.json',after)
    raw=sorted(events.values(),key=lambda e:(float(e.get('ts') or 0),e['id']))
    save('remote_events.json',raw)
    stable=(before['tasks']==after['tasks']==len(tasks) and before['events']==after['events']==len(raw))
    issues=[]
    if not stable:issues.append('导出期间远端统计与抓取数量不完全一致；非事务快照，不能声称全量一致。')
    if not raw:raise RuntimeError('No events exported')
    store=TraceStore(enabled=True,db_path=root/'trace.db',default_days=0,prune_days=0,
                     max_tasks=max(5000,len(tasks)*2),max_timeline=60,summary_chars=200,store_probe_events=True)
    for e in raw:store.record({k:v for k,v in e.items() if k!='id'})
    migration=store.backfill_links()
    with store.db.lock:
        rows=[dict(r) for r in store.db.conn.execute('SELECT * FROM tasks')]
    local_ids={r['task_id'] for r in rows}
    missing=sorted(set(tasks)-local_ids)
    if missing:issues.append('部分远端任务没有可重放事件：'+str(len(missing)))
    def missing_count(field):return sum(not t.get(field) for t in rows)
    fields=Counter(k for e in raw for k,v in e.items() if v not in (None,'',[],{}))
    parent=[r for r in rows if r.get('parent_task_id')]
    done=[r for r in rows if r['kind']=='request' and r['state']=='done']
    samples=[]
    for r in parent[:15]:
        graph=store.execution_graph(task_id=r['task_id'])
        samples.append(dict(task_id=r['task_id'],link_key=r['link_key'],requester=r['requester'],handler=r['handler'],
                            nodes=len(graph['nodes']),edges=len(graph['edges']),warnings=graph['warnings']))
    report=dict(source=base,exported_at=datetime.now().isoformat(),directory=str(root),is_demo=False,
        consistency='counts_matched_before_after' if stable and not missing else 'partial_or_changed',
        issues=issues,remote_before={k:before[k] for k in ('tasks','events')},remote_after={k:after[k] for k in ('tasks','events')},
        exported_tasks=len(tasks),exported_events=len(raw),replayed_tasks=len(rows),missing_task_ids=missing,
        states=dict(Counter(r['state'] for r in rows)),kinds=dict(Counter(r['kind'] for r in rows)),
        tasks_without_real_session=missing_count('handler_session_real'),
        tasks_with_different_real_session=sum(bool(r.get('handler_session_real')) and r['handler_session_real']!=r['handler_session'] for r in rows),
        tasks_without_requester_session=missing_count('requester_session'),
        missing_start=sum(not r.get('created_at') or bool(r.get('partial')) for r in rows),
        completed_requests=len(done),completed_missing_end=sum(not r.get('finished_at') for r in done),
        inferred_parent_tasks=len(parent),event_field_coverage=dict(fields),graph_samples=samples,
        link_count=store.query_links(limit=500)['total'],migration=migration)
    save('audit.json',report)
    store.close()
    print(json.dumps(report,ensure_ascii=False,indent=2))
    return 0 if not issues else 2

if __name__=='__main__':raise SystemExit(main())
