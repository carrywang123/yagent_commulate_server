/* Session connected components: topology is independent of task state and ancestry. */
(function(){
'use strict';
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
window.TraceDuration=function(value){
  if(value==null||!Number.isFinite(Number(value))||Number(value)<0)return '未记录';
  let n=Math.floor(Number(value));const d=Math.floor(n/86400);n%=86400;
  const h=Math.floor(n/3600);n%=3600;const m=Math.floor(n/60),s=n%60;
  return [d,h,m,s].map((x,i)=>x?String(x)+['天','h','m','s'][i]:'').join('') || '0s';
};
const eventNames={request:'发出请求',response:'收到回复',notify:'发送通知',cancel:'取消请求',error:'通信报错',probe:'查询处理进度',probe_ack:'返回处理进度'};
const duration=window.TraceDuration;
window.TraceDate=function(value,seconds=true){
 if(value==null||value===''||!Number.isFinite(Number(value)))return '未记录';
 const d=new Date(Number(value)*1000);if(!Number.isFinite(d.getTime()))return '未记录';
 const p=n=>String(n).padStart(2,'0');
 return (seconds||d.getFullYear()!==new Date().getFullYear()?d.getFullYear()+'年':'')+p(d.getMonth()+1)+'月'+p(d.getDate())+'日 '+p(d.getHours())+':'+p(d.getMinutes())+(seconds?':'+p(d.getSeconds()):'');
};
const date=window.TraceDate;
const failed=t=>t.state==='failed';
const reason=t=>t.error_message||t.error_code||'未记录失败原因';
const stateName=t=>({failed:'失败',done:'已完成',cancelled:'已取消',notified:'已发送通知',sent:'已发出',delivered:'已送达',working:'处理中',queued:'排队中'}[t.state]||t.state||'状态未记录');
const addr=(t,to)=>[to?t.handler:t.requester,to?(t.handler_session_real||t.handler_session):t.requester_session];
const key=(t,to)=>JSON.stringify(addr(t,to)[1]?addr(t,to):[...addr(t,to),t.task_id,to]);
// The first observed communication of a session pair sets its expansion direction.
// Later reverse traffic stays on the pair; it must not reopen earlier ancestors.
function scopeGraph(graph,options={}){
 const all=(graph.nodes||[]).slice().sort((a,b)=>(a.created_at||0)-(b.created_at||0)||a.task_id.localeCompare(b.task_id));
 if(!all.length)return {tasks:[],sessions:[],viewer:null};
 const entry=all.find(t=>t.task_id===options.taskId)||all.find(t=>t.selected)||all[0];
 const sessions=new Map(),routes=new Map();
 for(const t of all){
   const a=key(t,false),b=key(t,true);
   for(const [id,to] of [[a,false],[b,true]])if(!sessions.has(id))sessions.set(id,{id,name:addr(t,to)[0]||'未知',session:addr(t,to)[1]||''});
   const pair=JSON.stringify([a,b].sort());if(!routes.has(pair))routes.set(pair,{a,b});
 }
 let viewer=options.viewerKey;
 if(!sessions.has(viewer)){
   const endpoint=[key(entry,false),key(entry,true)].find(id=>sessions.get(id).name===options.viewerClient);
   viewer=endpoint||key(entry,false);
 }
 const parents=new Set([...routes.values()].filter(r=>r.b===viewer&&r.a!==viewer).map(r=>r.a));
 const downstream=new Set([viewer]),queue=[viewer];
 for(let i=0;i<queue.length;i++)for(const r of routes.values())if(r.a===queue[i]&&!parents.has(r.b)&&!downstream.has(r.b)){downstream.add(r.b);queue.push(r.b);}
 const visiblePairs=new Set([...routes].filter(([,r])=>(downstream.has(r.a)&&downstream.has(r.b))||(r.b===viewer&&parents.has(r.a))).map(([id])=>id));
 return {tasks:all.filter(t=>visiblePairs.has(JSON.stringify([key(t,false),key(t,true)].sort()))),sessions:[...sessions.values()],viewer:sessions.get(viewer)};
}
function graphSignature(tasks){
 // Include exact directed endpoints and record IDs; ignore only the selected item.
 return JSON.stringify(tasks.map(t=>[t.task_id,key(t,false),key(t,true)]).sort((a,b)=>a[0].localeCompare(b[0])));
}
window.TraceSessionExplorer={scope:scopeGraph,signature:graphSignature,mount(container,graph,options={}){
 const scoped=scopeGraph(graph,options);
 if(options.fixedBranch)scoped.tasks=(graph.nodes||[]).slice().sort((a,b)=>(a.created_at||0)-(b.created_at||0)||a.task_id.localeCompare(b.task_id));
 const tasks=scoped.tasks;
 const shared=t=>(options.sharedCounts||{})[t.link_key]||0;
 if(!tasks.length){container.innerHTML='<div class="tx-warning">没有会话通信记录</div>';return;}
 const people=new Map(),pairs=new Map(),byId=new Map(tasks.map(t=>[t.task_id,t]));
 tasks.forEach(t=>{
   const a=key(t,false),b=key(t,true);
   for(const [id,to] of [[a,false],[b,true]])if(!people.has(id))people.set(id,{id,name:addr(t,to)[0],session:addr(t,to)[1],neighbors:new Set(),tasks:[]});
   people.get(a).neighbors.add(b);people.get(b).neighbors.add(a);
   people.get(a).tasks.push(t);if(a!==b)people.get(b).tasks.push(t);
   const pk=JSON.stringify([a,b].sort());
   if(!pairs.has(pk))pairs.set(pk,{key:pk,a,b,tasks:[],index:pairs.size});
   const pair=pairs.get(pk);pair.tasks.push(t);t._pair=pk;
 });
 const seed=byId.get(options.taskId)||tasks.find(t=>t.selected)||tasks[0];
 // Choose a layout origin within the already scoped graph; never restore hidden ancestors.
 const incoming=new Set(tasks.map(t=>key(t,true)));
 const earliest=tasks.slice().sort((a,b)=>(a.created_at||0)-(b.created_at||0)||a.task_id.localeCompare(b.task_id));
 const origin=earliest.find(t=>!incoming.has(key(t,false)))||earliest[0],start=key(origin,false);
 let chosen=seed.task_id,scale=1,seq=0,context=tasks,panX=0,panY=0,drag=null,suppressNextClick=false;
 document.getElementById('drawer')?.classList.add('explorer-open');
 container.innerHTML='<section class="tx" data-selected-task=""><div class="tx-head"><div class="tx-title"><strong>会话通信链路</strong><span>'+people.size+' 会话 · '+pairs.size+' 会话对 · '+tasks.length+' 次通信</span><span class="tx-total"></span></div><div class="tx-crumb">当前会话的直接上游一层＋下游 · 全部历史，不筛状态</div><div class="tx-viewpoint"><label>当前查看会话 <select data-viewer-select aria-label="当前查看会话"></select></label></div></div><div class="tx-layout"><div class="tx-map"><div class="tx-toolbar"><button data-action="all">本视角全部通信</button><button data-action="failures">只看失败通信</button><button data-action="origin">定位入口通信</button><button data-action="fit">适应全图</button><button data-action="zoom-in">＋</button><button data-action="zoom-out">−</button><span class="tx-legend">滚轮缩放 · 按住左键拖动 · 点击方向次数查看消息 · 标记始终贴合连线</span></div><div class="tx-stage" id="execution-canvas"></div><div class="tx-caption">范围按会话对首次通信方向展开；双向通信分成上下两条单向线，每条单独显示次数；不沿回传展开更早上游。<br>直接上游只保留与当前会话的通信，不展开上游的其他分支；此为展示范围，不是访问权限控制。</div></div><aside class="tx-inspector" id="execution-detail"></aside></div></section>';
 const host=container.querySelector('.tx'),stage=container.querySelector('#execution-canvas'),detail=container.querySelector('#execution-detail');
 host.dataset.viewerKey=scoped.viewer.id;
 host.dataset.graphSignature=graphSignature(tasks);
 if(options.fixedBranch){
   host.dataset.branchId=options.branchId;
   container.querySelector('.tx-crumb').textContent=(options.viewerClient?'当前用户会话：直接上游一层＋下游':'本组完整下游')+' · 与组内列表一致 · 共享通信跨组展示';
   container.querySelector('.tx-caption').textContent='每对会话首次通信确定展开方向；回传保留，不反向扩展。共同下游标为共享通信，全局按通信 ID 去重。切换查看会话后按直接上游一层＋下游展示，不恢复本组之外的记录。';
   container.querySelector('[data-action="all"]').textContent='本组全部通信';
 }

 const failures=tasks.filter(failed);
 const failureButton=container.querySelector('[data-action="failures"]');failureButton.textContent=(options.fixedBranch?'本组失败 ':'本视角失败 ')+failures.length+' 次';failureButton.disabled=!failures.length;
 const viewerSelect=container.querySelector('[data-viewer-select]');
 viewerSelect.innerHTML=scoped.sessions.map(s=>'<option value="'+esc(s.id)+'">'+esc(s.name+' / '+(s.session||'会话未记录'))+'</option>').join('');
 viewerSelect.value=scoped.viewer.id;
 viewerSelect.addEventListener('change',()=>window.TraceSessionExplorer.mount(container,graph,{...options,viewerKey:viewerSelect.value,fixedBranch:false}));
 function bounds(list){const points=list.flatMap(t=>[t.created_at,t.first_seen,t.last_activity,t.state_at,t.finished_at]).filter(v=>Number.isFinite(v)&&v>0);return points.length?[Math.min(...points),Math.max(...points)]:[null,null];}
 const [first,last]=bounds(tasks);container.querySelector('.tx-total').textContent='最早到最新记录相隔 '+duration(first==null?null:last-first);
 // Undirected BFS makes incoming traffic part of the same network. No ancestry used.
 people.get(start).depth=0;const queue=[start];
 for(let i=0;i<queue.length;i++)for(const id of people.get(queue[i]).neighbors)if(people.get(id).depth===undefined){people.get(id).depth=people.get(queue[i]).depth+1;queue.push(id);}
 const cols=new Map();people.forEach(p=>{const d=p.depth??0;if(!cols.has(d))cols.set(d,[]);cols.get(d).push(p);});
 const layoutCols=[];
 [...cols.entries()].sort((a,b)=>a[0]-b[0]).forEach(([,col])=>{for(let i=0;i<col.length;i+=8)layoutCols.push(col.slice(i,i+8));});
 const height=Math.max(440,...layoutCols.map(c=>c.length*140+120)),width=Math.max(840,layoutCols.length*370+160);
 layoutCols.forEach((col,d)=>col.forEach((p,i)=>{p.x=65+d*370;p.y=(height-col.length*140)/2+i*140+70;}));
 let svg='<svg class="tx-svg" viewBox="0 0 '+width+' '+height+'" role="img" aria-label="会话通信方向图"><defs><marker id="session-arrow-forward" markerUnits="userSpaceOnUse" markerWidth="12" markerHeight="12" viewBox="0 0 12 12" refX="11" refY="6" orient="auto"><path d="M1 1 L11 6 L1 11 Z" fill="#65874e"/></marker><marker id="session-arrow-reverse" markerUnits="userSpaceOnUse" markerWidth="12" markerHeight="12" viewBox="0 0 12 12" refX="11" refY="6" orient="auto"><path d="M1 1 L11 6 L1 11 Z" fill="#557da1"/></marker><marker id="session-arrow-failed" markerUnits="userSpaceOnUse" markerWidth="12" markerHeight="12" viewBox="0 0 12 12" refX="11" refY="6" orient="auto"><path d="M1 1 L11 6 L1 11 Z" fill="#c03535"/></marker></defs>';
 const lanes=[];
 pairs.forEach(pair=>{
   const a=people.get(pair.a),b=people.get(pair.b);
   const forward=pair.tasks.filter(t=>key(t,false)===pair.a).length,reverse=pair.tasks.length-forward;
   pair.forward=forward;pair.reverse=reverse;
   for(const [direction,count] of [['forward',forward],['reverse',reverse]]){
     if(!count)continue;
     // Separate opposite directions into two rows, including separate hit areas.
     const off=forward&&reverse?(direction==='forward'?-22:22):0;
     let points;
     if(a.id===b.id){const x=a.x+178;points=[[x,a.y-22],[x+90,a.y-100],[x+90,a.y+100],[x,a.y+22]];}
     else if(a.x===b.x){const x=a.x+178,bend=x+100+(pair.index%3)*24+(direction==='reverse'?48:0);points=[[x,a.y+off],[bend,a.y+off],[bend,b.y+off],[x,b.y+off]];}
     else {const right=b.x>a.x,x=right?a.x+178:a.x-8,x2=right?b.x-8:b.x+178,mx=(x+x2)/2;points=[[x,a.y+off],[mx,a.y+off],[mx,b.y+off],[x2,b.y+off]];}
     if(direction==='reverse')points.reverse();
     const path='M '+points[0].join(' ')+' C '+points.slice(1).map(p=>p.join(' ')).join(' ');
     const from=direction==='forward'?a:b,to=direction==='forward'?b:a;
     const failedCount=pair.tasks.filter(t=>failed(t)&&(key(t,false)===pair.a)===(direction==='forward')).length;
     const sharedCount=Math.max(...pair.tasks.map(shared));
     const label=from.name+' 发给 '+to.name+' '+count+' 次'+(failedCount?'，其中失败 '+failedCount+' 次':'')+(sharedCount>1?'，共享通信 · '+sharedCount+' 组':'')+'，点击查看消息';
     const lane={pair,direction,count,label,points,failedCount,sharedCount};lanes.push(lane);
     svg+='<g class="tx-route '+direction+(failedCount?' has-failure':'')+'" data-session-pair="'+pair.index+'" data-direction="'+direction+'" tabindex="0" role="button" aria-label="'+esc(label)+'"><title>'+esc(label)+'</title><path class="tx-hit" d="'+path+'"/><path class="tx-line" d="'+path+'" marker-end="url(#session-arrow-'+(failedCount?'failed':direction)+')"/></g>';
   }
 });
 people.forEach(p=>{svg+='<g class="tx-person" data-session="'+esc(p.id)+'" tabindex="0" role="button" transform="translate('+p.x+' '+(p.y-38)+')"><title>'+esc(p.name+' / '+p.session)+'</title><rect width="170" height="76" rx="13"/><text class="tx-role" x="12" y="18">'+p.tasks.length+' 次通信</text><text class="tx-name" x="12" y="40">'+esc((p.name||'未知').slice(0,16))+'</text><text class="tx-session" x="12" y="62">'+esc(p.session?'…'+p.session.slice(-12):'会话未记录')+'</text></g>';});
 stage.innerHTML=svg+'</svg>';
 stage.querySelectorAll('[data-session]').forEach(el=>el.classList.toggle('is-viewer',el.dataset.session===scoped.viewer.id));
 // Place controls ON measured curve points, never offset into empty space.
 // Try several curve positions to avoid nodes/other controls. Store exact anchor for tests.
 const svgElement=stage.querySelector('svg'),ns='http://www.w3.org/2000/svg',occupied=[];
 const controlLayer=document.createElementNS(ns,'g');controlLayer.classList.add('tx-controls');svgElement.appendChild(controlLayer);
 const overlaps=(a,b,pad=6)=>a.x<b.x+b.w+pad&&a.x+a.w+pad>b.x&&a.y<b.y+b.h+pad&&a.y+a.h+pad>b.y;
 const nodeBoxes=[...people.values()].map(p=>({x:p.x,y:p.y-38,w:170,h:76}));
 lanes.forEach(lane=>{
   const {pair,direction,count,label,points,failedCount,sharedCount}=lane;
   const path=svgElement.querySelector('.tx-route[data-session-pair="'+pair.index+'"][data-direction="'+direction+'"] .tx-line'),length=path.getTotalLength();
   const w=failedCount?146:76,h=sharedCount>1?52:34;
   let best=null;
   for(const fraction of [.5,.4,.6,.3,.7,.2,.8]){
     const point=path.getPointAtLength(length*fraction),rect={x:point.x-w/2,y:point.y-17,w,h};
     const score=nodeBoxes.filter(b=>overlaps(rect,b)).length*100+occupied.filter(b=>overlaps(rect,b)).length*10+Math.abs(fraction-.5);
     if(!best||score<best.score)best={point,rect,score,fraction};
   }
   occupied.push(best.rect);
   const group=document.createElementNS(ns,'g');group.setAttribute('class','tx-edge-control');group.dataset.controlPair=pair.index;group.dataset.laneDirection=direction;
   group.dataset.fraction=best.fraction;group.setAttribute('transform',`translate(${best.point.x} ${best.point.y})`);
   const before=path.getPointAtLength(Math.max(0,length*best.fraction-2)),after=path.getPointAtLength(Math.min(length,length*best.fraction+2));
   const angle=Math.atan2(after.y-before.y,after.x-before.x)*180/Math.PI;
   group.innerHTML='<g class="tx-direction-chip '+direction+(failedCount?' has-failure':'')+'" data-session-pair="'+pair.index+'" data-direction="'+direction+'" role="button" tabindex="0" aria-label="'+esc(label)+'" transform="translate('+(-w/2)+' -17)"><title>'+esc(label)+'</title><rect width="'+w+'" height="34" rx="10"/><circle cx="18" cy="17" r="11"/><path class="tx-direction-arrow" d="M-6 0 H6 M1 -5 L6 0 L1 5" transform="translate(18 17) rotate('+angle+')"/><text x="49" y="22" text-anchor="middle">'+count+'次</text>'+(failedCount?'<text class="tx-fail-label" x="107" y="22" text-anchor="middle">失败 '+failedCount+'次</text>':'')+(sharedCount>1?'<text class="tx-shared-label" x="'+(w/2)+'" y="48" text-anchor="middle">共享 · '+sharedCount+'组</text>':'')+'</g>';
   controlLayer.appendChild(group);
 });
 stage.style.overflow='hidden';stage.style.position='relative';stage.style.cursor='grab';stage.style.touchAction='none';
 svgElement.style.position='absolute';svgElement.style.left='0';svgElement.style.top='0';svgElement.style.transformOrigin='0 0';
 svgElement.style.userSelect='none';stage.setAttribute('aria-label','链路画布，滚轮缩放，左键按住拖动');
 function resize(){svgElement.style.minHeight='0';svgElement.style.width=width+'px';svgElement.style.height=height+'px';svgElement.style.transform=`translate(${panX}px, ${panY}px) scale(${scale})`;stage.dataset.scale=String(scale);stage.dataset.panX=String(panX);stage.dataset.panY=String(panY);}
 function fit(){scale=Math.min(1.3,stage.clientWidth/width,stage.clientHeight/height)*.94;panX=(stage.clientWidth-width*scale)/2;panY=(stage.clientHeight-height*scale)/2;resize();}
 function zoom(factor,x=stage.clientWidth/2,y=stage.clientHeight/2){const next=Math.max(.02,Math.min(5,scale*factor)),ratio=next/scale;panX=x-(x-panX)*ratio;panY=y-(y-panY)*ratio;scale=next;resize();}
 stage.addEventListener('wheel',e=>{e.preventDefault();const box=stage.getBoundingClientRect();const delta=e.deltaY*(e.deltaMode===1?16:e.deltaMode===2?stage.clientHeight:1);zoom(Math.exp(-Math.max(-400,Math.min(400,delta))*.002),e.clientX-box.left,e.clientY-box.top);},{passive:false});
 stage.addEventListener('pointerdown',e=>{if(e.button!==0)return;drag={id:e.pointerId,x:e.clientX,y:e.clientY,panX,panY,moved:false};});
 stage.addEventListener('pointermove',e=>{if(!drag||drag.id!==e.pointerId)return;const dx=e.clientX-drag.x,dy=e.clientY-drag.y;if(!drag.moved&&Math.hypot(dx,dy)>4){drag.moved=true;stage.setPointerCapture(e.pointerId);stage.style.cursor='grabbing';}if(drag.moved){e.preventDefault();panX=drag.panX+dx;panY=drag.panY+dy;resize();}});
 function endDrag(e){if(!drag||drag.id!==e.pointerId)return;if(drag.moved){suppressNextClick=true;setTimeout(()=>{suppressNextClick=false;},0);}if(stage.hasPointerCapture(e.pointerId))stage.releasePointerCapture(e.pointerId);drag=null;stage.style.cursor='grab';}
 stage.addEventListener('pointerup',endDrag);stage.addEventListener('pointercancel',endDrag);
 stage.addEventListener('pointerleave',e=>{if(drag&&!drag.moved)endDrag(e);});
 stage.addEventListener('click',e=>{if(suppressNextClick){suppressNextClick=false;e.preventDefault();e.stopPropagation();}},true);
 function listHTML(list){
   return '<section class="tx-messages"><div class="tx-message-heading"><strong>跨端消息</strong><span>'+list.length+' 条</span></div><div class="tx-message-grid">'+list.map(t=>{
     const active=t.task_id===chosen;
     return '<button type="button" class="tx-message-card '+(active?'is-selected':'')+(failed(t)?' has-failure':'')+'" data-select-task="'+esc(t.task_id)+'" aria-pressed="'+active+'"><span class="tx-card-meta"><span class="tx-card-kind '+(t.kind==='notify'?'notify':'')+'">'+(t.kind==='notify'?'通知':'请求')+' · '+esc(stateName(t))+'</span><time>'+esc(date(t.created_at))+'</time></span><span class="tx-card-route"><span class="tx-avatar">'+esc((t.requester||'?').slice(0,1).toUpperCase())+'</span><span class="tx-card-person"><b>'+esc(t.requester)+'</b><small>'+esc((t.requester_session||'未记录').slice(-8))+'</small></span><svg class="tx-card-arrow" width="22" height="16" viewBox="0 0 22 16" aria-hidden="true"><path d="M1 8H20M14 2L20 8L14 14"/></svg><span class="tx-card-person"><b>'+esc(t.handler)+'</b><small>'+esc((t.handler_session_real||t.handler_session||'未记录').slice(-8))+'</small></span></span><span class="tx-card-excerpt">'+esc(t.summary||'无摘要')+'</span>'+(shared(t)>1?'<span class="tx-shared-note">共享通信 · 出现在 '+shared(t)+' 组</span>':'')+(failed(t)?'<span class="tx-card-error">失败：'+esc(reason(t))+'</span>':'')+'<span class="tx-card-footer"><span>'+esc(t.task_id.slice(-12))+'</span><span>'+(active?'正在查看':'查看消息')+' <span aria-hidden="true">↗</span></span></span></button>';
   }).join('')+'</div></section>';
 }

 async function select(id,list=context){
   const t=byId.get(id);if(!t)return;chosen=id;context=list;const current=++seq;host.dataset.selectedTask=id;
   const pair=pairs.get(t._pair);
   stage.querySelectorAll('[data-session-pair]').forEach(el=>el.classList.toggle('on',Number(el.dataset.sessionPair)===pair.index&&(!el.dataset.direction||(key(t,false)===pair.a)===(el.dataset.direction==='forward'))));
   stage.querySelectorAll('[data-session]').forEach(el=>el.classList.toggle('on',[key(t,false),key(t,true)].includes(el.dataset.session)));
   detail.innerHTML=(shared(t)>1?'<div class="tx-shared-note">共享通信 · 出现在 '+shared(t)+' 组，属于同一条记录</div>':'')+'<section class="tx-selection"><span class="tx-selection-eyebrow">当前通信</span><h3>'+esc(t.requester)+' <span class="tx-selection-arrow">→</span> '+esc(t.handler)+'</h3><div class="tx-selection-meta"><span class="tx-badge">'+(t.kind==='notify'?'通知':'请求')+' · '+esc(stateName(t))+'</span><time>'+esc(date(t.created_at))+'</time></div>'+(failed(t)?'<div class="tx-failure-detail" role="alert"><strong>这条通信失败了</strong><div>失败方向：'+esc(t.requester)+' 发给 '+esc(t.handler)+'</div><div>失败时间：'+esc(date(t.state_at||t.finished_at))+'</div><div>失败原因：'+esc(reason(t))+'</div>'+(t.error_code?'<div>错误代码：'+esc(t.error_code)+'</div>':'')+'</div>':'')+'<div class="tx-current-preview">'+esc(t.summary||'无摘要')+'</div><details class="tx-disclosure"><summary>查看已保存摘要与会话信息</summary><div class="tx-full-message">'+esc(t.summary||'无摘要')+'</div><div class="tx-info"><small>通信ID '+esc(t.task_id)+'</small><small>发起会话 '+esc(t.requester_session||'未记录')+'</small><small>目标会话 '+esc(t.handler_session_real||t.handler_session||'未记录')+'</small><small>入口 '+esc(t.handler_session||'未记录')+'</small></div></details><span class="tx-span-label" id="session-event-span">加载记录跨度…</span></section>'+listHTML(list)+'<details class="tx-disclosure tx-raw-events"><summary>原始通信记录与详细字段 <span id="tx-event-count"></span></summary><div class="tx-frames">加载中…</div></details>';

   detail.scrollTop=0;
   try{
      const r=await fetch('/api/trace/tasks/'+encodeURIComponent(id)+'/events?limit=20000');if(!r.ok)throw Error('HTTP '+r.status);const data=await r.json();if(seq!==current)return;
      const es=(data.events||[]).sort((a,b)=>a.ts-b.ts||a.id-b.id),start=es[0]?.ts,end=es.at(-1)?.ts;
      detail.querySelector('#session-event-span').textContent='最早到最新记录相隔 '+duration(start==null?null:end-start);
      detail.querySelector('#tx-event-count').textContent='（'+es.length+'）';
      detail.querySelector('.tx-frames').innerHTML=(es.length>=20000?'<div class="tx-warning">达到单次接口上限20000条，可能还有未展示事件。</div>':'')+es.map(e=>'<div class="tx-frame"><b>'+esc(eventNames[e.ev]||e.ev)+'</b> · '+esc(date(e.ts))+'<br>'+esc(e.from_client)+' → '+esc(e.to_client)+'<br>距首条 '+duration(e.ts-start)+'<details><summary>完整事件字段</summary><pre style="white-space:pre-wrap;overflow-wrap:anywhere">'+esc(JSON.stringify(e,null,2))+'</pre></details>'+esc(e.summary||e.message||e.peer_state||'')+'</div>').join('');
   }catch(err){if(seq===current){detail.querySelector('.tx-frames').textContent='事件加载失败：'+err;detail.querySelector('#session-event-span').textContent='记录跨度未记录';}}
 }
 host.addEventListener('click',e=>{
   const task=e.target.closest('[data-select-task]');if(task){select(task.dataset.selectTask);return;}
   const pairEl=e.target.closest('[data-session-pair]');if(pairEl){const pair=[...pairs.values()].find(p=>p.index===Number(pairEl.dataset.sessionPair));let list=pair.tasks;const dir=pairEl.dataset.direction;if(dir)list=list.filter(t=>(key(t,false)===pair.a)===(dir==='forward'));select((list.find(failed)||list.find(t=>t.task_id===chosen)||list[0]).task_id,list);return;}

   const node=e.target.closest('[data-session]');if(node){const list=people.get(node.dataset.session).tasks;select((list.find(t=>t.task_id===chosen)||list[0]).task_id,list);return;}
   const action=e.target.closest('[data-action]')?.dataset.action;
   if(action==='fit')fit();
   if(action==='origin')select(seed.task_id,tasks);
   if(action==='all')select(chosen,tasks);
   if(action==='failures'&&failures.length)select(failures[0].task_id,failures);
   if(action==='zoom-in')zoom(1.3);
   if(action==='zoom-out')zoom(1/1.3);
 });
 host.addEventListener('keydown',e=>{if(['Enter',' '].includes(e.key)&&e.target.matches('[data-session],[data-session-pair]')){e.preventDefault();e.target.dispatchEvent(new MouseEvent('click',{bubbles:true}));}});
 fit();select(chosen,tasks);
}};
})();

/* Branch membership is computed once; lists and graphs consume the same tasks. */
(function(){
'use strict';
const address=(t,to)=>[to?t.handler:t.requester,to?(t.handler_session_real||t.handler_session):t.requester_session];
const endpoint=(t,to)=>{const a=address(t,to);return JSON.stringify(a[1]?a:[...a,t.task_id,to]);};
function build(tasks){
 const ordered=tasks.slice().sort((a,b)=>(a.created_at||0)-(b.created_at||0)||a.task_id.localeCompare(b.task_id));
 const nodes=new Map(),pairs=new Map();
 for(const t of ordered){
  const a=endpoint(t,false),b=endpoint(t,true),id=JSON.stringify([a,b].sort());
  for(const [k,to] of [[a,false],[b,true]])if(!nodes.has(k))nodes.set(k,{id:k,name:address(t,to)[0]||'未知',session:address(t,to)[1]||'',next:new Set(),prev:new Set()});
  if(!pairs.has(id)){pairs.set(id,{a,b,tasks:[]});nodes.get(a).next.add(b);nodes.get(b).prev.add(a);}
  pairs.get(id).tasks.push(t);
 }
 // Iterative SCC decomposition: a closed cycle is one origin, not infinitely many branches.
 const seen=new Set(),finish=[];
 for(const id of nodes.keys()){
  if(seen.has(id))continue;
  seen.add(id);const stack=[[id,[...nodes.get(id).next],0]];
  while(stack.length){const top=stack[stack.length-1];if(top[2]===top[1].length){finish.push(top[0]);stack.pop();continue;}const x=top[1][top[2]++];if(!seen.has(x)){seen.add(x);stack.push([x,[...nodes.get(x).next],0]);}}
 }
 const component=new Map(),components=[];
 for(const id of finish.reverse()){
  if(component.has(id))continue;
  const index=components.length,part=[],stack=[id];component.set(id,index);
  while(stack.length){const n=stack.pop();part.push(n);for(const x of nodes.get(n).prev)if(!component.has(x)){component.set(x,index);stack.push(x);}}
  components.push(part.sort());
 }
 const incoming=new Set();for(const p of pairs.values())if(component.get(p.a)!==component.get(p.b))incoming.add(component.get(p.b));
 const groups=[];
 components.forEach((roots,i)=>{
  if(incoming.has(i))return;
  const reachable=new Set(roots),queue=roots.slice();
  for(let j=0;j<queue.length;j++)for(const n of nodes.get(queue[j]).next)if(!reachable.has(n)){reachable.add(n);queue.push(n);}
  const ids=new Set();for(const p of pairs.values())if(reachable.has(p.a))for(const t of p.tasks)ids.add(t.task_id);
  const records=ordered.filter(t=>ids.has(t.task_id));
  groups.push({id:JSON.stringify(roots),roots:roots.map(k=>nodes.get(k)),tasks:records,linkKeys:[...new Set(records.map(t=>t.link_key).filter(Boolean))]});
 });
 const shared=new Map();for(const g of groups)for(const id of g.linkKeys)shared.set(id,(shared.get(id)||0)+1);
 for(const g of groups)g.sharedCounts=Object.fromEntries(g.linkKeys.filter(id=>shared.get(id)>1).map(id=>[id,shared.get(id)]));
 return groups;
}
function forViewer(groups,client){
 if(!client)return groups;
 const result=[];
 for(const g of groups){
  const graph={nodes:g.tasks},sessions=window.TraceSessionExplorer.scope(graph).sessions.filter(s=>s.name===client);
  for(const viewer of sessions){
   const tasks=window.TraceSessionExplorer.scope(graph,{viewerKey:viewer.id}).tasks;
   if(!tasks.length)continue;
   result.push({...g,id:JSON.stringify([g.id,viewer.id]),viewerKey:viewer.id,tasks,linkKeys:[...new Set(tasks.map(t=>t.link_key).filter(Boolean))]});
  }
 }
 return result;
}
function title(tasks){
 const names=[...new Set(tasks.flatMap(t=>[t.requester||'未知',t.handler||'未知']))];
 if(names.length===1)return names[0]+' 的会话对话';
 return names.join(' ↔ ');
}
window.TraceBranches={build,forViewer,title};
})();
