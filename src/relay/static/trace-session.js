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
// 行首竖线颜色，与列表页 VCOLOR 同一套，保证两处看着是一个东西
// 树形行里只给时分秒：右侧面板宽度有限，完整日期会把整行挤爆，
// 需要完整时间的话下面「创建时间」字段里有。
const hms=ts=>{const d=new Date((Number(ts)||0)*1000);
  return isNaN(d.getTime())?'':String(d.getHours()).padStart(2,'0')+':'+
    String(d.getMinutes()).padStart(2,'0');};
// 标签配色：失败红、取消黄、通知蓝、完成绿
const stateTone=t=>({failed:'bad',cancelled:'warn',done:'ok',
  notified:'info',sent:'info',delivered:'info',working:'busy',queued:'idle'}[t.state]||'idle');
const railColor=t=>({failed:'#d64545',cancelled:'#d69e2e',done:'#2f855a',
  notified:'#3182ce',sent:'#3182ce',delivered:'#3182ce',
  working:'#805ad5',queued:'#718096'}[t.state]||'#a0aec0');
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
 // 标题里要写清是哪些用户在通信：同一个人可能有多个会话，按人聚合后标注会话数
 const byClient=new Map();
 scoped.sessions.forEach(x=>{
   const n=x.name||'未知';
   byClient.set(n,(byClient.get(n)||0)+1);
 });
 const clientLabel=[...byClient.entries()]
   .map(([n,c])=>c>1?n+'('+c+'会话)':n)
   .join(' · ');
 let chosen=seed.task_id,scale=1,seq=0,context=tasks,panX=0,panY=0,drag=null,suppressNextClick=false;
 document.getElementById('drawer')?.classList.add('explorer-open');
 container.innerHTML='<section class="tx" data-selected-task=""><div class="tx-head"><div class="tx-title"><strong>会话通信链路</strong><span class="tx-clients">'+esc(clientLabel)+'</span><span>'+people.size+' 会话 · '+pairs.size+' 会话对 · '+tasks.length+' 次通信</span><span class="tx-total"></span></div><div class="tx-crumb">当前会话的直接上游一层＋下游 · 全部历史，不筛状态</div><div class="tx-viewpoint"><label>当前查看会话 <select data-viewer-select aria-label="当前查看会话"></select></label></div></div><div class="tx-layout"><div class="tx-map"><div class="tx-toolbar"><span class="tx-panel-name">拓扑图</span><button data-action="all">本视角全部通信</button><button data-action="failures">只看失败通信</button><button data-action="origin">定位入口通信</button><button data-action="fit">适应全图</button><button data-action="zoom-in">＋</button><button data-action="zoom-out">−</button><span class="tx-legend">滚轮缩放 · 按住左键拖动 · 点击方向次数查看消息 · 标记始终贴合连线</span></div><div class="tx-stage" id="execution-canvas"></div><div class="tx-caption">范围按会话对首次通信方向展开；双向通信分成上下两条单向线，每条单独显示次数；不沿回传展开更早上游。<br>直接上游只保留与当前会话的通信，不展开上游的其他分支；此为展示范围，不是访问权限控制。</div></div><aside class="tx-inspector" id="execution-detail"></aside></div></section>';
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
 // 同方向通信最多的那条边决定了标签卡片能有多高，行距得跟着让开；
 // 否则 7 行摘要的卡片会直接压住隔壁那条边的标签。
 let maxBriefLines=0;
 pairs.forEach(pair=>{
   const f=pair.tasks.filter(t=>key(t,false)===pair.a).length;
   maxBriefLines=Math.max(maxBriefLines,Math.min(f,7),Math.min(pair.tasks.length-f,7));
 });
 const rowGap=Math.max(140,maxBriefLines*16+110);
 const height=Math.max(440,...layoutCols.map(c=>c.length*rowGap+120)),width=Math.max(840,layoutCols.length*370+160);
 layoutCols.forEach((col,d)=>col.forEach((p,i)=>{p.x=65+d*370;p.y=(height-col.length*rowGap)/2+i*rowGap+70;}));
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
     const dirTasks=pair.tasks.filter(t=>(key(t,false)===pair.a)===(direction==='forward'));
     const failedCount=dirTasks.filter(failed).length;
     const sharedCount=Math.max(...pair.tasks.map(shared));
     const label=from.name+' 发给 '+to.name+' '+count+' 次'+(failedCount?'，其中失败 '+failedCount+' 次':'')+(sharedCount>1?'，共享通信 · '+sharedCount+' 组':'')+'，点击查看消息';
     // 每一次请求的摘要都要能在图上看到，而不是只给个「N次」。
     // 同一方向有多条时按时间顺序逐条列出。
     const ordered=dirTasks.slice().sort((x,y)=>(x.created_at||0)-(y.created_at||0));
     const briefs=ordered.map(t=>String(t.summary||'').trim()||'（无摘要）');
     const lane={pair,direction,count,label,points,failedCount,sharedCount,briefs};lanes.push(lane);
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
 // 只数「有没有撞」不够：全部位置都撞时分不出优劣，结果就是挤在一起。
 // 改用重叠面积，才能选出「没那么撞」的那个。
 const overlapArea=(a,b,pad=6)=>{
   const w=Math.min(a.x+a.w+pad,b.x+b.w)-Math.max(a.x-pad,b.x);
   const h=Math.min(a.y+a.h+pad,b.y+b.h)-Math.max(a.y-pad,b.y);
   return w>0&&h>0?w*h:0;
 };
 const nodeBoxes=[...people.values()].map(p=>({x:p.x,y:p.y-38,w:170,h:76}));
 lanes.forEach(lane=>{
   const {pair,direction,count,label,points,failedCount,sharedCount,briefs}=lane;
   const path=svgElement.querySelector('.tx-route[data-session-pair="'+pair.index+'"][data-direction="'+direction+'"] .tx-line'),length=path.getTotalLength();
   // 边上只放一行，读起来才不费劲：
   //   单次 → 直接显示这条消息的摘要（超长截断，别把卡片撑爆）
   //   多次 → 只显示「N 次通信」，逐条摘要右侧消息明细里已经有了，这里再列一遍纯属重复
   const list=briefs||[];
   const multi=count>1||list.length>1;
   // 截得狠一点。标签钉在连线中点后就不再躲让，太宽会直接压到两头的节点框上
   // （实测 263px 的标签把右侧节点名字盖掉了）。完整摘要悬停和右侧明细里都有。
   // 有失败角标时要再短一截，否则摘要会和「失败 N」叠在一起（实测踩过）。
   const CAP=failedCount?8:12;
   const raw=(list[0]||'').trim();
   const text=multi?(count+' 次通信')
                   :(raw?(raw.length>CAP?raw.slice(0,CAP)+'…':raw):'（无摘要）');
   const h=34+(sharedCount>1?14:0);
   // 宽度先估：图标 + 文字 + 失败角标 + 右边距。插进 DOM 后还会按真实宽度校正一次。
   // 上限 190：节点间距就那么大，再宽就贴到节点上了。
   const w=Math.max(76,Math.min(190,Math.ceil(33+text.length*13+14+(failedCount?52:0))));
   // 标签一律钉在连线中点，不再做垂直错开。
   // 之前允许 dy 偏移是为了躲开节点，但代价是标签飘到别处、还要拉虚线找回来，
   // 反而更难读（实测出现过标签跑到上一层、和别的文字叠在一起）。
   // 现在宁可挤一点，也保证「标签一定在它那条线上」。
   const point=path.getPointAtLength(length*0.5);
   const rect={x:point.x-w/2,y:point.y-h/2,w,h};
   occupied.push(rect);
   const group=document.createElementNS(ns,'g');group.setAttribute('class','tx-edge-control');group.dataset.controlPair=pair.index;group.dataset.laneDirection=direction;
   group.dataset.fraction=0.5;group.setAttribute('transform',`translate(${point.x} ${point.y})`);
   const before=path.getPointAtLength(Math.max(0,length*0.5-2)),after=path.getPointAtLength(Math.min(length,length*0.5+2));
   const angle=Math.atan2(after.y-before.y,after.x-before.x)*180/Math.PI;
   var inner='<circle cx="17" cy="'+(h/2)+'" r="10"/>'+
     '<path class="tx-direction-arrow" d="M-5 0 H5 M1 -4 L5 0 L1 4" transform="translate(17 '+(h/2)+') rotate('+angle+')"/>'+
     '<text class="tx-chip-line solo" x="33" y="'+(h/2+4)+'">'+esc(text)+'</text>';
   if(failedCount)inner+='<text class="tx-fail-label" x="'+(w-12)+'" y="'+(h/2+4)+'" text-anchor="end">失败 '+failedCount+'</text>';
   if(sharedCount>1)inner+='<text class="tx-shared-label" x="14" y="'+(h-4)+'">共享 · '+sharedCount+'组</text>';
   // 悬停能看到全部摘要，截断只影响画布上的显示，不丢信息
   const briefTip=list.map(function(s,i){return (list.length>1?(i+1)+'. ':'')+s;}).join('\n');
   group.innerHTML='<g class="tx-direction-chip '+direction+(multi?' multi':'')+(failedCount?' has-failure':'')+'" data-session-pair="'+pair.index+'" data-direction="'+direction+'" role="button" tabindex="0" aria-label="'+esc(label)+(briefTip?'，摘要：'+esc(briefTip):'')+'" transform="translate('+(-w/2)+' '+(-h/2)+')"><title>'+esc(label)+(briefTip?'\n'+esc(briefTip):'')+'</title><rect width="'+w+'" height="'+h+'" rx="10"/>'+inner+'</g>';
   controlLayer.appendChild(group);
   // 文字宽度只能估，中英文混排更估不准。插进 DOM 后量一次真实宽度，
   // 不够就把卡片撑开——但不超过 210，宁可文字被切也不能盖住节点。
   (function(){
     const chip=group.querySelector('.tx-direction-chip');
     const box=chip&&chip.querySelector('rect');
     if(!box)return;
     let right=0;
     chip.querySelectorAll('text').forEach(function(t){
       let bb; try{bb=t.getBBox();}catch(e){return;}
       const x=parseFloat(t.getAttribute('x')||0);
       right=Math.max(right, t.getAttribute('text-anchor')==='end' ? x : x+bb.width);
     });
     const need=Math.min(210,Math.ceil(right+14));
     if(need>w){
       box.setAttribute('width',need);
       chip.setAttribute('transform','translate('+(-need/2)+' '+(-h/2)+')');
       rect.w=need; rect.x=point.x-need/2;
     }
     // 失败角标是右对齐的，它跟摘要可能撞上。撞了就把摘要再切短，
     // 直到不重叠为止——两块文字叠在一起谁都读不出来。
     const solo=chip.querySelector('.tx-chip-line.solo');
     const fail=chip.querySelector('.tx-fail-label');
     if(solo&&fail){
       const finalW=parseFloat(box.getAttribute('width'));
       let guard=0;
       while(guard++<12){
         let sb,fb; try{sb=solo.getBBox();fb=fail.getBBox();}catch(e){break;}
         const soloRight=parseFloat(solo.getAttribute('x'))+sb.width;
         const failLeft=finalW-12-fb.width;
         if(soloRight<=failLeft-6)break;
         const cur=solo.textContent.replace(/\u2026$/,'');
         if(cur.length<=2)break;
         solo.textContent=cur.slice(0,cur.length-1)+'\u2026';
       }
     }
   })();
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
   // 用缩进 + 行首竖线表达层级，和外层链路卡片展开后的行同构。
   // 原来的卡片一条占五六行，十来条就要滚半天，层级还看不出来。
   return '<section class="tx-messages"><div class="tx-message-heading">' +
     '<strong>跨端消息</strong><span>' + list.length + ' 条</span></div>' +
     '<div class="tx-msg-tree">' + list.map(t => {
     const active = t.task_id === chosen, d = Number(t.depth) || 0, bad = failed(t);
     const abnormal = bad || t.state === 'cancelled';
     const rs = (t.requester_session || '').slice(-6);
     const hs = (t.handler_session_real || t.handler_session || '').slice(-6);
     // 同一个人的两个会话在通信时，会话号是唯一能分清的东西，恰恰最不能省。
     // 只有两端真是同一个会话（自环）才没必要重复写。
     const selfLoop = rs && hs && rs === hs;
     return '<button type="button" class="tx-mrow' + (active ? ' on' : '') + (bad ? ' bad' : '') +
       '" data-select-task="' + esc(t.task_id) + '" aria-pressed="' + active +
       '" style="--d:' + d + ';--rc:' + railColor(t) + '">' +
       '<span class="tx-mrail"></span>' +
       '<span class="tx-ml1">' +
         (d ? '<span class="tx-mtee">└</span>' : '<span class="tx-mdot">●</span>') +
         '<b>' + esc(t.requester) + '</b>' + (rs ? '<small>…' + esc(rs) + '</small>' : '') +
         '<span class="tx-mdir">→</span>' +
         '<b>' + esc(t.handler) + '</b>' + (hs && !selfLoop ? '<small>…' + esc(hs) + '</small>' : '') +
         '<span class="tx-mgrow"></span>' +
         '<time>' + esc(hms(t.created_at)) + '</time>' +
       '</span>' +
       '<span class="tx-ml2">' + (esc(t.summary) || '<i>（无摘要）</i>') + '</span>' +
       // 正常态不占地方——行首竖线的颜色已经表达了状态；只有异常和共享才显式标出来
       (abnormal || shared(t) > 1 ?
         '<span class="tx-ml3">' +
           (abnormal ? '<span class="tx-mstate bad">' + esc(stateName(t)) +
             (bad ? '：' + esc(reason(t)) : '') + '</span>' : '') +
           (shared(t) > 1 ? '<span class="tx-mtag">共享 ' + shared(t) + ' 组</span>' : '') +
         '</span>' : '') +
     '</button>';
   }).join('') + '</div></section>';
 }

 async function select(id,list=context,opts={}){
   const t=byId.get(id);if(!t)return;
   // 从消息列表里点另一条时别把人弹回顶部：记住被点那行此刻在视口的高度，
   // 重建 DOM 后再挪回同一位置，视觉上这一行就是不动的。
   const rowSel='[data-select-task="'+CSS.escape(id)+'"]';
   const anchor=opts.keepScroll?detail.querySelector(rowSel):null;
   const keepAt=anchor?anchor.getBoundingClientRect().top:null;
   const realign=()=>{
     if(keepAt==null)return;
     const el=detail.querySelector(rowSel);
     if(el)detail.scrollTop+=el.getBoundingClientRect().top-keepAt;
   };
   chosen=id;context=list;const current=++seq;host.dataset.selectedTask=id;
   const pair=pairs.get(t._pair);
   stage.querySelectorAll('[data-session-pair]').forEach(el=>el.classList.toggle('on',Number(el.dataset.sessionPair)===pair.index&&(!el.dataset.direction||(key(t,false)===pair.a)===(el.dataset.direction==='forward'))));
   stage.querySelectorAll('[data-session]').forEach(el=>el.classList.toggle('on',[key(t,false),key(t,true)].includes(el.dataset.session)));
   detail.innerHTML='<div class="tx-panel-name inspector">消息明细</div>'+(shared(t)>1?'<div class="tx-shared-note">共享通信 · 出现在 '+shared(t)+' 组，属于同一条记录</div>':'')+'<section class="tx-selection"><span class="tx-selection-eyebrow">当前通信</span><h3>'+esc(t.requester)+' <span class="tx-selection-arrow">→</span> '+esc(t.handler)+'</h3><div class="tx-selection-meta"><span class="tx-tag kind '+(t.kind==='notify'?'notify':'req')+'">'+(t.kind==='notify'?'单向通知':'请求')+'</span><span class="tx-tag st '+stateTone(t)+'">'+esc(stateName(t))+'</span>'+(t.parent_task_id?'<span class="tx-tag sub">子任务</span>':'')+'<time>'+esc(date(t.created_at))+'</time></div>'+(failed(t)?'<div class="tx-failure-detail" role="alert"><strong>这条通信失败了</strong><div>失败方向：'+esc(t.requester)+' 发给 '+esc(t.handler)+'</div><div>失败时间：'+esc(date(t.state_at||t.finished_at))+'</div><div>失败原因：'+esc(reason(t))+'</div>'+(t.error_code?'<div>错误代码：'+esc(t.error_code)+'</div>':'')+'</div>':'')+'<div class="tx-current-preview">'+esc(t.summary||'无摘要')+'</div><div class="tx-body-block" id="tx-msg-body"><div class="tx-body-label">请求正文</div><div class="tx-body-text loading">加载中…</div></div><details class="tx-fold"><summary>技术字段<span class="tx-fold-hint">通信ID · 会话 · 时间</span></summary><div class="tx-info"><div><span>通信ID</span><code>'+esc(t.task_id)+'</code></div><div><span>发起会话</span><code>'+esc(t.requester_session||'未记录')+'</code></div><div><span>目标会话</span><code>'+esc(t.handler_session_real||t.handler_session||'未记录')+'</code></div><div><span>入口会话</span><code>'+esc(t.handler_session||'未记录')+'</code></div>'+(t.parent_task_id?'<div><span>父任务</span><code>'+esc(t.parent_task_id)+'</code></div>':'')+'<div><span>创建时间</span><code>'+esc(date(t.created_at))+'</code></div>'+(t.finished_at?'<div><span>结束时间</span><code>'+esc(date(t.finished_at))+'</code></div>':'')+'</div></details><span class="tx-span-label" id="session-event-span">加载记录跨度…</span></section>'+listHTML(list)+'<section class="tx-raw-events open"><div class="tx-message-heading"><strong>原始通信记录</strong><span id="tx-event-count"></span></div><div class="tx-frames">加载中…</div></section>';

   if(keepAt==null)detail.scrollTop=0; else realign();
   try{
      const r=await fetch('/api/trace/tasks/'+encodeURIComponent(id)+'/events?limit=20000');if(!r.ok)throw Error('HTTP '+r.status);const data=await r.json();if(seq!==current)return;
      const es=(data.events||[]).sort((a,b)=>a.ts-b.ts||a.id-b.id),start=es[0]?.ts,end=es.at(-1)?.ts;
      detail.querySelector('#session-event-span').textContent='最早到最新记录相隔 '+duration(start==null?null:end-start);
      detail.querySelector('#tx-event-count').textContent='（'+es.length+'）';
      realign();
      // 正文取自 request/notify 事件；回复正文取 response 事件
      (function(){
        var box=detail.querySelector('#tx-msg-body'); if(!box)return;
        var pick=function(kinds){
          for(var i=0;i<es.length;i++){
            if(kinds.indexOf(es[i].ev)>=0&&es[i].message&&String(es[i].message).trim())return es[i];
          }
          return null;
        };
        var req=pick(['request','notify']), rep=pick(['response']);
        var html='';
        if(req)html+='<div class="tx-body-label">'+(req.ev==='notify'?'通知正文':'请求正文')+'</div>'+
          '<div class="tx-body-text">'+esc(req.message)+'</div>';
        if(rep)html+='<div class="tx-body-label reply">回复正文</div>'+
          '<div class="tx-body-text reply">'+esc(rep.message)+'</div>';
        if(!html)html='<div class="tx-body-label">请求正文</div>'+
          '<div class="tx-body-text empty">这条记录没有正文。中继只对改动之后的新消息保存正文，'+
          '历史记录当时只存了摘要；也可能是 trace_payload_chars 被设成了 0。</div>';
        box.innerHTML=html;
      })();
      detail.querySelector('.tx-frames').innerHTML=(es.length>=20000?'<div class="tx-warning">达到单次接口上限20000条，可能还有未展示事件。</div>':'')+es.map(e=>'<div class="tx-frame"><div class="tx-frame-head"><b>'+esc(eventNames[e.ev]||e.ev)+'</b><time>'+esc(date(e.ts))+'</time></div><div class="tx-frame-sub">'+esc(e.from_client)+' → '+esc(e.to_client)+' · 距首条 '+duration(e.ts-start)+'</div>'+((e.summary||e.message||e.peer_state)?'<div class="tx-frame-text">'+esc(e.summary||e.message||e.peer_state)+'</div>':'')+'<details class="tx-fold json"><summary>原始字段<span class="tx-fold-hint">JSON</span></summary><pre class="tx-raw-json">'+esc(JSON.stringify(e,null,2))+'</pre></details></div>').join('');
      realign();
   }catch(err){if(seq===current){detail.querySelector('.tx-frames').textContent='事件加载失败：'+err;detail.querySelector('#session-event-span').textContent='记录跨度未记录';}}
 }
 host.addEventListener('click',e=>{
   const task=e.target.closest('[data-select-task]');if(task){select(task.dataset.selectTask,context,{keepScroll:true});return;}
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
