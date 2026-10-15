"""Bounded DB-free official raw acquisition; publication owns state commitment.

The caller authenticates the input state and durably reserves provider ceilings.
Returned state is tentative until its independent encrypted package is ready.
No identity, publication precision or settlement approval is performed here.
"""
import base64
import calendar
import copy
from datetime import date,datetime,timedelta,timezone
from email.utils import parsedate_to_datetime
import json
import math
import os
import re
import time

from .kr_evidence_sources import (
    KIND,KIS,MERGER_TR,MAX_BYTES,KIND_REPORT_NAMES,canonical,day,instant,
    _sha,_hash,_id,_code,_date_label,_kind_options,_kis_task,_kind_task,
    _kind_rows,_document_links,_viewer_document_task,_merger_rows,
    krx_task,kind_list_task,kind_list_rows,validate_task,
)

VERSION='research-collection-state-v1'
PROVIDERS=('krx','kis','kind')
MAX_STATE_BYTES=16*1024**2
MAX_PAYLOAD_BYTES=128*1024**2
STATE_KEYS={'version','start_date','end_date','sessions','events','pending','seen','completed',
            'index_pages','cursors','discovery','cooldowns','source_hashes'}
EVENT_KEYS={'event_id','raw_symbol','board','effective_date','reason','settlement_status',
            'source_export_sha256','original_export_sha256','source_url','event_type'}
_HEX=re.compile('[0-9a-f]{64}')


def _provider(task):
    return 'krx' if task['source']=='krx_reference' else 'kis' if task['source']=='kis_merger_split' else 'kind'


def _event(event):
    result={k:v for k,v in event.items() if k in EVENT_KEYS}
    if not isinstance(result.get('event_id'),str) or not 1<=len(result['event_id'])<=256:raise ValueError('Official event id required')
    _code(result.get('raw_symbol'));_date_label(result.get('effective_date'))
    if result.get('board') not in ('KOSPI','KOSDAQ'):raise ValueError('Official event board required')
    if result.get('settlement_status','UNKNOWN')!='UNKNOWN':raise ValueError('Raw event approval forbidden')
    result['settlement_status']='UNKNOWN'
    return result


def _walk(value):
    if isinstance(value,dict):
        for key,item in value.items():
            if not isinstance(key,str) or key.lower() in {'headers','authorization','auth_key','appkey','appsecret',
                'access_token','refresh_token','api_key','secret','sql','path','raw_path','local_path','price_targets'}:
                raise ValueError('Nonportable research control field')
            _walk(item)
    elif isinstance(value,list):
        for item in value:_walk(item)
    elif value is not None and not isinstance(value,(str,int,float,bool)):raise ValueError('JSON-only research state required')
    elif isinstance(value,float) and not math.isfinite(value):raise ValueError('Finite research control required')


def validate_state(state):
    if not isinstance(state,dict) or set(state)!=STATE_KEYS or state['version']!=VERSION:raise ValueError('Research state contract mismatch')
    _walk(state);start=_date_label(state['start_date']);end=_date_label(state['end_date'])
    if start<'2010-01-01' or end<start:raise ValueError('Explicit 2010+ research scope required')
    sessions=state['sessions']
    if not isinstance(sessions,list) or sessions!=sorted(set(sessions)):raise ValueError('Unique ordered KR sessions required')
    for label in sessions:
        if not start<=_date_label(label)<=end:raise ValueError('KR session outside research scope')
    if not isinstance(state['events'],list):raise ValueError('Portable event observations required')
    events={}
    for event in state['events']:
        if not isinstance(event,dict) or set(event)-EVENT_KEYS or _event(event)!=event:raise ValueError('Portable UNKNOWN event required')
        if event['event_id'] in events:raise ValueError('Duplicate research event')
        if not start<=event['effective_date']<=end:raise ValueError('Event outside research scope')
        events[event['event_id']]=event
    if not isinstance(state['pending'],dict) or set(state['pending'])!=set(PROVIDERS):raise ValueError('Provider task queues required')
    for provider,tasks in state['pending'].items():
        if not isinstance(tasks,list):raise ValueError('Bounded task list required')
        ids=set()
        for task in tasks:
            validate_task(task)
            if _provider(task)!=provider or _id(task) in ids:raise ValueError('Provider/duplicate task mismatch')
            ids.add(_id(task))
            if task['source']=='kind_index':
                event=events.get(task['event_id'])
                if event is None or task['params']['marketType']!=('1' if event['board']=='KOSPI' else '2'):raise ValueError('Exact index event required')
            if task['source']=='krx_reference' and task['requested_date'] not in sessions:raise ValueError('Unknown KRX session task')
    for name in ('seen','index_pages','cursors','discovery','cooldowns'):
        if not isinstance(state[name],dict):raise ValueError('Research control map required')
    for key,value in state['seen'].items():
        if not _HEX.fullmatch(key) or not isinstance(value,str) or not _HEX.fullmatch(value):raise ValueError('Research content hashes required')
    for name in ('completed','source_hashes'):
        if not isinstance(state[name],list) or len(state[name])!=len(set(state[name])) or any(not isinstance(v,str) or not _HEX.fullmatch(v) for v in state[name]):
            raise ValueError('Research hash list required')
    for name in ('provider','audit','kind_turn'):
        value=state['cursors'].get(name)
        if type(value) is not int or value<0:raise ValueError('Research cursor invalid')
    discovery=state['discovery'];names=discovery.get('report_names');lookback=discovery.get('lookback_days')
    if not isinstance(names,list) or not names or len(names)!=len(set(names)) or len(names)>8:raise ValueError('Bounded KIND titles required')
    for title in names:_kind_options(title,lookback)
    if not isinstance(discovery.get('documents'),dict):raise ValueError('Observed document tasks required')
    for key,task in discovery['documents'].items():
        validate_task(task)
        if _id(task)!=key or task['source'] not in ('kind_document','kind_document_path'):raise ValueError('Observed document identity invalid')
    for key,metadata in discovery.get('response_metadata',{}).items():
        if not _HEX.fullmatch(key) or not isinstance(metadata,dict) or set(metadata)-{'tr_cont'} or any(not isinstance(v,str) or len(v)>32 for v in metadata.values()):
            raise ValueError('Observed response metadata invalid')
    for provider,control in state['cooldowns'].items():
        if provider not in PROVIDERS or not isinstance(control,dict) or set(control)!={'reason','next_retry_at'}:raise ValueError('Provider cooldown invalid')
        if not isinstance(control['reason'],str) or not re.fullmatch('[A-Z0-9_]{1,80}',control['reason']):raise ValueError('Safe cooldown reason required')
        instant(control['next_retry_at'])
    if len(canonical(state).encode())>MAX_STATE_BYTES:raise ValueError('Research state exceeds bound')
    return state


def _enqueue(state,task,*,force=False,front=False):
    _enqueue_many(state,[task],force=force,front=front)


def _enqueue_many(state,tasks,*,force=False,front=False):
    groups={p:[] for p in PROVIDERS};done=set(state['completed'])
    known={p:{_id(t) for t in state['pending'][p]} for p in PROVIDERS}
    for task in tasks:
        validate_task(task);provider=_provider(task);key=_id(task)
        if key in known[provider] or (not force and key in done):continue
        known[provider].add(key);groups[provider].append(task)
    for provider,added in groups.items():
        if front:state['pending'][provider]=added+state['pending'][provider]
        else:state['pending'][provider].extend(added)


def _months(start,end):
    cursor=day(start);last=day(end)
    while cursor<=last:
        boundary=min(last,date(cursor.year,cursor.month,calendar.monthrange(cursor.year,cursor.month)[1]))
        yield _kis_task(str(cursor),str(boundary));cursor=boundary+timedelta(days=1)


def _index_tasks(state,event):
    for title in state['discovery']['report_names']:
        yield _kind_task(event,state['start_date'],state['end_date'],report_name=title,
            lookback_days=state['discovery']['lookback_days'])


def build_state(*,sessions,start_date='2010-01-01',end_date,events=(),source_hashes=(),completed_tasks=(),seen=None,
                report_names=KIND_REPORT_NAMES,lookback_days=90):
    """Portable plan from authenticated session/export controls, never a DB."""
    start=_date_label(str(start_date));end=_date_label(str(end_date));labels=sorted({_date_label(str(s)) for s in sessions})
    completed=[_id(t) if isinstance(t,dict) else t for t in completed_tasks]
    state=dict(version=VERSION,start_date=start,end_date=end,sessions=labels,
        events=sorted([_event(e) for e in events],key=lambda e:e['event_id']),pending={p:[] for p in PROVIDERS},
        seen=dict(seen or {}),completed=sorted(set(completed)),index_pages={},cursors=dict(provider=0,audit=0,kind_turn=0),
        discovery=dict(list_date=None,kis_end_date=end,report_names=list(report_names),lookback_days=lookback_days,
            documents={},kis_splits={},last_send_at={},response_metadata={},audit_date=None,audit_remaining={p:100 for p in PROVIDERS}),
        cooldowns={},source_hashes=sorted(set(source_hashes)))
    validate_state(state)
    tasks=[]
    for label in labels:
        for board in ('KOSPI','KOSDAQ'):
            for kind in ('master','daily'):tasks.append(krx_task(board,kind,label))
    tasks.extend(_months(start,end));tasks.extend(kind_list_task(board,start,end) for board in ('KOSPI','KOSDAQ'))
    for event in state['events']:tasks.extend(_index_tasks(state,event))
    _enqueue_many(state,tasks)
    return validate_state(state)


def extend_sessions(state,sessions):
    """Add only caller-authenticated KR session labels; never invent weekdays."""
    validate_state(state);result=copy.deepcopy(state)
    new=sorted({_date_label(str(s)) for s in sessions}-set(result['sessions']))
    if not new:return result
    if new[0]<result['start_date']:raise ValueError('New session predates research scope')
    old_end=result['end_date'];result['end_date']=max(old_end,new[-1]);result['sessions']=sorted(set(result['sessions'])|set(new))
    tasks=[krx_task(board,kind,label) for label in new for board in ('KOSPI','KOSDAQ') for kind in ('master','daily')]
    _enqueue_many(result,tasks,front=True)
    if result['end_date']>old_end:
        for task in _months(str(day(old_end)+timedelta(days=1)),result['end_date']):_enqueue(result,task)
        result['discovery']['kis_end_date']=result['end_date']
        for board in ('KOSPI','KOSDAQ'):_enqueue(result,kind_list_task(board,result['start_date'],result['end_date']))
        indexes=[]
        for event in result['events']:
            old={_id(t) for t in _index_tasks(state,event)}
            indexes.extend(t for t in _index_tasks(result,event) if _id(t) not in old)
        _enqueue_many(result,indexes,front=True)
    return validate_state(result)


class _Stop(Exception):
    def __init__(self,reason,retry_at=None):self.reason=reason;self.retry_at=retry_at


class _OfficialFetcher:
    uses_send=True
    def __init__(self,credentials,session):self.credentials=credentials;self.session=session;self.token=None
    @property
    def needs_kis_auth(self):return self.token is None
    def __call__(self,task,send):
        validate_task(task)
        if self.session is None:
            import requests
            self.session=requests.Session()
        if self.credentials is None:
            self.credentials={name:os.environ.get(name) for name in ('KRX_API_KEY','KIS_APP_KEY','KIS_APP_SECRET')}
        params=task.get('params',{});provider=_provider(task)
        if provider=='kis':
            key=self.credentials.get('KIS_APP_KEY');secret=self.credentials.get('KIS_APP_SECRET')
            if not key or not secret:raise _Stop('AUTH_UNAVAILABLE')
            if self.token is None:
                response=send(self.session,'POST',KIS+'/oauth2/tokenP',json=dict(grant_type='client_credentials',appkey=key,appsecret=secret))
                try:self.token=response.json()['access_token']
                except (ValueError,TypeError,KeyError):raise _Stop('AUTH_UNAVAILABLE') from None
                if not isinstance(self.token,str) or not self.token:raise _Stop('AUTH_UNAVAILABLE')
            response=send(self.session,'GET',task['url'],params=params,headers=dict(authorization='Bearer '+self.token,
                appkey=key,appsecret=secret,tr_id=MERGER_TR,custtype='P'))
        elif provider=='krx':
            key=self.credentials.get('KRX_API_KEY')
            if not key:raise _Stop('AUTH_UNAVAILABLE')
            response=send(self.session,'GET',task['url'],params=params,headers={'AUTH_KEY':key})
        else:response=send(self.session,task['method'],task['url'],**{'data' if task['method']=='POST' else 'params':params})
        return response.content,{'tr_cont':response.headers.get('tr_cont','')} if provider=='kis' else {}


def official_fetcher(credentials=None,*,session=None):
    """Lazy environment-only cloud transport; injected credentials stay in memory."""
    return _OfficialFetcher(credentials,session)


def _inventory(state):
    for label in state['sessions']:
        for board in ('KOSPI','KOSDAQ'):
            for kind in ('master','daily'):yield krx_task(board,kind,label)
    yield from _months(state['start_date'],state['end_date'])
    for event in state['events']:yield from _index_tasks(state,event)
    yield from state['discovery']['documents'].values()


def _plan(state,now):
    if state['discovery']['list_date']!=str(now.date()):
        for board in ('KOSPI','KOSDAQ'):_enqueue(state,kind_list_task(board,state['start_date'],state['end_date']),force=True)
        state['discovery']['list_date']=str(now.date())
        recent=[krx_task(board,kind,label) for label in state['sessions'][-6:] for board in ('KOSPI','KOSDAQ') for kind in ('master','daily')]
        _enqueue_many(state,recent,force=True,front=True)
        previous_month=(day(state['end_date']).replace(day=1)-timedelta(days=1)).replace(day=1)
        overlap_start=max(day(state['start_date']),previous_month)
        _enqueue_many(state,_months(str(overlap_start),state['end_date']),force=True)
    if state['discovery'].get('audit_date')!=str(now.date()):
        state['discovery']['audit_date']=str(now.date());state['discovery']['audit_remaining']={p:100 for p in PROVIDERS}
    remaining=state['discovery']['audit_remaining']
    empty={p for p in PROVIDERS if not state['pending'][p] and remaining[p]>0}
    if empty:
        completed=set(state['completed']);inventory=[t for t in _inventory(state) if _id(t) in completed]
        if inventory:
            cursor=state['cursors']['audit']%len(inventory)
            for offset in range(len(inventory)):
                task=inventory[(cursor+offset)%len(inventory)];provider=_provider(task)
                if provider in empty:
                    _enqueue(state,task,force=True);empty.remove(provider)
                    remaining[provider]-=1
                    state['cursors']['audit']+=offset+1
                    if not empty:break


def _select(state,provider):
    tasks=state['pending'][provider]
    if provider!='kind':return tasks[0]
    kinds=('kind_list','kind_index','kind_document','kind_document_path');turn=state['cursors']['kind_turn']
    for offset in range(len(kinds)):
        name=kinds[(turn+offset)%len(kinds)]
        task=next((t for t in tasks if t['source']==name),None)
        if task is not None:
            state['cursors']['kind_turn']=(turn+offset+1)%len(kinds)
            return task
    raise ValueError('Unknown KIND task queue')


def _index_scope(task):
    normalized=copy.deepcopy(task);normalized['params']['pageIndex']='1';return _id(normalized)


def _process(state,task,body,metadata):
    source=task['source'];key=_id(task);closed=True
    state['pending'][_provider(task)]=[t for t in state['pending'][_provider(task)] if t!=task]
    if source=='krx_reference':
        payload=json.loads(body);rows=payload.get('OutBlock_1') if isinstance(payload,dict) else None
        if not isinstance(rows,list) or not rows or any(not isinstance(r,dict) for r in rows):raise ValueError('KRX response contract')
        required={'BAS_DD','MKTCAP','LIST_SHRS','TDD_CLSPRC'} if task['kind']=='daily' else {'ISU_SRT_CD','LIST_DD','SECUGRP_NM'}
        for row in rows:
            if not required<=set(row):raise ValueError('KRX response kind mismatch')
            if row.get('BAS_DD') and str(day(row['BAS_DD']))!=task['requested_date']:raise ValueError('KRX response date mismatch')
            boards={str(row[k]).strip() for k in ('MKT_NM','MKT_TP_NM') if row.get(k)}
            if boards!={task['board']}:raise ValueError('KRX response board mismatch')
    elif source=='kis_merger_split':
        _,truncated=_merger_rows(json.loads(body),task,metadata)
        if truncated:
            start,end=day(task['start_date']),day(task['end_date'])
            if start==end:raise ValueError('KIS single-day truncation')
            middle=start+(end-start)//2
            children=[_kis_task(str(start),str(middle)),_kis_task(str(middle+timedelta(days=1)),str(end))]
            state['discovery']['kis_splits'][key]=dict(task=task,children=[_id(t) for t in children])
            for child in reversed(children):_enqueue(state,child,force=True,front=True)
            closed=False
    elif source=='kind_list':
        rows=kind_list_rows(body,task);events={e['event_id']:e for e in state['events']};indexes=[]
        for event in rows:
            previous=events.get(event['event_id']);event['source_export_sha256']=_sha(body)
            events[event['event_id']]=event
            if previous is None or {k:v for k,v in previous.items() if k!='source_export_sha256'}!={k:v for k,v in event.items() if k!='source_export_sha256'}:
                indexes.extend(_index_tasks(state,event))
        state['events']=sorted(events.values(),key=lambda e:e['event_id'])
        _enqueue_many(state,indexes,force=True)
    elif source=='kind_index':
        event=next(e for e in state['events'] if e['event_id']==task['event_id'])
        candidates,total,info=_kind_rows(body,event);page=int(task['params']['pageIndex']);scope=_index_scope(task)
        if total is None or total<0 or info['response_page'] not in (None,page):raise ValueError('KIND page contract')
        progress=state['index_pages'].get(scope)
        if page==1:progress=dict(total=total,row_ids=[],body_hashes=[],task_ids=[])
        if progress is None or progress['total']!=total:raise ValueError('KIND page total changed')
        ids=info['row_ids'];digest=_sha(body)
        if len(ids)!=min(100,max(0,total-(page-1)*100)) or len(ids)!=len(set(ids)) or set(ids)&set(progress['row_ids']) or digest in progress['body_hashes']:
            raise ValueError('KIND duplicate or incomplete page')
        progress['row_ids']+=ids;progress['body_hashes'].append(digest);progress['task_ids'].append(key)
        for candidate in candidates:
            _enqueue(state,dict(source='kind_document',url=candidate['url'],method='GET',receipt_id=candidate['receipt_id']))
        if page*100<total:
            if page>=1000:raise ValueError('KIND page bound')
            following=copy.deepcopy(task);following['params']['pageIndex']=str(page+1);_enqueue(state,following,force=True)
            state['index_pages'][scope]=progress;closed=False
        else:
            if len(progress['row_ids'])!=total:raise ValueError('KIND unique total mismatch')
            state['completed']=list(dict.fromkeys(state['completed']+progress['task_ids']));state['index_pages'].pop(scope,None)
    elif source in ('kind_document','kind_document_path'):
        if not body.startswith(b'%PDF-') and not re.search(br'<(?:html|!doctype|script|body|iframe|select|form)\b',body,re.I):raise ValueError('KIND document contract')
        state['discovery']['documents'][key]=task
        revised=state['seen'].get(key)!=_sha(body)
        for url in _document_links(body,task['url']):
            original=dict(source='kind_document',url=url,method='GET',receipt_id=task['receipt_id'])
            _enqueue(state,original,force=revised,front=True)
        if source=='kind_document' and '/external/' not in task['url']:
            path=_viewer_document_task(body,task['receipt_id'])
            if path:_enqueue(state,path,force=revised,front=True)
    if closed:state['completed']=list(dict.fromkeys(state['completed']+[key]))
    changed=True
    while changed:
        changed=False;done=set(state['completed'])
        for parent,split in state['discovery']['kis_splits'].items():
            if parent not in done and set(split['children'])<=done:state['completed'].append(parent);changed=True
    state['seen'][key]=_sha(body)
    state['discovery'].setdefault('response_metadata',{})[key]=dict(metadata)
    return state


def _retry(code,headers,now):
    if code==429:
        value=headers.get('Retry-After','')
        try:return now+timedelta(seconds=max(1,float(value)))
        except (ValueError,TypeError,OverflowError):
            try:return max(now+timedelta(seconds=1),instant(parsedate_to_datetime(value)))
            except (ValueError,TypeError,OverflowError):pass
    return now+timedelta(seconds=3600 if code in (401,403) else 60)


def collect_batch(state,*,fetch=None,reserve,now=None,max_requests=100,reservation_id=None,initial=None,
                  deadline=None,clock=None,sleep=time.sleep,min_interval=1.,max_records=256,can_send=None):
    """Return tentative raw records/state after a durable vector reservation.

    Injected fetch(task) is one actual source send. official_fetcher uses the
    provided send adapter, counting OAuth separately under the same KIS ceiling.
    The caller must never reuse a reservation ID for a new HTTP execution.
    """
    validate_state(state)
    if type(max_requests) is not int or not 0<=max_requests<=10000 or type(max_records) is not int or not 1<=max_records<=256:
        raise ValueError('Bounded research batch required')
    if isinstance(min_interval,bool) or not isinstance(min_interval,(int,float)) or not math.isfinite(min_interval) or min_interval<1:
        raise ValueError('Research spacing must be at least one second')
    clock=clock or (lambda:datetime.now(timezone.utc));observed=instant(now) if now is not None else instant(clock())
    deadline=instant(deadline) if deadline is not None else None
    counters={p:0 for p in PROVIDERS};records=[];unchanged=0;status='COLLECTED';work=copy.deepcopy(state)
    def result(current,reason):
        return dict(records=records,state=current,status=reason,requests_by_provider=dict(counters),unchanged_records=unchanged,
            pending_tasks={p:len(current['pending'][p]) for p in PROVIDERS})
    if can_send is not None and not can_send():return result(state,'WAITING_PRICE_WINDOW')
    if deadline is not None and instant(clock())>=deadline:return result(state,'WAITING_DEADLINE')
    if not max_requests:return result(state,'WAITING_BUDGET')
    _plan(work,observed)
    active=[p for p in PROVIDERS if work['pending'][p] and
        (p not in work['cooldowns'] or instant(work['cooldowns'][p]['next_retry_at'])<=observed)]
    if not active:return result(state,'WAITING_RETRY' if state['cooldowns'] else 'IDLE')
    fetch=fetch or official_fetcher();ceilings={p:0 for p in PROVIDERS};turn=work['cursors']['provider']
    ordered=[p for n in range(3) if (p:=PROVIDERS[(turn+n)%3]) in active]
    for n in range(max_requests):ceilings[ordered[n%len(ordered)]]+=1
    if getattr(fetch,'needs_kis_auth',False) and ceilings['kis']==1:
        donor=next((p for p in ordered if p!='kis' and ceilings[p]>0),None)
        if donor:ceilings[donor]-=1;ceilings['kis']+=1
        else:return result(state,'WAITING_BUDGET')
    reservation_id=reservation_id or _hash(dict(state=_hash(state),now=observed.isoformat()))
    bounds={p:0 for p in PROVIDERS};waiting='WAITING_RESERVATION'
    for provider in ordered:
        if not ceilings[provider]:continue
        counts={p:ceilings[provider] if p==provider else 0 for p in PROVIDERS};rid=reservation_id+'-'+provider
        granted=reserve(counts,reservation_id=rid,initial=initial,now=observed)
        if not isinstance(granted,dict) or granted.get('status')!='GRANTED':
            waiting=granted.get('status','WAITING_RESERVATION') if isinstance(granted,dict) else 'WAITING_RESERVATION'
            if isinstance(granted,dict) and granted.get('next_retry_at'):
                work['cooldowns'][provider]=dict(reason=waiting,next_retry_at=instant(granted['next_retry_at']).isoformat())
            continue
        granted_bounds=granted.get('ceilings')
        if granted.get('reservation_id')!=rid or not isinstance(granted_bounds,dict) or set(granted_bounds)!=set(PROVIDERS) or any(type(granted_bounds[p]) is not int or not 0<=granted_bounds[p]<=counts[p] for p in PROVIDERS):continue
        bounds[provider]=granted_bounds[provider]
    if not any(bounds.values()):return result(state,waiting)
    blocked=set();attempted=set();size=len(canonical(work).encode())
    while sum(counters.values())<max_requests and len(records)<max_records:
        candidates=[p for n in range(3) if (p:=PROVIDERS[(work['cursors']['provider']+n)%3]) not in blocked
            and work['pending'][p] and counters[p]<bounds[p]]
        if not candidates:break
        provider=candidates[0];before_turn=work['cursors']['kind_turn'];task=_select(work,provider);key=_id(task)
        if key in attempted:blocked.add(provider);continue
        attempted.add(key)
        def consume():
            if counters[provider]>=bounds[provider]:raise _Stop('WAITING_BUDGET')
            if can_send is not None and not can_send():raise _Stop('WAITING_PRICE_WINDOW')
            current=instant(clock())
            if deadline is not None and current>=deadline:raise _Stop('WAITING_DEADLINE')
            previous=work['discovery']['last_send_at'].get(provider)
            delay=min_interval-(current-instant(previous)).total_seconds() if previous else 0
            if delay>0:
                if deadline is not None and current+timedelta(seconds=delay)>=deadline:raise _Stop('WAITING_DEADLINE')
                sleep(delay);current=instant(clock())
                if can_send is not None and not can_send():raise _Stop('WAITING_PRICE_WINDOW')
                if deadline is not None and current>=deadline:raise _Stop('WAITING_DEADLINE')
            counters[provider]+=1;work['discovery']['last_send_at'][provider]=current.isoformat()
        def send(session,method,url,**kwargs):
            consume();remaining=(deadline-instant(clock())).total_seconds() if deadline is not None else 60
            if remaining<=.2:raise _Stop('WAITING_DEADLINE')
            connect=min(10,remaining/3);read=min(45,remaining-connect)
            try:response=session.request(method,url,timeout=(connect,read),allow_redirects=False,**kwargs)
            except Exception:raise _Stop('TRANSPORT_UNAVAILABLE') from None
            if response.status_code!=200:raise _Stop('HTTP_'+str(response.status_code),_retry(response.status_code,response.headers,instant(clock())))
            if len(response.content)>MAX_BYTES:raise _Stop('RESPONSE_TOO_LARGE')
            return response
        try:
            if getattr(fetch,'uses_send',False):received=fetch(task,send)
            else:consume();received=fetch(task)
            metadata={}
            if isinstance(received,tuple) and len(received)==2:received,metadata=received
            if isinstance(received,dict) and 'status_code' in received:
                if received['status_code']!=200:raise _Stop('HTTP_'+str(received['status_code']),_retry(received['status_code'],received.get('headers',{}),observed))
                metadata=received.get('response_metadata',{});received=received.get('body',b'')
            elif isinstance(received,dict) and 'payload' in received:
                metadata=received.get('response_metadata',{});received=received['payload']
            if not isinstance(metadata,dict) or set(metadata)-{'tr_cont'} or any(not isinstance(v,str) or len(v)>32 for v in metadata.values()):raise ValueError('Response metadata contract')
            body=received.encode() if isinstance(received,str) else received if isinstance(received,bytes) else canonical(received).encode()
            if not body or len(body)>MAX_BYTES:raise ValueError('Upstream body size contract')
            retrieved=instant(clock())
            candidate=_process(copy.deepcopy(work),task,body,metadata);validate_state(candidate)
            digest=_sha(body);record=dict(task=task,retrieved_at=retrieved.isoformat(),body_base64=base64.b64encode(body).decode(),body_sha256=digest,response_metadata=metadata)
            record_size=len(canonical(record).encode()) if (work['seen'].get(key)!=digest
                or work['discovery'].get('response_metadata',{}).get(key,{})!=metadata) else 0
            if size+record_size+len(canonical(candidate).encode())>MAX_PAYLOAD_BYTES-8192:raise _Stop('WAITING_PACKAGE_LIMIT')
            if record_size:records.append(record);size+=record_size
            else:unchanged+=1
            work=candidate;work['cooldowns'].pop(provider,None)
        except _Stop as stop:
            work['cursors']['kind_turn']=before_turn;blocked.add(provider)
            if stop.reason.startswith('WAITING_'):status=stop.reason
            else:
                work['cooldowns'][provider]=dict(reason=stop.reason,next_retry_at=instant(stop.retry_at or observed+timedelta(hours=1)).isoformat());status='DEFERRED'
        except Exception:
            work['cursors']['kind_turn']=before_turn;blocked.add(provider);status='DEFERRED'
            work['cooldowns'][provider]=dict(reason='RESPONSE_CONTRACT',next_retry_at=(observed+timedelta(minutes=5)).isoformat())
            if task['source']=='kind_index':
                scope=_index_scope(task);work['index_pages'].pop(scope,None)
                work['pending']['kind']=[t for t in work['pending']['kind'] if t['source']!='kind_index' or _index_scope(t)!=scope]
                restart=copy.deepcopy(task);restart['params']['pageIndex']='1';_enqueue(work,restart,force=True,front=True)
        work['cursors']['provider']=(PROVIDERS.index(provider)+1)%3
        if status in ('WAITING_DEADLINE','WAITING_PRICE_WINDOW','WAITING_PACKAGE_LIMIT'):break
    validate_state(work)
    return result(work,status if status!='COLLECTED' or records else 'UNCHANGED')
