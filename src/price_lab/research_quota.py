"""Authenticated CAS request accounting, without operator daily budgets."""
import base64
from datetime import datetime,timedelta,timezone
import os
from .daily_delta_transport import _api
from .research_delta_codec import _canonical,_loads,_date,_instant,sign_control,verify_control

PATH='/contents/.github/research-quota.json'
PROVIDERS=('krx','kis','kind')
# Integer validation is a wire bound, not a source request allowance. Provider
# pacing and HTTP Retry-After are enforced by the source collector itself.
_MAX_COUNT=2**63-1
_RECENT_RESERVATIONS=1024
KST=timezone(timedelta(hours=9))


def _counts(value):
    if not isinstance(value,dict) or set(value)-set(PROVIDERS) or any(type(v) is not int or not 0<=v<=_MAX_COUNT for v in value.values()):raise ValueError('Invalid request reservation')
    return {k:value.get(k,0) for k in PROVIDERS}


def reserve_requests(counts,*,reservation_id,initial=None,now=None,token=None,key=None):
    counts=_counts(counts);key=key or os.environ.get('PRICE_DATA_KEY')
    if not isinstance(reservation_id,str) or not 1<=len(reservation_id)<=160:raise ValueError('Invalid reservation identity')
    now=now or datetime.now(timezone.utc)
    now=_instant(now.isoformat() if isinstance(now,datetime) else now);today=now.astimezone(KST).date().isoformat()
    for attempt in range(4):
        try:remote=_api('GET',PATH,token=token)
        except RuntimeError as exc:
            if 'HTTP 404' not in str(exc):raise
            remote=None
        if remote:
            content=remote.get('content','')
            if not isinstance(content,str) or len(content)>2*1024**2:raise ValueError('Quota journal size limit')
            state=verify_control(_loads(base64.b64decode(content)),key)
            if state.get('version')!='research-quota-v1':raise ValueError('Invalid quota journal')
            _date(state['quota_day']);used=_counts(state['reserved'])
            if state['quota_day']>today:raise ValueError('Quota clock regression')
            if type(state.get('revision')) is not int or state['revision']<1 or not isinstance(state.get('reservations'),dict):raise ValueError('Invalid quota revision')
            if initial:
                floor=_counts(initial['used'])
                if (state['quota_day']<initial['quota_day'] or state['revision']<initial.get('revision',0) or
                    state['quota_day']==initial['quota_day'] and any(used[p]<floor[p] for p in PROVIDERS)):
                    raise ValueError('Authenticated quota regression')
            if state['quota_day']<today:
                state={**state,'quota_day':today,'reserved':{p:0 for p in PROVIDERS},'reservations':{},'reservation_order':[],'next_retry_at':None}
        else:
            if not initial or initial.get('allow_initialize') is not True:raise ValueError('Missing initialized quota journal')
            history=_api('GET','/commits?path=.github%2Fresearch-quota.json&per_page=1',token=token)
            if not isinstance(history,list) or history:raise ValueError('Missing previously initialized quota journal')
            _date(initial['quota_day'])
            if initial['quota_day']>today:raise ValueError('Handoff quota day is in the future')
            used=_counts(initial['used']) if initial['quota_day']==today else {p:0 for p in PROVIDERS}
            state=dict(version='research-quota-v1',quota_day=today,reserved=used,reservations={},next_retry_at=None,revision=0)
        old=state['reservations'].get(reservation_id)
        floor=dict(quota_day=state['quota_day'],used=dict(state['reserved']),revision=state['revision'])
        if old is not None:
            if old!=counts:raise ValueError('Conflicting reservation retry')
            return dict(status='GRANTED',reservation_id=reservation_id,ceilings=counts,quota_day=today,next_retry_at=None,quota_floor=floor)
        if state.get('next_retry_at') and _instant(state['next_retry_at'])>now:
            return dict(status='WAITING_RETRY',reservation_id=reservation_id,ceilings={},quota_day=today,next_retry_at=state['next_retry_at'],quota_floor=floor)
        if any(state['reserved'][p]+counts[p]>_MAX_COUNT for p in PROVIDERS):
            raise ValueError('Request accounting overflow')
        state['reserved']={p:state['reserved'][p]+counts[p] for p in PROVIDERS}
        # Accounting no longer gates sends. Bound recent uncertain-write
        # identities without erasing totals. Old evicted IDs conservatively
        # count again; callers use fresh UUIDs for every actual HTTP execution.
        # Persist order explicitly because canonical JSON sorts object keys.
        order=state.get('reservation_order',list(state['reservations']))
        if (not isinstance(order,list) or any(not isinstance(rid,str) for rid in order)
            or len(order)!=len(state['reservations']) or set(order)!=set(state['reservations'])):
            raise ValueError('Invalid reservation ordering')
        state['reservations'][reservation_id]=counts;state['revision']+=1
        state['reservation_order']=(order+[reservation_id])[-_RECENT_RESERVATIONS:]
        state['reservations']={rid:state['reservations'][rid] for rid in state['reservation_order']}
        update=dict(message='Reserve bounded research requests',branch='main',
            content=base64.b64encode(_canonical(sign_control(state,key))).decode())
        if remote:update['sha']=remote['sha']
        try:_api('PUT',PATH,token=token,payload=update)
        except RuntimeError:
            # An uncertain write must be read back before any provider HTTP send.
            continue
        # Confirm durable identity, including successful but uncertain API replies.
    raise ValueError('Request reservation could not be confirmed')
