"""Continuous bounded raw-reference batches. Cloud contains no database."""
import argparse
import copy
from datetime import datetime,timedelta,timezone
import json
import os
from pathlib import Path
import sys
import time
import uuid
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))


def price_window(now):
    ny=now.astimezone(ZoneInfo('America/New_York'));kr=now.astimezone(ZoneInfo('Asia/Seoul'))
    return 19*60+27<=ny.hour*60+ny.minute<20*60+7 or 20*60+27<=kr.hour*60+kr.minute<21*60+7


def continuation_ready(state,now):
    from price_lab.research_quota import LIMITS,KST
    if price_window(now):return False
    floor=state['discovery'].get('quota_floor',{})
    today=now.astimezone(KST).date().isoformat()
    used=floor.get('used',{}) if floor.get('quota_day')==today else {}
    for provider,tasks in state['pending'].items():
        retry=state['cooldowns'].get(provider,{}).get('next_retry_at')
        if retry and datetime.fromisoformat(retry.replace('Z','+00:00'))>now:continue
        if tasks and LIMITS[provider]-used.get(provider,0)>=(2 if provider=='kis' else 1):return True
    return False


def request_continuation():
    """At most one successor; fixed workflow/main only, cron remains recovery."""
    if (os.environ.get('GITHUB_ACTIONS')!='true' or os.environ.get('GITHUB_REF')!='refs/heads/main'
        or os.environ.get('GITHUB_EVENT_NAME') not in ('schedule','workflow_dispatch')):
        return {'status':'NOT_ACTIONS_MAIN'}
    if price_window(datetime.now(timezone.utc)):return {'status':'WAITING_PRICE_WINDOW'}
    from price_lab.daily_delta_transport import _api
    try:
        current=int(os.environ['GITHUB_RUN_ID'])
        for status in ('queued','in_progress'):
            runs=_api('GET','/actions/workflows/reference-evidence.yml/runs?status='+status+'&per_page=100')['workflow_runs']
            if not isinstance(runs,list) or len(runs)>100:raise ValueError('Invalid continuation listing')
            if any(r.get('id')!=current and r.get('status') in ('queued','in_progress','waiting','requested','pending') for r in runs):
                return {'status':'ALREADY_QUEUED'}
        _api('POST','/actions/workflows/reference-evidence.yml/dispatches',
            payload={'ref':'main','inputs':{'max_requests':'100','max_batches':'100'}})
        return {'status':'DISPATCHED'}
    except Exception:return {'status':'WAITING_SCHEDULE_RECOVERY'}


def assert_database_free_runtime():
    if 'duckdb' in sys.modules or 'price_lab.local_credentials' in sys.modules:
        raise ValueError('Cloud research must not import a database or private credentials')
    for pattern in ('*.duckdb','*.sqlite','*.sqlite3','*.pldata'):
        if next(ROOT.rglob(pattern),None) is not None:raise ValueError('Cloud research must contain raw delta packages only')


def run_pipeline(*,seconds=420,max_requests=100,max_batches=100,work_root=None):
    from price_lab.research_delta_codec import open_bootstrap,package_digest
    from price_lab.research_delta_transport import newest_package,load_package,publish_package
    from price_lab.research_delta_collect import collect_batch,official_fetcher,extend_sessions
    from price_lab.research_quota import reserve_requests
    from price_lab import daily_delta_transport as daily
    if not 60<=seconds<=420 or not 1<=max_requests<=200 or not 1<=max_batches<=100:raise ValueError('Invalid producer bounds')
    clock=lambda:datetime.now(timezone.utc)
    if price_window(clock()):return {'status':'WAITING_PRICE_WINDOW','batches':0,'cloud_database':False}
    work=Path(work_root or ROOT/'data/research');work.mkdir(parents=True,exist_ok=True)
    key=os.environ.get('PRICE_DATA_KEY');end=clock()+timedelta(seconds=seconds)
    bootstrap=open_bootstrap(ROOT/'config/research-bootstrap.pldelta',key)
    release=newest_package();prior=load_package(release,work,key=key) if release else None
    state=prior['state'] if prior else bootstrap['state']
    if prior and prior['bootstrap_anchor_sha256']!=bootstrap['anchor_sha256']:raise ValueError('Research bootstrap stream mismatch')
    latest_daily=daily.newest_package()
    if latest_daily:
        price=daily.load_package(latest_daily,work,key=key)
        state=extend_sessions(state,price['state']['sessions']['kr'])
        del price
    fetch=official_fetcher()
    invocation=f"{os.environ.get('GITHUB_RUN_ID','manual')}-{os.environ.get('GITHUB_RUN_ATTEMPT','1')}-{uuid.uuid4().hex}"
    sent=[];status='RUNNING';total_requests=0
    for index in range(max_batches):
        if price_window(clock()) or clock()>=end-timedelta(seconds=60):break
        initial={**state['discovery'].get('quota_floor',bootstrap['quota']),'allow_initialize':prior is None}
        last_floor=None
        def reserve(counts,**kwargs):
            nonlocal last_floor
            value=reserve_requests(counts,reservation_id=kwargs.get('reservation_id') or f"{invocation}-{index}",
                initial=initial,now=clock(),key=key)
            if value.get('quota_floor'):
                last_floor=value['quota_floor'];initial.update(last_floor)
            return value
        rid=f"{invocation}-{index}"
        result=collect_batch(copy.deepcopy(state),fetch=fetch,reserve=reserve,now=clock(),max_requests=max_requests,
            reservation_id=rid,initial=initial,deadline=end-timedelta(seconds=60),clock=clock,
            can_send=lambda:not price_window(clock()))
        state=result['state'];status=result['status']
        total_requests+=sum(result['requests_by_provider'].values())
        if last_floor:state['discovery']['quota_floor']=last_floor
        records=result['records']
        if not records and prior and state==prior['state']:break
        payload=dict(schema='research-delta-v1',stream_id='kr-reference-evidence-v1',
            sequence=prior['sequence']+1 if prior else 1,
            previous_package_sha256=package_digest(prior) if prior else None,
            bootstrap_anchor_sha256=bootstrap['anchor_sha256'],collected_at=clock().isoformat(),records=records,state=state)
        published=publish_package(payload,key=key,work_root=work)
        sent.append({'sequence':published['sequence'],'records':len(records),'package_sha256':published['package_sha256']})
        prior=payload
        print(json.dumps({'stage':'research_batch','status':status,**sent[-1],'cloud_database':False}),flush=True)
        if status=='IDLE' or status.startswith('WAITING'):break
    return {'status':status,'batches':len(sent),'published':sent,'cloud_database':False,
        'source_requests':total_requests,'continue_ready':total_requests>0 and continuation_ready(state,clock())}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--seconds',type=int,default=420)
    parser.add_argument('--max-requests',type=int,default=100);parser.add_argument('--max-batches',type=int,default=100)
    args=parser.parse_args();assert_database_free_runtime()
    result=run_pipeline(seconds=args.seconds,max_requests=args.max_requests,max_batches=args.max_batches)
    if args.max_batches>1 and result.get('continue_ready'):
        result['continuation']=request_continuation()
    assert_database_free_runtime();print(json.dumps(result),flush=True)


if __name__=='__main__':main()
