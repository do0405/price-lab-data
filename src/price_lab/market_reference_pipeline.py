"""Bounded DB-free market references, shared by Actions and local acquisition.

An independent namespace reuses the authenticated acquisition lease. Existing
Korean research/price cursors and jobs are never migrated or reset.
"""
import base64
import copy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.parse import urlsplit

import requests

from . import market_reference_sources as s
from .research_delta_codec import seal_package, open_package, package_digest
from .research_ownership import ResearchOwner, read_control
from .daily_delta_transport import _api

NAMESPACE='market-references'
ASSET='market-reference.pldelta'


def bootstrap(config_path,now=None):
    import exchange_calendars as xcals
    import re
    from zoneinfo import ZoneInfo
    now=now or datetime.now(timezone.utc)
    through=(now.astimezone(ZoneInfo('Asia/Seoul')).date()-timedelta(days=1)).isoformat()
    config=json.loads(Path(config_path).read_text(encoding='utf-8'))
    if set(config)!={'version','ciks'} or config['version']!=1: raise ValueError('Market bootstrap contract mismatch')
    if not isinstance(config['ciks'],list) or len(config['ciks'])>50000 or any(not isinstance(c,str) or not re.fullmatch('[0-9]{10}',c) for c in config['ciks']): raise ValueError('Official CIK seed required')
    kr=[str(x.date()) for x in xcals.get_calendar('XKRX',start='2010-01-01',end=through).sessions]
    us=[str(x.date()) for x in xcals.get_calendar('XNYS',start='2010-01-01',end=through).sessions]
    months={d[:7]:d for d in us if d[:7]<through[:7]}
    return dict(through=through,kr_sessions=kr,us_month_ends=list(months.values()),ciks=list(dict.fromkeys(config['ciks'])))


def _atomic(path, value):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+'.working')
    temporary.write_text(s.canonical(value),encoding='utf-8'); os.replace(temporary,path)


def tag(head):
    from .research_ownership import _head
    _head(head)
    if head is None: raise ValueError('Package head required')
    return f"delta-market-{head['sequence']:012d}-{head['package_sha256']}"


def load(head, directory, *, key=None):
    directory=Path(directory); directory.mkdir(parents=True,exist_ok=True)
    path=directory/(tag(head)+'.pldelta')
    if not path.exists():
        release=_api('GET','/releases/tags/'+tag(head),revalidate=True)
        if release.get('tag_name')!=tag(head) or release.get('draft') or release.get('prerelease'): raise ValueError('Committed reference release unavailable')
        assets=[a for a in release.get('assets',[]) if a.get('name')==ASSET]
        if len(assets)!=1 or not 0<assets[0]['size']<=135*1024**2: raise ValueError('Reference asset contract mismatch')
        asset=assets[0]; url=f"https://api.github.com/repos/do0405/price-lab-data/releases/assets/{int(asset['id'])}"
        partial=path.with_suffix('.working')
        try:
            headers={'Accept':'application/octet-stream','Cache-Control':'no-cache'}
            if os.environ.get('GITHUB_TOKEN'): headers['Authorization']='Bearer '+os.environ['GITHUB_TOKEN']
            with requests.get(url,headers=headers,timeout=(20,90),allow_redirects=False,stream=True) as response:
                if response.status_code==302:
                    target=response.headers.get('Location',''); u=urlsplit(target)
                    if u.scheme!='https' or u.hostname!='release-assets.githubusercontent.com' or u.username or u.password: raise ValueError('Unsafe reference asset redirect')
                elif response.status_code==200:
                    target=None
                    _save_response(response,partial,asset['size'])
                else: raise RuntimeError(f'Reference asset HTTP {response.status_code}')
            if target:
                with requests.get(target,timeout=(20,90),allow_redirects=False,stream=True) as response:
                    if response.status_code!=200: raise RuntimeError(f'Reference asset HTTP {response.status_code}')
                    _save_response(response,partial,asset['size'])
            payload=open_package(partial,key or os.environ.get('PRICE_DATA_KEY'))
            _check_head(payload,head); os.replace(partial,path)
        finally:
            if partial.exists(): partial.unlink()
    payload=open_package(path,key or os.environ.get('PRICE_DATA_KEY')); _check_head(payload,head)
    return payload


def _save_response(response,path,expected):
    total=0
    with path.open('wb') as stream:
        for chunk in response.iter_content(1024**2):
            total+=len(chunk)
            if total>expected: raise ValueError('Oversized reference download')
            stream.write(chunk)
    if total!=expected: raise ValueError('Truncated reference download')


def _check_head(payload,head):
    if payload['stream_id']!=s.STREAM or payload['sequence']!=head['sequence'] or package_digest(payload)!=head['package_sha256']: raise ValueError('Authenticated reference head mismatch')


def publish(payload,directory,owner,*,key=None):
    key=key or os.environ.get('PRICE_DATA_KEY'); digest=package_digest(payload)
    head=dict(sequence=payload['sequence'],package_sha256=digest); name=tag(head)
    path=Path(directory)/(name+'.pldelta'); path.parent.mkdir(parents=True,exist_ok=True)
    seal_package(payload,path,key); owner.guard(force=True)
    try: release=_api('GET','/releases/tags/'+name,revalidate=True)
    except RuntimeError as exc:
        if 'HTTP 404' not in str(exc): raise
        release=_api('POST','/releases',payload=dict(tag_name=name,target_commitish='main',name=name,draft=True,prerelease=False,body=s.canonical(head)))
    if release.get('tag_name')!=name: raise ValueError('Reference release identity mismatch')
    matching=[a for a in release.get('assets',[]) if a.get('name')==ASSET]
    if not matching:
        if not release.get('draft'): raise ValueError('Ready reference release has no asset')
        owner.guard(force=True)
        with path.open('rb') as stream:
            _api('POST',f"https://uploads.github.com/repos/do0405/price-lab-data/releases/{int(release['id'])}/assets?name={ASSET}",stream=stream)
    elif len(matching)!=1: raise ValueError('Duplicate reference assets')
    if release.get('draft'):
        _api('PATCH',f"/releases/{int(release['id'])}",payload=dict(draft=False,prerelease=False))
    # Verify uploaded bytes before committing a canonical head. Never reuse the
    # just-sealed local file as evidence of a successful remote upload.
    check=Path(directory)/'remote-check'; check.mkdir(exist_ok=True)
    verified=load(head,check,key=key)
    if package_digest(verified)!=digest: raise ValueError('Reference upload read-back mismatch')
    owner.commit_head(payload)
    for item in check.iterdir():
        if item.is_file() and item.name==name+'.pldelta': item.unlink()
    check.rmdir()
    return head


def initial_state(through):
    return dict(version='market-reference-state-v1',cursors={},seen={},checked={},cooldowns={},pending=[],requests={},through=through)


def plan(state, bootstrap, now):
    """Resume historical work, with a daily front slice for current snapshots."""
    today=now.date().isoformat(); through=min(bootstrap['through'],(now-timedelta(days=1)).date().isoformat())
    state['through']=max(state['through'],through)
    pending={s.digest(t):t for t in state['pending']}
    # Preserve old signed tasks, but do not keep retrying the retired catalog
    # endpoint. Its exact replacement is a different task identity.
    for key,old in list(pending.items()):
        if old['kind']=='SP_CATALOG':
            pending.pop(key)
            state['checked'][key]=dict(status='RETIRED_ENDPOINT_REPLACED')
            replacement=s.task('SP_ARCHIVE',old['entity'],day=old['requested_date'],page=old['page'])
            if s.digest(replacement) not in state['checked']: pending.setdefault(s.digest(replacement),replacement)
    if state['cursors'].get('lseg_count_contract',0)<2:
        for key,metadata in state['checked'].items():
            failed=metadata.get('task',{})
            if metadata.get('status')=='RESPONSE_CONTRACT' and failed.get('kind')=='LSEG_CATALOG':
                metadata.update(status='RECHECK_AFTER_COUNT_CONTRACT_FIX',retry_at=now.isoformat())
                pending.setdefault(key,failed)
        state['cursors']['lseg_count_contract']=2
    def add(t):
        key=s.digest(t)
        if key not in state['checked']: pending.setdefault(key,t)
    if state['cursors'].get('daily')!=today:
        current=[s.task('FUND_LATEST',f,day=today) for f in s.FUNDS]
        current += [s.task('KIND_INDUSTRY',day=today),s.task('RUSSELL_PAGE',day=today),s.task('SEC_MAP',day=today)]
        current += [s.task('FRED_INDEX',index,day=today) for index in ('SP500','NASDAQ100','NASDAQCOM')]
        current += [s.task('CBOE_INDEX',index,day=today) for index in ('VIX','VVIX')]
        current += [s.task('LSEG_CATALOG',str(now.year),day=today),s.task('SP_ARCHIVE',str(now.year),day=today)]
        current += [s.task('NASDAQ_NOTICE','https://ir.nasdaq.com/news-releases/news-release-details/annual-changes-nasdaq-100-indexr'+suffix) for suffix in ('-2','','-1','-3','-4','-5')]
        last=bootstrap['kr_sessions'][-1]
        current += [s.task('KRX_INDEX',family,last) for family in ('kospi','kosdaq','krx')]
        current += [s.task('KRX_MEMBERS',index,last) for index in ('KOSPI200','KOSDAQ150')]
        current += [s.task('KRX_SECTOR',market,last) for market in ('KOSPI','KOSDAQ')]
        pending={**{s.digest(t):t for t in current if s.digest(t) not in state['checked']},**pending}
        state['cursors']['daily']=today
    for key,metadata in state['checked'].items():
        if metadata.get('task') and metadata.get('retry_at','')<=now.isoformat():
            pending.setdefault(key,metadata['task'])
    # Top up all categories fairly; cursors are part of the signed checkpoint.
    issuers=list(dict.fromkeys(bootstrap['ciks']+state['cursors'].get('issuer_additions',[])))
    cycle=state['cursors'].setdefault('sec_cycle',str((now-timedelta(days=now.weekday())).date()))
    if state['cursors'].get('sec',0)>=len(issuers) and (now.date()-date.fromisoformat(cycle)).days>=7:
        state['cursors']['sec']=0; cycle=str((now-timedelta(days=now.weekday())).date()); state['cursors']['sec_cycle']=cycle
    categories=[('indices',bootstrap['kr_sessions']),('funds',bootstrap['us_month_ends']),('sec',issuers),('lseg',list(range(int(through[:4]),2009,-1))),('sp',list(range(int(through[:4]),2009,-1)))]
    kinds={'indices':{'KRX_INDEX'},'funds':{'FUND'},'sec':{'SEC_SUBMISSIONS','SEC_FACTS'},'lseg':{'LSEG_CATALOG'},'sp':{'SP_ARCHIVE'}}
    for name,values in categories:
        if sum(t['kind'] in kinds[name] for t in pending.values())>=10: continue
        cursor=state['cursors'].get(name,0)
        if cursor>=len(values): continue
        value=values[cursor]
        if name=='indices':
            for family in ('kospi','kosdaq','krx'): add(s.task('KRX_INDEX',family,value))
        elif name=='funds':
            for fund in s.FUNDS: add(s.task('FUND',fund,value))
        elif name=='sec':
            add(s.task('SEC_SUBMISSIONS',value,cycle)); add(s.task('SEC_FACTS',value,cycle))
        else: add(s.task('LSEG_CATALOG' if name=='lseg' else 'SP_ARCHIVE',str(value)))
        state['cursors'][name]=cursor+1
    state['pending']=list(pending.values())
    return s.validate_state(state)


class Fetcher:
    def __init__(self, *, credentials=None, guard=None):
        self.session=requests.Session(); self.credentials=credentials or os.environ; self.guard=guard
    def __call__(self, task):
        s.validate_task(task)
        if self.guard: self.guard()
        headers={'User-Agent':self.credentials.get('SEC_USER_AGENT') or 'PriceLab public reference research (https://github.com/do0405/price-lab-data)'}
        if task['kind']=='KRX_INDEX':
            value=self.credentials.get('KRX_API_KEY')
            if not value: return 401,{},b''
            headers['AUTH_KEY']=value
        options={'json':task['params']} if task['kind']=='LSEG_CATALOG' else {'data':task['params']} if task['method']=='POST' else {'params':task['params']}
        try:
            with self.session.request(task['method'],task['url'],headers=headers,timeout=(15,35),allow_redirects=False,stream=True,**options) as response:
                chunks=[]; total=0
                for chunk in response.iter_content(1024**2):
                    total+=len(chunk)
                    if total>s.MAX_BODY: raise ValueError('Oversized provider response')
                    chunks.append(chunk)
                return response.status_code,dict(response.headers),b''.join(chunks)
        except requests.RequestException: return 0,{},b''


def collect(state,bootstrap,*,max_requests=50,now=None,fetch=None,sleep=time.sleep,deadline=None):
    if type(max_requests) is not int or not 1<=max_requests<=100: raise ValueError('Bounded market requests required')
    fixed=now; now=now or datetime.now(timezone.utc); state=copy.deepcopy(s.validate_state(state)); records=[]; sent=0; unchanged=0
    # Legacy 404 endpoint failures are task-scoped. Keep authentication denials
    # intact; an absent catalog URL does not deny all notices on that host.
    for host,cooldown in list(state['cooldowns'].items()):
        if cooldown['reason']=='HTTP_404': state['cooldowns'].pop(host)
    fetch=fetch or Fetcher()
    for _ in range(max_requests):
        now=fixed or datetime.now(timezone.utc)
        if deadline is not None and time.monotonic()>=deadline-40: break
        plan(state,bootstrap,now)
        ready=[t for t in state['pending'] if state['cooldowns'].get(urlsplit(t['url']).hostname,{}).get('retry_at','')<=now.isoformat() and state['checked'].get(s.digest(t),{}).get('retry_at','')<=now.isoformat()]
        if not ready: break
        t=ready[0]; key=s.digest(t); host=urlsplit(t['url']).hostname
        status,headers,body=fetch(t); sent+=1; state['requests'][host]=state['requests'].get(host,0)+1
        reason=None
        if status==200:
            try: s.validate_response(t,body)
            except (ValueError,KeyError,TypeError,UnicodeError): reason='RESPONSE_CONTRACT'
        else: reason=f'HTTP_{status}' if status else 'TRANSPORT_UNAVAILABLE'
        if reason:
            delay=86400 if status in (400,401,403,404) or reason=='RESPONSE_CONTRACT' else 300
            if status==429:
                try: delay=max(60,min(86400,int(headers.get('Retry-After','300'))))
                except ValueError: delay=300
            state['cooldowns'][host]=dict(reason=reason,retry_at=(now+timedelta(seconds=delay)).isoformat())
            # A missing historical snapshot does not block current snapshots
            # from the same fund host. Its task remains a dated source gap.
            if status in (400,404) or (status==200 and reason=='RESPONSE_CONTRACT' and not body.lstrip().lower().startswith((b'<html',b'<!doctype'))):
                state['cooldowns'].pop(host,None)
                state['checked'][key]=dict(status='HISTORY_UNAVAILABLE' if t['kind']=='FUND' else reason,retry_at=(now+timedelta(days=30 if t['kind']=='FUND' else 1)).isoformat(),task=t)
                state['pending']=[x for x in state['pending'] if s.digest(x)!=key]
        else:
            sha=hashlib.sha256(body).hexdigest()
            if sha not in state['seen'].values():
                retrieved=fixed or datetime.now(timezone.utc)
                records.append(dict(task=t,retrieved_at=retrieved.isoformat(),body_base64=base64.b64encode(body).decode(),body_sha256=sha))
            else: unchanged+=1
            state['seen'][key]=sha; state['checked'][key]=dict(status='COLLECTED',checked_at=now.isoformat())
            if t['kind']=='SEC_MAP':
                value=json.loads(body); position=value['fields'].index('cik')
                observed=[str(int(row[position])).zfill(10) for row in value['data']]
                prior=state['cursors'].get('issuer_additions',[])
                state['cursors']['issuer_additions']=list(dict.fromkeys(prior+observed))
            state['pending']=[x for x in state['pending'] if s.digest(x)!=key]
            existing={s.digest(x) for x in state['pending']}
            for child in s.discovered(t,body):
                child_key=s.digest(child)
                if child_key not in existing and child_key not in state['seen']:
                    state['pending'].append(child); existing.add(child_key)
        if sum(len(r['body_base64']) for r in records)>60*1024**2: break
        sleep(1)
    return dict(state=state,records=records,requests=sent,unchanged=unchanged)


def run(bootstrap, directory, *, seconds=600,max_requests=50,max_batches=10,role='cloud',key=None,credentials=None):
    key=key or os.environ.get('PRICE_DATA_KEY'); directory=Path(directory); directory.mkdir(parents=True,exist_ok=True)
    owner=ResearchOwner(role,key=key,namespace=NAMESPACE)
    control=read_control(key=key,namespace=NAMESPACE)
    if not owner.acquire(initial_head=control['head'] if control else None,anchor_sha256=s.ANCHOR): return dict(status='WAITING_OTHER_COLLECTOR')
    end=time.monotonic()+seconds; summaries=[]
    try:
        prior=load(owner.head,directory,key=key) if owner.head else None
        state=prior['state'] if prior else initial_state(bootstrap['through'])
        for _ in range(max_batches):
            if time.monotonic()>=end-40: break
            owner.guard(force=True)
            result=collect(state,bootstrap,max_requests=max_requests,fetch=Fetcher(credentials=credentials,guard=owner.guard),deadline=end)
            if not result['requests']: break
            payload=dict(schema='research-delta-v1',stream_id=s.STREAM,sequence=prior['sequence']+1 if prior else 1,
                         previous_package_sha256=package_digest(prior) if prior else None,bootstrap_anchor_sha256=s.ANCHOR,
                         collected_at=datetime.now(timezone.utc).isoformat(),records=result['records'],state=result['state'])
            head=publish(payload,directory,owner,key=key); prior=payload; state=result['state']
            summary=dict(**head,records=len(result['records']),requests=result['requests'],cooldowns=state['cooldowns'],pending=len(state['pending']))
            summaries.append(summary); print(s.canonical(dict(stage='market_reference_batch',**summary)),flush=True)
    finally: owner.release()
    return dict(status='COLLECTED' if summaries else 'IDLE',batches=summaries,cloud_database=False)
