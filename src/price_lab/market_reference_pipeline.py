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
import re
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


def _config_targets(config):
    version=config.get('version')
    if type(version) is not int or version not in (1,2,3,4,5,6):raise ValueError('Market bootstrap contract mismatch')
    expected={'version','ciks'}|({'research_targets'} if version>=2 else set())|({'depositary_attachment_ids'} if version>=3 else set())|({'jpm_document_ids'} if version>=4 else set())
    if version>=5:expected|={'capitalization_ciks','sec_primary_documents'}
    if version>=6:expected|={'price_symbols'}
    if set(config)!=expected:
        raise ValueError('Market bootstrap contract mismatch')
    symbols=config.get('price_symbols',[])
    if not isinstance(symbols,list) or len(symbols)>1000 or len(set(symbols))!=len(symbols):raise ValueError('Bounded unique price symbols required')
    for symbol in symbols:s.task('YAHOO_PRICE',symbol,'2026-01-01')
    priority=config.get('capitalization_ciks',[])
    if not isinstance(priority,list) or len(priority)>500 or len(set(priority))!=len(priority) or any(not isinstance(c,str) or not re.fullmatch('[0-9]{10}',c) for c in priority):
        raise ValueError('Bounded capitalization CIKs required')
    primary=config.get('sec_primary_documents',[])
    if not isinstance(primary,list) or len(primary)>2000:raise ValueError('Bounded primary document targets required')
    primary_keys=[]
    for item in primary:
        if not isinstance(item,dict) or set(item)!={'entity','filed_date'}:raise ValueError('Exact primary document target required')
        t=s.task('SEC_PRIMARY',item['entity'],item['filed_date']);primary_keys.append(s.digest(t))
    if len(set(primary_keys))!=len(primary_keys):raise ValueError('Duplicate primary document targets')
    ids=config.get('depositary_attachment_ids',[])
    if not isinstance(ids,list) or len(ids)>5000 or any(not isinstance(v,str) or not re.fullmatch('[1-9][0-9]{0,7}',v) for v in ids) or len(set(ids))!=len(ids):
        raise ValueError('Bounded unique depositary attachment IDs required')
    documents=config.get('jpm_document_ids',[])
    if not isinstance(documents,list) or len(documents)>3000 or len(set(documents))!=len(documents):raise ValueError('Bounded unique JPM documents required')
    for document in documents:s.task('JPM_DOCUMENT',document)
    targets=config.get('research_targets',[])
    if not isinstance(targets,list) or len(targets)>200:raise ValueError('Bounded research snapshot targets required')
    seen=set()
    for row in targets:
        if (not isinstance(row,dict) or set(row)!={'fund','snapshot_date'} or
                not isinstance(row['fund'],str) or row['fund'] not in s.FUNDS or
                not isinstance(row['snapshot_date'],str)):
            raise ValueError('Public fund/date target required')
        if date.fromisoformat(row['snapshot_date']).isoformat()!=row['snapshot_date']:
            raise ValueError('Canonical target date required')
        key=(row['fund'],row['snapshot_date'])
        if key in seen:raise ValueError('Duplicate research target')
        seen.add(key)
    return targets


def enqueue_price_symbols(config_path,symbols):
    path=Path(config_path);config=json.loads(path.read_text(encoding='utf-8'));_config_targets(config)
    merged=list(dict.fromkeys([*config.get('price_symbols',[]),*symbols]))
    updated={**config,'version':6,'price_symbols':merged}
    for field in ('research_targets','depositary_attachment_ids','jpm_document_ids','capitalization_ciks','sec_primary_documents'):updated.setdefault(field,[])
    _config_targets(updated)
    if updated==config:return dict(status='UNCHANGED',symbols=len(merged),added=0)
    _atomic(path,updated);return dict(status='UPDATED',symbols=len(merged),added=len(merged)-len(config.get('price_symbols',[])))


def enqueue_ciks(config_path,ciks):
    """Append official issuer seeds without shifting the persisted scan cursor."""
    path=Path(config_path);config=json.loads(path.read_text(encoding='utf-8'))
    _config_targets(config)
    old=config.get('ciks')
    for values in (old,ciks):
        if not isinstance(values,list) or any(not isinstance(c,str) or not re.fullmatch('[0-9]{10}',c) for c in values):
            raise ValueError('Official CIK seed required')
    merged=list(dict.fromkeys([*old,*ciks]))
    if len(merged)>50000:raise ValueError('Bounded official CIK seed required')
    if merged==old:return dict(status='UNCHANGED',issuers=len(old),added=0)
    _atomic(path,{**config,'ciks':merged})
    return dict(status='UPDATED',issuers=len(merged),added=len(merged)-len(old))


def enqueue_targets(config_path,targets):
    """Caller holds its planning lock; preserve public issuer seeds and tasks."""
    path=Path(config_path); config=json.loads(path.read_text(encoding='utf-8'))
    old=_config_targets(config)
    merged={(t['fund'],t['snapshot_date']):t for t in old}
    for row in targets:
        _config_targets(dict(version=2,ciks=config['ciks'],research_targets=[row]))
        merged[(row['fund'],row['snapshot_date'])]=row
    combined=sorted(merged.values(),key=lambda t:(t['snapshot_date'],t['fund']))
    if combined==old:return dict(status='UNCHANGED',targets=len(old))
    updated={**config,'version':max(2,config['version']),'research_targets':combined}
    _config_targets(updated);_atomic(path,updated)
    return dict(status='UPDATED',targets=len(combined))


def enqueue_attachments(config_path,ids):
    """Caller holds the planning lock; discoveries must come from owned raw."""
    path=Path(config_path);config=json.loads(path.read_text(encoding='utf-8'));targets=_config_targets(config)
    if not isinstance(ids,list) or any(not isinstance(v,str) or not re.fullmatch('[1-9][0-9]{0,7}',v) for v in ids):
        raise ValueError('Exact public attachment IDs required')
    merged=sorted(set(config.get('depositary_attachment_ids',[]))|set(ids),key=int)
    updated={**config,'version':max(3,config['version']),'research_targets':targets,'depositary_attachment_ids':merged}
    _config_targets(updated)
    if updated==config:return dict(status='UNCHANGED',attachments=len(merged))
    _atomic(path,updated)
    return dict(status='UPDATED',attachments=len(merged),added=len(merged)-len(config.get('depositary_attachment_ids',[])))


def enqueue_jpm_documents(config_path,ids):
    path=Path(config_path);config=json.loads(path.read_text(encoding='utf-8'));targets=_config_targets(config)
    if not isinstance(ids,list):raise ValueError('Public document list required')
    for document in ids:s.task('JPM_DOCUMENT',document)
    merged=sorted(set(config.get('jpm_document_ids',[]))|set(ids))
    updated={**config,'version':max(4,config['version']),'research_targets':targets,
        'depositary_attachment_ids':config.get('depositary_attachment_ids',[]),'jpm_document_ids':merged}
    _config_targets(updated)
    if updated==config:return dict(status='UNCHANGED',documents=len(merged))
    _atomic(path,updated)
    return dict(status='UPDATED',documents=len(merged),added=len(merged)-len(config.get('jpm_document_ids',[])))


def enqueue_capitalization_targets(config_path,ciks,documents=(),*,replace_priority=False):
    """A small daily priority lane; do not rewind the 16k-issuer backfill."""
    path=Path(config_path);config=json.loads(path.read_text(encoding='utf-8'));_config_targets(config)
    prior=config.get('capitalization_ciks',[])
    priority=sorted(set(ciks) if replace_priority else set(prior)|set(ciks))
    combined={(d['entity'],d['filed_date']):d for d in config.get('sec_primary_documents',[])}
    for document in documents:combined[(document['entity'],document['filed_date'])]=document
    updated={**config,'version':max(5,config['version']),'research_targets':config.get('research_targets',[]),
        'depositary_attachment_ids':config.get('depositary_attachment_ids',[]),'jpm_document_ids':config.get('jpm_document_ids',[]),
        'capitalization_ciks':priority,'sec_primary_documents':sorted(combined.values(),key=lambda x:(x['filed_date'],x['entity']))}
    if replace_priority:
        # A displaced priority target still belongs to the resumable general
        # scan. Preserve its prefix so the saved position never shifts.
        updated['ciks']=list(dict.fromkeys([*config['ciks'],*prior,*priority]))
    _config_targets(updated)
    if updated==config:return dict(status='UNCHANGED',priority_issuers=len(priority),primary_documents=len(combined))
    _atomic(path,updated)
    return dict(status='UPDATED',priority_issuers=len(priority),primary_documents=len(combined))


def bootstrap(config_path,now=None):
    import exchange_calendars as xcals
    import re
    from zoneinfo import ZoneInfo
    now=now or datetime.now(timezone.utc)
    through=(now.astimezone(ZoneInfo('Asia/Seoul')).date()-timedelta(days=1)).isoformat()
    config=json.loads(Path(config_path).read_text(encoding='utf-8'))
    targets=_config_targets(config)
    if any(t['snapshot_date']>through for t in targets):raise ValueError('Future research snapshot target')
    if not isinstance(config['ciks'],list) or len(config['ciks'])>50000 or any(not isinstance(c,str) or not re.fullmatch('[0-9]{10}',c) for c in config['ciks']): raise ValueError('Official CIK seed required')
    kr=[str(x.date()) for x in xcals.get_calendar('XKRX',start='2010-01-01',end=through).sessions]
    us=[str(x.date()) for x in xcals.get_calendar('XNYS',start='2010-01-01',end=through).sessions]
    months={d[:7]:d for d in us if d[:7]<through[:7]}
    return dict(through=through,kr_sessions=kr,us_month_ends=list(months.values()),
                ciks=list(dict.fromkeys(config['ciks'])),research_targets=targets,
                depositary_attachment_ids=config.get('depositary_attachment_ids',[]),jpm_document_ids=config.get('jpm_document_ids',[]),
                capitalization_ciks=config.get('capitalization_ciks',[]),sec_primary_documents=config.get('sec_primary_documents',[]),price_symbols=config.get('price_symbols',[]))


def _atomic(path, value):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+'.working')
    temporary.write_text(s.canonical(value),encoding='utf-8'); os.replace(temporary,path)


def tag(head):
    from .research_ownership import _head
    _head(head)
    if head is None: raise ValueError('Package head required')
    return f"delta-market-{head['sequence']:012d}-{head['package_sha256']}"


def load(head, directory, *, key=None, token=None):
    directory=Path(directory); directory.mkdir(parents=True,exist_ok=True)
    path=directory/(tag(head)+'.pldelta')
    if not path.exists():
        release=_api('GET','/releases/tags/'+tag(head),revalidate=True,token=token)
        if release.get('tag_name')!=tag(head) or release.get('draft') or release.get('prerelease'): raise ValueError('Committed reference release unavailable')
        assets=[a for a in release.get('assets',[]) if a.get('name')==ASSET]
        if len(assets)!=1 or not 0<assets[0]['size']<=135*1024**2: raise ValueError('Reference asset contract mismatch')
        asset=assets[0]; url=f"https://api.github.com/repos/do0405/price-lab-data/releases/assets/{int(asset['id'])}"
        partial=path.with_suffix('.working')
        try:
            headers={'Accept':'application/octet-stream','Cache-Control':'no-cache'}
            credential=token or os.environ.get('GITHUB_TOKEN')
            if credential: headers['Authorization']='Bearer '+credential
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
    if state['cursors'].get('jpm_catalog_contract',0)<3:
        for key,metadata in state['checked'].items():
            t=metadata.get('task',{})
            if metadata.get('status')=='RESPONSE_CONTRACT' and t.get('kind') in ('JPM_UNIVERSE','JPM_ACTIONS'):
                metadata.update(status='RECHECK_AFTER_JPM_REPORTED_IDENTIFIER_AND_MISSING_ACTION_FIX',retry_at=now.isoformat())
                pending.setdefault(key,t)
        state['cursors']['jpm_catalog_contract']=3
    if state['cursors'].get('lseg_weight_encoding_contract',0)<1:
        catalog=s.task('LSEG_WEIGHT_CATALOG');key=s.digest(catalog);metadata=state['checked'].get(key,{})
        if metadata.get('status')=='RESPONSE_CONTRACT':
            metadata.update(status='RECHECK_AFTER_WEIGHT_ENCODING_FIX',retry_at=now.isoformat())
            pending.setdefault(key,catalog)
        state['cursors']['lseg_weight_encoding_contract']=1
    # Introduce new fixed public sources inside an already-running window too.
    seeds=[s.task('JPM_UNIVERSE'),s.task('JPM_ACTIONS'),s.task('CITI_CATALOG'),s.task('LSEG_WEIGHT_CATALOG'),s.task('RUSSELL_IPO_PAGE')]
    seeds += [s.task('LSEG_WEIGHTS',code) for code in ('US1000','US2000','US3000')]
    seeds += [s.task('CITI_NOTICE',code) for code in ('513','2621','1720')]
    seeds += [s.task('BNY_NOTICE','AD2000235')]
    seeds += [s.task('CITI_ATTACHMENT',id) for id in bootstrap.get('depositary_attachment_ids',[])]
    seeds += [s.task('JPM_DOCUMENT',id) for id in bootstrap.get('jpm_document_ids',[])]
    seeds += [s.task('RUSSELL_PDF',url) for url in s.RUSSELL_ARCHIVE_REPORTS]
    # Verified public files remain useful after they disappear from the
    # current-quarter page. Reuse their original tasks, never the filename
    # quarter as a substitute for the PDF's declared effective date/year.
    seeds += [s.task('RUSSELL_IPO_PDF',index,page=q) for index in ('1000','2000','3000') for q in (1,4)]
    seeds += [s.task('FINRA_DAILY',day=(date.fromisoformat(through)-timedelta(days=n)).isoformat())
              for n in range(5) if (date.fromisoformat(through)-timedelta(days=n)).isoformat()>='2018-12-18']
    fresh={s.digest(t):t for t in seeds if s.digest(t) not in state['checked'] and s.digest(t) not in pending}
    pending={**fresh,**pending}
    factsheet=s.task('NASDAQ_FACTSHEET','NDX');factsheet_key=s.digest(factsheet)
    if factsheet_key not in state['checked'] and factsheet_key not in pending:
        pending={factsheet_key:factsheet,**pending}
    weights=s.task('NASDAQ_WEIGHTS','NDX');weights_key=s.digest(weights)
    if weights_key not in state['checked'] and weights_key not in pending:
        pending={weights_key:weights,**pending}
    requested={}
    for target in bootstrap.get('research_targets',[]):
        t=s.task('FUND',target['fund'],target['snapshot_date']);key=s.digest(t)
        if key not in state['checked'] and key not in pending:requested[key]=t
    pending={**requested,**pending}
    # The newest index session may still be unpublished. Keep the preceding
    # completed week in scope instead of leaving all modern levels behind the
    # chronological 2010 backfill. Empty/failed tasks keep their own backoff.
    recent_indices={}
    for day in bootstrap['kr_sessions'][-5:]:
        for family in ('kospi','kosdaq','krx'):
            t=s.task('KRX_INDEX',family,day);key=s.digest(t)
            if key not in state['checked'] and key not in pending:recent_indices[key]=t
    pending={**recent_indices,**pending}
    # Fill the recent dated research window before the long historical queue.
    # The same stable task keys are reused by chronological backfill, so a
    # completed snapshot is neither downloaded nor stored twice.
    recent={}
    for day in reversed(bootstrap['us_month_ends'][-3:]):
        for fund in s.FUNDS:
            t=s.task('FUND',fund,day); key=s.digest(t)
            if key not in state['checked'] and key not in pending:recent[key]=t
    pending={**recent,**pending}
    # Immutable, observed official reports get one acquisition. A different
    # public host does not clear or bypass an IR host's denial/cooldown.
    for url in s.NASDAQ_REPORTS:
        report=s.task('NASDAQ_PDF',url); key=s.digest(report)
        if key not in state['checked'] and key not in pending:pending={key:report,**pending}
    refresh_kinds={'FUND_LATEST','KIND_INDUSTRY','RUSSELL_PAGE','FRED_INDEX','CBOE_INDEX','SEC_MAP','NASDAQ_FACTSHEET','NASDAQ_WEIGHTS','NASDAQ_DIRECTORY','NASDAQ_CAPS'}
    if not state['cursors'].get('us_market_snapshots_v1'):
        for t in [*[s.task('NASDAQ_DIRECTORY',feed) for feed in s.NASDAQ_DIRECTORIES],s.task('NASDAQ_CAPS')]:
            if s.digest(t) not in state['checked']:pending.setdefault(s.digest(t),t)
        state['cursors']['us_market_snapshots_v1']=1
    for key,old in list(pending.items()):
        if old['kind'] in refresh_kinds and old['requested_date']:
            pending.pop(key)
            replacement=s.task(old['kind'],old['entity'],page=old['page'])
            pending.setdefault(s.digest(replacement),replacement)
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
    if state['cursors'].get('sec_fund_map_contract',0)<1:
        # Official classes may be registered before a ticker is assigned.
        # Retry only the pre-fix schema rejection, preserving every other gap.
        for metadata in state['checked'].values():
            failed=metadata.get('task',{})
            if metadata.get('status')=='RESPONSE_CONTRACT' and failed.get('kind')=='SEC_FUND_MAP':
                metadata.update(status='RECHECK_AFTER_FUND_CLASS_CONTRACT_FIX',retry_at=now.isoformat())
                pending.setdefault(s.digest(failed),failed)
        state['cursors']['sec_fund_map_contract']=1
    def add(t,refresh=False):
        key=s.digest(t)
        checked=state['checked'].get(key,{})
        interval=1 if t['kind']=='SEC_FUND_MAP' else 7
        if key not in state['checked'] or (refresh and checked.get('checked_at','')<(now-timedelta(days=interval)).isoformat()): pending.setdefault(key,t)
    add(s.task('SEC_CIK_NAMES'),refresh=True)
    add(s.task('SEC_FUND_MAP'),refresh=True)
    window=today+f'T{now.hour//6*6:02d}'
    if state['cursors'].get('current_window')!=window:
        current=[s.task('FUND_LATEST',f) for f in s.FUNDS]
        current += [s.task('KIND_INDUSTRY'),s.task('RUSSELL_PAGE'),s.task('SEC_MAP'),s.task('SEC_FUND_MAP')]
        current += [s.task('NASDAQ_DIRECTORY',feed) for feed in s.NASDAQ_DIRECTORIES]
        current += [s.task('NASDAQ_CAPS')]
        current += [s.task('FRED_INDEX',index) for index in ('SP500','NASDAQ100','NASDAQCOM')]
        current += [s.task('CBOE_INDEX',index) for index in ('VIX','VVIX')]
        current += [s.task('NASDAQ_FACTSHEET','NDX')]
        current += [s.task('NASDAQ_WEIGHTS','NDX')]
        current += [s.task('CITI_CATALOG'),s.task('LSEG_WEIGHT_CATALOG'),s.task('RUSSELL_IPO_PAGE')]
        current += [s.task('JPM_UNIVERSE'),s.task('JPM_ACTIONS')]
        current += [s.task('LSEG_WEIGHTS',code) for code in s.LSEG_WEIGHT_INDICES]
        current += [s.task('FINRA_DAILY',day=(date.fromisoformat(through)-timedelta(days=n)).isoformat())
                    for n in range(5) if (date.fromisoformat(through)-timedelta(days=n)).isoformat()>='2018-12-18']
        current += [s.task('LSEG_CATALOG',str(now.year)),s.task('SP_ARCHIVE',str(now.year))]
        current += [s.task('NASDAQ_NOTICE','https://ir.nasdaq.com/news-releases/news-release-details/annual-changes-nasdaq-100-indexr'+suffix) for suffix in ('-2','','-1','-3','-4','-5')]
        last=bootstrap['kr_sessions'][-1]
        current += [s.task('KRX_INDEX',family,last) for family in ('kospi','kosdaq','krx')]
        current += [s.task('KRX_MEMBERS',index,last) for index in ('KOSPI200','KOSDAQ150')]
        current += [s.task('KRX_SECTOR',market,last) for market in ('KOSPI','KOSDAQ')]
        due={}
        for t in current:
            key=s.digest(t); stamp=state['checked'].get(key,{}).get('checked_at')
            interval=timedelta(days=1) if t['kind'] in ('CITI_CATALOG','JPM_UNIVERSE','JPM_ACTIONS') else timedelta(days=7) if t['kind'] in ('LSEG_WEIGHTS','RUSSELL_IPO_PAGE') else timedelta(hours=6)
            if stamp is None or datetime.fromisoformat(stamp)<=now-interval: due[key]=t
        pending={**due,**pending}
        state['cursors']['daily']=today
        state['cursors']['current_window']=window
    for key,metadata in state['checked'].items():
        if metadata.get('task') and metadata.get('retry_at','')<=now.isoformat():
            pending.setdefault(key,metadata['task'])
    # Top up all categories fairly; cursors are part of the signed checkpoint.
    issuers=list(dict.fromkeys(bootstrap['ciks']+state['cursors'].get('issuer_additions',[])))
    cycle=state['cursors'].setdefault('sec_cycle',str((now-timedelta(days=now.weekday())).date()))
    if state['cursors'].get('sec',0)>=len(issuers) and (now.date()-date.fromisoformat(cycle)).days>=7:
        state['cursors']['sec']=0; cycle=str((now-timedelta(days=now.weekday())).date()); state['cursors']['sec_cycle']=cycle
    categories=[('indices',bootstrap['kr_sessions']),('funds',bootstrap['us_month_ends']),('sec',issuers),('lseg',list(range(int(through[:4]),2009,-1))),('sp',list(range(int(through[:4]),2009,-1)))]
    categories.append(('finra',range(max(0,(date.fromisoformat(through)-date(2018,12,18)).days+1))))
    kinds={'indices':{'KRX_INDEX'},'funds':{'FUND'},'sec':{'SEC_SUBMISSIONS','SEC_FACTS'},'lseg':{'LSEG_CATALOG'},'sp':{'SP_ARCHIVE'}}
    kinds['finra']={'FINRA_DAILY'}
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
            add(s.task('SEC_SUBMISSIONS',value),refresh=True); add(s.task('SEC_FACTS',value),refresh=True)
        elif name=='finra':add(s.task('FINRA_DAILY',day=(date.fromisoformat(through)-timedelta(days=value)).isoformat()))
        else: add(s.task('LSEG_CATALOG' if name=='lseg' else 'SP_ARCHIVE',str(value)))
        state['cursors'][name]=cursor+1
    priority=set(bootstrap.get('capitalization_ciks',[]));urgent={}
    for item in bootstrap.get('sec_primary_documents',[]):
        t=s.task('SEC_PRIMARY',item['entity'],item['filed_date']);key=s.digest(t)
        if key not in state['checked']:pending.setdefault(key,t)
    for cik in sorted(priority):
        for kind in ('SEC_SUBMISSIONS','SEC_FACTS'):
            t=s.task(kind,cik);key=s.digest(t);stamp=state['checked'].get(key,{}).get('checked_at')
            if stamp is None or datetime.fromisoformat(stamp)<=now-timedelta(days=1):urgent[key]=t
    primary=dict(sorted(((k,t) for k,t in pending.items() if t['kind']=='SEC_PRIMARY'),
                        key=lambda item:item[1]['entity'].split(':',1)[0] not in priority))
    pending_quotes={t['entity'] for t in pending.values() if t['kind']=='YAHOO_PRICE'}
    for symbol in bootstrap.get('price_symbols',[]):
        stamp=state['cursors'].get('price_quote_checks',{}).get(symbol,{})
        if symbol not in pending_quotes and stamp.get('retry_at','')<=now.isoformat():
            t=s.task('YAHOO_PRICE',symbol,through);key=s.digest(t)
            if key not in state['checked']:pending.setdefault(key,t)
        primary_quote=state['cursors'].get('price_quote_checks',{}).get(symbol,{})
        secondary=state['cursors'].get('secondary_price_quote_checks',{}).get(symbol,{})
        if primary_quote.get('status') in ('HTTP_404','RESPONSE_CONTRACT') and secondary.get('retry_at','')<=now.isoformat():
            t=s.task('STOCKANALYSIS_PRICE',symbol,through);key=s.digest(t)
            if key not in state['checked']:pending.setdefault(key,t)
    catalogs={k:t for k,t in pending.items() if t['kind']=='SEC_CIK_NAMES'}
    state['pending']=list({**catalogs,**primary,**urgent,**pending}.values())
    return s.validate_state(state)


class Fetcher:
    def __init__(self, *, credentials=None, guard=None):
        self.session=requests.Session(); self.credentials=credentials or os.environ; self.guard=guard
    def __call__(self, task):
        s.validate_task(task)
        if self.guard: self.guard()
        headers={'User-Agent':'PriceLab public reference research (https://github.com/do0405/price-lab-data)'}
        if task['kind'].startswith('SEC_'):
            identity=s.sec_request_identity(self.credentials.get('SEC_USER_AGENT') or os.environ.get('SEC_USER_AGENT'))
            if identity is None:return 0,{'X-PriceLab-Reason':'SEC_IDENTITY_NOT_CONFIGURED'},b''
            headers.update({'User-Agent':identity,'Accept':'application/json'})
            if task['kind']=='SEC_PRIMARY':headers['Accept']='text/html,application/xhtml+xml,application/xml'
        if task['kind']=='NASDAQ_WEIGHTS':
            # The public PDF endpoint needs ordinary document Accept headers.
            # No cookies, login session or authorization is sent to this host.
            headers={'User-Agent':'Mozilla/5.0','Accept':'application/pdf','Accept-Language':'en-US,en;q=0.9'}
        if task['kind']=='NASDAQ_CAPS':
            headers={'User-Agent':'Mozilla/5.0','Accept':'application/json',
                'Origin':'https://www.nasdaq.com',
                'Referer':'https://www.nasdaq.com/market-activity/stocks/screener',
                'Accept-Language':'en-US,en;q=0.9'}
        if task['kind']=='FINRA_DAILY':headers['Accept']='application/json'
        if task['kind']=='KRX_INDEX':
            value=self.credentials.get('KRX_API_KEY')
            if not value: return 401,{},b''
            headers['AUTH_KEY']=value
        options={'json':task['params']} if task['kind'] in ('LSEG_CATALOG','FINRA_DAILY') else {'data':task['params']} if task['method']=='POST' else {'params':task['params']}
        try:
            with self.session.request(task['method'],task['url'],headers=headers,timeout=(15,35),allow_redirects=False,stream=True,**options) as response:
                chunks=[]; total=0
                for chunk in response.iter_content(1024**2):
                    total+=len(chunk)
                    if total>s.response_limit(task):
                        # Close this stream without losing the already fetched
                        # records or preventing other documents from collecting.
                        return 0,{'X-PriceLab-Reason':'RESPONSE_TOO_LARGE'},b''
                    chunks.append(chunk)
                return response.status_code,dict(response.headers),b''.join(chunks)
        except requests.RequestException as exc:
            # Exception messages can contain proxy credentials or contact data.
            reason=('TRANSPORT_READ_TIMEOUT' if isinstance(exc,requests.exceptions.ReadTimeout) else
                'TRANSPORT_CONNECT_TIMEOUT' if isinstance(exc,requests.exceptions.ConnectTimeout) else
                'TRANSPORT_TLS_ERROR' if isinstance(exc,requests.exceptions.SSLError) else
                'TRANSPORT_CONNECTION_ERROR' if isinstance(exc,requests.exceptions.ConnectionError) else
                'TRANSPORT_UNAVAILABLE')
            return 0,{'X-PriceLab-Reason':reason},b''


def _reserved_reference(ready, cursor):
    """Rotate the existing fifth request slot without bypassing eligibility."""
    others=[t for t in ready if t['kind'] not in ('CITI_NOTICE','CITI_ATTACHMENT')]
    def lane(t):
        kind=t['kind']
        if kind in ('YAHOO_PRICE','STOCKANALYSIS_PRICE'):return 4
        if kind.startswith('SEC_'):return 0
        if kind=='FINRA_DAILY':return 2
        if kind.startswith(('JPM_','CITI_','BNY_')):return 3
        return 1
    for offset in range(5):
        selected=(cursor+offset)%5
        candidate=next((t for t in others if lane(t)==selected),None)
        if candidate is not None:return candidate,(selected+1)%5
    return ready[0],cursor


def collect(state,bootstrap,*,max_requests=50,now=None,fetch=None,sleep=time.sleep,deadline=None,source_kinds=None,recover_sec_identity=False):
    if type(max_requests) is not int or not 1<=max_requests<=100: raise ValueError('Bounded market requests required')
    if source_kinds is not None and (not isinstance(source_kinds,(list,tuple)) or not 1<=len(source_kinds)<=50
            or any(not isinstance(kind,str) or not re.fullmatch(r'[A-Z_]{1,50}',kind) for kind in source_kinds)):
        raise ValueError('Bounded source kind selection required')
    fixed=now; now=now or datetime.now(timezone.utc); state=copy.deepcopy(s.validate_state(state)); records=[]; sent=0; unchanged=0
    pending_keys={s.digest(t) for t in state['pending']}
    contexts=state['cursors'].setdefault('primary_filing_contexts',{})
    for k in list(contexts):
        if k not in pending_keys:contexts.pop(k)
    # Legacy 404 endpoint failures are task-scoped. Keep authentication denials
    # intact; an absent catalog URL does not deny all notices on that host.
    for host,cooldown in list(state['cooldowns'].items()):
        if cooldown['reason']=='HTTP_404': state['cooldowns'].pop(host)
        elif host=='api.finra.org' and cooldown['reason']=='HTTP_204':state['cooldowns'].pop(host)
    fetch=fetch or Fetcher()
    recovery=[s.task('SEC_SUBMISSIONS','0000320193'),s.task('SEC_MAP')] if recover_sec_identity else []
    for _ in range(max_requests):
        now=fixed or datetime.now(timezone.utc)
        if deadline is not None and time.monotonic()>=deadline-40: break
        plan(state,bootstrap,now)
        ready=[t for t in state['pending'] if state['cooldowns'].get(urlsplit(t['url']).hostname,{}).get('retry_at','')<=now.isoformat() and state['checked'].get(s.digest(t),{}).get('retry_at','')<=now.isoformat()]
        if source_kinds is not None:ready=[t for t in ready if t['kind'] in source_kinds]
        canary=recovery.pop(0) if recovery else None
        if not ready and canary is None: break
        t=canary or ready[0]
        # Persist both positions in the signed checkpoint: even one-request
        # runs must reach the reserved slot. An older non-ADR backlog must
        # not monopolize it while SEC, OTC or depositary evidence is ready.
        slot=state['cursors'].get('reference_request_slot',0)
        if canary is None and slot==4:
            t,position=_reserved_reference(ready,state['cursors'].get('reference_other_lane',0))
            state['cursors']['reference_other_lane']=position
        elif t['kind'] in ('CITI_NOTICE','CITI_ATTACHMENT'):
            preferred='CITI_NOTICE' if slot==0 else 'CITI_ATTACHMENT'
            t=next((candidate for candidate in ready if candidate['kind']==preferred),t)
        key=s.digest(t); host=urlsplit(t['url']).hostname
        status,headers,body=fetch(t); sent+=1; state['requests'][host]=state['requests'].get(host,0)+1
        state['cursors']['reference_request_slot']=(slot+1)%5
        empty_finra=t['kind']=='FINRA_DAILY' and status==204 and not body
        if empty_finra:status,body=200,b'[]'
        reason=None
        if status==200:
            try: s.validate_response(t,body)
            except (ValueError,KeyError,TypeError,UnicodeError): reason='RESPONSE_CONTRACT'
        else:
            diagnostic=headers.get('X-PriceLab-Reason')
            safe={'RESPONSE_TOO_LARGE','SEC_IDENTITY_NOT_CONFIGURED','TRANSPORT_READ_TIMEOUT','TRANSPORT_CONNECT_TIMEOUT',
                'TRANSPORT_TLS_ERROR','TRANSPORT_CONNECTION_ERROR'}
            reason=f'HTTP_{status}' if status else diagnostic if diagnostic in safe else 'TRANSPORT_UNAVAILABLE'
        if reason=='RESPONSE_TOO_LARGE':
            state['checked'][key]=dict(status=reason,retry_at=(now+timedelta(days=30)).isoformat(),task=t)
        elif reason:
            delay=86400 if status in (400,401,403,404) or reason in ('RESPONSE_CONTRACT','SEC_IDENTITY_NOT_CONFIGURED') else 300
            if status==429:
                try: delay=max(60,min(86400,int(headers.get('Retry-After','300'))))
                except ValueError: delay=300
            state['cooldowns'][host]=dict(reason=reason,retry_at=(now+timedelta(seconds=delay)).isoformat())
            if t['kind'].startswith('SEC_') and status in (403,429):
                state['cooldowns'][host]['diagnostic']=s.sec_failure_diagnostic(status,headers,body)
            # A missing historical snapshot does not block current snapshots
            # from the same fund host. Its task remains a dated source gap.
            secondary_absent=t['kind']=='STOCKANALYSIS_PRICE' and (status in (301,302,404) or reason=='RESPONSE_CONTRACT')
            if status in (400,404) or secondary_absent or (status==200 and reason=='RESPONSE_CONTRACT' and not body.lstrip().lower().startswith((b'<html',b'<!doctype'))):
                state['cooldowns'].pop(host,None)
                state['checked'][key]=dict(status='HISTORY_UNAVAILABLE' if t['kind']=='FUND' else reason,retry_at=(now+timedelta(days=30 if t['kind']=='FUND' else 1)).isoformat(),task=t)
                state['pending']=[x for x in state['pending'] if s.digest(x)!=key]
                if t['kind']=='YAHOO_PRICE':
                    state['cursors'].setdefault('price_quote_checks',{})[t['entity']]=dict(retry_at=(now+timedelta(days=30)).isoformat(),status=reason)
                if t['kind']=='STOCKANALYSIS_PRICE':
                    state['cursors'].setdefault('secondary_price_quote_checks',{})[t['entity']]=dict(retry_at=(now+timedelta(days=30)).isoformat(),status=reason)
        else:
            state['cooldowns'].pop(host,None)
            if t['kind']=='YAHOO_PRICE':
                state['cursors'].setdefault('price_quote_checks',{})[t['entity']]=dict(retry_at=(now+timedelta(days=7)).isoformat(),status='COLLECTED')
            if t['kind']=='STOCKANALYSIS_PRICE':
                quote=s.stockanalysis_quote(t,body);age=(date.fromisoformat(t['requested_date'])-date.fromisoformat(quote['price_asof'])).days
                state['cursors'].setdefault('secondary_price_quote_checks',{})[t['entity']]=dict(retry_at=(now+timedelta(days=30 if age>30 else 7)).isoformat(),status='COLLECTED')
            if canary is not None:
                # Explicit operator recovery verifies the same official host
                # with a declared identity. Failed probes never lift denial.
                state['cooldowns'].pop(host,None)
                state['cursors'].setdefault('sec_identity_verified_hosts',{})[host]=now.isoformat()
                if t['kind']=='SEC_SUBMISSIONS':
                    fact=s.task('SEC_FACTS',t['entity'])
                    if s.digest(fact) not in state['checked']:state['pending'].insert(0,fact)
            semantic=None
            if t['kind']=='NASDAQ_DIRECTORY':
                import csv,io
                reader=csv.DictReader(io.StringIO(body.decode('utf-8-sig')),delimiter='|')
                field='Symbol' if t['entity']=='nasdaq' else 'ACT Symbol'
                meaningful=[r for r in reader if r.get('Test Issue')=='N' and not str(r.get(field,'')).startswith('File Creation Time')]
                semantic=s.digest([reader.fieldnames,sorted(meaningful,key=lambda r:r[field])])
            semantic_unchanged=bool(semantic and state['checked'].get(key,{}).get('semantic_sha256')==semantic and key in state['seen'])
            sha=state['seen'][key] if semantic_unchanged else hashlib.sha256(body).hexdigest()
            changed=state['seen'].get(key)!=sha
            if sha not in state['seen'].values():
                retrieved=fixed or datetime.now(timezone.utc)
                records.append(dict(task=t,retrieved_at=retrieved.isoformat(),body_base64=base64.b64encode(body).decode(),body_sha256=sha))
            else: unchanged+=1
            state['seen'][key]=sha; state['checked'][key]=dict(status='COLLECTED',checked_at=now.isoformat())
            if semantic:state['checked'][key]['semantic_sha256']=semantic
            if empty_finra:state['checked'][key].update(status='SOURCE_EMPTY',response_status=204)
            if t['kind']=='SEC_MAP':
                value=json.loads(body); position=value['fields'].index('cik')
                observed=[str(int(row[position])).zfill(10) for row in value['data']]
                prior=state['cursors'].get('issuer_additions',[])
                state['cursors']['issuer_additions']=list(dict.fromkeys(prior+observed))
            state['pending']=[x for x in state['pending'] if s.digest(x)!=key]
            existing={s.digest(x) for x in state['pending']}
            targeted=t['kind']=='SEC_SUBMISSIONS' and t['entity'] in bootstrap.get('capitalization_ciks',[])
            children=(s.discovered(t,body,primary=True,through=bootstrap['through']) if targeted else s.discovered(t,body))
            for child in children:
                child_key=s.digest(child)
                context=s.primary_filing_context(t,body,child)
                if context is not None:contexts[child_key]=context
                stamp=state['checked'].get(child_key,{}).get('checked_at')
                refresh=(changed and t['kind'] in ('LSEG_CATALOG','SP_ARCHIVE') and
                         (child['kind'] in ('LSEG_CATALOG','SP_ARCHIVE') or
                          stamp is None or datetime.fromisoformat(stamp)<=now-timedelta(days=1)))
                if t['kind'] in ('CITI_CATALOG','RUSSELL_IPO_PAGE'):
                    interval=timedelta(days=30 if t['kind']=='CITI_CATALOG' else 7)
                    refresh=stamp is not None and datetime.fromisoformat(stamp)<=now-interval
                if t['kind'] in ('JPM_UNIVERSE','JPM_ACTIONS'):
                    interval=timedelta(days=30 if child['kind']=='JPM_DOCUMENT' else 1)
                    refresh=stamp is not None and datetime.fromisoformat(stamp)<=now-interval
                if t['kind']=='CITI_NOTICE':
                    # A stable cover PDF can link to an in-place corrected
                    # attachment. Recheck that child on the monthly cycle too.
                    refresh=stamp is not None and (datetime.fromisoformat(stamp)<=now-timedelta(days=30)
                        or changed and datetime.fromisoformat(stamp)<=now-timedelta(days=1))
                if child_key not in existing and (child_key not in state['seen'] or refresh):
                    state['pending'].append(child); existing.add(child_key)
        if sum(len(r['body_base64']) for r in records)>60*1024**2: break
        # The secondary site rate-limits bursts; keep a conservative cadence
        # in addition to honoring its persisted 429/Retry-After cooldown.
        sleep(6 if t['kind']=='STOCKANALYSIS_PRICE' else 1)
    return dict(state=state,records=records,requests=sent,unchanged=unchanged)


def run(bootstrap, directory, *, seconds=600,max_requests=50,max_batches=10,role='cloud',key=None,credentials=None,source_kinds=None,recover_sec_identity=False):
    if recover_sec_identity and not s.sec_request_identity((credentials or {}).get('SEC_USER_AGENT') or os.environ.get('SEC_USER_AGENT')):
        raise ValueError('Verified declared SEC identity required for recovery')
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
            result=collect(state,bootstrap,max_requests=max_requests,fetch=Fetcher(credentials=credentials,guard=owner.guard),deadline=end,source_kinds=source_kinds,recover_sec_identity=recover_sec_identity and not summaries)
            if not result['requests']: break
            payload=dict(schema='research-delta-v1',stream_id=s.STREAM,sequence=prior['sequence']+1 if prior else 1,
                         previous_package_sha256=package_digest(prior) if prior else None,bootstrap_anchor_sha256=s.ANCHOR,
                         collected_at=datetime.now(timezone.utc).isoformat(),records=result['records'],state=result['state'])
            head=publish(payload,directory,owner,key=key); prior=payload; state=result['state']
            summary=dict(**head,records=len(result['records']),requests=result['requests'],cooldowns=state['cooldowns'],pending=len(state['pending']))
            summaries.append(summary); print(s.canonical(dict(stage='market_reference_batch',**summary)),flush=True)
    finally: owner.release()
    return dict(status='COLLECTED' if summaries else 'IDLE',batches=summaries,cloud_database=False)
