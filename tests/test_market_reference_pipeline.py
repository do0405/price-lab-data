import base64
from datetime import datetime,timezone
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import pytest
from price_lab import market_reference_sources as s,market_reference_pipeline as p
from test_market_reference_sources import fund_body,payload,jpm_body

NOW=datetime(2026,11,24,tzinfo=timezone.utc)


def test_targeted_reference_collection_preserves_other_pending_sources():
    from unittest.mock import patch
    state=p.initial_state('2026-11-23');other=s.task('CITI_NOTICE','513');target=s.task('JPM_UNIVERSE')
    state['pending']=[other,target];calls=[]
    def fetch(t):
        calls.append(t['kind']);return 200,{},jpm_body([{'cusip':'471115402','name':'Example'}])
    with patch.object(p,'plan',lambda state,*args:state):
        result=p.collect(state,bootstrap(),now=NOW,max_requests=5,fetch=fetch,sleep=lambda _:None,
            source_kinds=['JPM_UNIVERSE'])
    assert calls==['JPM_UNIVERSE']
    assert result['state']['pending']==[other] and result['requests']==1


def test_extended_sources_join_existing_window_without_replaying_completed_documents():
    b=bootstrap();state=p.initial_state('2026-11-23');state['cursors']['current_window']='2026-11-24T00'
    done=s.task('BNY_NOTICE','AD2000235');state['checked'][s.digest(done)]={'status':'COLLECTED','checked_at':NOW.isoformat()}
    p.plan(state,b,NOW)
    for t in [s.task('JPM_UNIVERSE'),s.task('JPM_ACTIONS'),s.task('CITI_CATALOG'),s.task('LSEG_WEIGHT_CATALOG'),s.task('LSEG_WEIGHTS','US1000'),s.task('RUSSELL_IPO_PAGE'),s.task('RUSSELL_IPO_PDF','3000',page=4)]:
        assert t in state['pending']
    assert done not in state['pending']


def test_citi_backfill_keeps_a_slot_for_other_reference_categories():
    state=p.initial_state('2026-11-23');state['pending']=[s.task('CITI_NOTICE',str(n)) for n in range(1,10)]+[s.task('FRED_INDEX','SP500')]
    calls=[]
    def fetch(t):
        calls.append(t['kind'])
        return 200,{},b'observation_date,SP500\n2026-11-20,100\n' if t['kind']=='FRED_INDEX' else b'%PDF-fixture'
    # Freeze planning to isolate lane fairness from current-window discovery.
    from unittest.mock import patch
    with patch.object(p,'plan',lambda state,*args:state):
        p.collect(state,bootstrap(),now=NOW,max_requests=5,fetch=fetch,sleep=lambda _:None)
    assert calls[:4]==['CITI_NOTICE']*4 and calls[4]=='FRED_INDEX'


def test_attachment_targets_preserve_issuer_and_fund_targets_and_are_idempotent(tmp_path):
    path=tmp_path/'bootstrap.json';initial=dict(version=2,ciks=['0000320193'],research_targets=[])
    path.write_text(json.dumps(initial))
    assert p.enqueue_attachments(path,['6832','6832'])['added']==1
    before=path.read_bytes();assert p.enqueue_attachments(path,['6832'])['status']=='UNCHANGED';assert path.read_bytes()==before
    p.enqueue_targets(path,[dict(fund='IWB',snapshot_date='2026-10-21')])
    config=json.loads(path.read_text());assert config['depositary_attachment_ids']==['6832'] and config['ciks']==initial['ciks'] and config['version']==3
    with pytest.raises(ValueError):p.enqueue_attachments(path,['https://evil.example/'])


def test_finra_empty_day_204_is_not_a_host_denial_or_an_invented_event():
    from unittest.mock import patch
    state=p.initial_state('2026-11-23');t=s.task('FINRA_DAILY',day='2026-11-22');state['pending']=[t]
    with patch.object(p,'plan',lambda state,*args:state):
        out=p.collect(state,bootstrap(),now=NOW,max_requests=1,fetch=lambda _: (204,{},b''),sleep=lambda _:None)
    assert not out['state']['cooldowns']
    assert out['state']['checked'][s.digest(t)]['status']=='SOURCE_EMPTY'
    assert base64.b64decode(out['records'][0]['body_base64'])==b'[]'


def test_monthly_unchanged_citi_cover_still_rechecks_old_attachment(monkeypatch):
    from datetime import timedelta
    import hashlib
    cover=s.task('CITI_NOTICE','2621');child=s.task('CITI_ATTACHMENT','6832');body=b'%PDF-fixture'
    state=p.initial_state('2026-11-23');state['pending']=[cover]
    state['seen'][s.digest(cover)]=hashlib.sha256(body).hexdigest();state['seen'][s.digest(child)]='a'*64
    state['checked'][s.digest(child)]={'status':'COLLECTED','checked_at':(NOW-timedelta(days=31)).isoformat()}
    monkeypatch.setattr(p,'plan',lambda state,*args:state)
    monkeypatch.setattr(s,'discovered',lambda *args:[child])
    out=p.collect(state,bootstrap(),now=NOW,max_requests=1,fetch=lambda _: (200,{},body),sleep=lambda _:None)
    assert child in out['state']['pending'] and out['records']==[]


def test_citi_attachment_backlog_is_not_left_behind_all_cover_notices(monkeypatch):
    state=p.initial_state('2026-11-23')
    state['pending']=[s.task('CITI_NOTICE',str(n)) for n in range(1,8)]+[s.task('CITI_ATTACHMENT',str(n)) for n in range(10,15)]+[s.task('FRED_INDEX','SP500')]
    calls=[]
    monkeypatch.setattr(p,'plan',lambda state,*args:state);monkeypatch.setattr(s,'discovered',lambda *args:[])
    def fetch(t):
        calls.append(t['kind'])
        body=b'observation_date,SP500\n2026-11-20,100\n' if t['kind']=='FRED_INDEX' else b'%PDF-fixture'
        return 200,{},body+str(len(calls)).encode() if t['kind']!='FRED_INDEX' else body
    p.collect(state,bootstrap(),now=NOW,max_requests=5,fetch=fetch,sleep=lambda _:None)
    assert calls==['CITI_NOTICE','CITI_ATTACHMENT','CITI_ATTACHMENT','CITI_ATTACHMENT','FRED_INDEX']


def bootstrap():
    return dict(through='2026-11-23',kr_sessions=['2010-01-04','2026-11-23'],us_month_ends=['2010-01-29','2026-10-30'],ciks=['0000320193'])


def test_403_is_a_checkpointed_host_cooldown_not_empty_success():
    state=p.initial_state('2026-11-23'); calls=[]
    def fetch(t): calls.append(t); return 403,{},b'denied'
    result=p.collect(state,bootstrap(),now=NOW,max_requests=20,fetch=fetch,sleep=lambda _:None)
    hosts=[__import__('urllib.parse',fromlist=['urlsplit']).urlsplit(t['url']).hostname for t in calls]
    assert len(hosts)==len(set(hosts)), 'A denied host must not be hammered for every queued security'
    assert not result['records'] and not result['state']['seen']
    assert all(v['reason']=='HTTP_403' for v in result['state']['cooldowns'].values())
    assert state==p.initial_state('2026-11-23'), 'Tentative collection cannot mutate its authenticated predecessor'


def test_successful_batch_resumes_without_refetching_completed_task():
    state=p.initial_state('2026-11-23'); calls=[]
    def fetch(t): calls.append(t); return 200,{},fund_body()
    first=p.collect(state,bootstrap(),now=NOW,max_requests=1,fetch=fetch,sleep=lambda _:None)
    assert len(first['records'])==1 and calls[0]['entity']=='IVV'
    p.collect(first['state'],bootstrap(),now=NOW,max_requests=1,fetch=lambda t:(calls.append(t) or (403,{},b'')),sleep=lambda _:None)
    assert calls[1]['entity']=='IWB' and calls[1]!=calls[0]


def test_pending_topup_is_bounded_even_with_denied_providers():
    state=p.initial_state('2026-11-23'); b=bootstrap(); b['kr_sessions']=['2010-01-04']*1000
    for _ in range(1000): p.plan(state,b,NOW)
    assert len(state['pending'])<100


def test_old_catalog_checkpoint_migrates_without_repeating_404_or_losing_scope():
    state=p.initial_state('2026-11-23'); old=s.task('SP_CATALOG','2024',page=1)
    state['pending']=[old]; state['cursors']['daily']=NOW.date().isoformat()
    p.plan(state,bootstrap(),NOW)
    assert not any(t['kind']=='SP_CATALOG' for t in state['pending'])
    assert s.task('SP_ARCHIVE','2024',page=1) in state['pending']
    assert state['checked'][s.digest(old)]['status']=='RETIRED_ENDPOINT_REPLACED'


def test_current_refresh_reuses_keys_every_six_hours_instead_of_growing_daily_ids():
    from datetime import timedelta
    state=p.initial_state('2026-11-23'); keys=set()
    for cycle in range(12):
        now=NOW+timedelta(hours=6*cycle)
        p.plan(state,bootstrap(),now)
        current=[t for t in state['pending'] if t['kind']=='FUND_LATEST']
        assert len(current)==5 and all(t['requested_date']=='' for t in current)
        keys.update(s.digest(t) for t in current)
        for t in current: state['checked'][s.digest(t)]=dict(status='COLLECTED',checked_at=now.isoformat())
        state['pending']=[t for t in state['pending'] if t['kind']!='FUND_LATEST']
    assert len(keys)==5


def test_weekly_sec_refresh_reuses_stable_issuer_task_identity():
    from datetime import timedelta
    state=p.initial_state('2026-11-23'); p.plan(state,bootstrap(),NOW)
    old=[t for t in state['pending'] if t['kind'] in ('SEC_FACTS','SEC_SUBMISSIONS')]
    assert len(old)==2
    for t in old: state['checked'][s.digest(t)]=dict(status='COLLECTED',checked_at=NOW.isoformat())
    state['pending']=[];state['cursors']['sec']=1
    p.plan(state,bootstrap(),NOW+timedelta(days=8))
    new=[t for t in state['pending'] if t['kind'] in ('SEC_FACTS','SEC_SUBMISSIONS')]
    assert new==old and all(t['requested_date']=='' for t in new)


def test_separate_owner_cannot_modify_existing_korean_stream(monkeypatch):
    from price_lab import research_ownership as own
    from test_research_ownership import Contents
    stores={}; paths=[]
    def api(method,path,**kw):
        paths.append(path)
        namespace='market' if 'market-reference-owner' in path else 'kr'
        return stores.setdefault(namespace,Contents()).api(method,path,**kw)
    monkeypatch.setattr(own,'_api',api)
    kr=own.ResearchOwner('local',key=b'x'*32,clock=lambda:NOW)
    market=own.ResearchOwner('cloud',key=b'x'*32,clock=lambda:NOW,namespace='market-references')
    assert kr.acquire(anchor_sha256='a'*64)
    before=stores['kr'].content
    assert market.acquire(anchor_sha256=s.ANCHOR)
    market.commit_head(payload()); market.release()
    assert stores['kr'].content==before
    assert any('/market-reference-owner.json' in path for path in paths)


def test_bootstrap_rejects_nonpublic_arbitrary_identifiers(tmp_path):
    config=tmp_path/'seed.json'; config.write_text(json.dumps({'version':1,'ciks':['http://evil/']}))
    with pytest.raises(ValueError): p.bootstrap(config,now=NOW)


def test_cloud_import_never_loads_database_or_private_credentials():
    root=Path(__file__).resolve().parents[1]
    code="import runpy,sys;runpy.run_path('scripts/market_reference_pipeline.py',run_name='inspect');from price_lab import market_reference_pipeline;assert 'duckdb' not in sys.modules;assert 'price_lab.local_credentials' not in sys.modules"
    result=subprocess.run([sys.executable,'-B','-c',code],cwd=root,capture_output=True)
    assert result.returncode==0,result.stderr.decode(errors='replace')
def test_immutable_nasdaq_report_is_discovered_once_without_resetting_denied_hosts():
    state=p.initial_state('2026-11-23')
    state['cooldowns']['ir.nasdaq.com']=dict(reason='HTTP_403',retry_at='2026-11-25T00:00:00+00:00')
    p.plan(state,bootstrap(),NOW)
    reports=[t for t in state['pending'] if t['kind']=='NASDAQ_PDF']
    assert len(reports)==1
    state['checked'][s.digest(reports[0])]=dict(status='COLLECTED',checked_at=NOW.isoformat())
    state['pending']=[t for t in state['pending'] if t['kind']!='NASDAQ_PDF']
    p.plan(state,bootstrap(),NOW)
    assert not any(t['kind']=='NASDAQ_PDF' for t in state['pending'])
    assert state['cooldowns']['ir.nasdaq.com']['reason']=='HTTP_403'


def test_recent_research_months_are_backfilled_without_replaying_checked_snapshots():
    state=p.initial_state('2026-11-23');b=bootstrap()
    b['us_month_ends']=['2010-01-29','2026-08-31','2026-09-30','2026-10-30']
    p.plan(state,b,NOW)
    expected=s.task('FUND','IVV','2026-09-30')
    assert expected in state['pending']
    state['checked'][s.digest(expected)]=dict(status='COLLECTED',checked_at=NOW.isoformat())
    state['pending']=[t for t in state['pending'] if t!=expected]
    p.plan(state,b,NOW)
    assert expected not in state['pending']


def test_targeted_snapshot_config_is_backward_compatible_and_idempotent(tmp_path):
    path=tmp_path/'seed.json';path.write_text(json.dumps({'version':1,'ciks':['0000320193']}))
    target=dict(fund='IWB',snapshot_date='2026-10-21')
    assert p.enqueue_targets(path,[target])['status']=='UPDATED'
    raw=path.read_bytes()
    assert p.enqueue_targets(path,[target])['status']=='UNCHANGED' and path.read_bytes()==raw
    b=p.bootstrap(path,now=NOW)
    assert b['research_targets']==[target] and b['ciks']==['0000320193']
    state=p.initial_state(b['through']);p.plan(state,b,NOW)
    task=s.task('FUND','IWB','2026-10-21')
    assert task in state['pending']
    state['checked'][s.digest(task)]=dict(status='COLLECTED',checked_at=NOW.isoformat())
    state['pending']=[t for t in state['pending'] if t!=task];p.plan(state,b,NOW)
    assert task not in state['pending']


@pytest.mark.parametrize('target',[
    dict(fund='EVIL',snapshot_date='2026-10-21'),
    dict(fund='IWB',snapshot_date='2999-01-01'),
    dict(fund='IWB',snapshot_date='invalid'),
    dict(fund='IWB',snapshot_date='2026-10-21',url='https://example.com'),
])
def test_targeted_snapshot_config_rejects_unbounded_or_nonpublic_inputs(tmp_path,target):
    path=tmp_path/'seed.json';path.write_text(json.dumps({'version':2,'ciks':[], 'research_targets':[target]}))
    with pytest.raises(ValueError):p.bootstrap(path,now=NOW)


def test_new_factsheet_is_found_even_inside_an_existing_refresh_window():
    state=p.initial_state('2026-11-23');p.plan(state,bootstrap(),NOW)
    task=s.task('NASDAQ_FACTSHEET','NDX')
    state['pending']=[t for t in state['pending'] if t!=task]
    p.plan(state,bootstrap(),NOW)
    assert task in state['pending']


def test_recent_completed_index_week_is_not_starved_by_unpublished_latest_session():
    b=bootstrap();b['kr_sessions']=['2026-11-16','2026-11-17','2026-11-18','2026-11-19','2026-11-20','2026-11-23']
    state=p.initial_state(b['through']);p.plan(state,b,NOW)
    for day in b['kr_sessions'][-5:]:
        assert all(s.task('KRX_INDEX',family,day) in state['pending'] for family in ('kospi','kosdaq','krx'))
    completed=s.task('KRX_INDEX','kospi','2026-11-20')
    state['checked'][s.digest(completed)]=dict(status='COLLECTED',checked_at=NOW.isoformat())
    state['pending']=[t for t in state['pending'] if t!=completed];p.plan(state,b,NOW)
    assert completed not in state['pending']
