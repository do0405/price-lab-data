import base64
from datetime import datetime,timezone
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import pytest
from price_lab import market_reference_sources as s,market_reference_pipeline as p
from test_market_reference_sources import fund_body,payload

NOW=datetime(2026,11,24,tzinfo=timezone.utc)


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
