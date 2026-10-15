"""Real managed-checkpoint integration with a deterministic official-body fixture."""
import gzip
import json
import os
from pathlib import Path
from datetime import timezone

import duckdb
import pytest

from price_lab import ingestion as ingestion
from price_lab import history_integration as integration
from price_lab import kr_reference_refresh as refresh_module
from price_lab.kr_reference_pipeline import prepare_delta_from_raw
from price_lab.kr_reference_sources import digest
from price_lab.kr_reference_store import apply_delta, coverage_report, select_asof
from test_ingestion import seed, FakeProvider

SESSIONS=['2023-12-28','2024-01-02','2024-01-03','2024-01-04','2024-01-05','2024-01-08','2024-01-09']


def managed_kr(tmp_path):
    source=seed(tmp_path/'seed.duckdb')
    with duckdb.connect(str(source)) as db:
        db.execute("INSERT INTO assets VALUES ('kr','000001','Example','KOSPI'),('kr','069500','KOSPI ETF','KOSPI'),('kr','229200','KOSDAQ ETF','KOSDAQ')")
        db.executemany("INSERT INTO trading_calendar VALUES ('kr',?)",[(d,) for d in SESSIONS])
        db.execute("""INSERT INTO backtest_bars SELECT market,symbol,session_date,
            100,101,99,100,1000,100000,100,100,100,1000,1,1,'KRW'
            FROM assets JOIN trading_calendar USING(market) WHERE market='kr'""")
    root=tmp_path/'managed'
    ingestion.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,
                      markets=('us',),now='2024-01-10T22:00Z')
    return root


def official_body(board,date,kind):
    symbol='000001' if board=='KOSPI' else '000002'
    if kind=='daily':
        return {'OutBlock_1':[dict(BAS_DD=date.replace('-',''),MKT_NM=board,ISU_CD=symbol,
            TDD_CLSPRC='100',MKTCAP='100000000',LIST_SHRS='1000000')]}
    return {'OutBlock_1':[dict(BAS_DD=date.replace('-',''),MKT_TP_NM=board,
        ISU_CD='KR7000001001' if board=='KOSPI' else 'KR7000002009',ISU_SRT_CD=symbol,
        LIST_DD='20000101',SECUGRP_NM='주권',KIND_STKCERT_TP_NM='보통주',ISU_NM='Example')]}


class Fetch:
    def __init__(self): self.calls=[]
    def __call__(self,board,date,kind):
        self.calls.append((board,date,kind)); return official_body(board,date,kind)


def prices(root):
    current=ingestion.resolve_current(root)
    with duckdb.connect(current['resolved_path'],read_only=True) as db:
        rows=db.execute('SELECT * FROM backtest_bars ORDER BY market,symbol,session_date').fetchall()
    return digest(rows),len(rows)


def test_recent_window_publishes_reference_without_changing_prices_or_old_reference(tmp_path):
    root=managed_kr(tmp_path); parent=ingestion.resolve_current(root); original=prices(root)
    # A historical reference cell outside this refresh window remains in the parent.
    old=tmp_path/'old-reference'; old.mkdir()
    path=old/'old.json.gz'
    with gzip.open(path,'wt',encoding='utf-8') as stream: json.dump(official_body('KOSPI','2024-01-02','daily'),stream)
    with duckdb.connect(str(old/'reference.duckdb')) as db:
        apply_delta(db,prepare_delta_from_raw(path,sessions=SESSIONS,min_age_seconds=0)); db.execute('CHECKPOINT')
    integration.integrate_reference(old/'reference.duckdb',root=root)
    before=ingestion.resolve_current(root); fetch=Fetch()
    result=refresh_module.refresh_recent_reference(root,fetch=fetch,max_sessions=2)
    assert result['status']=='PUBLISHED' and len(fetch.calls)==8
    assert result['sessions']==SESSIONS[-2:]
    assert all(date in SESSIONS[-2:] for _,date,_ in fetch.calls)
    current=ingestion.resolve_current(root)
    assert current['parent_sha256']==before['sha256'] and current['sequence']==before['sequence']+1
    assert prices(root)==original and ingestion.sha256_file(parent['resolved_path'])==parent['sha256']
    with duckdb.connect(current['resolved_path'],read_only=True) as db:
        assert db.execute('SELECT count(*) FROM kr_capitalization_observations').fetchone()[0]==5
        assert db.execute('SELECT count(*) FROM kr_security_aliases').fetchone()[0]==4
        rows=db.execute("SELECT DISTINCT asof_date,research_available_at FROM kr_capitalization_observations WHERE asof_date>='2024-01-08' ORDER BY 1").fetchall()
        assert str(rows[0][1].astimezone(timezone.utc))=='2024-01-09 00:00:00+00:00' and rows[1][1] is None
    assert not list((root/'reference-refresh'/'attempts').rglob('*.wal'))


def test_repeat_is_unchanged_without_fetch_factory_or_publication(tmp_path,monkeypatch):
    root=managed_kr(tmp_path); fetch=Fetch()
    assert refresh_module.refresh_recent_reference(root,fetch=fetch,max_sessions=2)['status']=='PUBLISHED'
    before=(root/'current.json').read_bytes(); calls=len(fetch.calls)
    monkeypatch.setattr(refresh_module,'official_fetcher',lambda:pytest.fail('Complete references must not initialize credentials'))
    result=refresh_module.refresh_recent_reference(root,max_sessions=2)
    assert result['status']=='UNCHANGED' and result['network_requests']==0
    assert len(fetch.calls)==calls and (root/'current.json').read_bytes()==before


def test_only_latest_present_fills_earlier_gap_without_duplicate_requests(tmp_path):
    root=managed_kr(tmp_path); first=Fetch()
    assert refresh_module.refresh_recent_reference(root,fetch=first,max_sessions=1)['status']=='PUBLISHED'
    fetch=Fetch(); result=refresh_module.refresh_recent_reference(root,fetch=fetch,max_sessions=2)
    assert result['status']=='PUBLISHED'
    assert len(fetch.calls)==4 and {d for _,d,_ in fetch.calls}=={'2024-01-08'}


def test_collection_failure_keeps_published_price_pointer_and_safe_errors(tmp_path):
    root=managed_kr(tmp_path); before=(root/'current.json').read_bytes(); original=prices(root)
    def fail(board,date,kind):
        if kind=='master': raise RuntimeError('AUTH_KEY=very-secret HTTP response body')
        return official_body(board,date,kind)
    result=refresh_module.refresh_recent_reference(root,fetch=fail,max_sessions=2)
    assert result['status'] in ('FAILED','PARTIAL')
    assert (root/'current.json').read_bytes()==before and prices(root)==original
    assert 'very-secret' not in json.dumps(result) and 'response body' not in json.dumps(result)
    assert all('very-secret' not in p.read_text(encoding='utf-8') for p in (root/'reference-refresh').rglob('*.json'))


def test_immediate_retry_reuses_successful_atomic_cache_and_only_requests_failed_kind(tmp_path):
    root=managed_kr(tmp_path)
    def partial(board,date,kind):
        if kind=='master': raise RuntimeError('access unavailable')
        return official_body(board,date,kind)
    assert refresh_module.refresh_recent_reference(root,fetch=partial,max_sessions=2)['status']=='PARTIAL'
    fetch=Fetch(); result=refresh_module.refresh_recent_reference(root,fetch=fetch,max_sessions=2)
    assert result['status']=='PUBLISHED' and len(fetch.calls)==4
    assert {kind for _,_,kind in fetch.calls}=={'master'}


def test_force_checks_cache_without_refetching_complete_cells(tmp_path):
    root=managed_kr(tmp_path); fetch=Fetch()
    refresh_module.refresh_recent_reference(root,fetch=fetch,max_sessions=2)
    before=(root/'current.json').read_bytes(); fetch.calls.clear()
    result=refresh_module.refresh_recent_reference(root,fetch=fetch,max_sessions=2,force=True)
    assert result['status']=='UNCHANGED' and fetch.calls==[]
    assert (root/'current.json').read_bytes()==before


def test_validation_error_after_fetch_retains_current_and_reports_request_count(tmp_path,monkeypatch):
    root=managed_kr(tmp_path); before=(root/'current.json').read_bytes(); fetch=Fetch()
    def invalid(*args,**kwargs): raise ValueError('unexpected secret validation detail')
    monkeypatch.setattr(refresh_module,'coverage_report',invalid)
    result=refresh_module.refresh_recent_reference(root,fetch=fetch,max_sessions=2)
    assert result['status']=='FAILED' and (root/'current.json').read_bytes()==before
    assert result['network_requests']==len(fetch.calls)==8
    assert 'secret' not in json.dumps(result)


def test_existing_price_raw_and_refresh_raw_cache_are_reused_without_network(tmp_path,monkeypatch):
    root=managed_kr(tmp_path)
    for kind,folder in [('daily',root/'raw'),('master',root/'reference-refresh'/'raw')]:
        folder.mkdir(parents=True,exist_ok=True)
        for date in SESSIONS[-2:]:
            for board in ('KOSPI','KOSDAQ'):
                path=folder/f'{kind}-{board}-{date}.json.gz'
                with gzip.open(path,'wt',encoding='utf-8') as stream: json.dump(official_body(board,date,kind),stream)
                os.utime(path,(1_700_000_000,1_700_000_000))
    monkeypatch.setattr(refresh_module,'official_fetcher',lambda:pytest.fail('Completed raw must be reused before credentials'))
    result=refresh_module.refresh_recent_reference(root,max_sessions=2)
    assert result['status']=='PUBLISHED' and result['network_requests']==0
    assert prices(root)[1]>0


def test_reference_merge_rebases_on_new_daily_parent_after_collection(tmp_path):
    root=managed_kr(tmp_path); latest={}; seen=[]
    def fetch(board,date,kind):
        if not seen:
            seen.append(True)
            ingestion.refresh(FakeProvider(),seed_path=tmp_path/'seed.duckdb',supplement_path=None,
                root=root,markets=('us',),now='2024-01-11T22:00Z')
            latest['manifest']=ingestion.resolve_current(root); latest['prices']=prices(root)
        return official_body(board,date,kind)
    result=refresh_module.refresh_recent_reference(root,fetch=fetch,max_sessions=2)
    assert result['status']=='PUBLISHED'
    assert ingestion.resolve_current(root)['parent_sha256']==latest['manifest']['sha256']
    assert prices(root)==latest['prices']


def test_duplicate_identity_is_retained_but_consumer_view_excludes_conflict(tmp_path,monkeypatch):
    root=managed_kr(tmp_path); original=refresh_module.collect_reference
    def conflict(db,**kwargs):
        result=original(db,**kwargs)
        if kwargs['kind']=='master' and db.execute('SELECT count(*) FROM kr_security_aliases').fetchone()[0]:
            # Official disagreement is evidence to retain, never a guessed merge.
            source=db.execute("SELECT * FROM kr_security_aliases WHERE board='KOSPI' ORDER BY effective_from LIMIT 1")
            cols=[r[0] for r in source.description]; row=dict(zip(cols,source.fetchone()))
            row.update(alias_id='conflicting-official-alias',security_id='conflicting-security')
            if not db.execute('SELECT 1 FROM kr_security_aliases WHERE alias_id=?',[row['alias_id']]).fetchone():
                db.execute('INSERT INTO kr_security_aliases VALUES ('+','.join('?' for _ in cols)+')',[row[c] for c in cols])
        return result
    monkeypatch.setattr(refresh_module,'collect_reference',conflict)
    before=prices(root); result=refresh_module.refresh_recent_reference(root,fetch=Fetch(),max_sessions=2)
    assert result['status']=='PUBLISHED' and prices(root)==before
    with duckdb.connect(ingestion.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert coverage_report(db)['identity_conflicts']>0
        assert db.execute("SELECT count(*) FROM kr_capitalization_daily WHERE board='KOSPI' AND asof_date='2024-01-08'").fetchone()[0]==0


@pytest.mark.parametrize('value',[0,7,False])
def test_network_window_has_hard_session_bound(tmp_path,value):
    with pytest.raises(ValueError): refresh_module.refresh_recent_reference(tmp_path,max_sessions=value)
