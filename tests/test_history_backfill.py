from pathlib import Path
import duckdb
import pandas as pd
import pytest

from price_lab import history_backfill as h
from price_lab.providers import MarketBatch


class Provider:
    calls=0
    fail=False
    def fetch_market_part(self,market,sessions,assets,audit=False,action_start=None):
        self.calls+=1
        if self.fail:raise RuntimeError('credentials must never be serialized')
        rows=[dict(market=market,symbol='BTC/USD',session_date=d,open=10.,high=12.,low=9.,close=11.,volume=100.,turnover=1100.,vwap=11.,currency='USD') for d in sessions]
        return MarketBatch(market,pd.DataFrame(rows),assets,[],list(sessions),{'actions_complete':True,'benchmarks_complete':True})


def plan(tmp_path,part_sessions=2):
    assets=pd.DataFrame([dict(market='crypto',symbol='BTC/USD',name='Bitcoin',exchange='CRYPTO')])
    return h.create_plan(tmp_path,market='crypto',start='2021-01-01',end='2021-01-05',assets=assets,
                         sessions=list(pd.date_range('2021-01-01','2021-01-05')),part_sessions=part_sessions)


def test_archive_resume_ledger_and_unique_keys(tmp_path):
    plan(tmp_path);p=Provider()
    first=h.collect(tmp_path,p,max_parts=1)
    assert first['status']=='PAUSED' and first['completed_parts']==1 and first['total_parts']==3
    assert first['consumer_ready'] is False
    again=h.collect(tmp_path,p)
    assert again['status']=='COMPLETE' and again['completed_parts']==3 and p.calls==3
    before=(tmp_path/'status.json').read_bytes()
    assert h.collect(tmp_path,p)['status']=='COMPLETE' and p.calls==3
    with duckdb.connect(str(tmp_path/'raw_history.duckdb'),read_only=True) as db:
        assert db.execute('SELECT count(*),count(DISTINCT session_date) FROM raw_bars').fetchone()==(5,5)
    assert (tmp_path/'status.json').read_bytes()==before


def test_failed_part_retains_prior_commit_and_redacts_error(tmp_path):
    plan(tmp_path);p=Provider();h.collect(tmp_path,p,max_parts=1);p.fail=True
    failed=h.collect(tmp_path,p)
    assert failed['status']=='FAILED' and failed['completed_parts']==1
    assert 'credentials' not in (tmp_path/'status.json').read_text()
    p.fail=False
    assert h.collect(tmp_path,p)['status']=='COMPLETE'


def test_cached_part_is_not_applied_twice_after_transaction_retry(tmp_path,monkeypatch):
    plan(tmp_path);p=Provider();original=h._apply_part
    def fail(*args,**kwargs):raise RuntimeError('crash before commit')
    monkeypatch.setattr(h,'_apply_part',fail)
    assert h.collect(tmp_path,p,max_parts=1)['status']=='FAILED'
    assert p.calls==1
    monkeypatch.setattr(h,'_apply_part',original)
    assert h.collect(tmp_path,p)['status']=='COMPLETE' and p.calls==3


def test_manifest_freeze_and_cache_hash_tamper(tmp_path):
    plan(tmp_path);p=Provider();h.collect(tmp_path,p,max_parts=1)
    with pytest.raises(ValueError,match='plan'):
        h.create_plan(tmp_path,market='crypto',start='2020-01-01',end='2021-01-05',assets=pd.DataFrame(),sessions=[])
    payload=next((tmp_path/'parts').rglob('bars.parquet'))
    payload.write_bytes(b'corrupt')
    # Already applied parts are immutable provenance and must also be checked.
    assert h.collect(tmp_path,p)['status']=='FAILED'


def test_invalid_numeric_or_duplicate_raw_rows_never_commit(tmp_path):
    plan(tmp_path)
    class Bad(Provider):
        def fetch_market_part(self,*args,**kwargs):
            batch=super().fetch_market_part(*args,**kwargs)
            batch.bars=pd.concat([batch.bars,batch.bars.iloc[[0]]],ignore_index=True)
            return batch
    assert h.collect(tmp_path,Bad())['status']=='FAILED'
    with duckdb.connect(str(tmp_path/'raw_history.duckdb'),read_only=True) as db:
        assert db.execute('SELECT count(*) FROM raw_bars').fetchone()[0]==0


def test_partial_database_tamper_cannot_be_certified_on_resume(tmp_path):
    plan(tmp_path);p=Provider();h.collect(tmp_path,p,max_parts=1)
    with duckdb.connect(str(tmp_path/'raw_history.duckdb')) as db:
        db.execute('UPDATE raw_bars SET close=11.5')
    result=h.collect(tmp_path,p)
    assert result['status']=='FAILED' and result['consumer_ready'] is False
