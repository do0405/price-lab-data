import importlib.util
import json
from pathlib import Path
import duckdb
import pandas as pd
import pytest


def module():
    assert importlib.util.find_spec('price_lab.ingestion'), 'durable ingestion missing'
    from price_lab import ingestion
    return ingestion


def seed(path):
    with duckdb.connect(str(path)) as db:
        db.execute('CREATE TABLE assets(market VARCHAR,symbol VARCHAR,name VARCHAR,exchange VARCHAR)')
        db.execute("INSERT INTO assets VALUES('us','A','A','NYSE'),('us','SPY','SPY','ARCA'),('us','QQQ','QQQ','NASDAQ')")
        db.execute('CREATE TABLE trading_calendar(market VARCHAR,session_date DATE)')
        db.execute("INSERT INTO trading_calendar SELECT 'us',d FROM unnest([DATE '2024-01-02',DATE '2024-01-03',DATE '2024-01-04'])t(d)")
        db.execute('CREATE TABLE backtest_bars AS SELECT market,symbol,session_date,100.0::DOUBLE AS open,101.0::DOUBLE AS high,99.0::DOUBLE AS low,100.0::DOUBLE AS close,1000.0::DOUBLE AS volume,100000.0::DOUBLE AS turnover,100.0::DOUBLE AS vwap,100.0::DOUBLE AS raw_open,100.0::DOUBLE AS raw_close,1000.0::DOUBLE AS raw_volume,1.0::DOUBLE AS split_price_factor,1.0::DOUBLE AS split_share_factor,\'USD\' AS currency FROM assets JOIN trading_calendar USING(market)')
        db.execute("CREATE TABLE quarantined_source AS SELECT 'retained' evidence")
    return path


class FakeProvider:
    def __init__(self,fail=False,actions=None):self.fail=fail;self.actions=actions or [];self.calls=[]
    def fetch_market(self,market,sessions,assets,audit=False):
        from price_lab.providers import MarketBatch
        self.calls.append(list(sessions))
        if self.fail:raise RuntimeError('provider unavailable')
        rows=[dict(market=market,symbol=s,session_date=d,open=100.,high=101.,low=99.,close=100.,volume=1000.,turnover=100000.,vwap=100.,currency='USD') for d in sessions for s in ['A','SPY','QQQ']]
        return MarketBatch(market,pd.DataFrame(rows),assets[['market','symbol','name','exchange']],self.actions,list(sessions),{'provider':'fixture','actions_complete':True,'benchmarks_complete':True})


def test_atomic_bootstrap_merges_preserves_raw_and_does_not_truncate(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');before=i.sha256_file(source)
    result=i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=tmp_path/'managed',markets=('us',),now='2024-01-05T22:00Z')
    current=i.resolve_current(tmp_path/'managed')
    assert result['status']=='PUBLISHED' and current['sequence']==1
    assert current['includes_supplement'] and current['markets']['us']['actual']=='2024-01-05'
    with duckdb.connect(str(current['resolved_path']),read_only=True) as db:
        assert db.execute('SELECT count(*) FROM backtest_bars').fetchone()[0]==12
        assert db.execute('SELECT evidence FROM quarantined_source').fetchone()[0]=='retained'
    assert i.sha256_file(source)==before and not Path(str(current['resolved_path'])+'.wal').exists()


def test_partial_failure_retains_previous_current_and_attempt(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    before=(root/'current.json').read_bytes()
    result=i.refresh(FakeProvider(fail=True),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-08T22:00Z')
    assert result['status']=='FAILED' and (root/'current.json').read_bytes()==before
    assert list((root/'attempts').glob('*/status.json'))


def test_split_idempotence_revision_and_raw_preservation(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    action=dict(provider='alpaca',event_id='s1',revision='1',market='us',symbol='A',effective_date='2024-01-05',price_factor=.5,share_factor=2.,kind='split',confirmed=True)
    first=i.refresh(FakeProvider(actions=[action]),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    second=i.refresh(FakeProvider(actions=[action]),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    assert first['status']=='PUBLISHED' and second['status']=='UNCHANGED'
    revised={**action,'revision':'2','price_factor':.25,'share_factor':4.}
    third=i.refresh(FakeProvider(actions=[revised]),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    current=i.resolve_current(root)
    assert third['status']=='PUBLISHED' and current['sequence']==2 and current['parent_sha256']==first['manifest']['sha256']
    with duckdb.connect(str(current['resolved_path']),read_only=True) as db:
        row=db.execute("SELECT open,raw_open,volume,raw_volume,turnover FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-02'").fetchone()
        assert row==(25.,100.,4000.,1000.,100000.)
        assert db.execute("SELECT open FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-05'").fetchone()[0]==100.


def test_unknown_action_quarantines_without_publishing(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    action=dict(provider='x',event_id='unknown',revision='1',market='us',symbol='A',effective_date='2024-01-05',kind='unknown',confirmed=False)
    result=i.refresh(FakeProvider(actions=[action]),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    assert result['status']=='FAILED' and i.resolve_current(root) is None
    assert list((root/'attempts').glob('*/quarantine.json'))


def test_current_rejects_escape_hash_and_database_tamper(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    manifest=json.loads((root/'current.json').read_text());manifest['database']='../seed.duckdb'
    (root/'current.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError,match='path'):i.resolve_current(root)


def test_missing_benchmark_or_session_never_becomes_completed_market(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    class Partial(FakeProvider):
        def fetch_market(self,*args,**kwargs):
            result=super().fetch_market(*args,**kwargs);result.bars=result.bars[result.bars.symbol!='SPY'];return result
    result=i.refresh(Partial(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    assert result['status']=='FAILED' and i.resolve_current(root) is None


def test_seed_quarantined_row_is_not_silently_reenabled_by_overlap(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    with duckdb.connect(str(source)) as db:
        db.execute('ALTER TABLE backtest_bars ADD COLUMN adjustment_ready BOOLEAN DEFAULT true')
        db.execute("UPDATE backtest_bars SET adjustment_ready=false WHERE symbol='A' AND session_date='2024-01-03'")
    result=i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    assert result['status']=='PUBLISHED'
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert db.execute("SELECT count(*) FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-03'").fetchone()[0]==0
        assert db.execute("SELECT count(*) FROM seed_backtest_bars WHERE NOT adjustment_ready").fetchone()[0]==1


def test_seed_split_baseline_is_not_reapplied_and_corrected_exdate_rebases(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    raw={'id':'seed-event','symbol':'A','old_rate':1,'new_rate':2,'ex_date':'2024-01-04'}
    with duckdb.connect(str(source)) as db:
        db.execute("UPDATE backtest_bars SET open=open*.5,high=high*.5,low=low*.5,close=close*.5,volume=volume*2,split_price_factor=.5,split_share_factor=2 WHERE symbol='A' AND session_date<'2024-01-04'")
        db.execute('CREATE TABLE split_events(market VARCHAR,symbol VARCHAR,effective_date DATE,price_factor DOUBLE,new_shares_per_old_share DOUBLE,source VARCHAR,source_id VARCHAR,source_record_json VARCHAR)')
        db.execute("INSERT INTO split_events VALUES('us','A','2024-01-04',.5,2,'alpaca_corporate_actions','seed-event',?)",[json.dumps(raw)])
    action=dict(provider='alpaca',event_id='seed-event',revision='fresh',market='us',symbol='A',effective_date='2024-01-04',price_factor=.5,share_factor=2.,kind='split',confirmed=True)
    assert i.refresh(FakeProvider(actions=[action]),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')['status']=='PUBLISHED'
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert db.execute("SELECT open FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-02'").fetchone()[0]==50
    assert i.refresh(FakeProvider(actions=[{**action,'revision':'revised-date','effective_date':'2024-01-05'}]),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')['status']=='PUBLISHED'
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert db.execute("SELECT open FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-04'").fetchone()[0]==50


def test_null_prices_and_invalid_benchmark_mid_window_cannot_publish(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb')
    class Invalid(FakeProvider):
        def fetch_market(self,*args,**kwargs):
            batch=super().fetch_market(*args,**kwargs);batch.bars.loc[batch.bars.symbol=='SPY','open']=0.;return batch
    assert i.refresh(Invalid(),seed_path=source,supplement_path=None,root=tmp_path/'managed',markets=('us',),now='2024-01-05T22:00Z')['status']=='FAILED'
    with duckdb.connect(str(source)) as db:db.execute("UPDATE backtest_bars SET open=NULL WHERE symbol='A'")
    with pytest.raises(ValueError):i.validate_database(source)


def test_failed_quarantine_retry_refetches_corrected_actions(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    action=dict(provider='x',event_id='s1',revision='1',market='us',symbol='A',effective_date='2024-01-05',price_factor=.5,share_factor=2.,kind='split',confirmed=False)
    assert i.refresh(FakeProvider(actions=[action]),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')['status']=='FAILED'
    assert i.refresh(FakeProvider(actions=[{**action,'confirmed':True}]),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')['status']=='PUBLISHED'


def test_seed_missing_turnover_remains_raw_but_is_quarantined(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    with duckdb.connect(str(source)) as db:db.execute("UPDATE backtest_bars SET turnover=NULL WHERE symbol='A' AND session_date='2024-01-03'")
    result=i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    assert result['status']=='PUBLISHED'
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert db.execute("SELECT count(*) FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-03'").fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM seed_backtest_bars WHERE turnover IS NULL').fetchone()[0]==1


def test_future_announced_action_survives_until_due_without_provider_repeat(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    future=dict(provider='alpaca',event_id='future',revision='1',market='us',symbol='A',effective_date='2024-01-08',price_factor=.5,share_factor=2.,kind='split',confirmed=True)
    first=i.refresh(FakeProvider(actions=[future]),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    assert first['status']=='PUBLISHED'
    second=i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-08T22:00Z')
    assert second['status']=='PUBLISHED'
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert db.execute("SELECT open FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-02'").fetchone()[0]==50
        assert db.execute("SELECT count(*) FROM ingest_pending_actions").fetchone()[0]==0


def test_ksd_relisting_requires_krx_reference_and_never_record_date(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed';mapping={'A':'005930','SPY':'069500','QQQ':'229200'}
    with duckdb.connect(str(source)) as db:
        for table in ('assets','backtest_bars'):
            db.execute(f"UPDATE {table} SET market='kr',symbol=CASE symbol WHEN 'A' THEN '005930' WHEN 'SPY' THEN '069500' ELSE '229200' END")
        db.execute("UPDATE assets SET exchange='KOSPI'");db.execute("UPDATE backtest_bars SET currency='KRW'");db.execute("UPDATE trading_calendar SET market='kr'")
    from price_lab.providers import parse_ksd_action
    action=parse_ksd_action({'sht_cd':'005930','record_date':'20240102','list_dt':'20240105','inter_bf_face_amt':'5000','inter_af_face_amt':'2500'})
    class KR(FakeProvider):
        def fetch_market(self,*args,**kwargs):
            batch=super().fetch_market(*args,**kwargs);batch.bars['symbol']=batch.bars.symbol.map(mapping);batch.bars['currency']='KRW';batch.bars['source']='krx'
            batch.bars['reference_previous_close']=100.
            mask=(batch.bars.symbol=='005930')&(batch.bars.session_date==pd.Timestamp('2024-01-05'))
            batch.bars.loc[mask,['open','high','low','close','vwap','reference_previous_close']]=[50,50.5,49.5,50,50,50]
            batch.actions=[action];return batch
    result=i.refresh(KR(),seed_path=source,supplement_path=None,root=root,markets=('kr',),now='2024-01-05T08:00Z')
    assert result['status']=='PUBLISHED'
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert db.execute("SELECT effective_date FROM ingest_actions WHERE symbol='005930'").fetchone()[0].isoformat()=='2024-01-05'
        assert db.execute("SELECT open FROM backtest_bars WHERE symbol='005930' AND session_date='2024-01-03'").fetchone()[0]==50


def test_verified_current_cache_revalidates_after_file_stat_changes(tmp_path,monkeypatch):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    i._verified.cache_clear();calls=[];original=i.sha256_file
    def counted(path):calls.append(str(path));return original(path)
    monkeypatch.setattr(i,'sha256_file',counted)
    current=i.resolve_current(root);i.resolve_current(root);assert len(calls)==1
    with open(current['resolved_path'],'ab') as stream:stream.write(b'tamper')
    with pytest.raises(ValueError,match='hash'):i.resolve_current(root)


def test_cached_checkpoint_still_rejects_new_wal(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    current=i.resolve_current(root);Path(current['resolved_path']+'.wal').write_bytes(b'uncommitted')
    with pytest.raises(ValueError,match='WAL'):i.resolve_current(root)


def test_conflicting_duplicate_action_revisions_are_quarantined(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    action=dict(provider='alpaca',event_id='same',revision='1',market='us',symbol='A',effective_date='2024-01-05',price_factor=.5,share_factor=2.,kind='split',confirmed=True)
    result=i.refresh(FakeProvider(actions=[action,{**action,'revision':'conflict','price_factor':.25,'share_factor':4.}]),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    assert result['status']=='FAILED' and i.resolve_current(root) is None


def test_all_selected_markets_commit_together_and_retry_reuses_good_batch(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    with duckdb.connect(str(source)) as db:
        db.execute("INSERT INTO assets VALUES('crypto','BTC/USD','Bitcoin','CRYPTO')")
        db.execute("INSERT INTO trading_calendar SELECT 'crypto',session_date FROM trading_calendar WHERE market='us'")
        db.execute("INSERT INTO backtest_bars SELECT 'crypto','BTC/USD',session_date,open,high,low,close,volume,turnover,vwap,raw_open,raw_close,raw_volume,split_price_factor,split_share_factor,currency FROM backtest_bars WHERE symbol='SPY'")
    class Crypto(FakeProvider):
        def fetch_market(self,*args,**kwargs):
            batch=super().fetch_market(*args,**kwargs);batch.bars=batch.bars[batch.bars.symbol=='A'].copy();batch.bars['symbol']='BTC/USD';return batch
    us=FakeProvider();crypto=Crypto(fail=True);sources={'us':us,'crypto':crypto}
    assert i.refresh(sources,seed_path=source,supplement_path=None,root=root,markets=('us','crypto'),now='2024-01-05T22:00Z')['status']=='FAILED'
    assert i.resolve_current(root) is None
    crypto.fail=False
    crypto.calendar_evidence={'crypto':{'2024-01-01':{'open':False,'provider':'fixture'}}}
    assert i.refresh(sources,seed_path=source,supplement_path=None,root=root,markets=('us','crypto'),now='2024-01-05T22:00Z')['status']=='PUBLISHED'
    assert len(us.calls)==1 and len(crypto.calls)==2


def test_optional_official_calendar_reconciler_filters_uncollected_holiday(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    class Reconciled(FakeProvider):
        calendar_evidence={'us':{'2024-01-05':{'open':False,'provider':'fixture'}}}
        def reconcile_sessions(self,market,all_dates,known_sessions):
            assert pd.Timestamp('2024-01-04') in known_sessions
            return [d for d in all_dates if d!=pd.Timestamp('2024-01-05')]
    provider=Reconciled()
    result=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    assert result['status']=='PUBLISHED' and result['manifest']['markets']['us']['actual']=='2024-01-04'
    assert max(provider.calls[0])==pd.Timestamp('2024-01-04')
    assert result['manifest']['calendar_evidence']['us']['2024-01-05']['open'] is False


def test_merged_and_new_kis_benchmark_bars_keep_supplement_lineage(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');supp=seed(tmp_path/'supp.duckdb');root=tmp_path/'managed';mapping={'A':'005930','SPY':'069500','QQQ':'229200'}
    for path in (source,supp):
        with duckdb.connect(str(path)) as db:
            for table in ('assets','backtest_bars'):db.execute(f"UPDATE {table} SET market='kr',symbol=CASE symbol WHEN 'A' THEN '005930' WHEN 'SPY' THEN '069500' ELSE '229200' END")
            db.execute("UPDATE backtest_bars SET currency='KRW'");db.execute("UPDATE trading_calendar SET market='kr'")
            db.execute("DELETE FROM backtest_bars WHERE "+("symbol<>'005930'" if path==source else "symbol='005930'"))
    class KR(FakeProvider):
        def fetch_market(self,*args,**kwargs):
            batch=super().fetch_market(*args,**kwargs);batch.bars['symbol']=batch.bars.symbol.map(mapping);batch.bars['currency']='KRW';return batch
    result=i.refresh(KR(),seed_path=source,supplement_path=supp,root=root,markets=('kr',),now='2024-01-05T08:00Z')
    assert result['status']=='PUBLISHED'
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert db.execute("SELECT count(*) FROM backtest_bars WHERE symbol IN('069500','229200') AND NOT is_supplement").fetchone()[0]==0
        assert db.execute("SELECT count(*) FROM backtest_bars WHERE symbol='005930' AND is_supplement").fetchone()[0]==0
        assert db.execute("SELECT count(*) FROM backtest_bars WHERE symbol IN('069500','229200') AND session_date='2024-01-05' AND is_supplement").fetchone()[0]==2


def test_calendar_provider_failure_is_recorded_without_publishing(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    from price_lab.providers import ProviderError
    class CalendarFailure(FakeProvider):
        def reconcile_sessions(self,*args):raise ProviderError('KIS calendar requested day missing')
    result=i.refresh(CalendarFailure(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    assert result['status']=='FAILED' and 'calendar' in result['reason']
    assert list((root/'attempts').glob('*/status.json')) and i.resolve_current(root) is None


def test_ksd_official_ongoing_halt_defers_but_unverified_resume_blocks(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed';mapping={'A':'005930','SPY':'069500','QQQ':'229200'}
    with duckdb.connect(str(source)) as db:
        for table in ('assets','backtest_bars'):db.execute(f"UPDATE {table} SET market='kr',symbol=CASE symbol WHEN 'A' THEN '005930' WHEN 'SPY' THEN '069500' ELSE '229200' END")
        db.execute("UPDATE backtest_bars SET currency='KRW'");db.execute("UPDATE trading_calendar SET market='kr'")
    from price_lab.providers import parse_ksd_action
    action=parse_ksd_action({'sht_cd':'005930','record_date':'20240104','list_dt':'','td_stop_dt':'2024/01/04 ~','inter_bf_face_amt':'5000','inter_af_face_amt':'2500'})
    class Halted(FakeProvider):
        def fetch_market(self,*args,**kwargs):
            batch=super().fetch_market(*args,**kwargs);batch.bars['symbol']=batch.bars.symbol.map(mapping);batch.bars['currency']='KRW';batch.bars['source']='krx'
            halted=(batch.bars.symbol=='005930')&batch.bars.session_date.between('2024-01-04','2024-01-05')
            batch.bars.loc[halted,['volume','turnover']]=0.;return batch
    provider=Halted(actions=[action])
    result=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('kr',),now='2024-01-05T08:00Z')
    assert result['status']=='PUBLISHED'
    before=(root/'current.json').read_bytes()
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        pending=json.loads(db.execute('SELECT payload FROM ingest_pending_actions').fetchone()[0])
        assert pending['pending_status']=='pending_unknown_effective' and pending['missing_effective_date'] is True
        assert pending['halt_start']=='2024-01-04' and pending['zero_volume_sessions']==2
        assert db.execute("SELECT open,split_price_factor FROM backtest_bars WHERE symbol='005930' AND session_date='2024-01-02'").fetchone()==(100.,1.)
    provider.actions=[]
    resumed=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('kr',),now='2024-01-08T08:00Z')
    assert resumed['status']=='FAILED' and (root/'current.json').read_bytes()==before


def test_raw_initializer_keeps_nontradable_seed_ohlcv_for_halt_evidence(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb')
    with duckdb.connect(str(source)) as db:
        db.execute('ALTER TABLE backtest_bars ADD COLUMN adjustment_ready BOOLEAN DEFAULT true')
        db.execute("UPDATE backtest_bars SET adjustment_ready=false WHERE symbol='A' AND session_date='2024-01-03'")
        db.execute("CREATE TABLE ohlcv AS SELECT market,symbol,session_date,raw_open AS open,high,low,raw_close AS close,raw_volume AS volume,turnover,vwap,currency,'krx' AS source FROM backtest_bars")
        db.execute("UPDATE ohlcv SET open=0,high=0,low=0,close=0,volume=0,turnover=0 WHERE symbol='A' AND session_date='2024-01-03'")
        i._initialize(db,None)
        assert db.execute("SELECT volume,open FROM ingest_raw_bars WHERE symbol='A' AND session_date='2024-01-03'").fetchone()==(0.,0.)
        assert db.execute("SELECT count(*) FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-03'").fetchone()[0]==0


def test_full_audit_collects_at_most_twenty_sessions_per_part(tmp_path,monkeypatch):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed';dates=list(pd.bdate_range('2024-01-02',periods=40))
    monkeypatch.setattr(i,'completed_sessions',lambda *args:dates)
    class Bounded(FakeProvider):
        def fetch_market(self,market,sessions,assets,audit=False):
            assert len(sessions)<=20,'audit collection must be bounded'
            return super().fetch_market(market,sessions,assets,audit)
    provider=Bounded()
    out=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-04-01T22:00Z',audit=True)
    assert out['status']=='PUBLISHED' and [len(d) for d in provider.calls]==[20,20]
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert db.execute('SELECT count(*) FROM backtest_bars').fetchone()[0]==120


def test_audit_second_part_failure_preserves_current_and_reuses_first(tmp_path,monkeypatch):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    before=(root/'current.json').read_bytes();dates=list(pd.bdate_range('2024-01-02',periods=40));monkeypatch.setattr(i,'completed_sessions',lambda *args:dates)
    class FailsSecond(FakeProvider):
        fail_second=True
        def fetch_market(self,market,sessions,assets,audit=False):
            assert len(sessions)<=20,'audit collection must be bounded'
            if len(self.calls)==1 and self.fail_second:
                self.calls.append(list(sessions));raise RuntimeError('transient second part failure')
            return super().fetch_market(market,sessions,assets,audit)
    provider=FailsSecond()
    first=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-04-01T22:00Z',audit=True)
    assert first['status']=='FAILED' and (root/'current.json').read_bytes()==before
    provider.fail_second=False
    again=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-04-01T22:00Z',audit=True)
    assert again['status']=='PUBLISHED' and [len(d) for d in provider.calls]==[20,20,20]


def test_audit_actions_cover_nontrading_gap_between_parts(tmp_path,monkeypatch):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed';dates=list(pd.bdate_range('2024-01-08',periods=40))
    monkeypatch.setattr(i,'completed_sessions',lambda *args:dates)
    class PartProvider(FakeProvider):
        action_windows=[]
        def fetch_market_part(self,market,sessions,assets,audit=False,action_start=None):
            self.action_windows.append((pd.Timestamp(action_start),sessions[-1]))
            batch=super().fetch_market(market,sessions,assets,audit)
            if pd.Timestamp(action_start)<=pd.Timestamp('2024-02-03')<=sessions[-1]:
                batch.actions=[dict(provider='alpaca',event_id='weekend-event',revision='1',market='us',symbol='A',effective_date='2024-02-05',price_factor=.5,share_factor=2.,kind='split',confirmed=True)]
            return batch
    provider=PartProvider()
    out=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-04-01T22:00Z',audit=True)
    assert out['status']=='PUBLISHED' and provider.action_windows==[(dates[0],dates[19]),(pd.Timestamp('2024-02-03'),dates[-1])]
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert db.execute("SELECT open FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-02'").fetchone()[0]==50.


def test_audit_fills_old_hole_using_audited_seed_split_baseline(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    raw={'id':'seed-event','symbol':'A','old_rate':1,'new_rate':2,'ex_date':'2024-01-04'}
    with duckdb.connect(str(source)) as db:
        db.execute("UPDATE backtest_bars SET open=open*.5,high=high*.5,low=low*.5,close=close*.5,volume=volume*2,split_price_factor=.5,split_share_factor=2 WHERE symbol='A' AND session_date<'2024-01-04'")
        db.execute("DELETE FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-02'")
        db.execute('CREATE TABLE split_events(market VARCHAR,symbol VARCHAR,effective_date DATE,price_factor DOUBLE,new_shares_per_old_share DOUBLE,source VARCHAR,source_id VARCHAR,source_record_json VARCHAR)')
        db.execute("INSERT INTO split_events VALUES('us','A','2024-01-04',.5,2,'alpaca_corporate_actions','seed-event',?)",[json.dumps(raw)])
    out=i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z',audit=True)
    assert out['status']=='PUBLISHED'
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert db.execute("SELECT open,raw_open,volume,raw_volume,base_price_factor FROM managed_bars WHERE symbol='A' AND session_date='2024-01-02'").fetchone()==(50.,100.,2000.,1000.,.5)
        assert db.execute("SELECT open FROM managed_bars WHERE symbol='A' AND session_date='2024-01-03'").fetchone()[0]==50.


def test_audit_publishes_new_tradable_row_even_when_preserved_raw_is_identical(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    with duckdb.connect(str(source)) as db:
        db.execute("CREATE TABLE ohlcv AS SELECT market,symbol,session_date,raw_open AS open,high,low,raw_close AS close,raw_volume AS volume,turnover,vwap,currency,'provider' AS source FROM backtest_bars")
        db.execute("DELETE FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-02'")
    first=i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z',overlap_sessions=1)
    assert first['status']=='PUBLISHED'
    audited=i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z',audit=True)
    assert audited['status']=='PUBLISHED' and audited['manifest']['sequence']==2
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert db.execute("SELECT open FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-02'").fetchone()==(100.,)
def test_revision_returning_to_seed_factor_restores_all_old_prices(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    with duckdb.connect(str(source)) as db:
        db.execute("UPDATE backtest_bars SET open=50,high=50.5,low=49.5,close=50,volume=2000,vwap=50,split_price_factor=.5,split_share_factor=2 WHERE symbol='A' AND session_date<'2024-01-04'")
        db.execute('CREATE TABLE split_events(market VARCHAR,symbol VARCHAR,effective_date DATE,price_factor DOUBLE,new_shares_per_old_share DOUBLE,source VARCHAR,source_id VARCHAR,source_record_json VARCHAR)')
        db.execute("INSERT INTO split_events VALUES('us','A','2024-01-04',.5,2,'alpaca','seed-event','{}')")
    original=dict(provider='alpaca',event_id='seed-event',revision='original',market='us',symbol='A',effective_date='2024-01-04',price_factor=.5,share_factor=2.,kind='split',confirmed=True)
    # One-session overlap deliberately leaves the first historical prices untouched by raw upsert.
    for action,expected in [({**original,'revision':'correction','price_factor':.25,'share_factor':4.},25.),({**original,'revision':'restored'},50.)]:
        out=i.refresh(FakeProvider(actions=[action]),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z',overlap_sessions=1)
        assert out['status']=='PUBLISHED'
        with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
            assert db.execute("SELECT close FROM backtest_bars WHERE symbol='A' AND session_date='2024-01-02'").fetchone()[0]==expected


def test_decimal_raw_storage_does_not_publish_identical_float_batch(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    with duckdb.connect(str(source)) as db:
        db.execute("CREATE TABLE ohlcv AS SELECT market,symbol,session_date,raw_open AS open,high,low,raw_close AS close,raw_volume AS volume,turnover::DECIMAL(38,12) AS turnover,vwap,currency,'provider' AS source FROM backtest_bars")
    class DecimalProvider(FakeProvider):
        value=2644.0155280000004
        def fetch_market(self,*args,**kwargs):
            batch=super().fetch_market(*args,**kwargs)
            batch.bars['turnover']=self.value
            return batch
    provider=DecimalProvider()
    first=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    assert first['status']=='PUBLISHED'
    before=(root/'current.json').read_bytes()
    for _ in range(2):
        again=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
        assert again['status']=='UNCHANGED'
        assert (root/'current.json').read_bytes()==before
    provider.value+=.000001
    corrected=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    assert corrected['status']=='PUBLISHED' and corrected['manifest']['sequence']==2
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert dict((r[1],r[2]) for r in db.execute("PRAGMA table_info('ingest_raw_bars')").fetchall())['turnover']=='DECIMAL(38,12)'


def test_publication_wait_preserves_current_and_catches_up_without_new_clock_date(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    class PublicationProvider(FakeProvider):
        ready=False
        def published_sessions(self,market,sessions,now=None):
            return sessions if self.ready else sessions[:-1]
    provider=PublicationProvider()
    first=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-08T22:00Z')
    assert first['status']=='PUBLISHED' and first['manifest']['markets']['us']['actual']=='2024-01-05'
    assert all(max(dates)<=pd.Timestamp('2024-01-05') for dates in provider.calls)
    provider.ready=True
    caught_up=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-08T22:00Z')
    assert caught_up['status']=='PUBLISHED' and caught_up['manifest']['markets']['us']['actual']=='2024-01-08'
    before=(root/'current.json').read_bytes();provider.ready=False
    kept=i.refresh(provider,seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-08T22:00Z')
    assert kept['status']=='UNCHANGED' and (root/'current.json').read_bytes()==before


def test_publication_hook_cannot_hide_a_historical_calendar_hole(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    class InvalidPublication(FakeProvider):
        def published_sessions(self,market,sessions,now=None):return sessions[1:]
    out=i.refresh(InvalidPublication(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-08T22:00Z')
    assert out['status']=='FAILED' and i.resolve_current(root) is None


def test_publication_hook_cannot_defer_old_missing_tail(tmp_path):
    i=module();source=seed(tmp_path/'seed.duckdb');root=tmp_path/'managed'
    class StalePublication(FakeProvider):
        def published_sessions(self,market,sessions,now=None):return sessions[:-2]
    out=i.refresh(StalePublication(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-08T22:00Z')
    assert out['status']=='FAILED' and i.resolve_current(root) is None

