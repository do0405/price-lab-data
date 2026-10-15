from pathlib import Path
import duckdb
import pandas as pd
import pytest
from price_lab import history_backfill as h,ingestion as i
from price_lab.history_promotion import promote_crypto
from test_ingestion import seed,FakeProvider
from test_history_backfill import Provider,plan


def managed(tmp_path):
    source=seed(tmp_path/'seed.duckdb')
    with duckdb.connect(str(source)) as db:
        db.execute("INSERT INTO assets VALUES('crypto','BTC/USD','Bitcoin','CRYPTO')")
        db.execute("INSERT INTO trading_calendar VALUES('crypto','2021-01-06')")
        db.execute("INSERT INTO backtest_bars SELECT 'crypto','BTC/USD',DATE '2021-01-06',100,101,99,100,1000,100000,100,100,100,1000,1,1,'USD'")
    root=tmp_path/'managed'
    i.refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    return root


def test_crypto_promotion_preserves_parent_and_adds_raw_and_consumer(tmp_path):
    root=managed(tmp_path);parent=i.resolve_current(root);before=i.sha256_file(parent['resolved_path'])
    archive=tmp_path/'archive';plan(archive);h.collect(archive,Provider())
    result=promote_crypto(archive,root=root)
    assert result['consumer_ready'] and result['rows_added']==5
    current=i.resolve_current(root)
    assert current['sequence']==parent['sequence']+1 and current['parent_sha256']==before
    assert i.sha256_file(parent['resolved_path'])==before
    with duckdb.connect(str(current['resolved_path']),read_only=True) as db:
        assert db.execute("SELECT count(*) FROM backtest_bars WHERE market='crypto'").fetchone()[0]==6
        assert db.execute("SELECT count(*) FROM ingest_raw_bars WHERE market='crypto'").fetchone()[0]==6
        assert db.execute("SELECT open,raw_open,split_price_factor FROM backtest_bars WHERE market='crypto' AND session_date='2021-01-01'").fetchone()==(10,10,1)


def test_incomplete_or_tampered_history_cannot_replace_current(tmp_path):
    root=managed(tmp_path);before=(root/'current.json').read_bytes()
    archive=tmp_path/'archive';plan(archive);h.collect(archive,Provider(),max_parts=1)
    with pytest.raises(ValueError,match='incomplete'):promote_crypto(archive,root=root)
    h.collect(archive,Provider())
    next((archive/'parts').rglob('bars.parquet')).write_bytes(b'bad')
    with pytest.raises(ValueError,match='hash'):promote_crypto(archive,root=root)
    assert (root/'current.json').read_bytes()==before


def test_unsupported_equity_history_is_explicit(tmp_path):
    archive=tmp_path/'archive'
    assets=pd.DataFrame([dict(market='us',symbol='SPY')])
    h.create_plan(archive,market='us',start='2016-01-04',end='2016-01-04',assets=assets,sessions=[pd.Timestamp('2016-01-04')])
    with pytest.raises(ValueError,match='corporate-action'):promote_crypto(archive,root=tmp_path/'market')


def test_raw_database_tamper_blocks_promotion(tmp_path):
    root=managed(tmp_path);before=(root/'current.json').read_bytes()
    archive=tmp_path/'archive';plan(archive);h.collect(archive,Provider())
    with duckdb.connect(str(archive/'raw_history.duckdb')) as db:
        db.execute('UPDATE raw_bars SET close=999 WHERE session_date=(SELECT min(session_date) FROM raw_bars)')
    with pytest.raises(ValueError,match='database hash'):promote_crypto(archive,root=root)
    assert (root/'current.json').read_bytes()==before


def test_receipt_failure_after_commit_is_success_and_retry_is_noop(tmp_path,monkeypatch):
    from price_lab import history_promotion as p
    root=managed(tmp_path);archive=tmp_path/'archive';plan(archive);h.collect(archive,Provider())
    original=p.atomic_json
    def fail_receipt(path,value):
        if Path(path)==archive/'promotion.json':raise OSError('simulated receipt failure')
        return original(path,value)
    monkeypatch.setattr(p,'atomic_json',fail_receipt)
    result=p.promote_crypto(archive,root=root)
    assert result['status']=='PUBLISHED' and result['warnings']
    before=(root/'current.json').read_bytes()
    monkeypatch.setattr(p,'atomic_json',original)
    assert p.promote_crypto(archive,root=root)['status']=='ALREADY_PUBLISHED'
    assert (root/'current.json').read_bytes()==before


def test_pointer_failure_reuses_prepared_immutable_checkpoint(tmp_path,monkeypatch):
    from price_lab import history_promotion as p
    root=managed(tmp_path);archive=tmp_path/'archive';plan(archive);h.collect(archive,Provider())
    original=p.atomic_json
    def fail_pointer(path,value):
        if Path(path)==root/'current.json':raise OSError('simulated pointer failure')
        return original(path,value)
    before=(root/'current.json').read_bytes()
    monkeypatch.setattr(p,'atomic_json',fail_pointer)
    with pytest.raises(OSError):p.promote_crypto(archive,root=root)
    assert (root/'current.json').read_bytes()==before
    prepared=next((root/'attempts').glob('history-crypto-*/prepared-manifest.json'))
    dest=root/h._read(prepared)['database'];digest=i.sha256_file(dest)
    monkeypatch.setattr(p,'atomic_json',original)
    assert p.promote_crypto(archive,root=root)['status']=='PUBLISHED'
    assert i.sha256_file(dest)==digest


def test_interrupted_promotion_does_not_reserve_daily_sequence_filename(tmp_path,monkeypatch):
    from price_lab import history_promotion as p
    root=managed(tmp_path);archive=tmp_path/'archive';plan(archive);h.collect(archive,Provider())
    original=p.atomic_json
    def fail_pointer(path,value):
        if Path(path)==root/'current.json':raise OSError('pointer interruption')
        return original(path,value)
    monkeypatch.setattr(p,'atomic_json',fail_pointer)
    with pytest.raises(OSError):p.promote_crypto(archive,root=root)
    daily=i.refresh(FakeProvider(),seed_path=tmp_path/'seed.duckdb',supplement_path=None,root=root,markets=('us',),now='2024-01-08T22:00Z')
    assert daily['status']=='PUBLISHED'
    monkeypatch.setattr(p,'atomic_json',original)
    assert p.promote_crypto(archive,root=root)['sequence']==3
    assert i.resolve_current(root)['markets']['us']['actual']=='2024-01-08'
