import json
from pathlib import Path

import duckdb
import pytest

from price_lab import history_backfill as h, ingestion as i
from price_lab import history_integration as u
from test_history_backfill import plan, Provider
from test_history_promotion import managed


def archive(tmp_path):
    path=tmp_path/'archive';plan(path);h.collect(path,Provider())
    return path


def test_incomplete_waits_without_opening_writer_database(tmp_path):
    path=tmp_path/'archive';plan(path);h.collect(path,Provider(),max_parts=1)
    result=u.archive_state(path)
    assert result['status']=='WAITING_COLLECTION' and not result['consumer_ready']


def test_raw_union_preserves_price_parent_and_repeat_is_noop(tmp_path):
    root=managed(tmp_path);before=i.resolve_current(root);path=archive(tmp_path)
    result=u.integrate_archives([path],root=root)
    assert result['status']=='PUBLISHED'
    current=i.resolve_current(root)
    assert current['parent_sha256']==before['sha256']
    assert i.sha256_file(before['resolved_path'])==before['sha256']
    with duckdb.connect(current['resolved_path'],read_only=True) as db:
        assert db.execute('SELECT count(*) FROM history_raw_bars').fetchone()[0]==5
        assert db.execute("SELECT count(*) FROM backtest_bars WHERE market='crypto'").fetchone()[0]==1
        assert db.execute('SELECT status FROM history_archive_registry').fetchone()[0]=='RAW_VALIDATED'
    assert u.integrate_archives([path],root=root)['status']=='UNCHANGED'


def test_tampered_archive_does_not_publish(tmp_path):
    root=managed(tmp_path);before=(root/'current.json').read_bytes();path=archive(tmp_path)
    (path/'parts/00000/bars.parquet').write_bytes(b'bad')
    with pytest.raises(ValueError,match='hash'):u.integrate_archives([path],root=root)
    assert (root/'current.json').read_bytes()==before


def test_asset_observation_tamper_before_old_archive_seal_is_rejected(tmp_path):
    path=tmp_path/'archive';plan(path);h.collect(path,Provider(),max_parts=1)
    with duckdb.connect(str(path/'raw_history.duckdb')) as db:
        db.execute("UPDATE asset_observations SET payload=replace(payload,'Bitcoin','CHANGED BEFORE SEAL')")
    assert h.collect(path,Provider())['status']=='COMPLETE'
    with pytest.raises(ValueError,match='asset observations'):u.verify_archive(path)


def test_closed_digest_is_required_and_old_complete_can_be_sealed(tmp_path):
    path=archive(tmp_path);value=h._read(path/'status.json')
    value.pop('database_sha256');value.pop('raw_reconciled');i.atomic_json(path/'status.json',value)
    with pytest.raises(ValueError,match='sealed'):u.verify_archive(path)
    assert u.seal_completed_archive(path)['raw_reconciled']
    assert u.verify_archive(path)['rows']==5


def test_reference_only_merge_cannot_change_prices_and_is_idempotent(tmp_path):
    root=managed(tmp_path);candidate=tmp_path/'candidate';u.prepare_candidate_root(root,candidate)
    path=archive(tmp_path);u.integrate_archives([path],root=candidate)
    cp=i.resolve_current(candidate)
    first=u.merge_candidate(cp['resolved_path'],cp,root=root)
    assert first['status']=='PUBLISHED'
    assert u.merge_candidate(cp['resolved_path'],cp,root=root)['status']=='UNCHANGED'
    with duckdb.connect(i.resolve_current(root)['resolved_path'],read_only=True) as db:
        assert db.execute('SELECT count(*) FROM integration_candidate_receipts').fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM history_raw_bars').fetchone()[0]==5


def test_candidate_merges_onto_new_daily_parent(tmp_path):
    root=managed(tmp_path);candidate=tmp_path/'candidate';u.prepare_candidate_root(root,candidate)
    u.integrate_archives([archive(tmp_path)],root=candidate);cp=i.resolve_current(candidate)
    from test_ingestion import FakeProvider
    i.refresh(FakeProvider(),seed_path=tmp_path/'seed.duckdb',supplement_path=None,root=root,markets=('us',),now='2024-01-08T22:00Z')
    latest=i.resolve_current(root)
    u.merge_candidate(cp['resolved_path'],cp,root=root)
    current=i.resolve_current(root)
    assert current['parent_sha256']==latest['sha256']
    assert current['markets']['us']['actual']=='2024-01-08'


def test_pointer_failure_retries_without_daily_sequence_collision(tmp_path,monkeypatch):
    root=managed(tmp_path);path=archive(tmp_path);original=u.atomic_json
    def fail_pointer(p,v):
        if Path(p)==root/'current.json':raise OSError('pointer interrupted')
        return original(p,v)
    monkeypatch.setattr(u,'atomic_json',fail_pointer)
    with pytest.raises(OSError):u.integrate_archives([path],root=root)
    monkeypatch.setattr(u,'atomic_json',original)
    assert u.integrate_archives([path],root=root)['status']=='PUBLISHED'


def test_integration_status_never_confuses_raw_with_consumer_ready(tmp_path):
    root=managed(tmp_path);u.integrate_archives([archive(tmp_path)],root=root)
    report=u.integration_status(root)
    assert report['archives'][0]['raw_integrated']
    assert not report['archives'][0]['consumer_ready']
    assert report['archives'][0]['reasons']
