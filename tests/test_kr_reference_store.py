from datetime import datetime, timezone
from decimal import Decimal
import duckdb
import pytest

from price_lab.kr_reference_schema import create_schema, REFERENCE_TABLES
from price_lab.kr_reference_sources import parse_capitalization, parse_master
from price_lab.kr_reference_store import apply_delta, select_asof, coverage_report, export_delta
from price_lab.kr_reference_validation import reconcile_cap, validate_reference
from test_kr_reference_sources import daily, master


def delta(cap='100000000', sha='a', publication='2024-01-02T09:00:00Z', retrieved='2024-01-02T09:01:00Z'):
    rows=parse_capitalization(daily(cap=cap),board='KOSPI',requested_date='2024-01-02')
    aliases=parse_master(master(),board='KOSPI',requested_date='2024-01-02')
    basis='PUBLICATION_TIME_VERIFIED' if publication else 'EFFECTIVE_DATE_RECONSTRUCTED'
    common=dict(source_id='fixture-official',manifest_id=sha*64,retrieved_at=retrieved,
        source_published_at=publication,availability_basis=basis)
    for row in rows+aliases: row.update(common)
    return {'manifests':[dict(common,raw_sha256=sha*64,source_url='https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd',
        requested_date='2024-01-02',board='KOSPI',record_count=1,completeness='COMPLETE')],
        'capitalization':rows,'aliases':aliases,'receipt_id':sha*64}


def test_append_only_idempotent_revisions_and_receipt_atomicity():
    db=duckdb.connect(); create_schema(db)
    assert apply_delta(db,delta())['inserted']>0
    assert apply_delta(db,delta())['inserted']==0
    apply_delta(db,delta('101000000','b','2024-03-01T00:00:00Z','2024-03-01T01:00:00Z'))
    assert db.execute('select count(*) from kr_capitalization_observations').fetchone()[0]==2
    assert db.execute('select count(*) from kr_reference_receipts').fetchone()[0]==2
    broken=delta(sha='c'); broken['capitalization'][0]['listed_shares']=-1
    with pytest.raises(ValueError): apply_delta(db,broken)
    assert db.execute("select count(*) from kr_source_manifest where manifest_id=?",['c'*64]).fetchone()[0]==0
    assert db.execute('select count(*) from kr_reference_receipts').fetchone()[0]==2


def test_publication_cutoff_excludes_future_revision_and_unknown_publication():
    db=duckdb.connect(); apply_delta(db,delta())
    apply_delta(db,delta('101000000','b','2024-03-01T00:00:00Z','2024-03-01T01:00:00Z'))
    rows=select_asof(db,'2024-02-01T00:00:00Z',asof_date='2024-01-02')
    assert len(rows)==1 and rows[0]['market_cap_krw']==Decimal(100_000_000)
    assert select_asof(db,'2024-01-02T08:59:00Z',asof_date='2024-01-02')==[]
    other=duckdb.connect(); apply_delta(other,delta(publication=None))
    assert select_asof(other,'2025-01-01T00:00:00Z')==[]
    assert len(select_asof(other,'2025-01-01T00:00:00Z',mode='effective_date_reconstructed'))==1
    assert select_asof(other,'2020-01-01T00:00:00Z',mode='effective_date_reconstructed')==[]


def test_identity_conflicts_are_quarantined_and_coverage_is_partial():
    db=duckdb.connect(); d=delta()
    extra=dict(d['aliases'][0],security_id='different-security',alias_id='conflicting-alias')
    d['aliases'].append(extra); apply_delta(db,d)
    assert select_asof(db,'2025-01-01T00:00:00Z')==[]
    report=validate_reference(db)
    assert report['status']=='PARTIAL' and report['identity_conflicts']>0
    assert coverage_report(db)['status']=='PARTIAL'


def test_explicit_reconstruction_lag_admits_baseline_but_freezes_later_unknown_revision():
    db=duckdb.connect(); d=delta(publication=None,retrieved='2026-01-01T00:00:00Z')
    for row in d['capitalization']+d['aliases']:
        row.update(research_available_at='2024-01-03T00:00:00Z',research_policy_version='first-observed-baseline-next-session-v1')
    apply_delta(db,d)
    assert select_asof(db,'2024-01-02T23:59:00Z',mode='effective_date_reconstructed')==[]
    assert select_asof(db,'2024-02-01T00:00:00Z',mode='effective_date_reconstructed')[0]['market_cap_krw']==100000000
    later=delta('999000000','b',publication=None,retrieved='2026-03-01T00:00:00Z')
    for row in later['capitalization']+later['aliases']:
        row.update(research_available_at='2024-01-03T00:00:00Z',research_policy_version='first-observed-baseline-next-session-v1')
    apply_delta(db,later)
    assert select_asof(db,'2024-02-01T00:00:00Z',mode='effective_date_reconstructed')[0]['market_cap_krw']==100000000
    assert select_asof(db,'2024-02-01T00:00:00Z')==[]


def test_reconcile_raw_share_split_and_mismatch_gate():
    assert reconcile_cap(dict(raw_close=100,listed_shares=1_000_000,market_cap_krw=100_000_000))['valid']
    assert reconcile_cap(dict(raw_close=50,listed_shares=2_000_000,market_cap_krw=100_000_000))['valid']
    assert not reconcile_cap(dict(raw_close=50,listed_shares=1_000_000,market_cap_krw=100_000_000))['valid']
    assert not reconcile_cap(dict(raw_close=0,listed_shares=1_000_000,market_cap_krw=0))['valid']


def test_export_delta_copies_all_reference_contract_tables():
    source=duckdb.connect(); apply_delta(source,delta())
    target=duckdb.connect(); exported=export_delta(source); apply_delta(target,exported)
    assert len(select_asof(target,'2025-01-01T00:00:00Z'))==1
    assert 'kr_capitalization_observations' in REFERENCE_TABLES


def test_read_only_validation_report_is_json_serializable(tmp_path):
    import json
    path=tmp_path/'x.duckdb'
    with duckdb.connect(str(path)) as db: apply_delta(db,delta())
    with duckdb.connect(str(path),read_only=True) as db:
        report=validate_reference(db)
        assert json.loads(json.dumps(report))['observations']==1


def test_unverified_alias_publication_and_lifecycle_cannot_claim_complete_pit():
    db=duckdb.connect(); d=delta()
    for row in d['aliases']:
        row.update(source_published_at=None,availability_basis='EFFECTIVE_DATE_RECONSTRUCTED')
    apply_delta(db,d)
    report=validate_reference(db,sessions=['2024-01-02'])
    assert report['publication_time_verified'] is False
    assert report['lifecycle_status']=='UNVERIFIED'
    assert report['status']=='PARTIAL'


def test_changed_content_cannot_reuse_an_existing_revision_key():
    db=duckdb.connect(); apply_delta(db,delta())
    changed=delta(sha='b'); changed['capitalization'][0]['listed_shares']=2_000_000
    with pytest.raises(ValueError,match='revision collision'): apply_delta(db,changed)
    assert db.execute('select count(*) from kr_reference_receipts').fetchone()[0]==1
