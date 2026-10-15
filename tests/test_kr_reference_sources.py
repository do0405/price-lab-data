import gzip
import json
from decimal import Decimal

import pytest

from price_lab.kr_reference_sources import (
    read_completed_raw, parse_capitalization, parse_master, parse_kind_export,
    inspect_payload, SourceContractError, RawNotReady,
)


def daily(day='20240102', cap='100,000,000', shares='1,000,000', close='100'):
    return {'OutBlock_1': [{'BAS_DD': day, 'MKT_NM': 'KOSPI', 'ISU_CD': '000001',
        'TDD_CLSPRC': close, 'MKTCAP': cap, 'LIST_SHRS': shares}]}


def test_cap_preserves_exact_integer_units_zero_and_missing():
    rows = parse_capitalization(daily(), board='KOSPI', requested_date='2024-01-02')
    assert rows[0]['market_cap_krw'] == Decimal(100_000_000)
    assert rows[0]['listed_shares'] == 1_000_000
    assert rows[0]['raw_symbol'] == '000001'
    zero = parse_capitalization(daily(cap='0', shares='-', close='0'), board='KOSPI')[0]
    assert zero['market_cap_krw'] == 0 and zero['listed_shares'] is None
    assert zero['quality_status'] != 'VALID'
    assert zero['null_reasons']['listed_shares'] == 'missing_marker'


@pytest.mark.parametrize('field,value', [('MKTCAP','1.5'), ('LIST_SHRS','NaN'), ('LIST_SHRS','9223372036854775808')])
def test_bad_integer_units_rejected(field, value):
    payload = daily(); payload['OutBlock_1'][0][field] = value
    with pytest.raises(SourceContractError): parse_capitalization(payload, board='KOSPI')


def test_cap_date_board_and_duplicate_contract():
    with pytest.raises(SourceContractError):
        parse_capitalization(daily(), board='KOSDAQ')
    with pytest.raises(SourceContractError):
        parse_capitalization(daily(), board='KOSPI', requested_date='2024-01-03')
    payload = daily(); payload['OutBlock_1'] *= 2
    with pytest.raises(SourceContractError): parse_capitalization(payload, board='KOSPI')


def test_completed_raw_crc_hash_and_inflight_protection(tmp_path):
    path = tmp_path / 'one.json.gz'
    with gzip.open(path, 'wt', encoding='utf-8') as stream: json.dump(daily(), stream)
    result = read_completed_raw(path, min_age_seconds=0)
    assert len(result.sha256) == 64 and result.payload == daily()
    assert inspect_payload(result.payload)['board'] == 'KOSPI'
    with pytest.raises(RawNotReady): read_completed_raw(path, min_age_seconds=600)
    path.write_bytes(path.read_bytes()[:-7])
    with pytest.raises(RawNotReady): read_completed_raw(path, min_age_seconds=0)


def master(listed='20000101', isin='KR7000001001'):
    return {'OutBlock_1': [{'ISU_CD': isin, 'ISU_SRT_CD':'000001', 'MKT_TP_NM':'KOSPI',
        'LIST_DD':listed, 'SECUGRP_NM':'주권','KIND_STKCERT_TP_NM':'보통주', 'ISU_NM':'Example'}]}


def test_historical_master_current_membership_response_is_rejected():
    with pytest.raises(SourceContractError, match='listing date'):
        parse_master(master('20240101'), board='KOSPI', requested_date='2010-01-04')


def test_master_snapshot_does_not_project_identity_back_to_listing_date():
    row = parse_master(master(), board='KOSPI', requested_date='2010-01-04')[0]
    assert str(row['effective_from']) == '2010-01-04'
    assert str(row['effective_to']) == '2010-01-05'
    assert row['listing_date'].isoformat() == '2000-01-01'
    assert row['availability_basis'] == 'EFFECTIVE_DATE_RECONSTRUCTED'
    assert row['security_type'] == 'stock'


def test_kind_import_never_infers_unknown_recovery_or_publication(tmp_path):
    path=tmp_path/'kind.csv'
    path.write_text('event_id,raw_symbol,board,event_type,effective_date,payment_date,cash_per_raw_share,status\nE1,000001,KOSPI,DELISTING,2024-01-03,,,UNKNOWN\n',encoding='utf-8')
    delta=parse_kind_export(path, source_url='https://kind.krx.co.kr/investwarn/delcompany.do', retrieved_at='2024-01-05T00:00:00Z')
    assert delta['events'][0]['effective_date'].isoformat()=='2024-01-03'
    assert delta['settlements'][0]['cash_per_raw_share'] is None
    assert delta['settlements'][0]['status']=='UNKNOWN'
    assert delta['events'][0]['source_published_at'] is None


def test_kind_import_rejects_unofficial_source(tmp_path):
    path=tmp_path/'x.csv'; path.write_text('event_id\n1\n')
    with pytest.raises(SourceContractError): parse_kind_export(path,source_url='https://example.com/x')
