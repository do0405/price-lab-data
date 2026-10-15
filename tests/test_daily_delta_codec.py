import copy
import json

import pytest


def payload():
    assets=[{'market':m,'symbol':s,'name':s,'exchange':'test'} for m,s in
            [('us','SPY'),('kr','069500'),('crypto','BTC/USD')]]
    state={'assets':assets,'sessions':{m:['2024-01-03','2024-01-04'] for m in ('us','kr','crypto')},
           'watermarks':{m:'2024-01-04' for m in ('us','kr','crypto')},'calendar_evidence':{},'audit_cursor':{}}
    parts=[]
    for asset in assets:
        m=asset['market']
        bar=dict(market=m,symbol=asset['symbol'],session_date='2024-01-04',open=10.,high=11.,low=9.,close=10.,
                 volume=100.,turnover=1000.,vwap=10.,currency='KRW' if m=='kr' else 'USD',source='test',reference_previous_close=None)
        if m=='kr':bar['is_supplement']=True
        parts.append(dict(market=m,sessions=['2024-01-04'],action_start='2024-01-04',bars=[bar],assets=[asset],actions=[],provenance={'actions_complete':True,'benchmarks_complete':True}))
    return dict(schema='price-delta-v1',stream_id='price-daily-v1',sequence=1,previous_package_sha256=None,
                bootstrap_anchor_sha256='a'*64,collected_at='2024-01-05T20:00:00Z',state=state,parts=parts,reference_raw=[])


def test_authenticated_package_roundtrip_and_canonical_digest(tmp_path):
    from price_lab import daily_delta_codec as c
    original=payload();path=tmp_path/'daily.pldelta';result=c.seal_package(original,path,b'x'*32)
    assert c.open_package(path,b'x'*32)==original
    assert result['package_sha256']==c.package_digest(original)
    assert result['package_sha256']==c.package_digest(dict(reversed(list(original.items()))))
    assert result['sequence']==1 and result['stream_id']=='price-daily-v1'
    assert b'SPY' not in path.read_bytes() and b'BTC/USD' not in path.read_bytes()


@pytest.mark.parametrize('mutation',['sequence','previous','date','watermark','duplicate','unknown','nan','secret'])
def test_payload_schema_rejects_unsafe_or_inconsistent_values(mutation):
    from price_lab import daily_delta_codec as c
    value=payload()
    if mutation=='sequence':value['sequence']=True
    elif mutation=='previous':value['sequence']=2
    elif mutation=='date':value['parts'][0]['sessions']=['2024-99-01']
    elif mutation=='watermark':value['state']['watermarks']['us']='2024-01-03'
    elif mutation=='duplicate':value['parts'][0]['bars']*=2
    elif mutation=='unknown':value['sql']='DROP TABLE bars'
    elif mutation=='nan':value['parts'][0]['bars'][0]['close']=float('nan')
    elif mutation=='secret':value['parts'][0]['provenance']['ALPACA_API_KEY']='never serialize'
    with pytest.raises(ValueError):c.validate_payload(value)


def test_wrong_key_and_tamper_fail_before_json_or_gzip(tmp_path,monkeypatch):
    from price_lab import daily_delta_codec as c
    path=tmp_path/'daily';c.seal_package(payload(),path,b'x'*32)
    monkeypatch.setattr(c.json,'loads',lambda *a,**k:pytest.fail('unauthenticated JSON parsed'))
    monkeypatch.setattr(c.gzip,'GzipFile',lambda *a,**k:pytest.fail('unauthenticated gzip decompressed'))
    with pytest.raises(ValueError,match='authentication'):c.open_package(path,b'y'*32)
    data=bytearray(path.read_bytes());data[-1]^=1;path.write_bytes(data)
    with pytest.raises(ValueError,match='authentication'):c.open_package(path,b'x'*32)


def test_raw_and_header_limits_apply_before_output_or_decompression(tmp_path,monkeypatch):
    from price_lab import daily_delta_codec as c
    path=tmp_path/'daily';c.seal_package(payload(),path,b'x'*32)
    monkeypatch.setattr(c,'MAX_RAW_BYTES',1024)
    with pytest.raises(ValueError,match='limit'):c.open_package(path,b'x'*32)
    with pytest.raises(ValueError,match='limit'):c.seal_package(payload(),tmp_path/'too-big',b'x'*32)
    assert not (tmp_path/'too-big').exists()
    monkeypatch.setattr(c,'MAX_RAW_BYTES',512*1024**2);monkeypatch.setattr(c,'MAX_HEADER_BYTES',16)
    with pytest.raises(ValueError,match='header'):c.seal_package(payload(),tmp_path/'header',b'x'*32)


def test_bootstrap_hmac_is_domain_bound_and_returns_verified_body():
    from price_lab import daily_delta_codec as c
    value=payload();body={k:copy.deepcopy(value['state'][k]) for k in ('assets','sessions','watermarks','calendar_evidence')}
    body.update(schema='price-delta-bootstrap-v1',anchor_sha256='a'*64)
    signed=c.sign_bootstrap(body,b'x'*32)
    assert c.verify_bootstrap(signed,b'x'*32)==body
    changed=copy.deepcopy(signed);changed['assets'][0]['name']='changed'
    with pytest.raises(ValueError):c.verify_bootstrap(changed,b'x'*32)
    with pytest.raises(ValueError):c.verify_bootstrap(signed,b'y'*32)


def test_codec_import_does_not_load_database_modules():
    from pathlib import Path
    import subprocess,sys,os
    code="import sys; import price_lab.daily_delta_codec; assert not any(k in sys.modules for k in ('duckdb','price_lab.ingestion','price_lab.cloud_data'))"
    run=subprocess.run([sys.executable,'-c',code],env={**os.environ,'PYTHONPATH':str(Path(__file__).resolve().parents[1]/'src')},capture_output=True,text=True)
    assert run.returncode==0,run.stderr


@pytest.mark.parametrize('days',[[{}],[None],[1,'2024-01-04']])
def test_malformed_session_types_raise_fixed_validation_error(days):
    from price_lab import daily_delta_codec as c
    value=payload();value['state']['sessions']['us']=days
    with pytest.raises(ValueError):c.validate_payload(value)


def test_ordinary_invalid_ohlc_remains_exact_authenticated_raw_evidence(tmp_path):
    from price_lab import daily_delta_codec as c
    value=payload();ordinary={**value['parts'][0]['bars'][0],'symbol':'HALTED',
        'open':0.,'high':0.,'low':0.,'close':100.,'volume':0.,'turnover':0.,'vwap':0.}
    value['parts'][0]['bars'].append(ordinary)
    assert c.validate_payload(value) is value
    path=tmp_path/'halted.pldelta';c.seal_package(value,path,b'x'*32)
    assert c.open_package(path,b'x'*32)['parts'][0]['bars'][-1]==ordinary
