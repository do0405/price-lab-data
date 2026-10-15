import base64
import copy
import hashlib
import pytest
from price_lab import research_delta_codec as codec


def payload():
    body=b'{"OutBlock_1":[]}'
    return dict(schema='research-delta-v1',stream_id='kr-reference-evidence-v1',sequence=1,
        previous_package_sha256=None,bootstrap_anchor_sha256='a'*64,collected_at='2026-10-15T15:00:00Z',
        records=[dict(task=dict(source='krx_reference',board='KOSPI',kind='master',requested_date='2026-10-14',
            url='https://data-dbg.krx.co.kr/svc/apis/sto/stk_isu_base_info',method='GET',params={'basDd':'20261014'}),
            retrieved_at='2026-10-15T15:00:00Z',body_base64=base64.b64encode(body).decode(),
            body_sha256=hashlib.sha256(body).hexdigest(),response_metadata={})],
        state=dict(version='research-collection-state-v1',start_date='2010-01-01',end_date='2026-10-14',
            sessions=['2026-10-14'],events=[],pending={'krx':[],'kis':[],'kind':[]},seen={},completed=[],
            index_pages={},cursors={'provider':0,'audit':0,'kind_turn':0},
            discovery={'list_date':None,'kis_end_date':'2026-10-14','report_names':['상장폐지'],
                'lookback_days':90,'documents':{}},cooldowns={},source_hashes=[]))


def test_authenticated_roundtrip_and_tamper(tmp_path):
    p=payload();path=tmp_path/'research.pldelta';key=b'x'*32
    codec.seal_package(p,path,key)
    assert codec.open_package(path,key)==p
    raw=bytearray(path.read_bytes());raw[-1]^=1;path.write_bytes(raw)
    with pytest.raises(ValueError,match='authentication'):codec.open_package(path,key)


@pytest.mark.parametrize('change',[
    lambda p:p['records'][0]['task'].update(url='https://example.com/'),
    lambda p:p['records'][0]['task'].update(destination='D:/data'),
    lambda p:p['records'][0].update(body_sha256='b'*64),
    lambda p:p['state'].update(sql='select 1'),
    lambda p:p.update(stream_id='price-daily-v1'),
    lambda p:p['records'][0]['task']['params'].update(AUTH_KEY='secret'),
])
def test_reject_untrusted_fields(change):
    p=payload();change(p)
    with pytest.raises(ValueError):codec.validate_payload(p)


def test_bootstrap_and_control_have_distinct_authentication_domains():
    p=payload();body=dict(schema='research-bootstrap-v1',anchor_sha256='a'*64,state=p['state'],
        quota={'quota_day':'2026-10-16','used':{'krx':1000,'kis':0,'kind':0}})
    key=b'x'*32;signed=codec.sign_bootstrap(body,key)
    assert codec.verify_bootstrap(signed,key)==body
    with pytest.raises(ValueError):codec.verify_control(signed,key)


def test_cross_domain_daily_cipher_rejected(tmp_path):
    p=payload();path=tmp_path/'x';codec.seal_package(p,path,b'x'*32)
    from price_lab.daily_delta_codec import open_package
    with pytest.raises(ValueError):open_package(path,b'x'*32)
