import base64
import json
from datetime import datetime,timezone
import pytest
from price_lab import research_delta_transport as transport
from price_lab import research_quota as quota


def test_research_tags_cannot_collide_with_daily():
    assert transport.package_tag(1,'a'*64).startswith('delta-research-')
    assert transport.ASSET=='research.pldelta'


def test_quota_is_durable_idempotent_and_does_not_reset_on_restart(monkeypatch):
    stored={};key=b'x'*32
    def api(method,path,**kwargs):
        if path.startswith('/commits?'):return []
        if method=='GET':
            if not stored:raise RuntimeError('HTTP 404')
            return {'sha':str(stored['revision']),'content':stored['content']}
        data=kwargs['payload']
        if stored and data.get('sha')!=str(stored['revision']):raise RuntimeError('HTTP 409')
        stored.update(content=data['content'],revision=stored.get('revision',0)+1)
        return {}
    monkeypatch.setattr(quota,'_api',api)
    now=datetime(2026,10,15,16,tzinfo=timezone.utc)
    initial={'quota_day':'2026-10-16','used':{'krx':7900,'kis':0,'kind':0},'allow_initialize':True}
    a=quota.reserve_requests({'krx':80},reservation_id='job1-0',initial=initial,now=now,key=key)
    assert a['status']=='GRANTED'
    assert quota.reserve_requests({'krx':80},reservation_id='job1-0',initial=initial,now=now,key=key)['status']=='GRANTED'
    b=quota.reserve_requests({'krx':30},reservation_id='job2-0',initial=initial,now=now,key=key)
    assert b['status']=='WAITING_QUOTA'
    with pytest.raises(ValueError):quota.reserve_requests({'krx':70},reservation_id='job1-0',initial=initial,now=now,key=key)


def test_missing_quota_after_stream_initialization_is_not_reset(monkeypatch):
    monkeypatch.setattr(quota,'_api',lambda *a,**k:(_ for _ in ()).throw(RuntimeError('HTTP 404')))
    with pytest.raises(ValueError):quota.reserve_requests({'kind':1},reservation_id='later',initial=None,key=b'x'*32)


def release(seq,digest,previous=None):
    tag=transport.package_tag(seq,digest)
    body=dict(purpose=transport.PURPOSE,schema_version=1,ready=True,stream_id='kr-reference-evidence-v1',
        sequence=seq,package_sha256=digest,previous_package_sha256=previous,ciphertext_sha256='f'*64)
    return dict(tag_name=tag,body=json.dumps(body),draft=False,prerelease=False,assets=[dict(id=seq,
        name=transport.ASSET,size=100,browser_download_url=f'https://github.com/{transport.REPOSITORY}/releases/download/{tag}/{transport.ASSET}')])


@pytest.mark.parametrize('rows,after,cursor',[
    ([release(1,'a'*64),release(3,'c'*64,'b'*64)],0,None),
    ([release(1,'a'*64),release(1,'b'*64)],0,None),
    ([release(1,'a'*64)],2,'b'*64),
    ([release(2,'b'*64,'a'*64)],1,'a'*64),
    ([release(1,'a'*64),release(2,'b'*64,'a'*64)],1,'c'*64),
])
def test_research_log_rejects_gaps_forks_rollback_and_missing_cursor(monkeypatch,rows,after,cursor):
    monkeypatch.setattr(transport,'_api',lambda *a,**k:rows)
    with pytest.raises(ValueError):transport.list_packages(after_sequence=after,expected_cursor_hash=cursor)


def test_existing_daily_releases_are_ignored(monkeypatch):
    rows=[{'tag_name':'delta-daily-0001','draft':False},release(1,'a'*64)]
    monkeypatch.setattr(transport,'_api',lambda *a,**k:rows)
    assert transport.list_packages()==[rows[1]]


def test_deleted_initialized_journal_is_not_recreated_before_first_package(monkeypatch):
    def api(method,path,**kwargs):
        if path.startswith('/commits?'):return [{'sha':'previous-initialization'}]
        raise RuntimeError('HTTP 404')
    monkeypatch.setattr(quota,'_api',api)
    with pytest.raises(ValueError,match='Missing'):
        quota.reserve_requests({'kind':1},reservation_id='retry',initial={'quota_day':'2026-10-16',
            'used':{},'allow_initialize':True},now=datetime(2026,10,15,16,tzinfo=timezone.utc),key=b'x'*32)


def test_signed_journal_cannot_regress_below_authenticated_packet_floor(monkeypatch):
    from price_lab.research_delta_codec import sign_control,_canonical
    body=dict(version='research-quota-v1',quota_day='2026-10-16',reserved={'krx':100,'kis':0,'kind':0},
        reservations={},next_retry_at=None,revision=2)
    monkeypatch.setattr(quota,'_api',lambda *a,**k:dict(sha='x',content=base64.b64encode(_canonical(sign_control(body,b'x'*32))).decode()))
    with pytest.raises(ValueError,match='regression'):
        quota.reserve_requests({'kind':1},reservation_id='retry',initial={'quota_day':'2026-10-16',
            'used':{'krx':200,'kis':0,'kind':0},'revision':3,'allow_initialize':False},
            now=datetime(2026,10,15,16,tzinfo=timezone.utc),key=b'x'*32)

