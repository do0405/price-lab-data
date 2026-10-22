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
    assert b['status']=='GRANTED'
    assert b['quota_floor']['used']['krx']==8010
    with pytest.raises(ValueError):quota.reserve_requests({'krx':70},reservation_id='job1-0',initial=initial,now=now,key=key)


def test_missing_quota_after_stream_initialization_is_not_reset(monkeypatch):
    monkeypatch.setattr(quota,'_api',lambda *a,**k:(_ for _ in ()).throw(RuntimeError('HTTP 404')))
    with pytest.raises(ValueError):quota.reserve_requests({'kind':1},reservation_id='later',initial=None,key=b'x'*32)


def test_continuous_accounting_bounds_recent_retry_ids_without_erasing_totals(monkeypatch):
    from price_lab.research_delta_codec import sign_control, _canonical, _loads, verify_control
    key=b'x'*32
    entries={f'old-{i:04d}':{'krx':0,'kis':0,'kind':1} for i in range(1100)}
    body=dict(version='research-quota-v1',quota_day='2026-10-16',reserved={'krx':0,'kis':0,'kind':1100},
        reservations=entries,next_retry_at=None,revision=1100)
    stored={'sha':'old','content':base64.b64encode(_canonical(sign_control(body,key))).decode()}
    def api(method,path,**kwargs):
        if method=='GET':return dict(stored)
        stored.update(sha='new',content=kwargs['payload']['content'])
        return {}
    monkeypatch.setattr(quota,'_api',api)
    options=dict(reservation_id='current-uncertain-write',now=datetime(2026,10,16,2,tzinfo=timezone.utc),key=key)
    first=quota.reserve_requests({'kind':1},**options)
    journal=verify_control(_loads(base64.b64decode(stored['content'])),key)
    assert len(journal['reservations'])<=1024
    assert journal['reserved']['kind']==1101
    assert options['reservation_id'] in journal['reservations']
    assert len(stored['content'])<512*1024
    assert quota.reserve_requests({'kind':1},**options)['quota_floor']==first['quota_floor']
    assert 'old-0000' not in journal['reservations']
    replay=quota.reserve_requests({'kind':1},**{**options,'reservation_id':'old-0000'})
    assert replay['quota_floor']['used']['kind']==1102
    latest=verify_control(_loads(base64.b64decode(stored['content'])),key)
    assert latest['reservation_order'][-2:]==['current-uncertain-write','old-0000']
    assert len(latest['reservations'])==1024


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


@pytest.mark.parametrize('discovery',['list','newest'])
def test_transient_empty_research_listing_is_confirmed_before_rollback_or_bootstrap(monkeypatch,discovery):
    first=release(1,'a'*64);second=release(2,'b'*64,'a'*64);calls=[]
    def api(method,path,**kwargs):
        calls.append(path)
        if len(calls)==1:return []
        if path.endswith('&page=1'):return [dict(tag_name='unrelated')]*100
        return [second,first]
    monkeypatch.setattr(transport,'_api',api)
    if discovery=='list':
        assert transport.list_packages(after_sequence=1,expected_cursor_hash='a'*64)==[second]
    else:assert transport.newest_package()==second
    assert calls==['/releases?per_page=100&page=1']*2+['/releases?per_page=100&page=2']


@pytest.mark.parametrize('kind',['missing_cursor','gap'])
def test_incomplete_research_listing_is_rechecked_without_combining_snapshots(monkeypatch,kind):
    first=release(1,'a'*64);second=release(2,'b'*64,'a'*64);third=release(3,'c'*64,'b'*64)
    calls=[]
    def api(*args,**kwargs):
        calls.append(1)
        if len(calls)==1:return [third,first] if kind=='gap' else [third,second]
        return [third,second,first]
    monkeypatch.setattr(transport,'_api',api)
    assert transport.list_packages(after_sequence=1,expected_cursor_hash='a'*64)==[second,third]
    assert len(calls)==2


def test_persistently_empty_research_log_rechecks_then_rejects_existing_cursor(monkeypatch):
    calls=[]
    monkeypatch.setattr(transport,'_api',lambda *a,**k:calls.append(1) or [])
    with pytest.raises(ValueError,match='Research package catalog unavailable after repeated empty listings'):
        transport.list_packages(after_sequence=1,expected_cursor_hash='a'*64)
    assert len(calls)==3


def test_research_confirmation_does_not_assemble_complementary_incomplete_snapshots(monkeypatch):
    first=release(1,'a'*64);second=release(2,'b'*64,'a'*64);third=release(3,'c'*64,'b'*64);calls=[]
    def api(*args,**kwargs):
        calls.append(1)
        return [third,first] if len(calls)==1 else [second]
    monkeypatch.setattr(transport,'_api',api)
    with pytest.raises(ValueError,match='sequence gap'):transport.list_packages()
    assert len(calls)==3


@pytest.mark.parametrize('after,blank_middle',[(0,False),(1,False),(1,True)])
def test_research_confirmation_cannot_erase_observed_gap_with_empty_results(monkeypatch,after,blank_middle):
    first=release(1,'a'*64);third=release(3,'c'*64,'b'*64);calls=[]
    def api(*args,**kwargs):
        calls.append(1)
        if len(calls)==1:return [third,first]
        if len(calls)==2 and blank_middle:return []
        return [first] if after else []
    monkeypatch.setattr(transport,'_api',api)
    with pytest.raises(ValueError,match='Research package sequence gap'):
        transport.list_packages(after_sequence=after,expected_cursor_hash='a'*64 if after else None)
    assert len(calls)==3


def test_empty_research_catalog_can_recover_an_exact_unchanged_cursor(monkeypatch):
    calls=[]
    def api(*args,**kwargs):
        calls.append(1)
        return [] if len(calls)==1 else [release(1,'a'*64)]
    monkeypatch.setattr(transport,'_api',api)
    assert transport.list_packages(after_sequence=1,expected_cursor_hash='a'*64)==[]
    assert len(calls)==2


def test_research_latest_confirmation_cannot_hide_invalid_metadata_as_empty_bootstrap(monkeypatch):
    invalid=release(1,'a'*64);body=json.loads(invalid['body']);body['package_sha256']='f'*64
    invalid['body']=json.dumps(body);calls=[]
    def api(*args,**kwargs):
        calls.append(1)
        return [invalid] if len(calls)==2 else []
    monkeypatch.setattr(transport,'_api',api)
    with pytest.raises(ValueError,match='tag does not match'):transport.newest_package()
    assert len(calls)==2


@pytest.mark.parametrize('discovery',['list','newest'])
def test_empty_research_bootstrap_requires_bounded_confirmation(monkeypatch,discovery):
    calls=[]
    monkeypatch.setattr(transport,'_api',lambda *a,**k:calls.append(1) or [])
    assert (transport.list_packages() if discovery=='list' else transport.newest_package())==([] if discovery=='list' else None)
    assert len(calls)==3


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

