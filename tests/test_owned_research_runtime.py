from datetime import datetime,timezone
from types import SimpleNamespace
import sys

import pytest

from test_cloud_research_runtime import script


class Owner:
    def __init__(self,role='cloud',busy=False,yielding=False):
        self.role=role;self.busy=busy;self.yielding=yielding
        self.control=None;self.calls=[]
    def acquire(self,**kwargs):
        self.calls.append('acquire')
        if self.busy:return False
        self.control={'head':kwargs.get('initial_head')};return True
    def prefer_local(self):self.calls.append('prefer')
    def guard(self,force=False):self.calls.append('guard')
    def should_yield(self):return self.yielding
    def commit_head(self,payload):
        from price_lab.research_delta_codec import package_digest
        self.calls.append('commit')
        self.control={'head':dict(sequence=payload['sequence'],package_sha256=package_digest(payload))}
    def release(self):self.calls.append('release')


def setup(tmp_path,monkeypatch,owner):
    from price_lab import research_delta_collect as collect,research_delta_transport as wire,research_quota
    from price_lab import daily_delta_transport as daily
    from price_lab.research_delta_codec import seal_bootstrap,package_digest
    from test_research_delta_collect import raw_reference
    p=script();monkeypatch.setattr(p,'ROOT',tmp_path);monkeypatch.setattr(p,'price_window',lambda now:False)
    monkeypatch.setenv('PRICE_DATA_KEY',__import__('base64').b64encode(b'x'*32).decode())
    state=collect.build_state(sessions=['2024-01-15'],end_date='2024-01-31')
    state['pending']['kis']=[];state['pending']['kind']=[]
    state['discovery']['list_date']=datetime.now(timezone.utc).date().isoformat()
    seal_bootstrap(dict(schema='research-bootstrap-v1',anchor_sha256='a'*64,state=state,
        quota={'quota_day':'2026-10-16','used':{'krx':0,'kis':0,'kind':0}}),tmp_path/'config/research-bootstrap.pldelta',b'x'*32)
    monkeypatch.setitem(sys.modules,'price_lab.research_ownership',SimpleNamespace(read_control=lambda **kw:owner.control))
    monkeypatch.setattr(wire,'newest_package',lambda **k:None)
    monkeypatch.setattr(wire,'canonical_newest',lambda *a,**k:None,raising=False)
    monkeypatch.setattr(daily,'newest_package',lambda:None)
    fetched=[];published=[]
    def fetch(task):fetched.append(task);return raw_reference(task)
    monkeypatch.setattr(collect,'official_fetcher',lambda:fetch)
    monkeypatch.setattr(research_quota,'reserve_requests',lambda counts,**kwargs:dict(status='GRANTED',
        reservation_id=kwargs['reservation_id'],ceilings=counts,quota_day='2026-10-16',next_retry_at=None))
    def publish(payload,**kwargs):
        assert 'expected_head' in kwargs
        published.append(payload)
        return {'sequence':payload['sequence'],'package_sha256':package_digest(payload)}
    monkeypatch.setattr(wire,'publish_package',publish)
    return p,fetched,published


def test_busy_owner_does_no_source_send_or_publication(tmp_path,monkeypatch):
    owner=Owner(busy=True);p,fetched,published=setup(tmp_path,monkeypatch,owner)
    out=p.run_pipeline(owner=owner,max_requests=1,max_batches=2)
    assert out['status']=='WAITING_OTHER_COLLECTOR'
    assert not fetched and not published


@pytest.mark.parametrize('role,expected',[('cloud',1),('local',2)])
def test_cloud_yields_after_closed_batch_and_local_immediately_continues(tmp_path,monkeypatch,role,expected):
    owner=Owner(role=role,yielding=True);p,fetched,published=setup(tmp_path,monkeypatch,owner)
    out=p.run_pipeline(owner=owner,max_requests=1,max_batches=2)
    assert out['batches']==expected and len(fetched)==expected and len(published)==expected
    assert owner.calls.count('commit')==expected
    assert owner.calls[-1]=='release'


def test_lost_owner_never_sends_or_publishes_and_releases(tmp_path,monkeypatch):
    owner=Owner();p,fetched,published=setup(tmp_path,monkeypatch,owner)
    def lost(force=False):raise RuntimeError('lease lost')
    owner.guard=lost
    with pytest.raises(RuntimeError,match='lease lost'):
        p.run_pipeline(owner=owner,max_requests=1,max_batches=1)
    assert not fetched and not published and owner.calls[-1]=='release'


def test_local_takeover_rechecks_only_old_transport_failures_without_resetting_http_retry_after():
    from datetime import timedelta
    from copy import deepcopy
    p=script()
    assert hasattr(p,'takeover_state'),'Local transport recheck policy missing'
    now=datetime(2026,10,15,17,22,tzinfo=timezone.utc)
    state={'cooldowns':{
        'krx':{'reason':'TRANSPORT_UNAVAILABLE','next_retry_at':(now+timedelta(minutes=30)).isoformat()},
        'kind':{'reason':'HTTP_429','next_retry_at':(now+timedelta(hours=1)).isoformat()}}}
    prior={'collected_at':(now-timedelta(minutes=20)).isoformat(),'state':deepcopy(state)}
    local=p.takeover_state(prior,Owner(role='local'),now)
    assert 'krx' not in local['cooldowns'] and local['cooldowns']['kind']==state['cooldowns']['kind']
    assert prior['state']==state
    assert p.takeover_state(prior,Owner(role='cloud'),now)==state
    prior['collected_at']=(now-timedelta(seconds=30)).isoformat()
    assert p.takeover_state(prior,Owner(role='local'),now)==state
