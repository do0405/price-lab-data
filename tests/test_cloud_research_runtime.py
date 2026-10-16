from datetime import datetime,timezone
import importlib.util
import pytest
from pathlib import Path

def script():
    p=Path(__file__).resolve().parents[1]/'scripts/reference_pipeline.py'
    spec=importlib.util.spec_from_file_location('reference_pipeline',p)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module

def test_actual_start_guard_preserves_price_window():
    p=script()
    assert p.price_window(datetime(2026,10,15,11,37,tzinfo=timezone.utc))
    assert p.price_window(datetime(2026,10,15,23,37,tzinfo=timezone.utc))
    assert not p.price_window(datetime(2026,10,15,16,0,tzinfo=timezone.utc))


def test_continuation_does_not_duplicate_an_existing_pending_run(monkeypatch):
    from price_lab import daily_delta_transport as api
    p=script();monkeypatch.setattr(p,'price_window',lambda now:False)
    for k,v in {'GITHUB_ACTIONS':'true','GITHUB_RUN_ID':'123','GITHUB_REF':'refs/heads/main',
                'GITHUB_EVENT_NAME':'workflow_dispatch'}.items():monkeypatch.setenv(k,v)
    calls=[]
    def request(method,path,**kwargs):
        calls.append((method,path,kwargs))
        return {'workflow_runs':[{'id':123,'status':'in_progress'},{'id':124,'status':'queued'}]}
    monkeypatch.setattr(api,'_api',request)
    assert p.request_continuation()['status']=='ALREADY_QUEUED'
    assert len(calls)==1


def test_continuation_requests_only_the_same_main_workflow_and_fails_safely(monkeypatch):
    from price_lab import daily_delta_transport as api
    p=script();monkeypatch.setattr(p,'price_window',lambda now:False)
    for k,v in {'GITHUB_ACTIONS':'true','GITHUB_RUN_ID':'123','GITHUB_REF':'refs/heads/main',
                'GITHUB_EVENT_NAME':'schedule'}.items():monkeypatch.setenv(k,v)
    calls=[]
    def request(method,path,**kwargs):
        calls.append((method,path,kwargs))
        return {'workflow_runs':[{'id':123,'status':'in_progress'}]} if method=='GET' else {}
    monkeypatch.setattr(api,'_api',request)
    assert p.request_continuation()['status']=='DISPATCHED'
    assert calls[-1][0]=='POST' and calls[-1][1]=='/actions/workflows/reference-evidence.yml/dispatches'
    assert calls[-1][2]['payload']=={'ref':'main','inputs':{'max_requests':'100','max_batches':'100'}}
    monkeypatch.setattr(api,'_api',lambda *a,**k:(_ for _ in ()).throw(RuntimeError('HTTP failure')))
    assert p.request_continuation()['status']=='WAITING_SCHEDULE_RECOVERY'


def test_continuation_eligibility_respects_independent_provider_cooldown():
    p=script();now=datetime(2026,10,15,16,tzinfo=timezone.utc)
    state={'pending':{'krx':[{}],'kis':[],'kind':[]},'cooldowns':{},
        'discovery':{'quota_floor':{'quota_day':'2026-10-16','used':{'krx':8000,'kis':0,'kind':0}}}}
    assert p.continuation_ready(state,now)
    state['cooldowns']['krx']={'next_retry_at':'2026-10-16T16:00:00Z'}
    state['pending']['kind']=[{}]
    assert p.continuation_ready(state,now)
    state['cooldowns']['kind']={'next_retry_at':'2026-10-16T16:00:00Z'}
    assert not p.continuation_ready(state,now)

def test_runtime_import_does_not_load_database_or_credentials():
    import subprocess,sys
    root=Path(__file__).resolve().parents[1]
    code="import runpy,sys;runpy.run_path('scripts/reference_pipeline.py',run_name='inspect');assert 'duckdb' not in sys.modules;assert 'price_lab.local_credentials' not in sys.modules"
    assert subprocess.run([sys.executable,'-c',code],cwd=root,capture_output=True).returncode==0


@pytest.mark.parametrize('already_seen',[False,True])
def test_closed_batches_immediately_continue_without_schedule_wait(tmp_path,monkeypatch,already_seen):
    from price_lab import research_delta_collect as collect,research_delta_transport as wire,research_quota
    from price_lab import daily_delta_transport as daily
    from price_lab.research_delta_codec import seal_bootstrap,package_digest
    from test_research_delta_collect import raw_reference
    p=script();monkeypatch.setattr(p,'ROOT',tmp_path);monkeypatch.setattr(p,'price_window',lambda now:False)
    monkeypatch.setenv('PRICE_DATA_KEY',__import__('base64').b64encode(b'x'*32).decode())
    state=collect.build_state(sessions=['2024-01-15'],end_date='2024-01-31')
    state['pending']['kis']=[];state['pending']['kind']=[]
    state['discovery']['list_date']=datetime.now(timezone.utc).date().isoformat()
    if already_seen:
        from price_lab.kr_evidence_sources import _id,_sha,canonical
        for task in state['pending']['krx']:
            state['seen'][_id(task)]=_sha(canonical(raw_reference(task)).encode())
            state['discovery']['response_metadata'][_id(task)]={}
    seal_bootstrap(dict(schema='research-bootstrap-v1',anchor_sha256='a'*64,state=state,
        quota={'quota_day':'2026-10-16','used':{'krx':0,'kis':0,'kind':0}}),tmp_path/'config/research-bootstrap.pldelta',b'x'*32)
    monkeypatch.setattr(wire,'newest_package',lambda:None);monkeypatch.setattr(daily,'newest_package',lambda:None)
    monkeypatch.setattr(collect,'official_fetcher',lambda:raw_reference)
    reservations=[]
    def reserve(counts,**kwargs):
        reservations.append(kwargs['reservation_id'])
        return dict(status='GRANTED',reservation_id=kwargs['reservation_id'],ceilings=counts,quota_day='2026-10-16',next_retry_at=None)
    monkeypatch.setattr(research_quota,'reserve_requests',reserve)
    published=[]
    def publish(payload,**kwargs):
        published.append(payload)
        return {'sequence':payload['sequence'],'package_sha256':package_digest(payload)}
    monkeypatch.setattr(wire,'publish_package',publish)
    result=p.run_pipeline(seconds=420,max_requests=1,max_batches=2)
    assert result['batches']==2 and len(published)==2
    assert published[1]['previous_package_sha256']==package_digest(published[0])
    if already_seen:assert not published[0]['records'] and not published[1]['records']
    else:assert published[0]['records'][0]['task']!=published[1]['records'][0]['task']
    old_ids=set(reservations);reservations.clear()
    p.run_pipeline(seconds=420,max_requests=1,max_batches=1)
    assert old_ids.isdisjoint(reservations),'A new producer invocation cannot reuse HTTP reservations'
    prior=published[1];before=package_digest(prior);published.clear()
    monkeypatch.setattr(wire,'newest_package',lambda:{'id':1})
    monkeypatch.setattr(wire,'load_package',lambda *a,**k:prior)
    monkeypatch.setattr(research_quota,'reserve_requests',lambda counts,**kwargs:dict(status='WAITING_QUOTA',
        reservation_id=kwargs['reservation_id'],ceilings={},next_retry_at='2027-01-01T00:00:00Z',
        quota_floor={'quota_day':'2026-10-16','used':{'krx':8000,'kis':0,'kind':0},'revision':100}))
    waiting=p.run_pipeline(seconds=420,max_requests=1,max_batches=1)
    assert package_digest(prior)==before,'Authenticated predecessor must remain immutable'
    assert waiting['batches']==1 and published[0]['state']['discovery']['quota_floor']['revision']==100
