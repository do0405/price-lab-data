"""Production cloud entrypoint is testable without database libraries or files."""
import ast
import importlib.util
from pathlib import Path
import pytest


def pipeline_module():
    path=Path(__file__).resolve().parents[1]/'scripts/price_pipeline.py'
    spec=importlib.util.spec_from_file_location('cloud_daily_pipeline',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_six_hour_check_reads_only_authenticated_latest_package(monkeypatch,tmp_path):
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    events=[]
    monkeypatch.setattr(t,'newest_package',lambda:events.append('discover') or {'id':1})
    monkeypatch.setattr(t,'load_package',lambda *a,**k:events.append('authenticate') or {'sequence':7})
    monkeypatch.setattr(t,'record_periodic_status',lambda *a,**k:{'recorded':False})
    def forbidden(*a,**k):raise AssertionError('Six-hour check collected or published data')
    monkeypatch.setattr(c,'collect_delta',forbidden);monkeypatch.setattr(t,'publish_package',forbidden)
    result=pipeline_module().run_pipeline(root=tmp_path,integration_only=True)
    assert events==['discover','authenticate'] and result['sequence']==7
    assert result['cloud_database'] is False and not list(tmp_path.rglob('*.duckdb'))


def test_cloud_daily_collects_then_publishes_without_restoring_any_database(monkeypatch,tmp_path):
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    events=[]
    monkeypatch.setattr(t,'newest_package',lambda:{'id':1})
    monkeypatch.setattr(t,'load_package',lambda *a,**k:{'sequence':6})
    def collect(provider,**kwargs):
        events.append('collect');assert kwargs['prior_payload']=={'sequence':6}
        return {'sequence':7,'parts':[]}
    monkeypatch.setattr(c,'collect_delta',collect)
    monkeypatch.setattr(t,'publish_package',lambda *a,**k:events.append('publish') or {'sequence':7})
    monkeypatch.setattr(t,'record_periodic_status',lambda *a,**k:{'recorded':False})
    result=pipeline_module().run_pipeline(root=tmp_path,providers=object(),reference_fetch=lambda *a:{})
    assert events==['collect','publish'] and result['cloud_database'] is False
    assert not list(tmp_path.rglob('*.duckdb'))


def test_retired_full_publication_cannot_be_requested(monkeypatch,tmp_path):
    from price_lab import daily_delta_transport as t
    monkeypatch.setattr(t,'newest_package',lambda:None)
    with pytest.raises(ValueError,match='retired'):
        pipeline_module().run_pipeline(root=tmp_path,publish_only=True)


def test_production_collector_imports_no_database_runtime():
    root=Path(__file__).resolve().parents[1]
    for relative in ('scripts/price_pipeline.py','src/price_lab/daily_delta_collect.py','src/price_lab/daily_delta_codec.py'):
        tree=ast.parse((root/relative).read_text(encoding='utf-8'))
        names=[n.module or '' for n in ast.walk(tree) if isinstance(n,ast.ImportFrom)]
        names += [a.name for n in ast.walk(tree) if isinstance(n,ast.Import) for a in n.names]
        assert not any(any(term in name for term in ('duckdb','ingestion','cloud_data','history_integration','history_transport','release_transport')) for name in names)


def test_cloud_provider_failure_logs_endpoint_and_code_without_private_content(capsys):
    from price_lab.providers import HttpClient, KisProvider, ProviderError
    module=pipeline_module()
    class Response:
        status_code=403
        headers={}
        content=b'{"error_code":"EGW00133","access_token":"PRIVATE"}'
        def json(self):return {'error_code':'EGW00133','access_token':'PRIVATE','msg1':'PRIVATE'}
    class Session:
        def request(self,*a,**kw):return Response()
    provider=module.observe_provider(KisProvider('PRIVATE','PRIVATE',HttpClient(Session(),retries=0,min_interval=0)),'kis')
    with pytest.raises(ProviderError):provider.fetch_benchmarks([__import__('pandas').Timestamp('2026-10-21')])
    import json
    output=capsys.readouterr().out
    event=json.loads(output)
    assert event['provider']=='kis' and event['operation']=='fetch_benchmarks'
    assert event['endpoint']=='/oauth2/tokenP' and event['http_status']==403
    assert event['error_code']=='EGW00133' and 'PRIVATE' not in output


def test_cloud_provider_empty_board_keeps_failure_and_reports_http_200(monkeypatch,capsys):
    from price_lab.providers import HttpClient, KrxProvider, ProviderError
    module=pipeline_module();wait=[];calls=[]
    monkeypatch.setattr(module.time,'sleep',wait.append)
    class Response:
        status_code=200
        headers={}
        content=b'{}'
        def json(self):return {'OutBlock_1':[],'msg1':'PRIVATE'}
    class Session:
        def request(self,*a,**kw):calls.append(kw);return Response()
    provider=module.observe_provider(KrxProvider('PRIVATE',HttpClient(Session(),retries=0,min_interval=0)),'krx')
    with pytest.raises(ProviderError,match='empty'):provider.fetch_day('2026-10-21')
    import json
    output=capsys.readouterr().out;events=[json.loads(line) for line in output.splitlines()]
    event=events[-1]
    assert event['provider']=='krx' and event['operation']=='fetch_day' and event['http_status']==200
    assert event['endpoint'].endswith('/stk_bydd_trd') and event['reason']=='EMPTY_BOARD'
    assert event['requested_date']=='2026-10-21'
    assert wait==[5,20,60] and len(calls)==4
    assert [e['attempt'] for e in events[:-1]]==[2,3,4]
    assert 'PRIVATE' not in output


@pytest.mark.parametrize('empty_responses',[1,2,3])
def test_cloud_krx_retries_only_the_empty_board_with_identical_request(monkeypatch,capsys,empty_responses):
    import json
    from price_lab.providers import HttpClient,KrxProvider
    module=pipeline_module();wait=[];calls=[]
    monkeypatch.setattr(module.time,'sleep',wait.append)
    row=dict(BAS_DD='20240102',ISU_CD='005930',TDD_OPNPRC='10',TDD_HGPRC='11',
        TDD_LWPRC='9',TDD_CLSPRC='10',ACC_TRDVOL='100',ACC_TRDVAL='1000')
    bodies=iter([{'OutBlock_1':[row]}]+[{'OutBlock_1':[]}]*empty_responses+
        [{'OutBlock_1':[{**row,'ISU_CD':'000001'}]}])
    class Response:
        status_code=200;headers={}
        def __init__(self,body):self.body=body;self.content=json.dumps(body).encode()
        def json(self):return self.body
    class Session:
        def request(self,*args,**kwargs):
            calls.append((args,kwargs));return Response(next(bodies))
    provider=module.observe_provider(KrxProvider('PRIVATE',HttpClient(Session(),retries=0,min_interval=0)),'krx')
    bars,assets=provider.fetch_day('2024-01-02')
    assert set(bars.symbol)=={'005930','000001'} and len(assets)==2
    assert len(calls)==empty_responses+2 and all(call==calls[1] for call in calls[1:])
    assert calls[0][0][1].endswith('/stk_bydd_trd') and calls[1][0][1].endswith('/ksq_bydd_trd')
    assert wait==[5,20,60][:empty_responses]
    output=capsys.readouterr().out;events=[json.loads(line) for line in output.splitlines()]
    assert [e['event'] for e in events]==['provider_response_retry']*empty_responses
    assert all(e['requested_date']=='2024-01-02' and e['http_status']==200 for e in events)
    assert 'PRIVATE' not in output


@pytest.mark.parametrize('endpoint,status,body',[
    ('/svc/apis/sto/stk_bydd_trd',200,{}),
    ('/svc/apis/sto/stk_bydd_trd',200,{'OutBlock_1':'invalid'}),
    ('/svc/apis/sto/stk_bydd_trd',403,{'OutBlock_1':[]}),
    ('/svc/apis/sto/stk_isu_base_info',200,{'OutBlock_1':[]}),
    ('/unrelated',200,{'OutBlock_1':[]}),
])
def test_cloud_empty_board_retry_excludes_other_responses(monkeypatch,capsys,endpoint,status,body):
    import json
    module=pipeline_module();wait=[];calls=[]
    monkeypatch.setattr(module.time,'sleep',wait.append)
    class Response:
        status_code=status
        def __init__(self):self.content=json.dumps(body).encode()
        def json(self):return body
    response=Response()
    class Session:
        def request(self,*args,**kwargs):calls.append(1);return response
    assert module._TraceSession(Session()).request('GET','https://data-dbg.krx.co.kr'+endpoint,
        params={'basDd':'20240102'}) is response
    assert calls==[1] and wait==[] and capsys.readouterr().out==''


@pytest.mark.parametrize('method,host,params',[
    ('POST','data-dbg.krx.co.kr',{'basDd':'20240102'}),
    ('GET','example.test',{'basDd':'20240102'}),
    ('GET','data-dbg.krx.co.kr',{'basDd':'PRIVATE'}),
    ('GET','data-dbg.krx.co.kr',{}),
])
def test_cloud_empty_board_retry_requires_exact_request_scope(monkeypatch,capsys,method,host,params):
    module=pipeline_module();calls=[]
    monkeypatch.setattr(module.time,'sleep',lambda seconds:pytest.fail('Unrelated request retried'))
    class Response:
        status_code=200;content=b'{"OutBlock_1":[]}'
        def json(self):return {'OutBlock_1':[]}
    response=Response()
    class Session:
        def request(self,*args,**kwargs):calls.append(1);return response
    assert module._TraceSession(Session()).request(method,'https://'+host+'/svc/apis/sto/stk_bydd_trd',
        params=params) is response
    assert calls==[1] and capsys.readouterr().out==''


def test_cloud_empty_board_retry_transport_failure_clears_stale_diagnostics(monkeypatch,capsys):
    import json,requests
    from price_lab.providers import HttpClient,KrxProvider,ProviderError
    module=pipeline_module();calls=[];wait=[]
    monkeypatch.setattr(module.time,'sleep',wait.append)
    class Response:
        status_code=200;content=b'{"OutBlock_1":[],"error_code":"EGW00133"}'
        def json(self):return {'OutBlock_1':[],'error_code':'EGW00133'}
    class Session:
        def request(self,*args,**kwargs):
            calls.append(1)
            if len(calls)==1:return Response()
            raise requests.ConnectionError('PRIVATE')
    provider=module.observe_provider(KrxProvider('PRIVATE',HttpClient(Session(),retries=0,min_interval=0)),'krx')
    with pytest.raises(ProviderError,match='transport'):provider.fetch_day('2024-01-02')
    output=capsys.readouterr().out;events=[json.loads(line) for line in output.splitlines()]
    assert calls==[1,1] and wait==[5] and len(events)==2
    assert events[-1]['event']=='provider_call_failed' and events[-1]['http_status'] is None
    assert events[-1]['requested_date']=='2024-01-02' and 'error_code' not in events[-1]
    assert 'PRIVATE' not in output


def test_cloud_provider_trace_discards_query_headers_body_and_previous_request(capsys):
    from price_lab.providers import HttpClient, ProviderError
    module=pipeline_module()
    class Response:
        status_code=403
        headers={}
        content=b'{}'
        def json(self):return {'error_code':'PRIVATE','msg1':'PRIVATE'}
    class Session:
        def request(self,*a,**kw):return Response()
    class Provider:
        client=HttpClient(Session(),retries=0,min_interval=0)
        def fetch(self):return self.client.request_json('GET','https://example.test/prices?key=PRIVATE',headers={'auth':'PRIVATE'},body={'secret':'PRIVATE'})
        def before_request(self):raise ProviderError('PRIVATE')
    provider=module.observe_provider(Provider(),'test')
    with pytest.raises(ProviderError):provider.fetch()
    with pytest.raises(ProviderError):provider.before_request()
    import json
    output=capsys.readouterr().out;events=[json.loads(x) for x in output.splitlines()]
    assert events[0]['endpoint']=='/prices'
    assert 'endpoint' not in events[1] and 'http_status' not in events[1]
    assert 'PRIVATE' not in output and all('error_code' not in e for e in events)


def test_cloud_provider_success_preserves_response_and_shared_state(capsys):
    module=pipeline_module()
    class Provider:
        client=type('Client',(),{'session':object()})()
        _calendar={}
        def session_status(self,days):self._calendar['day']=True;return days
    original=Provider();provider=module.observe_provider(original,'kis');value=['unchanged']
    assert provider.session_status(value) is value
    assert provider._calendar is original._calendar and provider._calendar=={'day':True}
    assert capsys.readouterr().out==''


@pytest.mark.parametrize('failures',[1,2])
def test_cloud_provider_retry_is_bounded_and_publishes_only_complete_payload(monkeypatch,tmp_path,failures):
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    module=pipeline_module();events=[];wait=[];prior={'sequence':6};calls=[]
    monkeypatch.setattr(t,'newest_package',lambda:{'id':1})
    monkeypatch.setattr(t,'load_package',lambda *a,**k:prior)
    monkeypatch.setattr(t,'record_periodic_status',lambda *a,**k:{})
    monkeypatch.setattr(module.time,'sleep',wait.append)
    def collect(provider,**kwargs):
        calls.append(kwargs);events.append('collect')
        assert kwargs['prior_payload'] is prior
        if len(calls)<=failures:raise ValueError('Required market collection failed (market=kr; stage=fetch; type=provider)')
        return {'sequence':7,'parts':[]}
    monkeypatch.setattr(c,'collect_delta',collect)
    monkeypatch.setattr(t,'publish_package',lambda *a,**k:events.append('publish') or {'sequence':7})
    if failures==1:
        module.run_pipeline(root=tmp_path,providers=object(),reference_fetch=lambda *a:{})
        assert events==['collect','collect','publish']
        assert calls[0]['now']==calls[1]['now']
    else:
        with pytest.raises(ValueError,match='type=provider'):module.run_pipeline(root=tmp_path,providers=object(),reference_fetch=lambda *a:{})
        assert events==['collect','collect']
    assert wait==[65]


@pytest.mark.parametrize('reason',[
    'Required market collection failed (market=kr; stage=normalize; type=validation)',
    'Required market collection failed (market=kr; stage=fetch; type=other)',
    'Authenticated daily package metadata mismatch',
])
def test_cloud_validation_or_program_errors_are_never_retried(monkeypatch,tmp_path,reason):
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    module=pipeline_module();calls=[];wait=[]
    monkeypatch.setattr(t,'newest_package',lambda:{'id':1})
    monkeypatch.setattr(t,'load_package',lambda *a,**k:{'sequence':6})
    monkeypatch.setattr(module.time,'sleep',wait.append)
    def collect(*a,**k):calls.append(1);raise ValueError(reason)
    monkeypatch.setattr(c,'collect_delta',collect)
    def forbidden(*a,**k):raise AssertionError('Failed collection was published')
    monkeypatch.setattr(t,'publish_package',forbidden)
    with pytest.raises(ValueError,match=__import__('re').escape(reason)):
        module.run_pipeline(root=tmp_path,providers=object(),reference_fetch=lambda *a:{})
    assert calls==[1] and wait==[]


@pytest.mark.parametrize('initial_package',[True,False])
def test_cloud_publish_conflict_recollects_authenticated_latest_state(monkeypatch,tmp_path,capsys,initial_package):
    import base64,copy,json
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    from price_lab.daily_delta_codec import package_digest,sign_bootstrap
    from test_daily_delta_collect import NOW,Provider,bootstrap,calendar
    calendar(monkeypatch)
    first=c.collect_delta(Provider(),bootstrap=bootstrap(),now=NOW)
    advanced=c.collect_delta(Provider(),prior_payload=first,now=NOW)
    advanced['state']['assets'].append(dict(market='us',symbol='REMOTE',name='Remote discovery',exchange='NYSE'))
    advanced['state']['calendar_evidence']={'kr':{'2024-01-03':{'open':False,'provider':'kis'}}}
    before=copy.deepcopy(advanced);provider=Provider();events=[];published=[];recorded=[]
    heads=iter([first if initial_package else None,advanced]);selected=None;reference_calls=[];wait=[]
    key=base64.b64encode(b'x'*32).decode()
    bootstrap_path=tmp_path/'bootstrap.json'
    if not initial_package:bootstrap_path.write_text(json.dumps(sign_bootstrap(bootstrap(),b'x'*32)),encoding='utf-8')
    def discover():
        nonlocal selected
        selected=next(heads);events.append('discover');return {'id':selected['sequence']} if selected else None
    def load(release,root,**kwargs):
        assert release['id']==selected['sequence'] and root==tmp_path and kwargs=={'key':key}
        events.append('authenticate');return selected
    def publish(value,**kwargs):
        published.append(value);events.append('publish')
        assert kwargs=={'work_root':tmp_path,'key':key}
        if len(published)==1:raise ValueError('Daily log advanced; collect again from the authenticated latest state')
        return {'sequence':value['sequence']}
    def reference(*args):reference_calls.append(args);return {}
    monkeypatch.setenv('PRICE_DATA_KEY',key)
    monkeypatch.setattr(t,'newest_package',discover);monkeypatch.setattr(t,'load_package',load)
    monkeypatch.setattr(t,'publish_package',publish)
    monkeypatch.setattr(t,'record_periodic_status',lambda value:recorded.append(value) or {})
    module=pipeline_module();monkeypatch.setattr(module.time,'sleep',wait.append)
    result=module.run_pipeline(root=tmp_path,providers=provider,reference_fetch=reference,now=NOW,audit=True,
        bootstrap_path=bootstrap_path)
    assert events==['discover']+(['authenticate'] if initial_package else [])+['publish','discover','authenticate','publish']
    assert provider.started==2 and all(call[-1] is True for call in provider.calls)
    assert all('REMOTE' not in assets.symbol.values for market,_,assets,_,_ in provider.calls[:3] if market=='us')
    assert all('REMOTE' in assets.symbol.values for market,_,assets,_,_ in provider.calls[3:] if market=='us')
    assert published[0]['sequence']==(2 if initial_package else 1)
    assert published[0]['previous_package_sha256']==(package_digest(first) if initial_package else None)
    assert published[1]['sequence']==3 and published[1]['previous_package_sha256']==package_digest(advanced)
    assert published[1]['state']['calendar_evidence']==advanced['state']['calendar_evidence']
    assert published[0]['collected_at']==published[1]['collected_at']==NOW.isoformat().replace('+00:00','Z')
    assert len(reference_calls)==32 and advanced==before and wait==[]
    assert recorded==[published[1]] and result['sequence']==3 and result['cloud_database'] is False
    assert key not in capsys.readouterr().out and not list(tmp_path.rglob('*.duckdb'))


def test_cloud_publish_conflicts_exhaust_one_recollection_without_recording_status(monkeypatch,tmp_path):
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    priors=iter([{'sequence':6},{'sequence':7}]);events=[];wait=[]
    monkeypatch.setattr(t,'newest_package',lambda:events.append('discover') or {'id':1})
    monkeypatch.setattr(t,'load_package',lambda *a,**k:events.append('authenticate') or next(priors))
    def collect(*a,**kwargs):
        events.append('collect');return {'sequence':kwargs['prior_payload']['sequence']+1,'parts':[]}
    def publish(*a,**kwargs):
        events.append('publish');raise ValueError('Daily log advanced; collect again from the authenticated latest state')
    monkeypatch.setattr(c,'collect_delta',collect);monkeypatch.setattr(t,'publish_package',publish)
    monkeypatch.setattr(t,'record_periodic_status',lambda *a,**k:pytest.fail('Unpublished state was recorded'))
    module=pipeline_module();monkeypatch.setattr(module.time,'sleep',wait.append)
    with pytest.raises(ValueError,match='Daily log advanced'):
        module.run_pipeline(root=tmp_path,providers=object(),reference_fetch=lambda *a:{})
    assert events==['discover','authenticate','collect','publish','discover','authenticate','collect','publish']
    assert wait==[]


@pytest.mark.parametrize('error',[
    ValueError('Authenticated daily package metadata mismatch'),
    ValueError('Daily log advanced; collect again from the authenticated latest state extra'),
    ValueError('Required market collection failed (market=kr; stage=fetch; type=provider)'),
    RuntimeError('Daily log advanced; collect again from the authenticated latest state'),
    RuntimeError('Daily package API transport failed'),
])
def test_cloud_publish_errors_other_than_exact_conflict_are_not_retried(monkeypatch,tmp_path,error):
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    events=[];wait=[]
    monkeypatch.setattr(t,'newest_package',lambda:events.append('discover') or {'id':1})
    monkeypatch.setattr(t,'load_package',lambda *a,**k:{'sequence':6})
    monkeypatch.setattr(c,'collect_delta',lambda *a,**k:events.append('collect') or {'sequence':7,'parts':[]})
    def publish(*a,**kwargs):events.append('publish');raise error
    monkeypatch.setattr(t,'publish_package',publish)
    monkeypatch.setattr(t,'record_periodic_status',lambda *a,**k:pytest.fail('Unpublished state was recorded'))
    module=pipeline_module();monkeypatch.setattr(module.time,'sleep',wait.append)
    with pytest.raises(type(error)) as caught:
        module.run_pipeline(root=tmp_path,providers=object(),reference_fetch=lambda *a:{})
    assert caught.value is error and events==['discover','collect','publish'] and wait==[]


@pytest.mark.parametrize('reloaded',[None,{'sequence':6},{'sequence':5}])
def test_cloud_publish_conflict_does_not_recollect_from_missing_stale_or_invalid_head(monkeypatch,tmp_path,capsys,reloaded):
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    heads=iter([{'sequence':6},reloaded]);selected=None;events=[]
    def discover():
        nonlocal selected
        selected=next(heads);events.append('discover');return selected
    monkeypatch.setattr(t,'newest_package',discover)
    monkeypatch.setattr(t,'load_package',lambda *a,**k:events.append('authenticate') or selected)
    monkeypatch.setattr(c,'collect_delta',lambda *a,**k:events.append('collect') or {'sequence':7,'parts':[]})
    def publish(*a,**kwargs):events.append('publish');raise ValueError('Daily log advanced; collect again from the authenticated latest state')
    monkeypatch.setattr(t,'publish_package',publish)
    monkeypatch.setattr(t,'record_periodic_status',lambda *a,**k:pytest.fail('Unpublished state was recorded'))
    with pytest.raises(ValueError,match='Daily log advanced'):
        pipeline_module().run_pipeline(root=tmp_path,providers=object(),reference_fetch=lambda *a:{})
    assert events==['discover','authenticate','collect','publish','discover']+(['authenticate'] if reloaded else [])
    import json
    records=[json.loads(line) for line in capsys.readouterr().out.splitlines()]
    reload=[record for record in records if record['event']=='publication_conflict_reload']
    assert reload==[dict(event='publication_conflict_reload',previous_sequence=6,
        latest_sequence=reloaded['sequence'] if reloaded else None,same_parent_digest_matches=False)]


@pytest.mark.parametrize('failure_stage',['discover','authenticate'])
def test_cloud_publish_conflict_reload_failure_prevents_recollection(monkeypatch,tmp_path,failure_stage):
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    discoveries=[];loads=[];collected=[];published=[]
    error=ValueError('Authenticated daily package metadata mismatch') if failure_stage=='authenticate' else RuntimeError('Daily package API transport failed')
    def discover():
        discoveries.append(1)
        if len(discoveries)==2 and failure_stage=='discover':raise error
        return {'id':len(discoveries)}
    def load(*a,**kwargs):
        loads.append(1)
        if len(loads)==2:raise error
        return {'sequence':6}
    def publish(*a,**kwargs):
        published.append(1);raise ValueError('Daily log advanced; collect again from the authenticated latest state')
    monkeypatch.setattr(t,'newest_package',discover);monkeypatch.setattr(t,'load_package',load)
    monkeypatch.setattr(c,'collect_delta',lambda *a,**k:collected.append(1) or {'sequence':7,'parts':[]})
    monkeypatch.setattr(t,'publish_package',publish)
    monkeypatch.setattr(t,'record_periodic_status',lambda *a,**k:pytest.fail('Unpublished state was recorded'))
    with pytest.raises(type(error)) as caught:
        pipeline_module().run_pipeline(root=tmp_path,providers=object(),reference_fetch=lambda *a:{})
    assert caught.value is error and collected==published==[1]
    assert len(discoveries)==2 and len(loads)==(2 if failure_stage=='authenticate' else 1)


@pytest.mark.parametrize('failures',[1,2])
def test_cloud_recollection_retains_bounded_provider_retry(monkeypatch,tmp_path,failures):
    from datetime import datetime,timezone
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    priors=iter([{'sequence':6},{'sequence':7}]);calls=[];published=[];recorded=[];wait=[]
    provider=object();reference=lambda *a:{};instant=datetime(2026,10,22,16,tzinfo=timezone.utc)
    monkeypatch.setattr(t,'newest_package',lambda:{'id':1})
    monkeypatch.setattr(t,'load_package',lambda *a,**k:next(priors))
    def collect(selected,**kwargs):
        assert selected is provider and kwargs['reference_fetch'] is reference
        assert kwargs['now'] is instant and kwargs['audit'] is True
        calls.append(kwargs['prior_payload']['sequence'])
        if 2<=len(calls)<=failures+1:raise ValueError('Required market collection failed (market=kr; stage=fetch; type=provider)')
        return {'sequence':calls[-1]+1,'parts':[]}
    def publish(value,**kwargs):
        published.append(value)
        if len(published)==1:raise ValueError('Daily log advanced; collect again from the authenticated latest state')
        return {'sequence':value['sequence']}
    monkeypatch.setattr(c,'collect_delta',collect);monkeypatch.setattr(t,'publish_package',publish)
    monkeypatch.setattr(t,'record_periodic_status',lambda value:recorded.append(value) or {})
    module=pipeline_module();monkeypatch.setattr(module.time,'sleep',wait.append)
    if failures==1:
        assert module.run_pipeline(root=tmp_path,providers=provider,reference_fetch=reference,now=instant,audit=True)['sequence']==8
        assert [value['sequence'] for value in published]==[7,8] and recorded==[published[-1]]
    else:
        with pytest.raises(ValueError,match='type=provider'):
            module.run_pipeline(root=tmp_path,providers=provider,reference_fetch=reference,now=instant,audit=True)
        assert len(published)==1 and recorded==[]
    assert calls==[6,7,7] and wait==[65]


@pytest.mark.parametrize('live_clock',[True,False])
def test_cloud_conflict_recollection_refreshes_only_an_implicit_clock(monkeypatch,tmp_path,live_clock):
    from datetime import timedelta,timezone
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    from price_lab.daily_delta_codec import package_digest
    from test_daily_delta_collect import NOW,Provider,bootstrap,calendar
    calendar(monkeypatch)
    first=c.collect_delta(Provider(),bootstrap=bootstrap(),now=NOW-timedelta(minutes=1))
    concurrent=c.collect_delta(Provider(),prior_payload=first,now=NOW+timedelta(minutes=1))
    refreshed_now=NOW+timedelta(minutes=2)
    heads=iter([first,concurrent]);selected=None;published=[];recorded=[];clock_calls=[]
    class Clock:
        @staticmethod
        def now(zone):
            assert zone is timezone.utc
            clock_calls.append(zone)
            return NOW if len(clock_calls)==1 else refreshed_now
    def discover():
        nonlocal selected
        selected=next(heads);return {'id':selected['sequence']}
    def publish(value,**kwargs):
        published.append(value)
        if len(published)==1:raise ValueError('Daily log advanced; collect again from the authenticated latest state')
        return {'sequence':value['sequence']}
    monkeypatch.setattr(t,'newest_package',discover)
    monkeypatch.setattr(t,'load_package',lambda *a,**k:selected)
    monkeypatch.setattr(t,'publish_package',publish)
    monkeypatch.setattr(t,'record_periodic_status',lambda value:recorded.append(value) or {})
    module=pipeline_module();monkeypatch.setattr(module,'datetime',Clock);provider=Provider()
    if live_clock:
        result=module.run_pipeline(root=tmp_path,providers=provider,reference_fetch=lambda *a:{})
        assert result['sequence']==3 and provider.started==2 and len(clock_calls)==2
        assert published[1]['previous_package_sha256']==package_digest(concurrent)
        assert published[1]['collected_at']==refreshed_now.isoformat().replace('+00:00','Z')
        assert recorded==[published[1]]
    else:
        with pytest.raises(ValueError,match='Prior package collection is in the future'):
            module.run_pipeline(root=tmp_path,providers=provider,reference_fetch=lambda *a:{},now=NOW)
        assert provider.started==1 and clock_calls==[] and recorded==[] and len(published)==1
    assert published[0]['collected_at']==NOW.isoformat().replace('+00:00','Z')
    assert all(call[-1] is False for call in provider.calls)


@pytest.mark.parametrize('scenario',['same_parent','different_parent','persistent_conflict'])
def test_cloud_publish_conflict_retries_only_an_identical_authenticated_parent(monkeypatch,tmp_path,capsys,scenario):
    import copy,json
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    from price_lab.daily_delta_codec import package_digest
    from test_daily_delta_collect import NOW,Provider,bootstrap,calendar
    calendar(monkeypatch)
    original=c.collect_delta(Provider(),bootstrap=bootstrap(),now=NOW)
    original['state']['assets'][0]['name']='PRIVATE'
    reloaded=copy.deepcopy(original)
    if scenario=='different_parent':reloaded['state']['assets'][0]['name']='PRIVATE changed'
    heads=iter([original,reloaded]);selected=None;published=[];recorded=[];discoveries=[]
    def discover():
        nonlocal selected
        discoveries.append(1);selected=next(heads);return {'id':selected['sequence']}
    def publish(value,**kwargs):
        published.append(value)
        if len(published)==1 or scenario=='persistent_conflict':
            error=ValueError('Daily log advanced; collect again from the authenticated latest state')
            error.publication_conflict=dict(package_sequence=2,expected_parent_sequence=1,
                discovered_head_sequence=None,predecessor_matches=False,unsafe='PRIVATE')
            raise error
        return {'sequence':value['sequence']}
    monkeypatch.setattr(t,'newest_package',discover)
    monkeypatch.setattr(t,'load_package',lambda *a,**k:selected)
    monkeypatch.setattr(t,'publish_package',publish)
    monkeypatch.setattr(t,'record_periodic_status',lambda value:recorded.append(value) or {})
    provider=Provider();module=pipeline_module()
    if scenario=='same_parent':
        result=module.run_pipeline(root=tmp_path,providers=provider,reference_fetch=lambda *a:{},now=NOW)
        assert result['sequence']==2 and recorded==[published[1]]
        assert published[0] is not published[1] and published[0]==published[1]
        assert published[1]['previous_package_sha256']==package_digest(original)
    else:
        with pytest.raises(ValueError,match='Daily log advanced'):
            module.run_pipeline(root=tmp_path,providers=provider,reference_fetch=lambda *a:{},now=NOW)
        assert recorded==[]
    assert len(discoveries)==2 and provider.started==len(published)==(1 if scenario=='different_parent' else 2)
    output=capsys.readouterr().out;records=[json.loads(line) for line in output.splitlines()]
    conflicts=[record for record in records if record.get('event')=='publication_conflict']
    assert len(conflicts)==(2 if scenario=='persistent_conflict' else 1)
    assert conflicts[0]==dict(event='publication_conflict',attempt=1,maximum_attempts=2,
        previous_sequence=1,payload_sequence=2,package_sequence=2,expected_parent_sequence=1,
        discovered_head_sequence=None,predecessor_matches=False)
    reload=[record for record in records if record.get('event')=='publication_conflict_reload']
    assert reload==[dict(event='publication_conflict_reload',previous_sequence=1,latest_sequence=1,
        same_parent_digest_matches=scenario!='different_parent')]
    assert 'PRIVATE' not in output and package_digest(original) not in output


def test_cloud_conflict_diagnostics_discard_wrong_types_and_private_fields(monkeypatch,tmp_path,capsys):
    import json
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    heads=iter([{'sequence':6},None])
    monkeypatch.setattr(t,'newest_package',lambda:next(heads))
    monkeypatch.setattr(t,'load_package',lambda value,*a,**k:value)
    monkeypatch.setattr(c,'collect_delta',lambda *a,**k:{'sequence':7,'parts':[]})
    def publish(*a,**kwargs):
        error=ValueError('Daily log advanced; collect again from the authenticated latest state')
        error.publication_conflict=dict(package_sequence=True,expected_parent_sequence='PRIVATE',
            discovered_head_sequence={'secret':'PRIVATE'},predecessor_matches=1,unsafe='PRIVATE')
        raise error
    monkeypatch.setattr(t,'publish_package',publish)
    with pytest.raises(ValueError,match='Daily log advanced'):
        pipeline_module().run_pipeline(root=tmp_path,providers=object(),reference_fetch=lambda *a:{})
    output=capsys.readouterr().out;records=[json.loads(line) for line in output.splitlines()]
    assert records
    assert records[0]==dict(event='publication_conflict',attempt=1,maximum_attempts=2,
        previous_sequence=6,payload_sequence=7)
    assert 'PRIVATE' not in output
