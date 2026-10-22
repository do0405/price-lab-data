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


def test_cloud_provider_empty_board_keeps_failure_and_reports_http_200(capsys):
    from price_lab.providers import HttpClient, KrxProvider, ProviderError
    module=pipeline_module()
    class Response:
        status_code=200
        headers={}
        content=b'{}'
        def json(self):return {'OutBlock_1':[],'msg1':'PRIVATE'}
    class Session:
        def request(self,*a,**kw):return Response()
    provider=module.observe_provider(KrxProvider('PRIVATE',HttpClient(Session(),retries=0,min_interval=0)),'krx')
    with pytest.raises(ProviderError,match='empty'):provider.fetch_day('2026-10-21')
    import json
    output=capsys.readouterr().out;event=json.loads(output)
    assert event['provider']=='krx' and event['operation']=='fetch_day' and event['http_status']==200
    assert event['endpoint'].endswith('/stk_bydd_trd') and event['reason']=='EMPTY_BOARD'
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
