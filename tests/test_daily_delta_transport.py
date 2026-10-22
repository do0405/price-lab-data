"""The public transport never restores or publishes a DuckDB file."""
import json
from pathlib import Path
import pytest


def metadata(sequence=1, digest='a'*64, previous=None):
    from price_lab import daily_delta_transport as t
    tag=t.package_tag(sequence,digest)
    return dict(id=sequence,draft=False,prerelease=False,tag_name=tag,
        html_url='https://github.com/do0405/price-lab-data/releases/tag/'+tag,
        body=json.dumps(dict(purpose=t.PURPOSE,schema_version=1,ready=True,
            stream_id='price-daily-v1',sequence=sequence,package_sha256=digest,
            previous_package_sha256=previous,ciphertext_sha256='b'*64)),
        assets=[dict(id=1,name=t.ASSET,size=8,
            browser_download_url='https://github.com/do0405/price-lab-data/releases/download/'+tag+'/'+t.ASSET)])


def test_discovery_ignores_legacy_full_databases_and_orders_deltas(monkeypatch):
    from price_lab import daily_delta_transport as t
    rows=[metadata(2,'c'*64,'a'*64),dict(id=9,tag_name='data-00000006-old'),metadata()]
    monkeypatch.setattr(t,'_api',lambda method,path,**kw:rows if 'page=1' in path else [])
    found=t.list_packages()
    assert [t.package_metadata(r)['sequence'] for r in found]==[1,2]


@pytest.mark.parametrize('mutation',['draft','tag','asset','url','sequence','previous'])
def test_invalid_package_identity_is_rejected_before_download(mutation):
    from price_lab import daily_delta_transport as t
    release=metadata()
    body=json.loads(release['body'])
    if mutation=='draft':release['draft']=True
    if mutation=='tag':release['tag_name']='delta-daily-unrelated'
    if mutation=='asset':release['assets'][0]['name']='checkpoint.pldata'
    if mutation=='url':release['assets'][0]['browser_download_url']='https://example.com/daily.pldelta'
    if mutation=='sequence':body['sequence']=True
    if mutation=='previous':body['previous_package_sha256']='c'*64
    release['body']=json.dumps(body)
    with pytest.raises(ValueError):t.package_metadata(release)


def test_download_rejects_cipher_hash_and_never_sends_credentials(monkeypatch,tmp_path):
    from price_lab import daily_delta_transport as t
    class Response:
        status_code=200;url='https://release-assets.githubusercontent.com/file'
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def iter_content(self,*a):yield b'12345678'
    def get(url,**kwargs):
        assert not {'auth','params','headers'} & set(kwargs)
        return Response()
    monkeypatch.setattr(t.requests,'get',get)
    with pytest.raises(ValueError,match='hash'):
        t.download_package(metadata(),tmp_path)


def test_published_same_package_retry_does_not_mutate_release(monkeypatch,tmp_path):
    from price_lab import daily_delta_transport as t
    from price_lab import daily_delta_codec as c
    release=metadata()
    calls=[]
    def api(method,path,**kw):
        calls.append(method)
        assert method=='GET'
        return release
    monkeypatch.setattr(t,'_api',api)
    monkeypatch.setattr(c,'validate_payload',lambda x:x)
    monkeypatch.setattr(c,'package_digest',lambda x:'a'*64)
    assert t.publish_package({'sequence':1},key=b'x'*32,token='not-logged',work_root=tmp_path)['unchanged']
    assert calls==['GET']


def test_cloud_runtime_sources_have_no_full_database_path():
    import ast
    root=Path(__file__).resolve().parents[1]
    text=(root/'scripts/price_pipeline.py').read_text(encoding='utf-8')
    assert 'daily_delta_collect' in text
    assert 'publish_current' not in text and 'process_pending' not in text
    imports=[node.module or '' for node in ast.walk(ast.parse(text)) if isinstance(node,ast.ImportFrom)]
    imports += [item.name for node in ast.walk(ast.parse(text)) if isinstance(node,ast.Import) for item in node.names]
    assert not any('ingestion' in name or 'duckdb' in name for name in imports)
    workflow=(root/'.github/workflows/daily-prices.yml').read_text(encoding='utf-8')
    assert 'tests/test_daily_delta_codec.py' in workflow
    assert 'python -m pytest tests -q' not in workflow
    assert 'checkpoint' not in workflow.lower()


def test_incomplete_upload_is_not_published_and_retry_roundtrips_real_gcm(monkeypatch,tmp_path):
    import copy
    from test_daily_delta_codec import payload
    from price_lab import daily_delta_transport as t,daily_delta_codec as c
    value=payload();remote=None;blob=None;fail=True;uploads=0
    def api(method,path,**kwargs):
        nonlocal remote,blob,fail,uploads
        if method=='GET' and path.startswith('/releases?'):return []
        if method=='GET':
            if remote is None:raise RuntimeError('Daily package API failed: HTTP 404')
            return copy.deepcopy(remote)
        if method=='POST' and path=='/releases':
            remote={**kwargs['payload'],'id':5,'assets':[],
                'upload_url':'https://uploads.github.com/repos/do0405/price-lab-data/releases/5/assets{?name}',
                'html_url':'https://github.com/do0405/price-lab-data/releases/tag/'+kwargs['payload']['tag_name']}
            return copy.deepcopy(remote)
        if method=='DELETE':remote['assets']=[];return {}
        if method=='PATCH':
            assert kwargs['payload']['tag_name']==remote['tag_name']
            remote.update(kwargs['payload']);return copy.deepcopy(remote)
        assert method=='POST' and set(kwargs)=={'token','stream'}
        assert kwargs['token']=='fake-token'
        uploads+=1;blob=kwargs['stream'].read()
        if fail:fail=False;raise RuntimeError('upload interrupted')
        asset=dict(id=10,name=t.ASSET,size=len(blob),browser_download_url=
            'https://github.com/do0405/price-lab-data/releases/download/'+remote['tag_name']+'/'+t.ASSET)
        remote['assets']=[asset];return asset
    monkeypatch.setattr(t,'_api',api)
    with pytest.raises(RuntimeError,match='interrupted'):
        t.publish_package(value,key=b'x'*32,token='fake-token',work_root=tmp_path)
    assert remote['draft'] and json.loads(remote['body'])['ready'] is False
    result=t.publish_package(value,key=b'x'*32,token='fake-token',work_root=tmp_path)
    assert not remote['draft'] and json.loads(remote['body'])['ready'] is True
    assert uploads==2 and result['package_sha256']==c.package_digest(value)
    encrypted=tmp_path/'received';encrypted.write_bytes(blob)
    assert c.open_package(encrypted,b'x'*32)==value


def test_stale_producer_is_rejected_before_encrypt_or_upload(monkeypatch,tmp_path):
    from test_daily_delta_codec import payload
    from price_lab import daily_delta_transport as t,daily_delta_codec as c
    def api(method,path,**kwargs):
        if path.startswith('/releases?'):return [metadata()]
        raise RuntimeError('Daily package API failed: HTTP 404')
    monkeypatch.setattr(t,'_api',api)
    monkeypatch.setattr(c,'seal_package',lambda *a,**k:pytest.fail('stale producer encrypted/uploaded'))
    with pytest.raises(ValueError,match='advanced'):
        t.publish_package(payload(),key=b'x'*32,token='fake-token',work_root=tmp_path)


@pytest.mark.parametrize('kind',['gap','fork','same_position'])
def test_missing_or_conflicting_log_entries_are_rejected(monkeypatch,kind):
    from price_lab import daily_delta_transport as t
    if kind=='gap':rows=[metadata(),metadata(3,'c'*64,'b'*64)]
    elif kind=='fork':rows=[metadata(),metadata(2,'c'*64,'b'*64)]
    else:rows=[metadata(),metadata(1,'c'*64)]
    monkeypatch.setattr(t,'_api',lambda *a,**k:rows)
    with pytest.raises(ValueError):t.list_packages()


def test_latest_discovery_finds_existing_log_behind_unrelated_or_incomplete_releases(monkeypatch):
    from price_lab import daily_delta_transport as t
    calls=[]
    def api(method,path,**kwargs):
        calls.append(path)
        if path.endswith('&page=1'):return [dict(tag_name='unrelated',draft=True)]*100
        return [metadata(2,'c'*64,'a'*64),metadata()]
    monkeypatch.setattr(t,'_api',api)
    assert t.package_metadata(t.newest_package())['sequence']==2
    assert len(calls)==2


def test_latest_discovery_bound_cannot_silently_restart_bootstrap(monkeypatch):
    from price_lab import daily_delta_transport as t
    monkeypatch.setattr(t,'MAX_PAGES',2)
    monkeypatch.setattr(t,'_api',lambda *a,**k:[dict(tag_name='unrelated',draft=True)]*100)
    with pytest.raises(ValueError,match='refusing a new bootstrap'):t.newest_package()


@pytest.mark.parametrize('kind',['rollback','fork','missing_cursor'])
def test_remote_log_cannot_report_unchanged_for_rollback_or_applied_cursor_conflict(monkeypatch,kind):
    from price_lab import daily_delta_transport as t
    if kind=='rollback':rows=[metadata()]
    elif kind=='fork':rows=[metadata(2,'f'*64,'a'*64),metadata()]
    else:rows=[metadata(3,'d'*64,'c'*64)]
    monkeypatch.setattr(t,'_api',lambda *a,**k:rows)
    with pytest.raises(ValueError):
        t.list_packages(after_sequence=2,expected_cursor_hash='c'*64)


def test_unchanged_log_requires_the_exact_locally_applied_cursor_hash(monkeypatch):
    from price_lab import daily_delta_transport as t
    monkeypatch.setattr(t,'_api',lambda *a,**k:[metadata(2,'c'*64,'a'*64),metadata()])
    assert t.list_packages(after_sequence=2,expected_cursor_hash='c'*64)==[]


@pytest.mark.parametrize('discovery',['list','newest'])
def test_transient_empty_listing_is_confirmed_before_rollback_or_bootstrap(monkeypatch,discovery):
    from price_lab import daily_delta_transport as t
    first=metadata();second=metadata(2,'c'*64,'a'*64);calls=[]
    def api(method,path,**kwargs):
        calls.append(path)
        if len(calls)==1:return []
        if path.endswith('&page=1'):return [dict(tag_name='unrelated')]*100
        return [second,first]
    monkeypatch.setattr(t,'_api',api)
    if discovery=='list':
        assert t.list_packages(after_sequence=1,expected_cursor_hash='a'*64)==[second]
    else:assert t.newest_package()==second
    assert calls==['/releases?per_page=100&page=1']*2+['/releases?per_page=100&page=2']


@pytest.mark.parametrize('kind',['missing_cursor','gap'])
def test_incomplete_daily_listing_is_rechecked_without_combining_snapshots(monkeypatch,kind):
    from price_lab import daily_delta_transport as t
    first=metadata();second=metadata(2,'c'*64,'a'*64);third=metadata(3,'d'*64,'c'*64)
    attempts=[]
    def api(*args,**kwargs):
        attempts.append(1)
        if len(attempts)==1:return [third,first] if kind=='gap' else [third,second]
        return [third,second,first]
    monkeypatch.setattr(t,'_api',api)
    assert t.list_packages(after_sequence=1,expected_cursor_hash='a'*64)==[second,third]
    assert len(attempts)==2


def test_persistently_empty_daily_log_rechecks_then_rejects_existing_cursor(monkeypatch):
    from price_lab import daily_delta_transport as t
    calls=[]
    monkeypatch.setattr(t,'_api',lambda *a,**k:calls.append(1) or [])
    with pytest.raises(ValueError,match='Daily package catalog unavailable after repeated empty listings'):
        t.list_packages(after_sequence=1,expected_cursor_hash='a'*64)
    assert len(calls)==3


def test_daily_confirmation_does_not_assemble_complementary_incomplete_snapshots(monkeypatch):
    from price_lab import daily_delta_transport as t
    first=metadata();second=metadata(2,'c'*64,'a'*64);third=metadata(3,'d'*64,'c'*64);calls=[]
    def api(*args,**kwargs):
        calls.append(1)
        return [third,first] if len(calls)==1 else [second]
    monkeypatch.setattr(t,'_api',api)
    with pytest.raises(ValueError,match='sequence gap'):t.list_packages()
    assert len(calls)==3


@pytest.mark.parametrize('after,blank_middle',[(0,False),(1,False),(1,True)])
def test_daily_confirmation_cannot_erase_observed_gap_with_empty_results(monkeypatch,after,blank_middle):
    from price_lab import daily_delta_transport as t
    first=metadata();third=metadata(3,'d'*64,'c'*64);calls=[]
    def api(*args,**kwargs):
        calls.append(1)
        if len(calls)==1:return [third,first]
        if len(calls)==2 and blank_middle:return []
        return [first] if after else []
    monkeypatch.setattr(t,'_api',api)
    with pytest.raises(ValueError,match='Daily package sequence gap'):
        t.list_packages(after_sequence=after,expected_cursor_hash='a'*64 if after else None)
    assert len(calls)==3


def test_empty_daily_catalog_can_recover_an_exact_unchanged_cursor(monkeypatch):
    from price_lab import daily_delta_transport as t
    calls=[]
    def api(*args,**kwargs):
        calls.append(1)
        return [] if len(calls)==1 else [metadata()]
    monkeypatch.setattr(t,'_api',api)
    assert t.list_packages(after_sequence=1,expected_cursor_hash='a'*64)==[]
    assert len(calls)==2


def test_daily_latest_confirmation_cannot_hide_invalid_metadata_as_empty_bootstrap(monkeypatch):
    from price_lab import daily_delta_transport as t
    invalid=metadata();body=json.loads(invalid['body']);body['package_sha256']='f'*64
    invalid['body']=json.dumps(body);calls=[]
    def api(*args,**kwargs):
        calls.append(1)
        return [invalid] if len(calls)==2 else []
    monkeypatch.setattr(t,'_api',api)
    with pytest.raises(ValueError,match='tag does not match'):t.newest_package()
    assert len(calls)==2


def test_daily_confirmation_restarts_after_an_empty_later_page(monkeypatch):
    from price_lab import daily_delta_transport as t
    first=metadata();second=metadata(2,'c'*64,'a'*64);calls=[]
    def api(method,path,**kwargs):
        calls.append(path)
        if path.endswith('&page=1'):return [second]+[dict(tag_name='unrelated')]*99
        return [] if len(calls)==2 else [first]
    monkeypatch.setattr(t,'_api',api)
    assert t.list_packages(after_sequence=1,expected_cursor_hash='a'*64)==[second]
    assert calls==['/releases?per_page=100&page=1','/releases?per_page=100&page=2']*2


@pytest.mark.parametrize('discovery',['list','newest'])
def test_empty_daily_bootstrap_requires_bounded_confirmation(monkeypatch,discovery):
    from price_lab import daily_delta_transport as t
    calls=[]
    monkeypatch.setattr(t,'_api',lambda *a,**k:calls.append(1) or [])
    assert (t.list_packages() if discovery=='list' else t.newest_package())==([] if discovery=='list' else None)
    assert len(calls)==3


def test_daily_confirmation_does_not_retry_conflicting_cursor(monkeypatch):
    from price_lab import daily_delta_transport as t
    calls=[]
    def api(*args,**kwargs):
        calls.append(1)
        return [] if len(calls)==1 else [metadata(2,'f'*64,'a'*64),metadata()]
    monkeypatch.setattr(t,'_api',api)
    with pytest.raises(ValueError,match='conflicts'):
        t.list_packages(after_sequence=2,expected_cursor_hash='c'*64)
    assert len(calls)==2


@pytest.mark.parametrize('module',['daily_delta_transport','research_delta_transport'])
def test_listing_confirmation_requests_a_fresh_response(monkeypatch,module):
    import importlib
    wire=importlib.import_module('price_lab.'+module);headers=[]
    class Response:
        status_code=200;content=b'[]'
        def json(self):return []
    def request(method,url,**kwargs):
        headers.append(kwargs['headers'])
        return Response()
    monkeypatch.setattr(wire.requests,'request',request)
    assert wire._api('GET','/releases?per_page=100&page=1')==[]
    assert headers[0]['Cache-Control']=='no-cache'
