import json
import pytest

def test_interrupted_draft_upload_is_resumed(monkeypatch,tmp_path):
    from price_lab import release_transport as r
    current={'sha256':'a'*64,'sequence':1,'parent_sha256':None,'resolved_path':str(tmp_path/'db')}
    monkeypatch.setattr(r,'resolve_current',lambda *a,**k:current)
    def encrypt(db,manifest,path,key=None):path.write_bytes(b'cipher');return {'ciphertext_sha256':'b'*64}
    monkeypatch.setattr(r,'encrypt_checkpoint',encrypt)
    def latest():raise RuntimeError('GitHub GET failed: HTTP 404')
    monkeypatch.setattr(r,'latest_release',latest)
    draft=None;failed=False
    def request(method,path,**kwargs):
        nonlocal draft,failed
        if method=='GET':
            if draft is None:raise RuntimeError('GitHub GET failed: HTTP 404')
            return draft
        if method=='POST' and path=='/releases':
            if draft is not None:raise AssertionError('duplicate draft creation')
            draft={'id':1,'draft':True,'tag_name':kwargs['payload']['tag_name'],'upload_url':'https://uploads.github.com/repos/x/assets{?name}','assets':[]}
            return draft
        if method=='POST':
            if not failed:failed=True;raise RuntimeError('upload interrupted')
            return {'id':2}
        if method=='PATCH' and kwargs['payload'].get('draft') is False:
            return {'html_url':'https://github.com/do0405/price-lab-data/releases/tag/data-1'}
        return draft
    monkeypatch.setattr(r,'_request',request)
    with pytest.raises(RuntimeError,match='interrupted'):r.publish_current(tmp_path,key=b'x'*32,token='test')
    assert r.publish_current(tmp_path,key=b'x'*32,token='test')['unchanged'] is False

def test_only_trusted_old_repo_records_monthly_public_watermarks(monkeypatch):
    from datetime import datetime,timezone
    from price_lab import release_transport as r
    calls=[]
    def request(method,path,**kwargs):
        calls.append((method,path,kwargs))
        if path=='/commits/main':return {'commit':{'committer':{'date':'2026-09-01T00:00:00Z'}}}
        if method=='GET':raise RuntimeError('GitHub GET failed: HTTP 404')
        return {}
    monkeypatch.setattr(r,'_request',request)
    monkeypatch.setenv('GITHUB_REF','refs/heads/fork')
    monkeypatch.setenv('GITHUB_EVENT_NAME','pull_request')
    manifest={'sequence':5,'sha256':'a'*64,'markets':{'us':{'actual':'2026-10-14','target':'2026-10-14'}}}
    assert r.record_periodic_status(manifest)['recorded'] is False and not calls
    monkeypatch.setenv('GITHUB_REF','refs/heads/main');monkeypatch.setenv('GITHUB_EVENT_NAME','schedule')
    assert r.record_periodic_status(manifest,now=datetime(2026,10,15,tzinfo=timezone.utc))['recorded'] is True
    import base64
    record=json.loads(base64.b64decode(calls[-1][2]['payload']['content']))
    assert set(record)=={'last_validated_at','sequence','sha256','markets'}

def test_same_previously_authenticated_release_skips_large_download(monkeypatch,tmp_path):
    from price_lab import release_transport as r
    release={'id':12,'tag_name':'data-5','html_url':'https://github.com/do0405/price-lab-data/releases/tag/data-5',
             'assets':[{'id':30,'size':100,'name':r.ASSET,'browser_download_url':'https://github.com/do0405/price-lab-data/releases/download/data-5/checkpoint.pldata'}]}
    monkeypatch.setattr(r,'latest_release',lambda:release)
    monkeypatch.setattr(r,'resolve_current',lambda root:{'sequence':5,'sha256':'a'*64})
    (tmp_path/'cloud-receipt.json').write_text(json.dumps({'release_id':12,'asset_id':30,'asset_size':100,'sha256':'a'*64}))
    def forbidden(*args,**kwargs):raise AssertionError('large download repeated')
    monkeypatch.setattr(r.requests,'get',forbidden)
    assert r.pull_latest(tmp_path,key=b'x'*32)['downloaded'] is False


class Github:
    """In-memory remote: no real API, credentials or network are used."""
    def __init__(self):self.release=None;self.blobs={};self.next_id=100;self.uploads=0;self.fail_upload=None;self.published=False;self.gets=[]
    def latest(self):
        if self.release is None or self.release['draft']:raise RuntimeError('GitHub GET failed: HTTP 404')
        return self.release
    def request(self,method,path,**kwargs):
        if method=='GET':
            if self.release is None:raise RuntimeError('GitHub GET failed: HTTP 404')
            return self.release
        if method=='POST' and path=='/releases':
            assert self.release is None
            self.release={'id':10,'draft':True,'tag_name':kwargs['payload']['tag_name'],'body':kwargs['payload']['body'],
                          'html_url':'https://github.com/do0405/price-lab-data/releases/tag/test','upload_url':'https://uploads.github.com/repos/x/assets{?name}','assets':[]}
            return self.release
        if method=='DELETE':
            self.release['assets']=[a for a in self.release['assets'] if a['id']!=int(path.rsplit('/',1)[-1])];return {}
        if method=='PATCH':
            self.release.update(kwargs['payload'])
            if kwargs['payload'].get('draft') is False:self.published=True
            return self.release
        assert method=='POST' and kwargs.get('token')=='fake-github-token'
        self.uploads+=1
        if self.uploads==self.fail_upload:raise RuntimeError('upload interrupted')
        name=path.split('?name=')[1];blob=kwargs['stream'].read();self.next_id+=1;self.blobs[name]=blob
        self.release['assets'].append({'id':self.next_id,'name':name,'size':len(blob),
            'browser_download_url':'https://github.com/do0405/price-lab-data/releases/download/test/'+name})
        return self.release['assets'][-1]
    def download(self,url,**kwargs):
        assert 'headers' not in kwargs and 'auth' not in kwargs and 'params' not in kwargs
        self.gets.append(url.rsplit('/',1)[-1]);blob=self.blobs[self.gets[-1]]
        class Response:
            status_code=200
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def iter_content(self,size):
                for offset in range(0,len(blob),min(size,97)):yield blob[offset:offset+min(size,97)]
        response=Response();response.url=url;return response


def install_github(monkeypatch,remote):
    from price_lab import release_transport as r
    monkeypatch.setattr(r,'_request',remote.request);monkeypatch.setattr(r,'latest_release',remote.latest);monkeypatch.setattr(r.requests,'get',remote.download)
    return r


def multipart_release(monkeypatch,tmp_path):
    from test_cloud_data import checkpoint
    from price_lab import cloud_data
    current=checkpoint(tmp_path);remote=Github();r=install_github(monkeypatch,remote)
    monkeypatch.setattr(r,'SINGLE_MAX_BYTES',1)
    encrypt=cloud_data.encrypt_checkpoint
    def small_parts(*args,**kwargs):
        result=encrypt(*args,**kwargs);monkeypatch.setattr(r,'PART_BYTES',max(1,args[2].stat().st_size//3));return result
    monkeypatch.setattr(r,'encrypt_checkpoint',small_parts)
    return r,remote,current


def test_multipart_real_gcm_roundtrip_and_receipt_noop(monkeypatch,tmp_path):
    r,remote,current=multipart_release(monkeypatch,tmp_path)
    r.publish_current(tmp_path/'source',key=b'x'*32,token='fake-github-token')
    names=[a['name'] for a in remote.release['assets']]
    assert r.ASSET not in names and r.INDEX_ASSET in names and len(names)>3
    restored=r.pull_latest(tmp_path/'restored',key=b'x'*32)
    assert restored['manifest']['sha256']==current['sha256'] and restored['downloaded']
    count=len(remote.gets)
    assert r.pull_latest(tmp_path/'restored',key=b'x'*32)['downloaded'] is False and len(remote.gets)==count


def test_multipart_failed_draft_is_replaced_before_publish(monkeypatch,tmp_path):
    r,remote,_=multipart_release(monkeypatch,tmp_path);remote.fail_upload=2
    with pytest.raises(RuntimeError,match='interrupted'):r.publish_current(tmp_path/'source',key=b'x'*32,token='fake-github-token')
    assert remote.release['draft'] and not remote.published and len(remote.release['assets'])==1
    remote.fail_upload=None
    assert r.publish_current(tmp_path/'source',key=b'x'*32,token='fake-github-token')['unchanged'] is False
    names=[a['name'] for a in remote.release['assets']]
    assert len(names)==len(set(names)) and remote.published
    assert r.pull_latest(tmp_path/'restored',key=b'x'*32)['downloaded']


@pytest.mark.parametrize('mutation',['index','part','missing','duplicate','order','name','fork','rollback'])
def test_multipart_rejects_invalid_release_before_decrypt_or_current_change(monkeypatch,tmp_path,mutation):
    import copy
    from test_transport_parts import resign
    r,remote,_=multipart_release(monkeypatch,tmp_path);r.publish_current(tmp_path/'source',key=b'x'*32,token='fake-github-token')
    parts=[a for a in remote.release['assets'] if a['name']!=r.INDEX_ASSET]
    if mutation=='index':
        value=json.loads(remote.blobs[r.INDEX_ASSET]);value['total_size']+=1;remote.blobs[r.INDEX_ASSET]=json.dumps(value).encode()
    elif mutation=='part':
        p=parts[-1];remote.blobs[p['name']]=b'z'*p['size']
    elif mutation=='missing':remote.release['assets'].remove(parts[-1])
    elif mutation=='duplicate':remote.release['assets'].append(copy.deepcopy(parts[0]))
    elif mutation=='order':
        value=json.loads(remote.blobs[r.INDEX_ASSET]);value['parts'].reverse();remote.blobs[r.INDEX_ASSET]=json.dumps(resign(value)).encode()
    elif mutation=='name':parts[0]['name']='../outside'
    elif mutation=='fork':monkeypatch.setattr(r,'resolve_current',lambda *args,**kwargs:{'sequence':1,'sha256':'c'*64})
    elif mutation=='rollback':monkeypatch.setattr(r,'resolve_current',lambda *args,**kwargs:{'sequence':2,'sha256':'c'*64})
    for asset in remote.release['assets']:
        if asset['name'] in remote.blobs:asset['size']=len(remote.blobs[asset['name']])
    def forbidden(*args,**kwargs):raise AssertionError('unverified multipart container reached decrypt')
    monkeypatch.setattr(r,'decrypt_checkpoint',forbidden)
    target=tmp_path/'rejected'
    with pytest.raises(ValueError):r.pull_latest(target,key=b'x'*32)
    assert not (target/'current.json').exists()
    if mutation in ('index','order','fork','rollback'):assert len(remote.gets)==1


def test_v1_single_roundtrip_remains_supported_and_downloads_without_secrets(monkeypatch,tmp_path):
    from test_cloud_data import checkpoint
    current=checkpoint(tmp_path);remote=Github();r=install_github(monkeypatch,remote)
    r.publish_current(tmp_path/'source',key=b'x'*32,token='fake-github-token')
    assert [a['name'] for a in remote.release['assets']]==[r.ASSET]
    assert r.pull_latest(tmp_path/'legacy',key=b'x'*32)['manifest']['sha256']==current['sha256']


@pytest.mark.parametrize('asset',[None,{'name':[]},{'name':'unrelated-file','id':1,'size':2,'browser_download_url':'https://github.com/do0405/price-lab-data/releases/download/x/unrelated-file'}])
def test_malformed_asset_metadata_is_rejected_without_download(monkeypatch,tmp_path,asset):
    from price_lab import release_transport as r
    monkeypatch.setattr(r,'latest_release',lambda:{'assets':[asset],'draft':False})
    def forbidden(*args,**kwargs):raise AssertionError('invalid metadata reached network')
    monkeypatch.setattr(r.requests,'get',forbidden)
    with pytest.raises(ValueError):r.pull_latest(tmp_path,key=b'x'*32)


def test_release_asset_listing_order_is_independent_of_authenticated_part_order(monkeypatch,tmp_path):
    r,remote,_=multipart_release(monkeypatch,tmp_path);r.publish_current(tmp_path/'source',key=b'x'*32,token='fake-github-token')
    remote.release['assets'].reverse()
    assert r.pull_latest(tmp_path/'restored',key=b'x'*32)['downloaded']


def test_large_asset_download_uses_verified_byte_ranges_without_tokens(monkeypatch,tmp_path):
    from price_lab import release_transport as r
    import hashlib
    blob=b'abcdefghij';calls=[]
    class Response:
        status_code=206;url='https://release-assets.githubusercontent.com/file'
        def __init__(self,start,end):self.start=start;self.end=end;self.headers={'Content-Range':f'bytes {start}-{end}/10'}
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def iter_content(self,*args):yield blob[self.start:self.end+1]
    def get(url,**kwargs):
        headers=kwargs.get('headers',{});assert set(headers)=={'Range'}
        start,end=map(int,headers['Range'].removeprefix('bytes=').split('-'));calls.append((start,end))
        return Response(start,end)
    monkeypatch.setattr(r,'RANGE_THRESHOLD_BYTES',1,raising=False)
    monkeypatch.setattr(r,'RANGE_CHUNK_BYTES',4,raising=False)
    monkeypatch.setattr(r.requests,'get',get)
    target=tmp_path/'download'
    result=r._download_asset({'size':10,'browser_download_url':'https://github.com/do0405/price-lab-data/releases/download/data/checkpoint.pldata'},target)
    assert target.read_bytes()==blob and result==hashlib.sha256(blob).hexdigest()
    assert sorted(calls)==[(0,3),(4,7),(8,9)]


def test_ranged_download_rejects_wrong_range_before_accepting_bytes(monkeypatch,tmp_path):
    from price_lab import release_transport as r
    class Response:
        status_code=206;url='https://release-assets.githubusercontent.com/file';headers={'Content-Range':'bytes 0-1/10'}
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def iter_content(self,*args):raise AssertionError('wrong range reached body')
    monkeypatch.setattr(r,'RANGE_THRESHOLD_BYTES',1,raising=False)
    monkeypatch.setattr(r,'RANGE_CHUNK_BYTES',4,raising=False)
    monkeypatch.setattr(r.requests,'get',lambda *a,**k:Response())
    with pytest.raises(ValueError,match='range'):
        r._download_asset({'size':10,'browser_download_url':'https://github.com/do0405/price-lab-data/releases/download/data/checkpoint.pldata'},tmp_path/'rejected')
