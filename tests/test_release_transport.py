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
