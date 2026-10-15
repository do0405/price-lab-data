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
