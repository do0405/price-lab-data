import json
import shutil

import pytest

from price_lab import ingestion as i, history_integration as u
from price_lab import history_transport as t
from test_history_integration import archive
from test_history_promotion import managed


def test_large_coverage_report_is_kept_out_of_release_body_without_mutation():
    from price_lab.release_transport import release_manifest
    source={'sha256':'a'*64,'validation':{'valid':True,'kr_reference':{'rows':[{'date':'2020-01-01','count':2500}]*10000}}}
    result=release_manifest(source)
    assert len(json.dumps(result))<10000
    assert len(source['validation']['kr_reference']['rows'])==10000


def test_release_summary_omits_unbounded_calendar_and_ancestry():
    from price_lab.release_transport import release_manifest
    source={'sha256':'a'*64,'parent_sha256':'b'*64,'sequence':6000,
        'ancestors':['c'*64]*6000,'calendar_evidence':{'kr':{str(n):{'open':True,'evidence':'official calendar'} for n in range(4000)}},
        'validation':{'history':{'present':True,'archives':3,'arbitrary_rows':['x']*10000},'valid':True}}
    result=release_manifest(source)
    assert len(json.dumps(result).encode())<10000
    assert 'ancestors' not in result and 'calendar_evidence' not in result
    assert result['sha256']==source['sha256'] and result['validation']['history']['archives']==3
    assert len(source['ancestors'])==6000 and len(source['calendar_evidence']['kr'])==4000
    assert result['sha256']==source['sha256'] and result['validation']['valid']


def test_candidate_metadata_requires_ready_purpose_and_exact_hash():
    base={'draft':True,'tag_name':'candidate-unified-'+'a'*64,
          'body':json.dumps({'purpose':'unified-history','schema_version':1,'ready':True,
                            'checkpoint':{'sha256':'a'*64},'ciphertext_sha256':'b'*64})}
    assert t.candidate_metadata(base)['checkpoint']['sha256']=='a'*64
    base['tag_name']='candidate-unified-'+'c'*64
    with pytest.raises(ValueError):t.candidate_metadata(base)


def test_discovery_ignores_incomplete_uploads_and_stable_releases(monkeypatch):
    def release(ready,draft=True):
        return {'draft':draft,'tag_name':'candidate-unified-'+'a'*64,'created_at':'2026-10-15',
                'body':json.dumps({'purpose':'unified-history','schema_version':1,'ready':ready,
                                  'checkpoint':{'sha256':'a'*64},'ciphertext_sha256':'b'*64})}
    monkeypatch.setattr(t,'_request',lambda *a,**k:[release(False),release(True,False),release(True)])
    assert len(t.pending_candidates(max_pages=1))==1


def test_completed_candidates_are_excluded_before_processing_limit(monkeypatch):
    releases=[]
    for n in range(10):
        digest=f'{n:064x}'
        releases.append({'draft':True,'tag_name':'candidate-unified-'+digest,'created_at':f'{n:03d}',
            'body':json.dumps({'purpose':'unified-history','schema_version':1,'ready':True,
                'checkpoint':{'sha256':digest},'ciphertext_sha256':'b'*64})})
    monkeypatch.setattr(t,'_request',lambda *a,**k:releases)
    found=t.pending_candidates(max_pages=1,limit=2,exclude={f'{n:064x}' for n in range(8)})
    assert [t.candidate_metadata(r)['checkpoint']['sha256'] for r in found]==[f'{n:064x}' for n in (8,9)]


def test_candidate_upload_and_restore_never_publish_local_or_stable(tmp_path,monkeypatch):
    root=managed(tmp_path);before=(root/'current.json').read_bytes();candidate=tmp_path/'candidate'
    u.prepare_candidate_root(root,candidate);u.integrate_archives([archive(tmp_path)],root=candidate)
    bodies=[];files={};state={}
    def request(method,path,**kwargs):
        if method=='GET' and path.startswith('/releases?'):return []
        if method=='GET':raise RuntimeError('HTTP 404')
        if method=='POST' and path=='/releases':
            state.update(kwargs['payload']);state.update(id=1,assets=[],upload_url='https://uploads.github.com/repos/do0405/price-lab-data/releases/1/assets{?name}',html_url='https://github.com/do0405/price-lab-data/releases/1')
        elif method=='POST':files[path.split('name=')[-1]]=kwargs['stream'].read()
        elif method=='PATCH':
            assert kwargs['payload'].get('tag_name')==state['tag_name'], 'GitHub draft PATCH must retain its tag explicitly'
            state.update(kwargs['payload'])
        bodies.append(dict(state));return dict(state)
    monkeypatch.setattr(t,'_request',request)
    out=t.upload_candidate(candidate,key=b'x'*32,token='test')
    assert out['status']=='UPLOADED_PENDING_INTEGRATION'
    assert all(b.get('draft') for b in bodies)
    assert (root/'current.json').read_bytes()==before
    assert files and t.candidate_metadata(state)['ready']


def test_uploaded_untagged_draft_is_rebound_without_reupload(tmp_path,monkeypatch):
    root=managed(tmp_path);cp=i.resolve_current(root);tag=t.PREFIX+cp['sha256']
    release={'id':9,'name':tag,'tag_name':'untagged-server-placeholder','draft':True,'assets':[{'id':1}],
        'body':json.dumps({'purpose':'unified-history','schema_version':1,'ready':True,
            'checkpoint':{'sha256':cp['sha256']},'ciphertext_sha256':'b'*64})}
    events=[]
    def request(method,path,**kwargs):
        if method=='GET' and path.startswith('/releases/tags/'):raise RuntimeError('HTTP 404')
        if method=='GET' and path.startswith('/releases?'):return [release]
        if method=='PATCH':
            assert kwargs['payload']['tag_name']==tag
            events.append('rebind');release.update(kwargs['payload']);return release
        raise AssertionError('uploaded draft was recreated or uploaded again')
    monkeypatch.setattr(t,'_request',request)
    monkeypatch.setattr(t,'encrypt_checkpoint',lambda *a,**k:(_ for _ in ()).throw(AssertionError('uploaded draft was encrypted again')))
    out=t.upload_candidate(root,key=b'x'*32,token='test')
    assert out['unchanged'] and out['tag']==tag and events==['rebind']


def test_restore_rejects_unexpected_ciphertext_before_db_use(tmp_path,monkeypatch):
    metadata={'purpose':'unified-history','schema_version':1,'ready':True,
              'checkpoint':{'sha256':'a'*64},'ciphertext_sha256':'b'*64}
    release={'draft':True,'tag_name':'candidate-unified-'+'a'*64,'body':json.dumps(metadata),
             'assets':[dict(id=1,name='checkpoint.pldata',size=4,browser_download_url='https://github.com/do0405/price-lab-data/releases/download/x/checkpoint.pldata')]}
    def download(asset,target,**kwargs):target.write_bytes(b'bad!');return i.sha256_file(target)
    monkeypatch.setattr(t,'_download_draft_asset',download)
    with pytest.raises(ValueError,match='ciphertext'):t.restore_candidate(release,tmp_path/'isolated',key=b'x'*32,token='test')


def test_integrated_receipt_skips_download_on_retry(tmp_path,monkeypatch):
    root=managed(tmp_path);candidate=tmp_path/'candidate';u.prepare_candidate_root(root,candidate)
    u.integrate_archives([archive(tmp_path)],root=candidate);cp=i.resolve_current(candidate)
    u.merge_candidate(cp['resolved_path'],cp,root=root)
    release={'draft':True,'tag_name':'candidate-unified-'+cp['sha256'],'body':json.dumps({
        'purpose':'unified-history','schema_version':1,'ready':True,'checkpoint':cp,'ciphertext_sha256':'b'*64})}
    monkeypatch.setattr(t,'pending_candidates',lambda **kwargs:[release])
    monkeypatch.setattr(t,'publish_current',lambda *args,**kwargs:{'release_url':'https://github.com/do0405/price-lab-data/releases/1','unchanged':True})
    def fail(*args,**kwargs):raise AssertionError('already integrated candidate downloaded')
    monkeypatch.setattr(t,'restore_candidate',fail)
    assert t.process_pending(root,key=b'x'*32)[0]['status']=='ALREADY_INTEGRATED'


def test_multipart_candidate_round_trip_uses_authenticated_checkpoint(tmp_path,monkeypatch):
    from price_lab.cloud_data import encrypt_checkpoint
    from price_lab.transport_parts import split_checkpoint
    root=managed(tmp_path);cp=i.resolve_current(root);key=b'x'*32
    encrypted=tmp_path/'checkpoint.pldata'
    envelope=encrypt_checkpoint(cp['resolved_path'],cp,encrypted,key=key)
    parts=tmp_path/'parts';index=split_checkpoint(encrypted,cp,parts,key=key,part_size=4096)
    files=list(parts.iterdir())
    release={'draft':True,'tag_name':'candidate-unified-'+cp['sha256'],'body':json.dumps({
        'purpose':'unified-history','schema_version':1,'ready':True,'checkpoint':cp,
        'ciphertext_sha256':envelope['ciphertext_sha256']}),
        'assets':[dict(id=n+1,name=p.name,size=p.stat().st_size,
            browser_download_url='https://github.com/do0405/price-lab-data/releases/download/x/'+p.name) for n,p in enumerate(files)]}
    def download(asset,target,**kwargs):shutil.copy2(parts/asset['name'],target);return i.sha256_file(target)
    monkeypatch.setattr(t,'_download_draft_asset',download)
    assert t.restore_candidate(release,tmp_path/'isolated',key=key,token='test')['sha256']==cp['sha256']
