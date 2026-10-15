"""Public GitHub Releases transport; checkpoint contents remain authenticated/encrypted."""
import base64
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import tempfile
from urllib.parse import urlsplit
import requests
from .cloud_data import decrypt_checkpoint,encrypt_checkpoint
from .ingestion import resolve_current,atomic_json,sha256_file,managed_lock
from .transport_parts import (INDEX_NAME as INDEX_ASSET,PART_BYTES,MAX_PARTS,
    MAX_INDEX_BYTES,read_index,split_checkpoint,assemble_checkpoint)

REPOSITORY='do0405/price-lab-data'
ASSET='checkpoint.pldata'
SINGLE_MAX_BYTES=2*1024**3
API='https://api.github.com/repos/'+REPOSITORY

def _request(method,path,*,token=None,payload=None,stream=None):
    headers={'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'}
    token=token or os.environ.get('GITHUB_TOKEN')
    if token:headers['Authorization']='Bearer '+token
    url=path if path.startswith('https://') else API+path
    if urlsplit(url).hostname not in ('api.github.com','uploads.github.com'):raise ValueError('Invalid GitHub API host')
    if stream is not None:headers['Content-Type']='application/octet-stream'
    response=requests.request(method,url,headers=headers,json=payload if stream is None else None,data=stream,timeout=(20,900),allow_redirects=False)
    if response.status_code not in (200,201,204):raise RuntimeError(f'GitHub {method} failed: HTTP {response.status_code}')
    return response.json() if response.content else {}

def latest_release():
    return _request('GET','/releases/latest')

def record_periodic_status(manifest,*,now=None):
    """Monthly verified operating record also keeps the public repo active."""
    if os.environ.get('GITHUB_REF')!='refs/heads/main' or os.environ.get('GITHUB_EVENT_NAME') not in ('schedule','workflow_dispatch'):
        return {'recorded':False,'reason':'trusted cloud runs only'}
    now=now or datetime.now(timezone.utc)
    head=_request('GET','/commits/main')
    last=datetime.fromisoformat(head['commit']['committer']['date'].replace('Z','+00:00'))
    if (now-last).total_seconds()<25*86400:return {'recorded':False,'reason':'recent repository activity'}
    path='/contents/.github/price-pipeline-status.json'
    try:old=_request('GET',path)
    except RuntimeError as exc:
        if 'HTTP 404' not in str(exc):raise
        old={}
    record={'last_validated_at':now.isoformat(),'sequence':manifest['sequence'],'sha256':manifest['sha256'],'markets':manifest['markets']}
    payload={'message':'Record verified price pipeline status','branch':'main','content':base64.b64encode((json.dumps(record,sort_keys=True,indent=2)+'\n').encode()).decode()}
    if old.get('sha'):payload['sha']=old['sha']
    _request('PUT',path,payload=payload)
    return {'recorded':True,'path':'.github/price-pipeline-status.json'}

def pull_latest(root,*,key=None):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    # Separate transport lock: decrypt_checkpoint owns the dataset publication lock.
    with managed_lock(root/'transport'):
        return _pull_latest(root,key=key)

def _pull_latest(root,*,key=None):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    release=latest_release()
    if release.get('draft') or release.get('prerelease'):raise ValueError('Only published stable checkpoints are accepted')
    assets=release.get('assets',[])
    if not isinstance(assets,list) or not 1<=len(assets)<=MAX_PARTS+1:raise ValueError('Invalid checkpoint release asset count')
    if any(not isinstance(a,dict) or not isinstance(a.get('name'),str) for a in assets):raise ValueError('Invalid checkpoint asset metadata')
    names=[a.get('name') for a in assets]
    if len(names)!=len(set(names)):raise ValueError('Duplicate checkpoint release asset')
    single=len(assets)==1 and names==[ASSET]
    if not single and (INDEX_ASSET not in names or ASSET in names):raise ValueError('Invalid checkpoint release layout')
    for item in assets:
        limit=SINGLE_MAX_BYTES if single else MAX_INDEX_BYTES if item.get('name')==INDEX_ASSET else PART_BYTES
        _validate_asset(item,limit)
    asset=assets[0] if single else next(a for a in assets if a['name']==INDEX_ASSET)
    current=resolve_current(root)
    receipt_path=root/'cloud-receipt.json'
    try:receipt=json.loads(receipt_path.read_text(encoding='utf-8'))
    except (OSError,ValueError):receipt={}
    signature=[{k:a[k] for k in ('id','name','size')} for a in sorted(assets,key=lambda a:a['name'])]
    legacy_match=single and receipt.get('asset_id')==asset['id'] and receipt.get('asset_size')==asset['size']
    parts_match=not single and receipt.get('transport')=='multipart-v1' and receipt.get('assets')==signature
    if current and receipt.get('release_id')==release['id'] and (legacy_match or parts_match) and receipt.get('sha256')==current['sha256']:
        return {'manifest':current,'release_url':release['html_url'],'release_tag':release['tag_name'],'unchanged':True,'downloaded':False}
    with tempfile.TemporaryDirectory(prefix='download-',dir=root) as temporary:
        temporary=Path(temporary);target=temporary/ASSET
        if single:_download_asset(asset,target)
        else:
            index_path=temporary/INDEX_ASSET;_download_asset(asset,index_path)
            index=read_index(index_path,key=key,current=current)
            expected={p['name']:p for p in index['parts']}
            if set(names)!={INDEX_ASSET,*expected}:raise ValueError('Missing/unexpected checkpoint part asset')
            remote={a['name']:a for a in assets}
            for part in index['parts']:
                item=remote[part['name']]
                if item['size']!=part['size']:raise ValueError('Remote checkpoint part size mismatch')
                digest=_download_asset(item,temporary/part['name'])
                if digest!=part['sha256']:raise ValueError('Downloaded checkpoint part hash mismatch')
            assemble_checkpoint(index,temporary,target,key=key,current=current)
        manifest=decrypt_checkpoint(target,root,key=key)
        receipt={'release_id':release['id'],'asset_id':asset['id'],'asset_size':asset['size'],'sha256':manifest['sha256'],'ciphertext_sha256':sha256_file(target)}
        if not single:receipt.update(transport='multipart-v1',assets=signature,index_sha256=sha256_file(index_path))
        atomic_json(receipt_path,receipt)
    return {'manifest':manifest,'release_url':release['html_url'],'release_tag':release['tag_name'],'downloaded':True}


def _validate_asset(asset,limit):
    if not isinstance(asset.get('name'),str) or '/' in asset['name'] or '\\' in asset['name'] or ':' in asset['name'] or asset['name'] in ('.','..'):
        raise ValueError('Unsafe checkpoint asset name')
    if type(asset.get('id')) is not int or asset['id']<1 or type(asset.get('size')) is not int or not 0<asset['size']<=limit:raise ValueError('Checkpoint download size/identity outside limit')
    parsed=urlsplit(asset.get('browser_download_url',''))
    if parsed.scheme!='https' or parsed.netloc!='github.com' or parsed.query or parsed.fragment or not parsed.path.startswith('/'+REPOSITORY+'/releases/download/') or not parsed.path.endswith('/'+asset['name']):
        raise ValueError('Unexpected checkpoint download URL')


def _download_asset(asset,target):
    """Stream public data without an API token or any provider credentials."""
    import hashlib
    total=0;digest=hashlib.sha256()
    with requests.get(asset['browser_download_url'],stream=True,timeout=(20,180),allow_redirects=True) as response:
        if response.status_code!=200:raise RuntimeError(f'Checkpoint download failed: HTTP {response.status_code}')
        if urlsplit(response.url).scheme!='https':raise ValueError('Insecure checkpoint redirect')
        with Path(target).open('wb') as out:
            for block in response.iter_content(4*1024*1024):
                total+=len(block)
                if total>asset['size']:raise ValueError('Checkpoint exceeds declared size')
                out.write(block);digest.update(block)
    if total!=asset['size']:raise ValueError('Incomplete checkpoint download')
    return digest.hexdigest()

def publish_current(root,*,key=None,token=None):
    current=resolve_current(root,verify=True)
    if current is None:raise ValueError('No validated checkpoint to publish')
    try:previous=latest_release()
    except RuntimeError as exc:
        if 'HTTP 404' not in str(exc):raise
        previous=None
    if previous:
        try:previous_manifest=json.loads(previous['body'])['checkpoint']
        except (ValueError,KeyError,TypeError):raise ValueError('Latest release metadata invalid') from None
        if previous_manifest['sha256']==current['sha256']:
            return {'release_url':previous['html_url'],'tag':previous['tag_name'],'unchanged':True}
        if current['parent_sha256']!=previous_manifest['sha256']:
            raise ValueError('Cloud checkpoint advanced; pull before publishing')
    tag=f"data-{current['sequence']:08d}-{current['sha256'][:12]}"
    public={k:v for k,v in current.items() if k!='resolved_path'}
    with tempfile.TemporaryDirectory(prefix='publish-',dir=Path(root)) as temporary:
        asset=Path(temporary)/ASSET
        envelope=encrypt_checkpoint(current['resolved_path'],public,asset,key=key)
        upload=[asset]
        if asset.stat().st_size>=SINGLE_MAX_BYTES:
            directory=Path(temporary)/'parts';index=split_checkpoint(asset,public,directory,key=key,part_size=PART_BYTES)
            if index['ciphertext_sha256']!=envelope['ciphertext_sha256']:raise ValueError('Encrypted checkpoint changed before upload')
            upload=[directory/p['name'] for p in index['parts']]+[directory/INDEX_ASSET]
        body=json.dumps({'checkpoint':public,'ciphertext_sha256':envelope['ciphertext_sha256']},sort_keys=True,separators=(',',':'))
        try:release=_request('GET','/releases/tags/'+tag,token=token)
        except RuntimeError as exc:
            if 'HTTP 404' not in str(exc):raise
            release=_request('POST','/releases',token=token,payload={'tag_name':tag,'name':tag,'draft':True,'prerelease':False,'body':body,'target_commitish':'main'})
        if not release.get('draft') or release.get('tag_name')!=tag:raise ValueError('Checkpoint draft identity mismatch')
        # A previous upload can have succeeded even if its response was lost.
        # Replace assets only while the release is still private/draft.
        for old in release.get('assets',[]):
            _request('DELETE','/releases/assets/'+str(old['id']),token=token)
        _request('PATCH','/releases/'+str(release['id']),token=token,payload={'body':body,'tag_name':tag,'name':tag,'target_commitish':'main'})
        for path in upload:
            with path.open('rb') as stream:
                _request('POST',release['upload_url'].split('{')[0]+'?name='+path.name,token=token,stream=stream)
        published=_request('PATCH','/releases/'+str(release['id']),token=token,payload={'draft':False,'make_latest':'true','tag_name':tag,'name':tag,'target_commitish':'main'})
    return {'release_url':published['html_url'],'tag':tag,'unchanged':False,'ciphertext_sha256':envelope['ciphertext_sha256']}
