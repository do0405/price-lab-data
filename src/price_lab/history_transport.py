"""Authenticated draft candidates; only the cloud writer publishes stable data."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import urlsplit

import duckdb
import requests

from .cloud_data import encrypt_checkpoint, decrypt_checkpoint
from .ingestion import resolve_current, sha256_file
from .release_transport import (_request, _validate_asset, publish_current,
    API, ASSET, SINGLE_MAX_BYTES, release_manifest)
from .transport_parts import (INDEX_NAME, MAX_INDEX_BYTES, MAX_PARTS,
    PART_BYTES, read_index, split_checkpoint, assemble_checkpoint)

PREFIX='candidate-unified-'


def candidate_metadata(release):
    try:body=json.loads(release['body'])
    except (KeyError,TypeError,ValueError):raise ValueError('Invalid integration candidate metadata') from None
    if not isinstance(body,dict) or body.get('purpose')!='unified-history' or body.get('schema_version')!=1 or body.get('ready') is not True or release.get('draft') is not True:
        raise ValueError('Candidate is not a ready integration draft')
    digest=body.get('checkpoint',{}).get('sha256')
    if not isinstance(digest,str) or not re.fullmatch('[0-9a-f]{64}',digest) or release.get('tag_name')!=PREFIX+digest:
        raise ValueError('Candidate tag/hash mismatch')
    if not isinstance(body.get('ciphertext_sha256'),str) or not re.fullmatch('[0-9a-f]{64}',body['ciphertext_sha256']):
        raise ValueError('Candidate ciphertext identity missing')
    return body


def pending_candidates(*,token=None,max_pages=5,limit=8,exclude=()):
    if not 1<=max_pages<=10 or not 1<=limit<=32:raise ValueError('Invalid discovery bounds')
    result=[]
    for page in range(1,max_pages+1):
        releases=_request('GET',f'/releases?per_page=100&page={page}',token=token)
        if not isinstance(releases,list):raise ValueError('Invalid candidate listing')
        for release in releases:
            if not isinstance(release,dict) or not str(release.get('tag_name','')).startswith(PREFIX):continue
            try:metadata=candidate_metadata(release)
            except ValueError:continue
            if metadata['checkpoint']['sha256'] in exclude:continue
            result.append(release)
        if len(releases)<100:break
    return sorted(result,key=lambda r:(r.get('created_at',''),r['tag_name']))[:limit]


def upload_candidate(root,*,key=None,token=None):
    current=resolve_current(root)
    if current is None:raise ValueError('Validated integration candidate required')
    tag=PREFIX+current['sha256']
    try:old=_request('GET','/releases/tags/'+tag,token=token)
    except RuntimeError as exc:
        if 'HTTP 404' not in str(exc):raise
        old=None
    if old:
        try:metadata=candidate_metadata(old)
        except ValueError:metadata=None
        if metadata and metadata['checkpoint']['sha256']==current['sha256']:
            return {'status':'UPLOADED_PENDING_INTEGRATION','tag':tag,'sha256':current['sha256'],'unchanged':True}
        if not old.get('draft'):raise ValueError('Candidate tag is not a draft')
    public={k:v for k,v in current.items() if k!='resolved_path'}
    with tempfile.TemporaryDirectory(prefix='candidate-upload-',dir=root) as temp:
        temp=Path(temp);encrypted=temp/ASSET
        envelope=encrypt_checkpoint(current['resolved_path'],public,encrypted,key=key)
        paths=[encrypted]
        if encrypted.stat().st_size>=SINGLE_MAX_BYTES:
            part_dir=temp/'parts';index=split_checkpoint(encrypted,public,part_dir,key=key,part_size=PART_BYTES)
            paths=[part_dir/p['name'] for p in index['parts']]+[part_dir/INDEX_NAME]
        body={'purpose':'unified-history','schema_version':1,'ready':False,
              'checkpoint':release_manifest(public),'ciphertext_sha256':envelope['ciphertext_sha256']}
        release=old or _request('POST','/releases',token=token,payload={
            'tag_name':tag,'name':tag,'draft':True,'prerelease':False,
            'target_commitish':'main','body':json.dumps(body,sort_keys=True)})
        # Incomplete drafts are not discovered. Clear old partial uploads only.
        _request('PATCH','/releases/'+str(release['id']),token=token,payload={'body':json.dumps(body,sort_keys=True)})
        for asset in release.get('assets',[]):_request('DELETE','/releases/assets/'+str(asset['id']),token=token)
        for path in paths:
            with path.open('rb') as stream:
                _request('POST',release['upload_url'].split('{')[0]+'?name='+path.name,token=token,stream=stream)
        body['ready']=True
        _request('PATCH','/releases/'+str(release['id']),token=token,payload={'body':json.dumps(body,sort_keys=True)})
    return {'status':'UPLOADED_PENDING_INTEGRATION','tag':tag,'sha256':current['sha256'],'unchanged':False}


def _download_draft_asset(asset,target,*,token=None):
    token=token or os.environ.get('GITHUB_TOKEN')
    if not token:raise ValueError('Authenticated candidate download required')
    url=API+'/releases/assets/'+str(asset['id'])
    response=requests.get(url,headers={'Authorization':'Bearer '+token,'Accept':'application/octet-stream',
        'X-GitHub-Api-Version':'2022-11-28'},stream=True,timeout=(20,180),allow_redirects=False)
    if response.status_code in (301,302,303,307,308):
        redirect=response.headers.get('Location','');response.close()
        parsed=urlsplit(redirect)
        if parsed.scheme!='https' or parsed.hostname not in ('release-assets.githubusercontent.com','objects.githubusercontent.com','github.com'):
            raise ValueError('Unexpected candidate asset redirect')
        # No provider/API credential is forwarded to the signed asset URL.
        response=requests.get(redirect,stream=True,timeout=(20,180),allow_redirects=False)
    with response:
        if response.status_code!=200:raise RuntimeError('Candidate asset download failed')
        total=0;digest=hashlib.sha256()
        with Path(target).open('wb') as out:
            for block in response.iter_content(4*1024*1024):
                total+=len(block)
                if total>asset['size']:raise ValueError('Candidate asset exceeds declared size')
                out.write(block);digest.update(block)
        if total!=asset['size']:raise ValueError('Candidate asset is incomplete')
    return digest.hexdigest()


def restore_candidate(release,isolated_root,*,key=None,token=None):
    body=candidate_metadata(release);assets=release.get('assets',[])
    if not isinstance(assets,list) or not 1<=len(assets)<=MAX_PARTS+1:raise ValueError('Invalid candidate asset count')
    names=[a.get('name') for a in assets]
    if len(names)!=len(set(names)):raise ValueError('Duplicate candidate assets')
    single=names==[ASSET]
    if not single and (INDEX_NAME not in names or ASSET in names):raise ValueError('Invalid candidate asset layout')
    for asset in assets:_validate_asset(asset,SINGLE_MAX_BYTES if single else MAX_INDEX_BYTES if asset['name']==INDEX_NAME else PART_BYTES)
    root=Path(isolated_root);root.mkdir(parents=True,exist_ok=True)
    if (root/'current.json').exists():raise ValueError('Candidate restore root must be isolated and empty')
    with tempfile.TemporaryDirectory(prefix='candidate-download-',dir=root) as temp:
        temp=Path(temp);encrypted=temp/ASSET
        if single:_download_draft_asset(assets[0],encrypted,token=token)
        else:
            index_path=temp/INDEX_NAME
            _download_draft_asset(next(a for a in assets if a['name']==INDEX_NAME),index_path,token=token)
            index=read_index(index_path,key=key,current=None)
            if index['checkpoint']['sha256']!=body['checkpoint']['sha256']:raise ValueError('Candidate part manifest mismatch')
            expected={p['name']:p for p in index['parts']}
            if set(names)!={INDEX_NAME,*expected}:raise ValueError('Candidate part layout mismatch')
            remote={a['name']:a for a in assets}
            for name,part in expected.items():
                if remote[name]['size']!=part['size']:raise ValueError('Candidate part size mismatch')
                digest=_download_draft_asset(remote[name],temp/name,token=token)
                if digest!=part['sha256']:raise ValueError('Candidate part hash mismatch')
            assemble_checkpoint(index,temp,encrypted,key=key,current=None)
        if sha256_file(encrypted)!=body['ciphertext_sha256']:raise ValueError('Candidate ciphertext hash mismatch')
        manifest=decrypt_checkpoint(encrypted,root,key=key)
    if manifest['sha256']!=body['checkpoint']['sha256']:raise ValueError('Candidate checkpoint identity mismatch')
    return manifest


def _received(root,digest):
    current=resolve_current(root)
    if not current:return False
    with duckdb.connect(current['resolved_path'],read_only=True) as db:
        names={r[0] for r in db.execute('SHOW TABLES').fetchall()}
        return 'integration_candidate_receipts' in names and bool(db.execute(
            'SELECT count(*) FROM integration_candidate_receipts WHERE candidate_sha256=?',[digest]).fetchone()[0])


def process_pending(root,*,key=None,token=None):
    from .history_integration import merge_candidate
    result=[]
    current=resolve_current(root)
    completed=set()
    if current:
        with duckdb.connect(current['resolved_path'],read_only=True) as db:
            if 'integration_candidate_receipts' in {r[0] for r in db.execute('SHOW TABLES').fetchall()}:
                completed={r[0] for r in db.execute('SELECT candidate_sha256 FROM integration_candidate_receipts').fetchall()}
    # Ensure local receipts are already stable before treating them as complete.
    if completed:publish_current(root,key=key,token=token)
    for release in pending_candidates(token=token,exclude=completed):
        body=candidate_metadata(release);digest=body['checkpoint']['sha256'];tag=release['tag_name']
        if _received(root,digest):
            # A prior attempt can have committed locally before a failed upload.
            # Confirm/publish that direct child before reporting remote completion.
            publish_current(root,key=key,token=token)
            result.append({'tag':tag,'status':'ALREADY_INTEGRATED'});continue
        try:
            with tempfile.TemporaryDirectory(prefix='candidate-restore-',dir=root) as temp:
                manifest=restore_candidate(release,Path(temp)/'isolated',key=key,token=token)
                out=merge_candidate(manifest['resolved_path'],manifest,root=root)
                # Publish this direct child before creating another checkpoint.
                published=publish_current(root,key=key,token=token)
            result.append({'tag':tag,'status':'INTEGRATED','sequence':out['sequence'],'release_url':published['release_url']})
        except Exception as exc:
            result.append({'tag':tag,'status':'FAILED','error':type(exc).__name__})
            break
    return result
