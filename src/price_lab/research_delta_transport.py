"""Immutable encrypted research packages. Full database transport is never used.

The cloud producer calls only discovery, load_package and publish_package.
Local synchronization imports the database applier inside pull_latest only.
"""
import hashlib
import base64
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import re
import tempfile
import time
from urllib.parse import urlsplit

import requests

REPOSITORY='do0405/price-lab-data'
API='https://api.github.com/repos/'+REPOSITORY
PURPOSE='kr-research-delta'
ASSET='research.pldelta'
MAX_ASSET_BYTES=129*1024**2
MAX_PAGES=100
_HEX=re.compile(r'[0-9a-f]{64}')


def package_tag(sequence,digest):
    if type(sequence) is not int or sequence<1 or not isinstance(digest,str) or not _HEX.fullmatch(digest):
        raise ValueError('Invalid research package identity')
    return f'delta-research-{sequence:012d}-{digest}'


def _api(method,path,*,token=None,payload=None,stream=None):
    url=path if path.startswith('https://') else API+path
    if urlsplit(url).hostname not in ('api.github.com','uploads.github.com'):
        raise ValueError('Invalid research package API host')
    headers={'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'}
    token=token or os.environ.get('GITHUB_TOKEN')
    if token:headers['Authorization']='Bearer '+token
    if stream is not None:headers['Content-Type']='application/octet-stream'
    try:
        response=requests.request(method,url,headers=headers,
            json=payload if stream is None else None,data=stream,
            timeout=(20,180),allow_redirects=False)
    except requests.RequestException:
        raise RuntimeError('Research package API transport failed') from None
    if response.status_code not in (200,201,204):
        raise RuntimeError(f'Research package API failed: HTTP {response.status_code}')
    try:return response.json() if response.content else {}
    except (ValueError,TypeError):raise ValueError('Invalid research package API response') from None


def package_metadata(release):
    if not isinstance(release,dict) or release.get('draft') or release.get('prerelease'):
        raise ValueError('Only published research packages are accepted')
    body=release.get('body')
    if not isinstance(body,str) or len(body.encode('utf-8'))>16000:
        raise ValueError('Invalid research package description')
    try:meta=json.loads(body)
    except (ValueError,TypeError):raise ValueError('Invalid research package description') from None
    if (not isinstance(meta,dict) or meta.get('purpose')!=PURPOSE or
            meta.get('schema_version')!=1 or meta.get('ready') is not True or
            meta.get('stream_id')!='kr-reference-evidence-v1'):
        raise ValueError('Invalid research package purpose')
    sequence=meta.get('sequence');digest=meta.get('package_sha256')
    if release.get('tag_name')!=package_tag(sequence,digest):
        raise ValueError('Research package tag does not match content identity')
    previous=meta.get('previous_package_sha256')
    if ((sequence==1 and previous is not None) or
            (sequence>1 and (not isinstance(previous,str) or not _HEX.fullmatch(previous)))):
        raise ValueError('Invalid research package predecessor')
    cipher=meta.get('ciphertext_sha256')
    if not isinstance(cipher,str) or not _HEX.fullmatch(cipher):
        raise ValueError('Invalid research package ciphertext hash')
    assets=release.get('assets')
    if not isinstance(assets,list) or len(assets)!=1 or not isinstance(assets[0],dict):
        raise ValueError('Research package must contain one encrypted payload')
    asset=assets[0]
    if (asset.get('name')!=ASSET or type(asset.get('id')) is not int or asset['id']<1
            or type(asset.get('size')) is not int or not 0<asset['size']<=MAX_ASSET_BYTES):
        raise ValueError('Invalid research package asset')
    parsed=urlsplit(asset.get('browser_download_url',''))
    expected='/'+REPOSITORY+'/releases/download/'+release['tag_name']+'/'+ASSET
    if (parsed.scheme!='https' or parsed.netloc!='github.com' or parsed.path!=expected
            or parsed.query or parsed.fragment):
        raise ValueError('Invalid research package download URL')
    return meta


def list_packages(*,after_sequence=0,expected_cursor_hash=None,max_pages=MAX_PAGES,token=None):
    if type(after_sequence) is not int or after_sequence<0 or not 1<=max_pages<=MAX_PAGES:
        raise ValueError('Invalid research package discovery bounds')
    if after_sequence and (not isinstance(expected_cursor_hash,str) or not _HEX.fullmatch(expected_cursor_hash)):
        raise ValueError('Applied research cursor hash is required')
    found={};seen_sequences={};reached_cursor=False;matched_cursor=False;latest_remote=0
    for page in range(1,max_pages+1):
        rows=_api('GET',f'/releases?per_page=100&page={page}',token=token)
        if not isinstance(rows,list) or len(rows)>100:raise ValueError('Invalid research package listing')
        for release in rows:
            if not isinstance(release,dict):raise ValueError('Invalid research package listing')
            if not str(release.get('tag_name','')).startswith('delta-research-'):continue
            # Incomplete uploads are not committed log entries.
            if release.get('draft') or release.get('prerelease'):continue
            meta=package_metadata(release);sequence=meta['sequence'];digest=meta['package_sha256']
            latest_remote=max(latest_remote,sequence)
            if sequence in seen_sequences and seen_sequences[sequence]!=digest:
                raise ValueError('Conflicting research packages at one sequence')
            seen_sequences[sequence]=digest
            if sequence<=after_sequence:
                reached_cursor=True
                if sequence==after_sequence:
                    if digest!=expected_cursor_hash:raise ValueError('Remote research cursor conflicts with locally applied content')
                    matched_cursor=True
            else:found[sequence]=release
        if len(rows)<100 or reached_cursor:break
    else:
        raise ValueError('Research package discovery bound reached; earlier pages required')
    if after_sequence and latest_remote<after_sequence:raise ValueError('Remote research package rollback detected')
    if after_sequence and not matched_cursor:raise ValueError('Applied research cursor is missing from the remote log')
    ordered=[found[k] for k in sorted(found)]
    expected=after_sequence+1
    previous=None
    for release in ordered:
        meta=package_metadata(release)
        if meta['sequence']!=expected:raise ValueError('Research package sequence gap')
        if previous and meta['previous_package_sha256']!=previous:
            raise ValueError('Research package predecessor mismatch')
        previous=meta['package_sha256'];expected+=1
    return ordered


def newest_package(*,token=None):
    # A producer needs only the latest authenticated state, not the complete log.
    for page in range(1,MAX_PAGES+1):
        rows=_api('GET',f'/releases?per_page=100&page={page}',token=token)
        if not isinstance(rows,list) or len(rows)>100:raise ValueError('Invalid research package listing')
        found=[]
        for release in rows:
            if not isinstance(release,dict):raise ValueError('Invalid research package listing')
            if str(release.get('tag_name','')).startswith('delta-research-') and not release.get('draft') and not release.get('prerelease'):
                found.append((package_metadata(release)['sequence'],release))
        if found:
            top=max(n for n,_ in found);matches=[r for n,r in found if n==top]
            if len({package_metadata(r)['package_sha256'] for r in matches})!=1:
                raise ValueError('Conflicting latest research package')
            return matches[0]
        if len(rows)<100:return None
    raise ValueError('Latest research package discovery bound reached; refusing a new bootstrap')


def download_package(release,directory):
    meta=package_metadata(release);asset=release['assets'][0]
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    target=directory/ASSET
    for attempt in range(3):
        digest=hashlib.sha256();total=0
        try:
            with requests.get(asset['browser_download_url'],stream=True,
                    timeout=(20,120),allow_redirects=True) as response:
                if response.status_code!=200:raise RuntimeError('Research package download HTTP failure')
                final=urlsplit(response.url)
                if final.scheme!='https' or final.hostname not in ('github.com','release-assets.githubusercontent.com'):
                    raise ValueError('Invalid research package redirect')
                with target.open('wb') as out:
                    for block in response.iter_content(1024*1024):
                        total+=len(block)
                        if total>asset['size']:raise ValueError('Research package exceeds declared size')
                        out.write(block);digest.update(block)
            if total!=asset['size']:raise ValueError('Incomplete research package download')
            if digest.hexdigest()!=meta['ciphertext_sha256']:raise ValueError('Research package ciphertext hash mismatch')
            return target
        except requests.exceptions.SSLError:
            raise RuntimeError('Research package TLS verification failed') from None
        except (requests.Timeout,requests.ConnectionError,requests.exceptions.ChunkedEncodingError):
            if attempt==2:raise RuntimeError('Research package download exhausted retries') from None
            time.sleep(attempt+1)
    raise RuntimeError('Research package download failed')


def load_package(release,work_root,*,key=None):
    from .research_delta_codec import open_package,package_digest
    meta=package_metadata(release)
    Path(work_root).mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='research-read-',dir=work_root) as temporary:
        payload=open_package(download_package(release,temporary),key=key or os.environ.get('PRICE_DATA_KEY'))
    if (package_digest(payload)!=meta['package_sha256'] or
            any(payload.get(k)!=meta.get(k) for k in ('sequence','stream_id','previous_package_sha256'))):
        raise ValueError('Authenticated research package metadata mismatch')
    return payload


def publish_package(payload,*,key=None,token=None,work_root=None):
    from .research_delta_codec import validate_payload,package_digest,seal_package
    validate_payload(payload);digest=package_digest(payload)
    tag=package_tag(payload['sequence'],digest)
    try:old=_api('GET','/releases/tags/'+tag,token=token)
    except RuntimeError as exc:
        if 'HTTP 404' not in str(exc):raise
        old=None
    if old and not old.get('draft'):
        meta=package_metadata(old)
        if meta['package_sha256']!=digest:raise ValueError('Immutable research package collision')
        return {'release_url':old['html_url'],'tag':tag,'unchanged':True,'sequence':payload['sequence'],'package_sha256':digest}
    latest=newest_package(token=token)
    parent=package_metadata(latest) if latest else None
    if (payload['sequence']!=(parent['sequence']+1 if parent else 1) or
            payload.get('previous_package_sha256')!=(parent['package_sha256'] if parent else None)):
        raise ValueError('Research log advanced; collect again from the authenticated latest state')
    Path(work_root or tempfile.gettempdir()).mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='research-publish-',dir=work_root) as temporary:
        path=Path(temporary)/ASSET;sealed=seal_package(payload,path,key=key or os.environ.get('PRICE_DATA_KEY'))
        body=dict(purpose=PURPOSE,schema_version=1,ready=False,stream_id='kr-reference-evidence-v1',
            sequence=payload['sequence'],package_sha256=digest,
            previous_package_sha256=payload.get('previous_package_sha256'),
            ciphertext_sha256=sealed['ciphertext_sha256'])
        if old:
            if old.get('tag_name')!=tag:raise ValueError('Research draft identity mismatch')
            for asset in old.get('assets',[]):_api('DELETE','/releases/assets/'+str(asset['id']),token=token)
            release=old
        else:
            release=_api('POST','/releases',token=token,payload=dict(tag_name=tag,name=tag,
                target_commitish='main',draft=True,prerelease=False,body=json.dumps(body,separators=(',',':'))))
        if not release.get('draft') or release.get('tag_name')!=tag:raise ValueError('Research draft identity mismatch')
        common=dict(tag_name=tag,name=tag,target_commitish='main')
        _api('PATCH','/releases/'+str(release['id']),token=token,
            payload={**common,'body':json.dumps(body,separators=(',',':'))})
        with path.open('rb') as stream:
            uploaded=_api('POST',release['upload_url'].split('{')[0]+'?name='+ASSET,token=token,stream=stream)
        if uploaded.get('name')!=ASSET or uploaded.get('size')!=path.stat().st_size:
            raise ValueError('Research package upload receipt mismatch')
        body['ready']=True
        published=_api('PATCH','/releases/'+str(release['id']),token=token,payload={**common,
            'body':json.dumps(body,separators=(',',':')),'draft':False,'make_latest':'false'})
        meta=package_metadata(published)
        if meta['package_sha256']!=digest:raise ValueError('Published research package identity mismatch')
    return {'release_url':published['html_url'],'tag':tag,'unchanged':False,
        'sequence':payload['sequence'],'package_sha256':digest,'ciphertext_bytes':uploaded['size']}


def pull_latest(root,*,key=None,max_packages=1,max_bytes=None,token=None):
    """Local only: bounded, contiguous research suffix; failures retain cursor."""
    import duckdb
    from .ingestion import resolve_current,managed_lock
    from .research_delta_apply import apply_packages
    if type(max_packages) is not int or not 1<=max_packages<=100:raise ValueError('Invalid research apply bound')
    if max_bytes is not None and (type(max_bytes) is not int or max_bytes<1):raise ValueError('Invalid research byte bound')
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    with managed_lock(root/'research-transport'):
        current=resolve_current(root)
        if current is None:raise ValueError('Preserved local baseline required')
        after=0;cursor_hash=None
        with duckdb.connect(current['resolved_path'],read_only=True) as db:
            if 'research_delta_receipts' in {r[0] for r in db.execute('SHOW TABLES').fetchall()}:
                row=db.execute("SELECT sequence,package_sha256 FROM research_delta_receipts WHERE stream_id='kr-reference-evidence-v1' ORDER BY sequence DESC LIMIT 1").fetchone()
                if row:after,cursor_hash=row
        releases=list_packages(after_sequence=after,expected_cursor_hash=cursor_hash,token=token)
        selected=[];size=0
        for release in releases[:max_packages]:
            amount=release['assets'][0]['size']
            if max_bytes is not None and size+amount>max_bytes:
                if not selected:raise ValueError('First pending package exceeds receive byte budget')
                break
            selected.append(release);size+=amount
        if selected:
            result=apply_packages((load_package(r,root/'research-download',key=key) for r in selected),root)
            if result.get('error_code')=='RESEARCH_STORAGE_RESERVE_REQUIRED':
                return {**result,'more_available':False,'downloaded':True,
                    'transport':'research-delta-v1','ciphertext_bytes':size,'cloud_database':False}
            if result['status'] not in ('PUBLISHED','UNCHANGED'):raise ValueError('Research validation failed; previous cursor retained')
        else:result={'status':'UNCHANGED','applied_packages':[],'manifest':current}
        more=len(selected)<len(releases)
        if selected and not more:
            # A multi-GB local publication can overlap new ready cloud batches.
            # Rediscovery is reporting after the committed cursor, not a second
            # application or a reason to roll back an already valid publication.
            try:
                more=bool(list_packages(after_sequence=result['applied_sequence'],
                    expected_cursor_hash=result['package_sha256'],token=token))
            except Exception:result['warning_code']='RESEARCH_TIP_CHECK_UNAVAILABLE'
        return {**result,'status':'MORE_AVAILABLE' if more else result['status'],
            'more_available':more,'downloaded':bool(selected),'transport':'research-delta-v1',
            'ciphertext_bytes':size,'cloud_database':False}


