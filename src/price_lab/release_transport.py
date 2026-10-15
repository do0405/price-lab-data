"""Public GitHub Releases transport; checkpoint contents remain authenticated/encrypted."""
import json
import os
from pathlib import Path
import tempfile
from urllib.parse import urlsplit
import requests
from .cloud_data import decrypt_checkpoint,encrypt_checkpoint
from .ingestion import resolve_current

REPOSITORY='do0405/price-lab-data'
ASSET='checkpoint.pldata'
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

def pull_latest(root,*,key=None):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    release=latest_release()
    if release.get('draft') or release.get('prerelease'):raise ValueError('Only published stable checkpoints are accepted')
    assets=[a for a in release.get('assets',[]) if a['name']==ASSET]
    if len(assets)!=1:raise ValueError('Release requires exactly one encrypted checkpoint')
    asset=assets[0];url=asset['browser_download_url'];parsed=urlsplit(url)
    if parsed.scheme!='https' or parsed.netloc!='github.com' or not parsed.path.startswith('/'+REPOSITORY+'/releases/download/'):
        raise ValueError('Unexpected checkpoint download URL')
    if not 0<asset['size']<=8*1024**3:raise ValueError('Checkpoint download size outside limit')
    with tempfile.TemporaryDirectory(prefix='download-',dir=root) as temporary:
        target=Path(temporary)/ASSET;total=0
        # Public download: credentials are never sent to an asset/CDN endpoint.
        with requests.get(url,stream=True,timeout=(20,180),allow_redirects=True) as response:
            if response.status_code!=200:raise RuntimeError(f'Checkpoint download failed: HTTP {response.status_code}')
            if urlsplit(response.url).scheme!='https':raise ValueError('Insecure checkpoint redirect')
            with target.open('wb') as out:
                for block in response.iter_content(4*1024*1024):
                    total+=len(block)
                    if total>asset['size']:raise ValueError('Checkpoint exceeds declared size')
                    out.write(block)
        if total!=asset['size']:raise ValueError('Incomplete checkpoint download')
        manifest=decrypt_checkpoint(target,root,key=key)
    return {'manifest':manifest,'release_url':release['html_url'],'release_tag':release['tag_name']}

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
        _request('PATCH','/releases/'+str(release['id']),token=token,payload={'body':body})
        with asset.open('rb') as stream:
            _request('POST',release['upload_url'].split('{')[0]+'?name='+ASSET,token=token,stream=stream)
        published=_request('PATCH','/releases/'+str(release['id']),token=token,payload={'draft':False,'make_latest':'true'})
    return {'release_url':published['html_url'],'tag':tag,'unchanged':False,'ciphertext_sha256':envelope['ciphertext_sha256']}
