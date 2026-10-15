"""Authenticated transport parts around the unchanged PLDATA01 GCM container.

The index binds the complete public dataset manifest, ordered part names/sizes/
SHA256 values and complete ciphertext SHA256. Its HMAC key is domain separated
from the AES key. Part files contain ciphertext only. Reassembly verifies every
byte and the bound container header before returning a path to cloud_data.
"""
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import uuid

from .cloud_data import MANIFEST_FIELDS,_header,_key,_lineage
from .ingestion import atomic_json,canonical,validate_manifest

INDEX_NAME='checkpoint.index.json'
FORMAT='price-lab-parts-v1'
PART_BYTES=1024**3
MAX_PARTS=128
# Match cloud_data's 32GiB plaintext limit, plus 32MiB for gzip/framing/header.
MAX_TOTAL_BYTES=32*1024**3+32*1024**2
MAX_INDEX_BYTES=8*1024**2
BLOCK=4*1024**2
_KEY_DOMAIN=b'price-lab/transport-index/key/v1'
_PAYLOAD_DOMAIN=b'price-lab/transport-index/payload/v1\0'


def _mac(payload,key):
    derived=hmac.new(_key(key),_KEY_DOMAIN,hashlib.sha256).digest()
    return hmac.new(derived,_PAYLOAD_DOMAIN+canonical(payload).encode('utf-8'),hashlib.sha256).hexdigest()


def _integer(value,maximum):return type(value) is int and 0<value<=maximum


def _digest(value):return isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value) is not None


def verify_index(index,*,key=None,current=None):
    """Authenticate and validate a bounded v1 index, including local lineage."""
    required={'format','checkpoint','ciphertext_sha256','total_size','parts','hmac_sha256'}
    if not isinstance(index,dict) or set(index)!=required:raise ValueError('Invalid multipart index fields')
    if len(canonical(index).encode('utf-8'))>MAX_INDEX_BYTES:raise ValueError('Multipart index exceeds size limit')
    payload={k:v for k,v in index.items() if k!='hmac_sha256'}
    if not _digest(index['hmac_sha256']) or not hmac.compare_digest(_mac(payload,key),index['hmac_sha256']):raise ValueError('Multipart index authentication failed')
    if index['format']!=FORMAT or not _digest(index['ciphertext_sha256']) or not _integer(index['total_size'],MAX_TOTAL_BYTES):raise ValueError('Invalid multipart container metadata')
    manifest=index['checkpoint']
    if not isinstance(manifest,dict) or not set(manifest)<=set(MANIFEST_FIELDS):raise ValueError('Invalid multipart dataset identity')
    validate_manifest(manifest);_lineage(manifest,current)
    parts=index['parts']
    if not isinstance(parts,list) or not 2<=len(parts)<=MAX_PARTS:raise ValueError('Multipart part count outside limit')
    total=0
    for number,part in enumerate(parts,1):
        expected=f'checkpoint.pldata.part-{number:05d}'
        if not isinstance(part,dict) or set(part)!={'name','size','sha256'} or part['name']!=expected:raise ValueError('Invalid multipart order/name')
        if not _integer(part['size'],PART_BYTES) or not _digest(part['sha256']):raise ValueError('Invalid multipart part size/hash')
        total+=part['size']
    if total!=index['total_size']:raise ValueError('Multipart total size mismatch')
    return index


def _unique_object(pairs):
    value={}
    for name,item in pairs:
        if name in value:raise ValueError('Duplicate multipart JSON field')
        value[name]=item
    return value


def read_index(path,*,key=None,current=None):
    with Path(path).open('rb') as source:raw=source.read(MAX_INDEX_BYTES+1)
    if len(raw)>MAX_INDEX_BYTES:raise ValueError('Multipart index exceeds size limit')
    try:index=json.loads(raw,object_pairs_hook=_unique_object)
    except (UnicodeError,json.JSONDecodeError):raise ValueError('Invalid multipart index JSON') from None
    return verify_index(index,key=key,current=current)


def _identity(path,manifest):
    with Path(path).open('rb') as source:
        _,_,header=_header(source);start=source.tell();source.seek(0,2)
        if source.tell()-start<16:raise ValueError('Truncated multipart container')
    if canonical(header)!=canonical(manifest):raise ValueError('Multipart container/dataset identity mismatch')


def split_checkpoint(encrypted_path,manifest,directory,*,key=None,part_size=PART_BYTES):
    """Stream an existing encrypted container into <=1GiB parts and sign index."""
    if not _integer(part_size,PART_BYTES):raise ValueError('Invalid multipart part size')
    source_path=Path(encrypted_path);size=source_path.stat().st_size
    if not _integer(size,MAX_TOTAL_BYTES) or not 2<=(size+part_size-1)//part_size<=MAX_PARTS:raise ValueError('Multipart size/count outside limit')
    public={k:manifest[k] for k in MANIFEST_FIELDS if k in manifest};validate_manifest(public);_identity(source_path,public)
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    if (directory/INDEX_NAME).exists():raise ValueError('Multipart index already exists')
    parts=[];total=0;full=hashlib.sha256();created=[]
    try:
        with source_path.open('rb') as source:
            while total<size:
                name=f'checkpoint.pldata.part-{len(parts)+1:05d}';path=directory/name;digest=hashlib.sha256();written=0
                with path.open('xb') as destination:
                    created.append(path)
                    while written<part_size:
                        block=source.read(min(BLOCK,part_size-written))
                        if not block:
                            if total+written<size:raise ValueError('Encrypted container changed while splitting')
                            break
                        destination.write(block);digest.update(block);full.update(block);written+=len(block)
                    destination.flush();os.fsync(destination.fileno())
                total+=written;parts.append({'name':name,'size':written,'sha256':digest.hexdigest()})
        if total!=size:raise ValueError('Encrypted container changed while splitting')
        payload={'format':FORMAT,'checkpoint':public,'ciphertext_sha256':full.hexdigest(),'total_size':total,'parts':parts}
        index={**payload,'hmac_sha256':_mac(payload,key)};verify_index(index,key=key)
        atomic_json(directory/INDEX_NAME,index)
        return index
    except Exception:
        for path in created:path.unlink(missing_ok=True)
        raise


def assemble_checkpoint(index,directory,output,*,key=None,current=None):
    """Verify all parts/full ciphertext/header before atomically returning it."""
    verify_index(index,key=key,current=current)
    directory=Path(directory).resolve();output=Path(output)
    output.parent.mkdir(parents=True,exist_ok=True);temp=output.with_name('.'+output.name+'.'+uuid.uuid4().hex+'.part')
    full=hashlib.sha256();total=0
    try:
        with temp.open('xb') as destination:
            for part in index['parts']:
                path=(directory/part['name']).resolve()
                if not path.is_relative_to(directory) or not path.is_file():raise ValueError('Missing/unsafe checkpoint part')
                if path.stat().st_size!=part['size']:raise ValueError('Checkpoint part size mismatch')
                digest=hashlib.sha256();received=0
                with path.open('rb') as source:
                    for block in iter(lambda:source.read(BLOCK),b''):
                        received+=len(block)
                        if received>part['size']:raise ValueError('Checkpoint part grew during assembly')
                        destination.write(block);digest.update(block);full.update(block)
                if received!=part['size'] or digest.hexdigest()!=part['sha256']:raise ValueError('Checkpoint part hash mismatch')
                total+=received
            destination.flush();os.fsync(destination.fileno())
        if total!=index['total_size'] or full.hexdigest()!=index['ciphertext_sha256']:raise ValueError('Complete ciphertext hash/size mismatch')
        _identity(temp,index['checkpoint'])
        os.replace(temp,output)
        return output
    finally:temp.unlink(missing_ok=True)
