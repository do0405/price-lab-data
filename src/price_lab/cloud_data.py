"""Streaming gzip/AES-256-GCM checkpoint envelope, independent of release transport.

Public header metadata is authenticated as GCM AAD. The database is compressed
and encrypted; no plaintext DB is released. Authentication completes before any
gzip decompression or DuckDB open. Temporary plaintext is removed on every exit.
Only an authenticated descendant of the local checkpoint may replace current.
PRICE_DATA_KEY is a32-byte key, normally URL-safe base64; it is never logged.
"""
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import struct
import tempfile
import uuid
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers import Cipher,algorithms,modes
from .ingestion import (MANAGED_ROOT,atomic_json,canonical,managed_lock,resolve_current,
                        sha256_file,validate_database,validate_manifest)

MAGIC=b'PLDATA01'
MAX_HEADER=4*1024*1024
BLOCK=4*1024*1024
MANIFEST_FIELDS=('schema_version','key_version','sequence','database','sha256','parent_sha256','ancestors','includes_supplement','markets','calendar_evidence','created_at','validation')


def _key(value):
    value=os.environ.get('PRICE_DATA_KEY') if value is None else value
    if isinstance(value,bytes):key=value
    elif isinstance(value,str):
        try:key=base64.b64decode(value.encode('ascii'),altchars=b'-_',validate=True)
        except (ValueError,UnicodeError):raise ValueError('PRICE_DATA_KEY must encode a32-byte key') from None
    else:raise ValueError('PRICE_DATA_KEY is required')
    if len(key)!=32:raise ValueError('PRICE_DATA_KEY must be a32-byte key')
    return key


class _EncryptWriter:
    def __init__(self,stream,encryptor):self.stream=stream;self.encryptor=encryptor
    def write(self,data):self.stream.write(self.encryptor.update(data));return len(data)
    def flush(self):self.stream.flush()


def encrypt_checkpoint(db_path,manifest,output,key=None):
    key=_key(key);db_path=Path(db_path);output=Path(output)
    public={k:manifest[k] for k in MANIFEST_FIELDS if k in manifest};validate_manifest(public)
    if sha256_file(db_path)!=public['sha256']:raise ValueError('Checkpoint hash mismatch before encryption')
    validate_database(db_path,public)
    header=canonical(public).encode('utf-8')
    if len(header)>MAX_HEADER:raise ValueError('Manifest too large')
    prefix=MAGIC+struct.pack('>I',len(header))+header;nonce=os.urandom(12)
    encryptor=Cipher(algorithms.AES(key),modes.GCM(nonce)).encryptor();encryptor.authenticate_additional_data(prefix)
    output.parent.mkdir(parents=True,exist_ok=True);temp=output.with_name('.'+output.name+'.'+uuid.uuid4().hex+'.part')
    try:
        with temp.open('wb') as destination:
            destination.write(prefix);destination.write(nonce)
            with db_path.open('rb') as source,gzip.GzipFile(fileobj=_EncryptWriter(destination,encryptor),mode='wb',compresslevel=3,mtime=0,filename='') as compressed:
                for block in iter(lambda:source.read(BLOCK),b''):compressed.write(block)
            destination.write(encryptor.finalize());destination.write(encryptor.tag);destination.flush();os.fsync(destination.fileno())
        os.replace(temp,output)
    finally:
        if temp.exists():temp.unlink()
    return {'envelope_version':1,'file':output.name,'ciphertext_sha256':sha256_file(output),'manifest':public,'encryption':'AES-256-GCM','compression':'gzip'}


def _header(stream):
    prefix=stream.read(12)
    if len(prefix)!=12 or prefix[:8]!=MAGIC:raise ValueError('Invalid encrypted checkpoint format')
    size=struct.unpack('>I',prefix[8:])[0]
    if not 1<=size<=MAX_HEADER:raise ValueError('Encrypted manifest length invalid')
    raw=stream.read(size);nonce=stream.read(12)
    if len(raw)!=size or len(nonce)!=12:raise ValueError('Truncated encrypted checkpoint')
    try:manifest=json.loads(raw)
    except (ValueError,UnicodeError):raise ValueError('Encrypted manifest invalid') from None
    return prefix+raw,nonce,manifest


def _lineage(manifest,current):
    if current is None:return
    if manifest['sequence']<current['sequence']:raise ValueError('Checkpoint rollback rejected')
    if manifest['sequence']==current['sequence']:
        if manifest['sha256']!=current['sha256']:raise ValueError('Checkpoint rollback/conflicting sequence rejected')
        return
    if current['sha256'] not in manifest['ancestors']:raise ValueError('Checkpoint unrelated lineage rejected')


def decrypt_checkpoint(encrypted_path,root=MANAGED_ROOT,key=None,max_uncompressed_bytes=32*1024**3):
    key=_key(key);root=Path(root).resolve()
    with managed_lock(root):
        current=resolve_current(root)
        # Work is private to this attempt; neither encrypted header nor gzip is trusted yet.
        with tempfile.TemporaryDirectory(prefix='cloud-pull-',dir=root) as temporary:
            temporary=Path(temporary);compressed_path=temporary/'authenticated.gz';database=temporary/'validated.duckdb'
            with Path(encrypted_path).open('rb') as source:
                aad,nonce,manifest=_header(source);cipher_start=source.tell();source.seek(0,2);end=source.tell()
                if end-cipher_start<16:raise ValueError('Truncated encrypted checkpoint tag')
                source.seek(end-16);tag=source.read(16);source.seek(cipher_start)
                decryptor=Cipher(algorithms.AES(key),modes.GCM(nonce,tag)).decryptor();decryptor.authenticate_additional_data(aad)
                try:
                    with compressed_path.open('wb') as destination:
                        remaining=end-16-cipher_start
                        while remaining:
                            block=source.read(min(BLOCK,remaining))
                            if not block:raise ValueError('Truncated ciphertext')
                            destination.write(decryptor.update(block));remaining-=len(block)
                        destination.write(decryptor.finalize())
                except InvalidTag:raise ValueError('Checkpoint authentication failed') from None
            validate_manifest(manifest);_lineage(manifest,current)
            total=0;digest=hashlib.sha256()
            try:
                with gzip.open(compressed_path,'rb') as source,database.open('wb') as destination:
                    for block in iter(lambda:source.read(BLOCK),b''):
                        total+=len(block)
                        if total>max_uncompressed_bytes:raise ValueError('Checkpoint decompression limit exceeded')
                        digest.update(block);destination.write(block)
                    destination.flush();os.fsync(destination.fileno())
            except (OSError,EOFError):raise ValueError('Authenticated gzip checkpoint invalid') from None
            if digest.hexdigest()!=manifest['sha256']:raise ValueError('Decrypted database hash mismatch')
            validate_database(database,manifest)
            if current and manifest['sequence']==current['sequence']:return current
            target=(root/manifest['database']).resolve()
            if not target.is_relative_to(root):raise ValueError('Checkpoint path outside managed root')
            target.parent.mkdir(parents=True,exist_ok=True)
            if target.exists():
                if sha256_file(target)!=manifest['sha256']:raise ValueError('Immutable checkpoint path collision')
            else:os.replace(database,target)
            atomic_json(root/'current.json',manifest)
            return {**manifest,'resolved_path':str(target)}
