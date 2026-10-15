"""Authenticated DB-free daily price packages. Importing performs no I/O.

The bounded public header is GCM AAD. Neither JSON nor gzip is interpreted until
the complete ciphertext authenticates. Payloads contain records, never SQL or
archive paths. Bootstrap HMAC uses a separate domain from package encryption.
Finite nonnegative ordinary raw bars remain evidence even when their OHLC
envelope is ineligible. The collector/local applier enforce benchmark OHLC;
only the local applier decides which ordinary rows enter managed price views.
"""
import base64
from datetime import date, datetime, timezone
import gzip
import hashlib
import hmac
import io
import json
import math
import os
from pathlib import Path
import re
import struct
import tempfile

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from .price_contract import BAR_METADATA,ASSET_METADATA,validate_metadata

MAGIC=b'PLDELTA1'
MAX_HEADER_BYTES=4096
MAX_RAW_BYTES=512*1024**2
MARKETS=('us','kr','crypto')
RAW_COLUMNS=('market','symbol','session_date','open','high','low','close','volume',
             'turnover','vwap','currency','source','reference_previous_close')
ASSET_COLUMNS=('market','symbol','name','exchange')
_IDENTITY=('schema','stream_id','sequence','previous_package_sha256','bootstrap_anchor_sha256','collected_at')
_SECRET_FIELDS={'authorization','apikey','apisecret','appkey','appsecret','accesstoken',
                'refreshtoken','pricedatakey','githubtoken','alpacapikey','alpacapisecret',
                'alpacaapikey','alpacaapisecret','krxapikey','kisappkey','kisappsecret','password'}


def _key(value):
    if isinstance(value,str):
        try:value=base64.b64decode(value.encode('ascii'),altchars=b'-_',validate=True)
        except (ValueError,UnicodeError):raise ValueError('A32-byte data key is required') from None
    if not isinstance(value,bytes) or len(value)!=32:raise ValueError('A32-byte data key is required')
    return value


def _canonical(value):
    try:return json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode('utf-8')
    except (ValueError,TypeError,UnicodeError,RecursionError):raise ValueError('Invalid package JSON values') from None


def _json_tree(value,depth=0):
    if depth>24:raise ValueError('Package nesting limit exceeded')
    if value is None or type(value) is bool:return
    if type(value) in (int,float):
        try:valid=math.isfinite(value)
        except OverflowError:valid=False
        if not valid:raise ValueError('Nonfinite package number')
    elif isinstance(value,str):
        if len(value)>1024*1024 or '\x00' in value:raise ValueError('Package string limit exceeded')
    elif isinstance(value,list):
        if len(value)>2_000_000:raise ValueError('Package list limit exceeded')
        for item in value:_json_tree(item,depth+1)
    elif isinstance(value,dict):
        if len(value)>100_000:raise ValueError('Package mapping limit exceeded')
        for name,item in value.items():
            if not isinstance(name,str) or len(name)>256:raise ValueError('Invalid package field name')
            clean=re.sub('[^a-z0-9]','',name.lower())
            if clean in _SECRET_FIELDS or clean.endswith(('apikey','apisecret','appkey','appsecret','accesstoken','refreshtoken','authkey')):
                raise ValueError('Credential field is forbidden in packages')
            _json_tree(item,depth+1)
    else:raise ValueError('Package requires JSON-native values')


def _fields(value,required,optional=()):
    if not isinstance(value,dict) or not set(required)<=set(value) or set(value)-set(required)-set(optional):
        raise ValueError('Package schema fields mismatch')


def _hash(value):
    if not isinstance(value,str) or re.fullmatch('[0-9a-f]{64}',value) is None:raise ValueError('Invalid package hash')


def _date(value):
    if not isinstance(value,str) or re.fullmatch(r'\d{4}-\d{2}-\d{2}',value) is None:raise ValueError('Invalid session date')
    try:return date.fromisoformat(value)
    except ValueError:raise ValueError('Invalid session date') from None


def _instant(value):
    if not isinstance(value,str):raise ValueError('UTC collection timestamp required')
    try:stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
    except ValueError:raise ValueError('UTC collection timestamp required') from None
    if stamp.tzinfo is None or stamp.utcoffset().total_seconds()!=0:raise ValueError('UTC collection timestamp required')
    return stamp


def _dates(values,limit=100_000):
    if not isinstance(values,list) or len(values)>limit or any(not isinstance(v,str) for v in values) or values!=sorted(set(values)):raise ValueError('Sessions must be sorted and unique within limit')
    for value in values:_date(value)


def _assets(values):
    if not isinstance(values,list):raise ValueError('Asset records required')
    seen=set()
    for row in values:
        _fields(row,ASSET_COLUMNS,ASSET_METADATA)
        validate_metadata(row,ASSET_METADATA)
        if row['market'] not in MARKETS or not isinstance(row['symbol'],str) or not row['symbol'] or len(row['symbol'])>128:raise ValueError('Invalid asset identity')
        if any(row[k] is not None and not isinstance(row[k],str) for k in ('name','exchange')):raise ValueError('Invalid asset metadata')
        key=(row['market'],row['symbol'])
        if key in seen:raise ValueError('Duplicate asset identity')
        seen.add(key)


def _state(value):
    _fields(value,('assets','sessions','watermarks','calendar_evidence'),('audit_cursor',))
    _assets(value['assets'])
    for field in ('sessions','watermarks'):
        if not isinstance(value[field],dict) or set(value[field])!=set(MARKETS):raise ValueError('All required market state is required')
    for market in MARKETS:
        sessions=value['sessions'][market];_dates(sessions)
        if not sessions or value['watermarks'][market]!=sessions[-1]:raise ValueError('Watermark must equal last actual session')
    if not isinstance(value['calendar_evidence'],dict):raise ValueError('Calendar evidence mapping required')
    cursor=value.get('audit_cursor',{})
    if not isinstance(cursor,dict) or set(cursor)-set(MARKETS) or any(type(v) is not int or v<0 for v in cursor.values()):raise ValueError('Invalid audit cursor')


def validate_payload(payload):
    """Validate JSON-native records and chronology; return the original mapping."""
    _fields(payload,(*_IDENTITY,'state','parts','reference_raw'),('reference_errors',))
    _json_tree(payload)
    if payload['schema']!='price-delta-v1' or payload['stream_id']!='price-daily-v1':raise ValueError('Unsupported daily package schema/stream')
    seq=payload['sequence']
    if type(seq) is not int or not 1<=seq<=2**63-1:raise ValueError('Invalid package sequence')
    previous=payload['previous_package_sha256']
    if seq==1:
        if previous is not None:raise ValueError('First package cannot have a predecessor')
    else:_hash(previous)
    _hash(payload['bootstrap_anchor_sha256']);stamp=_instant(payload['collected_at'])
    _state(payload['state'])
    if any(_date(d)>stamp.date() for days in payload['state']['sessions'].values() for d in days):raise ValueError('Future state session rejected')
    if not isinstance(payload['parts'],list) or len(payload['parts'])>20_000:raise ValueError('Invalid package parts')
    seen_sessions={m:set() for m in MARKETS};seen_rows=set()
    for part in payload['parts']:
        _fields(part,('market','sessions','action_start','bars','assets','actions','provenance'))
        market=part['market']
        if market not in MARKETS:raise ValueError('Invalid part market')
        days=part['sessions'];_dates(days,20)
        if not days or not set(days)<=set(payload['state']['sessions'][market]) or seen_sessions[market]&set(days):raise ValueError('Invalid/overlapping part sessions')
        seen_sessions[market].update(days)
        if _date(part['action_start'])>_date(days[0]):raise ValueError('Action boundary after requested bars')
        _assets(part['assets'])
        if any(a['market']!=market for a in part['assets']):raise ValueError('Part asset market mismatch')
        if not isinstance(part['bars'],list) or not isinstance(part['actions'],list) or not isinstance(part['provenance'],dict):raise ValueError('Invalid part records')
        covered=set()
        for row in part['bars']:
            _fields(row,RAW_COLUMNS,('is_supplement',*BAR_METADATA))
            validate_metadata(row,BAR_METADATA)
            if row['market']!=market or row['session_date'] not in days or not isinstance(row['symbol'],str) or not row['symbol']:raise ValueError('Bar identity/session mismatch')
            key=(market,row['symbol'],row['session_date'])
            if key in seen_rows:raise ValueError('Duplicate bar identity')
            seen_rows.add(key);covered.add(row['session_date'])
            for name in ('open','high','low','close','volume','turnover','vwap','reference_previous_close'):
                number=row[name]
                if number is None and name in ('vwap','reference_previous_close'):continue
                if type(number) not in (int,float) or not math.isfinite(number) or number<0:raise ValueError('Invalid raw bar number')
            if not isinstance(row['currency'],str) or not isinstance(row['source'],str):raise ValueError('Invalid raw bar source/currency')
            if 'is_supplement' in row and type(row['is_supplement']) is not bool:raise ValueError('Invalid supplement lineage')
        if covered!=set(days):raise ValueError('Missing completed session bars')
    if not isinstance(payload['reference_raw'],list):raise ValueError('Reference records required')
    ref_seen=set()
    for row in payload['reference_raw']:
        _fields(row,('board','requested_date','kind','payload','retrieved_at'))
        if row['board'] not in ('KOSPI','KOSDAQ') or row['kind'] not in ('daily','master') or row['requested_date'] not in payload['state']['sessions']['kr']:raise ValueError('Invalid reference identity/date')
        _instant(row['retrieved_at'])
        if not isinstance(row['payload'],dict):raise ValueError('Raw reference response must be a mapping')
        key=(row['board'],row['requested_date'],row['kind'])
        if key in ref_seen:raise ValueError('Duplicate reference identity')
        ref_seen.add(key)
    errors=payload.get('reference_errors',[])
    if not isinstance(errors,list) or any(e!='KR_REFERENCE_COLLECTION_FAILED' for e in errors):raise ValueError('Invalid reference error code')
    return payload


def package_digest(payload):
    validate_payload(payload)
    return hashlib.sha256(_canonical(payload)).hexdigest()


def _pairs(items):
    out={}
    for key,value in items:
        if key in out:raise ValueError('Duplicate JSON field')
        out[key]=value
    return out


def _loads(raw):
    try:return json.loads(raw,object_pairs_hook=_pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite JSON value')))
    except (ValueError,UnicodeError,RecursionError):raise ValueError('Invalid package JSON') from None


def seal_package(payload,path,key):
    validate_payload(payload);raw=_canonical(payload)
    if len(raw)>MAX_RAW_BYTES:raise ValueError('Package raw size limit exceeded')
    digest=hashlib.sha256(raw).hexdigest()
    public={k:payload[k] for k in _IDENTITY};public['package_sha256']=digest
    header=_canonical(public)
    if len(header)>MAX_HEADER_BYTES:raise ValueError('Package header limit exceeded')
    aad=MAGIC+struct.pack('>I',len(header))+header;nonce=os.urandom(12)
    encrypted=aad+nonce+AESGCM(_key(key)).encrypt(nonce,gzip.compress(raw,compresslevel=3,mtime=0),aad)
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent,prefix='.delta-',delete=False) as stream:
            temporary=Path(stream.name);stream.write(encrypted);stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
    finally:
        if temporary is not None and temporary.exists():temporary.unlink()
    return {**public,'ciphertext_sha256':hashlib.sha256(encrypted).hexdigest(),'size':len(encrypted),'encryption':'AES-256-GCM','compression':'gzip'}


def open_package(path,key):
    path=Path(path)
    if path.stat().st_size>MAX_RAW_BYTES+MAX_RAW_BYTES//1000+MAX_HEADER_BYTES+128:raise ValueError('Package ciphertext size limit exceeded')
    with path.open('rb') as stream:
        prefix=stream.read(12)
        if len(prefix)!=12 or prefix[:8]!=MAGIC:raise ValueError('Invalid daily package format')
        length=struct.unpack('>I',prefix[8:])[0]
        if not 1<=length<=MAX_HEADER_BYTES:raise ValueError('Package header limit exceeded')
        header=stream.read(length);nonce=stream.read(12);ciphertext=stream.read()
    if len(header)!=length or len(nonce)!=12 or len(ciphertext)<16:raise ValueError('Truncated daily package')
    try:compressed=AESGCM(_key(key)).decrypt(nonce,ciphertext,prefix+header)
    except InvalidTag:raise ValueError('Daily package authentication failed') from None
    public=_loads(header)
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(compressed),mode='rb') as source:raw=source.read(MAX_RAW_BYTES+1)
    except (OSError,EOFError):raise ValueError('Invalid authenticated package compression') from None
    if len(raw)>MAX_RAW_BYTES:raise ValueError('Package raw size limit exceeded')
    payload=validate_payload(_loads(raw))
    expected={k:payload[k] for k in _IDENTITY};expected['package_sha256']=hashlib.sha256(_canonical(payload)).hexdigest()
    if public!=expected:raise ValueError('Package header/payload identity mismatch')
    return payload


def _bootstrap_body(body):
    _fields(body,('schema','anchor_sha256','assets','sessions','watermarks','calendar_evidence'),('audit_cursor',))
    _json_tree(body)
    if body['schema']!='price-delta-bootstrap-v1':raise ValueError('Invalid daily bootstrap schema')
    _hash(body['anchor_sha256']);_state({k:v for k,v in body.items() if k not in ('schema','anchor_sha256')})
    if len(_canonical(body))>16*1024**2:raise ValueError('Bootstrap size limit exceeded')
    return body


def sign_bootstrap(state,key):
    body=_bootstrap_body(state)
    signature=hmac.new(_key(key),b'price-delta-bootstrap-v1\x00'+_canonical(body),hashlib.sha256).hexdigest()
    return {**body,'hmac_sha256':signature}


def verify_bootstrap(envelope,key):
    if not isinstance(envelope,dict):raise ValueError('Invalid daily bootstrap envelope')
    body={k:v for k,v in envelope.items() if k!='hmac_sha256'}
    expected=sign_bootstrap(body,key)['hmac_sha256'];actual=envelope.get('hmac_sha256')
    if not isinstance(actual,str) or not hmac.compare_digest(actual,expected):raise ValueError('Bootstrap authentication failed')
    return body
