"""Authenticated raw research observations, isolated from the daily price stream."""
import base64
import gzip
import hashlib
import hmac
import io
import os
from pathlib import Path
import re
import struct
import tempfile
from urllib.parse import urlsplit,parse_qs

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from .daily_delta_codec import _canonical,_key,_fields,_hash,_date,_instant,_dates,_json_tree,_loads

MAGIC=b'PLRSH001'
MAX_HEADER_BYTES=4096
MAX_RAW_BYTES=128*1024**2
MAX_BODY_BYTES=30*1024**2
MAX_STATE_BYTES=16*1024**2
STREAM='kr-reference-evidence-v1'
_IDENTITY=('schema','stream_id','sequence','previous_package_sha256','bootstrap_anchor_sha256','collected_at')
_STATE=('version','start_date','end_date','sessions','events','pending','seen','completed','index_pages',
        'cursors','discovery','cooldowns','source_hashes')
_ENDPOINTS={('KOSPI','daily'):'stk_bydd_trd',('KOSDAQ','daily'):'ksq_bydd_trd',
    ('KOSPI','master'):'stk_isu_base_info',('KOSDAQ','master'):'ksq_isu_base_info'}


def validate_task(task):
    _json_tree(task)
    common=('source','url','method');source=task.get('source') if isinstance(task,dict) else None
    shapes={'krx_reference':('board','kind','requested_date','params'),
        'kis_merger_split':('start_date','end_date','params'),
        'kind_list':('board','start_date','end_date','params'),
        'kind_index':('event_id','params'),'kind_document':('receipt_id',),
        'kind_document_path':('receipt_id','params')}
    if source not in shapes:raise ValueError('Unsupported research source')
    _fields(task,common+shapes[source],('lookback_days',) if source=='kind_index' else ())
    u=urlsplit(task['url'])
    if u.scheme!='https' or u.username or u.password or u.fragment:raise ValueError('Official source URL required')
    params=task.get('params',{})
    if not isinstance(params,dict) or any(not isinstance(v,(str,int)) or isinstance(v,bool) for v in params.values()):raise ValueError('Invalid research parameters')
    if source=='krx_reference':
        endpoint=_ENDPOINTS.get((task['board'],task['kind']))
        _date(task['requested_date'])
        if (not endpoint or task['url']!='https://data-dbg.krx.co.kr/svc/apis/sto/'+endpoint or
            task['method']!='GET' or params!={'basDd':task['requested_date'].replace('-','')}):raise ValueError('Invalid KRX source task')
    elif source=='kis_merger_split':
        _date(task['start_date']);_date(task['end_date'])
        if (task['url']!='https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/ksdinfo/merger-split' or
            task['method']!='GET' or set(params)!={'F_DT','T_DT','SHT_CD','CTS'} or params['SHT_CD']!='' or
            params['CTS']!='' or params['F_DT']!=task['start_date'].replace('-','') or
            params['T_DT']!=task['end_date'].replace('-','') or task['end_date']<task['start_date']):raise ValueError('Invalid KIS source task')
    else:
        if u.netloc!='kind.krx.co.kr':raise ValueError('Invalid KIND source host')
        if source=='kind_list':
            _date(task['start_date']);_date(task['end_date'])
            if task['board'] not in ('KOSPI','KOSDAQ') or u.path!='/investwarn/delcompany.do' or task['method']!='POST' or u.query or params.get('method')!='searchDelCompanySub':raise ValueError('Invalid KIND list task')
            allowed={'method','currentPageSize','pageIndex','orderMode','orderStat','tabType','searchMode','searchCodeType','searchCorpName','repIsuSrtCd','forward','searchType','marketType','searchCorpNameTmp','fromDate','toDate'}
        elif source=='kind_index':
            if not isinstance(task['event_id'],str) or not task['event_id'] or u.path!='/disclosure/details.do' or task['method']!='POST' or u.query or params.get('method')!='searchDetailsSub':raise ValueError('Invalid KIND index task')
            allowed={'method','forward','currentPageSize','pageIndex','orderMode','orderStat','reportNm','reportNmTemp','searchCorpName','repIsuSrtCd','allRepIsuSrtCd','marketType','fromDate','toDate'}
        elif source=='kind_document_path':
            if u.path!='/common/disclsviewer.do' or u.query or task['method']!='GET' or set(params)!={'method','docNo'} or params['method']!='searchContents' or not re.fullmatch(r'[0-9A-Za-z_-]{1,80}',str(params['docNo'])):raise ValueError('Invalid KIND document lookup')
            allowed=set(params)
        else:
            if task['method']!='GET':raise ValueError('Invalid KIND document method')
            if u.path=='/common/disclsviewer.do':
                q=parse_qs(u.query,keep_blank_values=True)
                if set(q)!={'method','acptNo','docNo'} or q['method']!=['searchInitInfo'] or q['acptNo']!=[task['receipt_id']] or q['docNo']!=['']:raise ValueError('Invalid KIND viewer URL')
            elif not re.fullmatch(r'/external/[0-9]{4}/[0-9]{2}/[0-9]{2}/[A-Za-z0-9_./-]+\.(?:htm|html|pdf)',u.path) or u.query or '..' in u.path:raise ValueError('Invalid KIND original URL')
            allowed=set()
        if set(params)-allowed:raise ValueError('Unexpected research request parameter')
        if 'receipt_id' in task and not re.fullmatch(r'[0-9]{14}',task['receipt_id']):raise ValueError('Invalid native receipt ID')
    from .kr_evidence_sources import validate_task as source_contract
    source_contract(task)
    return task


def validate_state(state):
    _fields(state,_STATE);_json_tree(state)
    if state['version']!='research-collection-state-v1':raise ValueError('Invalid research state version')
    start,end=_date(state['start_date']),_date(state['end_date'])
    if str(start)<'2010-01-01' or end<start:raise ValueError('Invalid research scope')
    _dates(state['sessions'])
    if any(not str(start)<=d<=str(end) for d in state['sessions']):raise ValueError('Session outside research scope')
    _fields(state['pending'],('krx','kis','kind'))
    for tasks in state['pending'].values():
        if not isinstance(tasks,list):raise ValueError('Task queue required')
        for task in tasks:validate_task(task)
    for event in state['events']:
        if not isinstance(event,dict) or not event.get('event_id') or not re.fullmatch('[0-9A-Z]{6}',str(event.get('raw_symbol',''))) or event.get('board') not in ('KOSPI','KOSDAQ'):raise ValueError('Invalid event observation')
        _date(event['effective_date'])
        if any(k in event for k in ('path','raw_path','price_targets','sql')):raise ValueError('Private event state forbidden')
    for name in ('seen','index_pages','cursors','discovery','cooldowns'):
        if not isinstance(state[name],dict):raise ValueError('Research control mapping required')
    for k,v in state['seen'].items():_hash(k);_hash(v)
    for v in state['completed']+state['source_hashes']:_hash(v)
    if len(_canonical(state))>MAX_STATE_BYTES:raise ValueError('Research state size limit exceeded')
    from .research_delta_collect import validate_state as acquisition_contract
    acquisition_contract(state)
    return state


def validate_payload(payload):
    _fields(payload,(*_IDENTITY,'records','state'))
    if payload['schema']!='research-delta-v1' or payload['stream_id']!=STREAM:raise ValueError('Unsupported research stream')
    seq=payload['sequence']
    if type(seq) is not int or not 1<=seq<2**63:raise ValueError('Invalid research sequence')
    if seq==1:
        if payload['previous_package_sha256'] is not None:raise ValueError('First research predecessor forbidden')
    else:_hash(payload['previous_package_sha256'])
    _hash(payload['bootstrap_anchor_sha256']);stamp=_instant(payload['collected_at']);validate_state(payload['state'])
    if _date(payload['state']['end_date'])>stamp.date():raise ValueError('Future research scope')
    records=payload['records']
    if not isinstance(records,list) or len(records)>256:raise ValueError('Research record count limit')
    seen=set()
    for record in records:
        _fields(record,('task','retrieved_at','body_base64','body_sha256','response_metadata'))
        validate_task(record['task']);retrieved=_instant(record['retrieved_at'])
        if retrieved>stamp:raise ValueError('Research retrieval after collection')
        _fields(record['response_metadata'],(),('tr_cont',));_json_tree(record['response_metadata'])
        encoded=record['body_base64']
        if not isinstance(encoded,str) or len(encoded)>((MAX_BODY_BYTES+2)//3)*4:raise ValueError('Research body size limit')
        try:body=base64.b64decode(encoded,validate=True)
        except (ValueError,UnicodeError):raise ValueError('Invalid research body encoding') from None
        if not body or len(body)>MAX_BODY_BYTES or hashlib.sha256(body).hexdigest()!=record['body_sha256']:raise ValueError('Research body hash/size mismatch')
        task_id=hashlib.sha256(_canonical(record['task'])).hexdigest()
        if task_id in seen:raise ValueError('Duplicate research record')
        seen.add(task_id)
    if len(_canonical(payload))>MAX_RAW_BYTES:raise ValueError('Research payload size limit')
    return payload


def package_digest(payload):
    return hashlib.sha256(_canonical(validate_payload(payload))).hexdigest()


def sign_control(body,key):
    _json_tree(body)
    if len(_canonical(body))>MAX_STATE_BYTES:raise ValueError('Control size limit')
    return {**body,'hmac_sha256':hmac.new(_key(key),b'research-control-v1\0'+_canonical(body),hashlib.sha256).hexdigest()}


def verify_control(envelope,key):
    if not isinstance(envelope,dict):raise ValueError('Invalid research control')
    body={k:v for k,v in envelope.items() if k!='hmac_sha256'}
    actual=envelope.get('hmac_sha256');expected=sign_control(body,key)['hmac_sha256']
    if not isinstance(actual,str) or not hmac.compare_digest(actual,expected):raise ValueError('Research control authentication failed')
    return body


def sign_bootstrap(body,key):
    _fields(body,('schema','anchor_sha256','state','quota'))
    if body['schema']!='research-bootstrap-v1':raise ValueError('Invalid research bootstrap')
    _hash(body['anchor_sha256']);validate_state(body['state']);_json_tree(body['quota'])
    return {**body,'hmac_sha256':hmac.new(_key(key),b'research-bootstrap-v1\0'+_canonical(body),hashlib.sha256).hexdigest()}


def verify_bootstrap(envelope,key):
    if not isinstance(envelope,dict):raise ValueError('Invalid research bootstrap')
    body={k:v for k,v in envelope.items() if k!='hmac_sha256'}
    actual=envelope.get('hmac_sha256');expected=sign_bootstrap(body,key)['hmac_sha256']
    if not isinstance(actual,str) or not hmac.compare_digest(actual,expected):raise ValueError('Research bootstrap authentication failed')
    return body


def seal_bootstrap(body,path,key):
    raw=_canonical(sign_bootstrap(body,key));nonce=os.urandom(12)
    encrypted=b'PLRSBOOT'+nonce+AESGCM(_key(key)).encrypt(nonce,gzip.compress(raw,mtime=0),b'research-bootstrap-v1')
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(encrypted)
    return {'sha256':hashlib.sha256(encrypted).hexdigest(),'bytes':len(encrypted)}


def open_bootstrap(path,key):
    path=Path(path)
    if path.stat().st_size>MAX_STATE_BYTES+1024*1024:raise ValueError('Bootstrap ciphertext size limit')
    raw=path.read_bytes()
    if raw[:8]!=b'PLRSBOOT' or len(raw)<36:raise ValueError('Invalid bootstrap envelope')
    try:compressed=AESGCM(_key(key)).decrypt(raw[8:20],raw[20:],b'research-bootstrap-v1')
    except InvalidTag:raise ValueError('Bootstrap authentication failed') from None
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as source:plain=source.read(MAX_STATE_BYTES+1)
    if len(plain)>MAX_STATE_BYTES:raise ValueError('Bootstrap raw size limit')
    return verify_bootstrap(_loads(plain),key)


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
        if len(prefix)!=12 or prefix[:8]!=MAGIC:raise ValueError('Invalid research package format')
        length=struct.unpack('>I',prefix[8:])[0]
        if not 1<=length<=MAX_HEADER_BYTES:raise ValueError('Package header limit exceeded')
        header=stream.read(length);nonce=stream.read(12);ciphertext=stream.read()
    if len(header)!=length or len(nonce)!=12 or len(ciphertext)<16:raise ValueError('Truncated research package')
    try:compressed=AESGCM(_key(key)).decrypt(nonce,ciphertext,prefix+header)
    except InvalidTag:raise ValueError('Research package authentication failed') from None
    public=_loads(header)
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(compressed),mode='rb') as source:raw=source.read(MAX_RAW_BYTES+1)
    except (OSError,EOFError):raise ValueError('Invalid authenticated package compression') from None
    if len(raw)>MAX_RAW_BYTES:raise ValueError('Package raw size limit exceeded')
    payload=validate_payload(_loads(raw))
    expected={k:payload[k] for k in _IDENTITY};expected['package_sha256']=hashlib.sha256(_canonical(payload)).hexdigest()
    if public!=expected:raise ValueError('Package header/payload identity mismatch')
    return payload


