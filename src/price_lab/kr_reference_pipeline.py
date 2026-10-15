"""Independent reference staging. Never reads or changes managed price checkpoints."""
from contextlib import nullcontext
from datetime import datetime, time, timezone
import gzip
import hashlib
import json
import os
import re
from pathlib import Path
import tempfile

from .kr_reference_schema import create_schema
from .kr_reference_sources import (
    KRX_BASE, ENDPOINTS, SourceContractError, RawNotReady, canonical, digest, day,
    instant, inspect_payload, parse_capitalization, parse_master, read_completed_raw,
)
from .kr_reference_store import apply_delta, coverage_report, refresh_coverage, merge_attached_reference

RECONSTRUCTION_POLICY = 'first-observed-baseline-next-session-v1'


def _atomic_json(path,value):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    descriptor,name=tempfile.mkstemp(prefix=path.name+'.',suffix='.tmp',dir=path.parent)
    try:
        with os.fdopen(descriptor,'w',encoding='utf-8') as stream:
            stream.write(canonical(value)); stream.flush(); os.fsync(stream.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name): os.unlink(name)


def _research_time(asof,sessions):
    later=next((s for s in sorted({day(v) for v in sessions or []}) if s>day(asof)),None)
    # 09:00 Asia/Seoul = 00:00 UTC; this is an explicit research assumption,
    # never a claim about a historical document's actual publication time.
    return datetime.combine(later,time(0),timezone.utc) if later else None


def prepare_delta_from_raw(path,*,sessions=None,min_age_seconds=2):
    raw=read_completed_raw(path,min_age_seconds=min_age_seconds)
    info=inspect_payload(raw.payload,raw.metadata)
    if not info: return None
    source_id='krx_'+ENDPOINTS[(info['board'],info['kind'])]
    source_url=KRX_BASE+ENDPOINTS[(info['board'],info['kind'])]
    manifest_id=digest({'source_id':source_id,'raw_sha256':raw.sha256})
    published=raw.metadata.get('source_published_at')
    # Only an explicit externally verified evidence assertion can certify a
    # publication timestamp; HTTP retrieval time can never do so.
    basis='PUBLICATION_TIME_VERIFIED' if published and raw.metadata.get('publication_evidence_verified') is True else 'EFFECTIVE_DATE_RECONSTRUCTED'
    research=_research_time(info['asof_date'],sessions)
    common=dict(source_id=source_id,manifest_id=manifest_id,retrieved_at=raw.retrieved_at,
        source_published_at=instant(published) if published else None,availability_basis=basis,
        research_available_at=research,research_policy_version=RECONSTRUCTION_POLICY if research else None)
    rows=(parse_capitalization(raw.payload,board=info['board'],requested_date=info['asof_date']) if info['kind']=='daily'
          else parse_master(raw.payload,board=info['board'],requested_date=info['asof_date']))
    for row in rows: row.update(common)
    manifest=dict(common,source_url=source_url,raw_sha256=raw.sha256,raw_path=str(raw.path.resolve()),
        requested_date=info['asof_date'],board=info['board'],record_count=info['record_count'],completeness='COMPLETE_RESPONSE',
        metadata={'kind':info['kind'],'compressed_sha256':raw.compressed_sha256,
            'http_envelope_verified':bool(raw.metadata.get('collector_contract')=='kr-reference-http-v1'),
            'requested_date_evidence':'collector_envelope_and_body' if raw.metadata else 'body_only',
            'historical_membership_semantics':'OBSERVED_NOT_CONTINUITY_CERTIFIED' if info['kind']=='master' else None,
            'research_policy_version':RECONSTRUCTION_POLICY if research else None,
            'reconstruction_assumption':'available at next supplied KR session 09:00 KST; first observed baseline only' if research else None,
            'historical_publication_revision_verified':basis=='PUBLICATION_TIME_VERIFIED'})
    return dict(manifests=[manifest],receipt_id=manifest_id,
        capitalization=rows if info['kind']=='daily' else [],aliases=rows if info['kind']=='master' else [])


def _write_raw(root,payload,*,board,requested_date,kind):
    retrieved=datetime.now(timezone.utc)
    metadata=dict(collector_contract='kr-reference-http-v1',board=board,requested_date=str(day(requested_date)),
        kind=kind,source_url=KRX_BASE+ENDPOINTS[(board,kind)],retrieved_at=retrieved.isoformat())
    envelope={'_kr_reference':metadata,'payload':payload}; sha=digest(envelope)
    folder=Path(root)/'raw'/'krx'/kind/str(day(requested_date)); folder.mkdir(parents=True,exist_ok=True)
    final=folder/(board+'-'+sha+'.json.gz')
    descriptor,name=tempfile.mkstemp(prefix='reference-',suffix='.part',dir=folder)
    try:
        with os.fdopen(descriptor,'wb') as stream:
            stream.write(gzip.compress(canonical(envelope).encode('utf-8'),mtime=0)); stream.flush(); os.fsync(stream.fileno())
        os.replace(name,final)
    finally:
        if os.path.exists(name): os.unlink(name)
    return final


def official_fetcher(key=None):
    """Credential bridge stays internal; successful raw only, fixed safe errors.

    HttpClient is configured without raw_dir so failed/error responses and any
    authenticated envelopes cannot be persisted by the legacy price collector.
    """
    import os
    from .providers import HttpClient, ProviderError
    key=key or os.environ.get('KRX_API_KEY')
    if not key:raise ValueError('KRX credential is unavailable')
    client=HttpClient(retries=2,min_interval=1,raw_dir=None)
    def fetch(board,date,kind):
        try:
            return client.request_json('GET',KRX_BASE+ENDPOINTS[(board,kind)],headers={'AUTH_KEY':key},
                params={'basDd':day(date).strftime('%Y%m%d')})
        except ProviderError as error:
            code=re.search(r'Provider HTTP (\d{3})',str(error))
            safe=SourceContractError('Official KRX request failed; retry or access verification required')
            safe.reason_code='HTTP_'+code.group(1) if code else 'TRANSPORT_OR_PROVIDER_JSON'
            raise safe from None
    return fetch


def collect_reference(db,*,sessions,raw_roots,reference_root,fetch=None,limit=0,scope='latest750',
                      kind='daily',min_age_seconds=2,max_files=None,lock_held=False,progress=None,availability_sessions=None):
    """Reuse complete raw first; fetch missing date/board cells within a hard budget.

    limit bounds NETWORK requests, not cached-file reuse. limit=0 is offline.
    Caller may hold managed_lock(reference_root) before opening a writer connection
    and then pass lock_held=True. Otherwise this function takes that same lock.
    """
    from .ingestion import managed_lock
    if kind not in ('daily','master') or scope not in ('latest750','history'):
        raise ValueError('Unsupported reference collection scope')
    if isinstance(limit,bool) or not isinstance(limit,int) or limit<0: raise ValueError('Nonnegative finite HTTP budget required')
    labels=sorted({day(s).isoformat() for s in sessions})
    if not labels: raise ValueError('Explicit completed KR sessions required')
    if scope=='latest750': labels=labels[-750:]
    availability_sessions=availability_sessions or labels
    root=Path(reference_root); wanted={(d,b) for d in labels for b in ('KOSPI','KOSDAQ')}
    result=dict(scope=scope,kind=kind,reused=0,fetched=0,scanned=0,not_ready=0,unsupported=0,failed=0,inserted=0,
        network_requests=0,errors=[],requested_date_boards=len(wanted),first_session=labels[0],last_session=labels[-1],
        reconstruction_policy_version=RECONSTRUCTION_POLICY)
    with (nullcontext() if lock_held else managed_lock(root)):
        create_schema(db)
        table='kr_capitalization_observations' if kind=='daily' else 'kr_security_aliases'
        datecol='asof_date' if kind=='daily' else 'effective_from'
        present={(str(r[0]),r[1]) for r in db.execute(f'SELECT DISTINCT {datecol},board FROM {table}').fetchall()}
        paths=[]; seen=set()
        # Own immutable successful downloads are always a restart source.
        for raw_root in [*(Path(p) for p in raw_roots),root/'raw']:
            if not raw_root.is_dir(): continue
            for path in sorted(raw_root.rglob('*.json.gz')):
                key=str(path.resolve())
                if key not in seen: paths.append(path); seen.add(key)
        for path in paths:
            if max_files is not None and result['scanned']>=max_files: break
            result['scanned']+=1
            try:
                raw=read_completed_raw(path,min_age_seconds=min_age_seconds)
                info=inspect_payload(raw.payload,raw.metadata)
                if not info or info['kind']!=kind:
                    result['unsupported']+=1; continue
                pair=(info['asof_date'],info['board'])
                if pair not in wanted: continue
                # Avoid a second decompression on resumed already committed files.
                source='krx_'+ENDPOINTS[(info['board'],kind)]
                receipt=digest({'source_id':source,'raw_sha256':raw.sha256})
                if db.execute('SELECT 1 FROM kr_reference_receipts WHERE receipt_id=?',[receipt]).fetchone():
                    present.add(pair); continue
                delta=prepare_delta_from_raw(path,sessions=availability_sessions,min_age_seconds=min_age_seconds)
                applied=apply_delta(db,delta)
                result['inserted']+=applied['inserted']; result['reused']+=1; present.add(pair)
            except RawNotReady:
                result['not_ready']+=1
            except (SourceContractError,ValueError):
                result['failed']+=1
                if len(result['errors'])<20: result['errors'].append({'stage':'raw_contract','status':'FAILED','file':path.name})
            if progress and result['scanned']%50==0: progress(dict(result))
        # A bounded/incomplete reuse scan cannot establish absence. Do not issue
        # replacement HTTP calls until every configured raw root has been checked.
        scan_complete=max_files is None or result['scanned']>=len(paths)
        result['raw_scan_complete']=scan_complete
        if fetch and scan_complete:
            for label,board in sorted(wanted-present):
                if result['network_requests']>=limit: break
                result['network_requests']+=1
                try:
                    payload=fetch(board,label,kind)
                    # Keep successful official data bodies even when their historic
                    # semantics fail review. Error/unknown response shapes are never
                    # saved. No response headers or error bodies are accepted here.
                    info=inspect_payload(payload,{'requested_date':label,'board':board})
                    if not info or info['kind']!=kind: raise SourceContractError('Official response contract missing')
                    path=_write_raw(root,payload,board=board,requested_date=label,kind=kind)
                    delta=prepare_delta_from_raw(path,sessions=availability_sessions,min_age_seconds=0)
                    applied=apply_delta(db,delta)
                    result['inserted']+=applied['inserted']; result['fetched']+=1; present.add((label,board))
                except Exception as error:
                    result['failed']+=1
                    if len(result['errors'])<20:
                        safe_reason=getattr(error,'reason_code',None)
                        if safe_reason is None:
                            safe_reason='HISTORICAL_FUTURE_LISTING' if isinstance(error,SourceContractError) and str(error)=='Historical master contains a future listing date' else 'CONTRACT_OR_LOCAL_IO'
                        result['errors'].append({'stage':'official_request_or_contract','date':label,'board':board,'status':'FAILED','reason_code':safe_reason})
                if progress: progress(dict(result))
        result['remaining_date_boards']=[{'asof_date':d,'board':b} for d,b in sorted(wanted-present)]
        result['status']='PARTIAL' if result['remaining_date_boards'] or result['failed'] or kind=='master' else 'RAW_COMPLETE_REFERENCE_PARTIAL'
        report=refresh_coverage(db,sessions=labels,scope=scope)
        result['coverage_status']=report['status']
        result['publication_time_verified']=report['publication_time_verified']
        (root/'reports').mkdir(parents=True,exist_ok=True)
        _atomic_json(root/'reports'/f'{scope}-{kind}-collection.json',result)
    return result


def reference_status(db,*,sessions=None,scope='observed'):
    return coverage_report(db,sessions=sessions,scope=scope)


def merge_reference_database(db,source_alias,optional=True):
    """Caller owns its publication transaction; no nested BEGIN/COMMIT here."""
    return merge_attached_reference(db,source_alias,optional=optional)


def finalize_staging_status(db_path,reference_root,*,collection=None,sessions=None,scope='observed'):
    """After CHECKPOINT + closing writer, hash the immutable-at-lock staging file.

    The caller MUST hold managed_lock(reference_root) through this operation.
    Monitors read the resulting JSON and never inspect the active writer database.
    """
    import duckdb
    path=Path(db_path)
    if Path(str(path)+'.wal').exists(): raise ValueError('Close and checkpoint staging writer before finalizing')
    sha=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''): sha.update(chunk)
    with duckdb.connect(str(path),read_only=True) as db:
        report=reference_status(db,sessions=sessions,scope=scope)
    report.update(staging_sha256=sha.hexdigest(),staging_bytes=path.stat().st_size,
        finalized_at=datetime.now(timezone.utc).isoformat(),staging_database=path.name,collection=collection or {})
    _atomic_json(Path(reference_root)/'reports'/'status.json',report)
    return report
