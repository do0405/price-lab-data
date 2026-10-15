"""DB-free, injected-provider daily collection; publishing owns cursor advancement.

``prior_payload`` must have come from open_package and ``bootstrap`` from
verify_bootstrap. Validation here checks structure, not an external signature.
No DB, filesystem cache, credentials or default provider is created. Failures of
required price/action/benchmark collection produce no next payload. Reference
responses are independent raw evidence and never invent publication availability.
"""
import copy
from datetime import date,datetime,timezone
import math

import pandas as pd

from .daily_delta_codec import (ASSET_COLUMNS,MARKETS,RAW_COLUMNS,MAX_RAW_BYTES,
                               _bootstrap_body,_canonical,_json_tree,package_digest,validate_payload)
from .providers import BENCHMARKS,MarketBatch,completed_sessions
from .price_contract import BAR_METADATA,ASSET_METADATA,metadata_columns,normalize_frame_metadata


def _native(value):
    if isinstance(value,dict):return {k:_native(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [_native(v) for v in value]
    if value is None or value is pd.NA or value is pd.NaT:return None
    if isinstance(value,(datetime,date,pd.Timestamp)):return value.isoformat()
    if hasattr(value,'item'):return _native(value.item())
    if isinstance(value,float) and math.isnan(value):return None
    return value


def _days(values):
    result=[]
    for value in values:
        stamp=pd.Timestamp(value)
        if stamp.tzinfo is not None or pd.isna(stamp) or stamp!=stamp.normalize():raise ValueError('Invalid source session label')
        result.append(stamp)
    if len(result)!=len(set(result)):raise ValueError('Duplicate source session label')
    return sorted(result)


def _asset_records(frame):
    if not isinstance(frame,pd.DataFrame) or not set(ASSET_COLUMNS)<=set(frame):raise ValueError('Incomplete asset observations')
    return _native(frame.drop_duplicates(['market','symbol'],keep='last')[metadata_columns(frame,ASSET_COLUMNS,ASSET_METADATA)].to_dict('records'))


def _part(batch,market,days,action_start):
    if not isinstance(batch,MarketBatch) or batch.market!=market or _days(batch.completed_sessions)!=days:raise ValueError('Incomplete required market session coverage')
    if batch.provenance.get('actions_complete') is not True or batch.provenance.get('benchmarks_complete') is not True:raise ValueError('Incomplete required actions/benchmarks')
    frame=normalize_frame_metadata(batch.bars,BAR_METADATA)
    required=set(RAW_COLUMNS)-{'source','reference_previous_close'}
    if frame.empty or not required<=set(frame):raise ValueError('Incomplete raw bars')
    labels=[]
    for value in frame.session_date:
        stamp=pd.Timestamp(value)
        if stamp.tzinfo is not None or pd.isna(stamp) or stamp!=stamp.normalize():raise ValueError('Invalid raw bar session label')
        labels.append(str(stamp.date()))
    frame['session_date']=labels
    expected={str(d.date()) for d in days}
    if set(frame.session_date)!=expected or set(frame.market)!={market} or frame.duplicated(['market','symbol','session_date']).any():raise ValueError('Raw bar identity/session mismatch')
    for symbol in BENCHMARKS[market]:
        benchmark=frame[frame.symbol==symbol]
        if set(benchmark.session_date)!=expected:raise ValueError('Incomplete required benchmark bars')
        if ((benchmark[['open','low','close']]<=0).any().any()
                or (benchmark.high<benchmark[['open','low','close']].max(axis=1)).any()
                or (benchmark.low>benchmark[['open','close']].min(axis=1)).any()):
            raise ValueError('Invalid required benchmark OHLC')
    if 'source' not in frame:frame['source']='provider'
    if 'reference_previous_close' not in frame:frame['reference_previous_close']=None
    rows=_native(frame[metadata_columns(frame,RAW_COLUMNS,BAR_METADATA)].to_dict('records'))
    supplements=frame.is_supplement.tolist() if 'is_supplement' in frame else [None]*len(rows)
    for row,supplementary in zip(rows,supplements):
        if supplementary is not None and not pd.isna(supplementary):
            supplementary=_native(supplementary)
            if type(supplementary) is not bool:raise ValueError('Invalid supplement lineage')
            row['is_supplement']=supplementary
        elif row['source']=='kis_etf_raw':row['is_supplement']=True
    rows.sort(key=lambda r:(r['session_date'],r['symbol']))
    return dict(market=market,sessions=sorted(expected),action_start=str(action_start.date()),bars=rows,
                assets=_asset_records(batch.assets),actions=_native(batch.actions),provenance=_native(batch.provenance))


def _chunks(wanted,all_days):
    """Avoid wide API date spans when audit and recent repair dates are disjoint."""
    positions={day:n for n,day in enumerate(all_days)};chunk=[]
    for day in wanted:
        if chunk and (len(chunk)==20 or positions[day]!=positions[chunk[-1]]+1):
            yield chunk;chunk=[]
        chunk.append(day)
    if chunk:yield chunk


def collect_delta(providers,prior_payload=None,bootstrap=None,now=None,
                  overlap_sessions=5,audit=False,reference_fetch=None,reference_sessions=6):
    """Return the next uncommitted payload; callers publish ready before saving it.

    Recent overlap is 1..20 completed sessions. A weekly audit adds at most20
    historical sessions per market and advances only that authenticated cursor.
    Session requests are contiguous and at most20, including prolonged downtime.
    Inactive symbols remain in cumulative assets and future fetch universes.
    """
    if type(overlap_sessions) is not int or not 1<=overlap_sessions<=20:raise ValueError('Overlap must be one to twenty sessions')
    if type(reference_sessions) is not int or not 1<=reference_sessions<=6:raise ValueError('Reference window must be one to six sessions')
    if type(audit) is not bool:raise ValueError('Audit flag must be boolean')
    instant=pd.Timestamp(now if now is not None else datetime.now(timezone.utc))
    if instant.tzinfo is None:raise ValueError('Collection requires timezone-aware now')
    instant=instant.tz_convert('UTC')
    if prior_payload is not None:
        validate_payload(prior_payload)
        if pd.Timestamp(prior_payload['collected_at'])>instant:raise ValueError('Prior package collection is in the future')
        state=copy.deepcopy(prior_payload['state']);anchor=prior_payload['bootstrap_anchor_sha256']
        sequence=prior_payload['sequence']+1;previous=package_digest(prior_payload)
        if bootstrap is not None and _bootstrap_body(bootstrap)['anchor_sha256']!=anchor:raise ValueError('Bootstrap anchor mismatch')
    else:
        if bootstrap is None:raise ValueError('Verified bootstrap or prior package required')
        body=_bootstrap_body(bootstrap);state=copy.deepcopy({k:v for k,v in body.items() if k not in ('schema','anchor_sha256')})
        anchor=body['anchor_sha256'];sequence=1;previous=None
    if any(pd.Timestamp(d)>instant.tz_localize(None).normalize() for days in state['sessions'].values() for d in days):raise ValueError('Future bootstrap session rejected')
    if hasattr(providers,'begin_refresh'):providers.begin_refresh()
    if hasattr(providers,'load_session_evidence'):providers.load_session_evidence(copy.deepcopy(state['calendar_evidence']))
    universe={(a['market'],a['symbol']):a for a in state['assets']};parts=[];plans={}
    state.setdefault('audit_cursor',{})
    stage='calendar';market='all'
    try:
        original_state_bytes=len(_canonical(state));payload_bytes=original_state_bytes
        if payload_bytes>MAX_RAW_BYTES:raise ValueError('Package raw size limit exceeded')
        # Resolve all exchange/publication contracts before fetching any bars.
        for market in MARKETS:
            known=_days(state['sessions'][market]);known_set=set(known)
            actual=_days(completed_sessions(market,known[0],now=instant))
            if any(d>instant.tz_localize(None).normalize() for d in actual):raise ValueError('Future provider session rejected')
            if hasattr(providers,'reconcile_sessions'):
                reconciled=_days(providers.reconcile_sessions(market,list(actual),list(known)))
                if not set(reconciled)<=set(actual):raise ValueError('Calendar reconciliation added unexpected sessions')
                actual=reconciled
            published=actual
            if hasattr(providers,'published_sessions'):
                published=_days(providers.published_sessions(market,list(actual),now=instant))
                if not set(published)<=set(actual):raise ValueError('Publication policy added unexpected sessions')
            all_days=sorted(known_set|set(published))
            wanted=(set(all_days)-known_set)|set(all_days[-overlap_sessions:])
            if audit:
                cursor=state['audit_cursor'].get(market,0)%len(all_days)
                audit_days=all_days[cursor:cursor+20];wanted.update(audit_days)
                state['audit_cursor'][market]=(cursor+len(audit_days))%len(all_days)
            plans[market]=(all_days,sorted(wanted))
        for market in MARKETS:
            all_days,wanted=plans[market];positions={d:n for n,d in enumerate(all_days)}
            for days in _chunks(wanted,all_days):
                position=positions[days[0]]
                action_start=all_days[position-1]+pd.Timedelta(days=1) if position else days[0]
                assets=pd.DataFrame([a for (m,_),a in universe.items() if m==market])
                stage='fetch'
                batch=providers.fetch_market_part(market,list(days),assets,audit=audit,action_start=action_start)
                stage='normalize'
                part=_part(batch,market,days,action_start)
                stage='encode'
                payload_bytes+=len(_canonical(part))
                if payload_bytes>MAX_RAW_BYTES:raise ValueError('Package raw size limit exceeded')
                parts.append(part)
                for asset in part['assets']:
                    key=(asset['market'],asset['symbol'])
                    universe[key]={**universe.get(key,{}),**{k:v for k,v in asset.items() if v is not None}}
            state['sessions'][market]=[str(d.date()) for d in all_days]
            state['watermarks'][market]=state['sessions'][market][-1]
    except Exception as exc:
        from .providers import ProviderError
        category='provider' if isinstance(exc,ProviderError) else 'validation' if isinstance(exc,ValueError) else 'other'
        raise ValueError(f'Required market collection failed (market={market}; stage={stage}; type={category})') from None
    state['assets']=sorted(universe.values(),key=lambda a:(a['market'],a['symbol']))
    state['calendar_evidence']=_native(copy.deepcopy(getattr(providers,'calendar_evidence',state['calendar_evidence'])))
    payload_bytes+=len(_canonical(state))-original_state_bytes+1024
    if payload_bytes>MAX_RAW_BYTES:raise ValueError('Required market collection failed')
    references=[];reference_errors=[]
    if reference_fetch is not None:
        for day in state['sessions']['kr'][-reference_sessions:]:
            for kind in ('daily','master'):
                for board in ('KOSPI','KOSDAQ'):
                    try:
                        response=_native(reference_fetch(board,day,kind))
                        if not isinstance(response,dict):raise ValueError('Reference response mapping required')
                        _json_tree(response)
                        retrieved=instant if now is not None else pd.Timestamp(datetime.now(timezone.utc))
                        record=dict(board=board,requested_date=day,kind=kind,payload=response,
                            retrieved_at=retrieved.isoformat().replace('+00:00','Z'))
                        size=len(_canonical(record))+1
                        if payload_bytes+size>MAX_RAW_BYTES:raise ValueError('Reference raw size limit exceeded')
                        references.append(record);payload_bytes+=size
                    except Exception:
                        if not reference_errors:reference_errors.append('KR_REFERENCE_COLLECTION_FAILED')
    finished=instant if now is not None else pd.Timestamp(datetime.now(timezone.utc))
    payload=dict(schema='price-delta-v1',stream_id='price-daily-v1',sequence=sequence,
        previous_package_sha256=previous,bootstrap_anchor_sha256=anchor,collected_at=finished.isoformat().replace('+00:00','Z'),
        state=state,parts=parts,reference_raw=references)
    if reference_errors:payload['reference_errors']=reference_errors
    return validate_payload(payload)
