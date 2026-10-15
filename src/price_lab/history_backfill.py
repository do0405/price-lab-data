"""Resumable historical RAW archive, separate from verified research snapshots.

Each part has immutable provenance and a transactional database receipt. Collection
does not certify corporate-action completeness or turn raw prices into adjusted
prices. Credentials, exception bodies and HTTP headers never enter these files.
"""
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import shutil
import uuid
import duckdb
import pandas as pd
from .ingestion import RAW_COLUMNS, atomic_json, canonical, managed_lock, sha256_file
from .providers import MarketBatch


def _day(value):return str(pd.Timestamp(value).date())
def _digest(value):
    import hashlib
    return hashlib.sha256(canonical(value).encode()).hexdigest()
def _read(path):return json.loads(Path(path).read_text(encoding='utf-8'))
def _parquet(frame,path):
    with duckdb.connect() as db:
        db.register('frame',frame)
        db.execute('COPY frame TO ? (FORMAT PARQUET)',[str(path)])


def create_plan(root,*,market,start,end,assets,sessions,part_sessions=20,basis=None):
    """Freeze dates, symbols and scope. A new scope always needs a new folder."""
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    with managed_lock(root):
        if (root/'plan.json').exists():raise ValueError('History plan already exists; resume it or use a new plan directory')
        if market not in ('us','kr','crypto') or not 1<=part_sessions<=20:raise ValueError('Invalid history plan')
        start,end=_day(start),_day(end)
        days=[_day(d) for d in sessions]
        if not days or days!=sorted(set(days)) or days[0]<start or days[-1]>end:raise ValueError('Invalid history plan sessions')
        if not {'market','symbol'}<=set(assets) or assets.empty:raise ValueError('Invalid history plan assets')
        assets=assets.copy().sort_values(['market','symbol']).reset_index(drop=True)
        if set(assets.market)!={market} or assets.symbol.isna().any() or assets.duplicated(['market','symbol']).any():raise ValueError('Invalid history plan assets')
        _parquet(assets,root/'assets.parquet')
        value={'format':'price-lab-raw-history-v1','market':market,'start':start,'end':end,
               'sessions':days,'part_sessions':part_sessions,'asset_sha256':sha256_file(root/'assets.parquet'),
               'basis':basis or {},'consumer_ready':False,
               'parts':[days[i:i+part_sessions] for i in range(0,len(days),part_sessions)]}
        value['plan_sha256']=_digest(value)
        atomic_json(root/'plan.json',value)
        return value


def _plan(root):
    plan=_read(root/'plan.json');body={k:v for k,v in plan.items() if k!='plan_sha256'}
    if plan.get('format')!='price-lab-raw-history-v1' or plan.get('plan_sha256')!=_digest(body):raise ValueError('History plan integrity failed')
    if sha256_file(root/'assets.parquet')!=plan['asset_sha256']:raise ValueError('History asset integrity failed')
    return plan


def _database(root):
    db=duckdb.connect(str(root/'raw_history.duckdb'))
    db.execute("SET memory_limit='1GB'");db.execute('SET threads=4')
    db.execute('''CREATE TABLE IF NOT EXISTS raw_bars(
        market VARCHAR,symbol VARCHAR,session_date DATE,open DOUBLE,high DOUBLE,low DOUBLE,close DOUBLE,
        volume DOUBLE,turnover DECIMAL(38,8),vwap DOUBLE,currency VARCHAR,source VARCHAR,
        reference_previous_close DOUBLE,PRIMARY KEY(market,symbol,session_date))''')
    db.execute('CREATE TABLE IF NOT EXISTS part_receipts(part INTEGER PRIMARY KEY,manifest_sha256 VARCHAR,rows BIGINT)')
    db.execute('CREATE TABLE IF NOT EXISTS history_actions(part INTEGER,event_key VARCHAR,payload VARCHAR,PRIMARY KEY(part,event_key))')
    db.execute('CREATE TABLE IF NOT EXISTS asset_observations(part INTEGER,market VARCHAR,symbol VARCHAR,payload VARCHAR,PRIMARY KEY(part,market,symbol))')
    db.execute('''CREATE VIEW IF NOT EXISTS raw_quality AS SELECT *,
        open>0 AND low>0 AND close>0 AND high>=greatest(open,close,low) AND low<=least(open,close)
        AND turnover IS NOT NULL AND vwap>0 AS price_eligible FROM raw_bars''')
    return db


def _normalize(batch,market,days):
    if not isinstance(batch,MarketBatch) or batch.market!=market:raise ValueError('History batch market mismatch')
    if [_day(x) for x in batch.completed_sessions]!=days:raise ValueError('History batch session coverage mismatch')
    frame=batch.bars.copy();required=set(RAW_COLUMNS)-{'source','reference_previous_close'}
    if not required<=set(frame):raise ValueError('History raw columns missing')
    frame['session_date']=pd.to_datetime(frame.session_date).dt.normalize()
    if frame.empty or set(frame.market)!={market} or frame.symbol.isna().any() or frame.duplicated(['market','symbol','session_date']).any():raise ValueError('History raw keys invalid')
    if not set(frame.session_date.map(_day))<=set(days):raise ValueError('History raw dates outside part')
    if set(frame.session_date.map(_day))!=set(days):raise ValueError('History raw session missing')
    for col in ('open','high','low','close','volume','turnover','vwap'):
        frame[col]=pd.to_numeric(frame[col],errors='raise')
        # Missing turnover/VWAP are preserved as missing in the raw archive only.
        nullable=col in ('turnover','vwap')
        if any((not math.isfinite(float(v)) or float(v)<0) for v in frame[col] if not (nullable and pd.isna(v))):raise ValueError('History numeric field invalid')
    # Keep malformed/zero OHLC observations as raw evidence. raw_quality marks
    # them ineligible; the adjusted consumer promotion has a stricter gate.
    expected='KRW' if market=='kr' else 'USD'
    if not frame.currency.eq(expected).all():raise ValueError('History currency invalid')
    if 'source' not in frame:frame['source']='historical_provider_raw'
    if 'reference_previous_close' not in frame:frame['reference_previous_close']=None
    return frame[RAW_COLUMNS]


def _cache(root,number,plan,batch,days):
    frame=_normalize(batch,plan['market'],days)
    folder=root/'parts'/f'{number:05d}';temp=folder.with_name(folder.name+'.'+uuid.uuid4().hex+'.partial')
    temp.mkdir(parents=True)
    try:
        _parquet(frame,temp/'bars.parquet');_parquet(batch.assets,temp/'assets.parquet')
        atomic_json(temp/'actions.json',batch.actions)
        manifest={'plan_sha256':plan['plan_sha256'],'part':number,'sessions':days,'rows':len(frame),
                  'provenance':batch.provenance,'files':{name:sha256_file(temp/name) for name in ('bars.parquet','assets.parquet','actions.json')}}
        atomic_json(temp/'manifest.json',manifest)
        temp.rename(folder)
    finally:
        if temp.exists():shutil.rmtree(temp)
    return folder


def _verify(folder,number,plan):
    value=_read(folder/'manifest.json')
    if value['plan_sha256']!=plan['plan_sha256'] or value['part']!=number or value['sessions']!=plan['parts'][number]:raise ValueError('History part provenance mismatch')
    if set(value['files'])!={'bars.parquet','assets.parquet','actions.json'}:raise ValueError('History part file manifest invalid')
    for name,digest in value['files'].items():
        if sha256_file(folder/name)!=digest:raise ValueError('History part hash mismatch')
    return value


def _apply_part(db,folder,number,manifest):
    """Bars and receipt commit together; a failed commit reuses the cached part."""
    db.execute('BEGIN TRANSACTION')
    try:
        db.execute('INSERT OR REPLACE INTO raw_bars SELECT '+','.join(RAW_COLUMNS)+' FROM read_parquet(?)',[str(folder/'bars.parquet')])
        for event in _read(folder/'actions.json'):
            db.execute('INSERT OR REPLACE INTO history_actions VALUES(?,?,?)',[number,_digest(event),canonical(event)])
        assets=db.execute('SELECT * FROM read_parquet(?)',[str(folder/'assets.parquet')]).fetchdf()
        observed=pd.DataFrame([{'part':number,'market':asset['market'],'symbol':asset['symbol'],'payload':canonical(asset)} for asset in assets.to_dict('records')])
        db.register('observed_assets',observed)
        db.execute('INSERT OR REPLACE INTO asset_observations SELECT part,market,symbol,payload FROM observed_assets')
        db.execute('INSERT INTO part_receipts VALUES(?,?,?)',[number,sha256_file(folder/'manifest.json'),manifest['rows']])
        db.execute('COMMIT')
    except Exception:
        db.execute('ROLLBACK');raise


def _reconcile_raw(root,db,plan):
    paths=[str(root/'parts'/f'{n:05d}'/'bars.parquet') for n in range(len(plan['parts']))]
    schema=db.execute("PRAGMA table_info('raw_bars')").fetchall()
    projection=','.join('CAST("'+r[1]+'" AS '+r[2]+') AS "'+r[1]+'"' for r in schema)
    db.execute('CREATE TEMP TABLE expected_raw AS SELECT '+projection+' FROM read_parquet(?)', [paths])
    try:
        mismatch=db.execute('SELECT count(*) FROM ((SELECT * FROM expected_raw EXCEPT ALL SELECT * FROM raw_bars) UNION ALL (SELECT * FROM raw_bars EXCEPT ALL SELECT * FROM expected_raw))').fetchone()[0]
        if mismatch:raise ValueError('History raw reconciliation failed')
    finally:db.execute('DROP TABLE expected_raw')


def _status(root,db,plan,state,error=None):
    count=db.execute('SELECT count(*) FROM part_receipts').fetchone()[0]
    rows,first,last=db.execute('SELECT count(*),min(session_date),max(session_date) FROM raw_bars').fetchone()
    value={'status':state,'market':plan['market'],'completed_parts':count,'total_parts':len(plan['parts']),
           'rows':rows,'first_observed':str(first) if first else None,'last_observed':str(last) if last else None,
           'consumer_ready':False,'plan_sha256':plan['plan_sha256'],
           'updated_at':datetime.now(timezone.utc).isoformat(),
           'next_step':'Validate action coverage, historical identities and benchmark inception before adjusted snapshot promotion'}
    if error:value['error']=error
    if state=='COMPLETE':
        _reconcile_raw(root,db,plan)
        value['raw_reconciled']=True
        db.execute('CHECKPOINT')
        db.close() # Windows denies opening a file held by DuckDB's writer.
        value['database_sha256']=sha256_file(root/'raw_history.duckdb')
    atomic_json(root/'status.json',value)
    return value


def collect(root,provider,*,max_parts=None):
    root=Path(root)
    with managed_lock(root):
        plan=_plan(root);db=_database(root)
        try:
            receipts=dict(db.execute('SELECT part,manifest_sha256 FROM part_receipts').fetchall())
            # Verify even committed cache: it is the durable provenance, not a disposable download.
            for number,digest in receipts.items():
                folder=root/'parts'/f'{number:05d}';_verify(folder,number,plan)
                if sha256_file(folder/'manifest.json')!=digest:raise ValueError('History receipt hash mismatch')
            if len(receipts)==len(plan['parts']) and (root/'status.json').is_file():
                previous=_read(root/'status.json')
                if previous.get('status')=='COMPLETE':
                    # The initial archive format had no closed-file digest.
                    # Upgrade it only after exact reconciliation to all parts.
                    if not previous.get('database_sha256') or not previous.get('raw_reconciled'):
                        return _status(root,db,plan,'COMPLETE')
                    db.close()
                    if previous['database_sha256']!=sha256_file(root/'raw_history.duckdb'):
                        db=_database(root);raise ValueError('History database integrity failed')
                    return previous
            assets=db.execute('SELECT * FROM read_parquet(?)',[str(root/'assets.parquet')]).fetchdf()
            fetched=0
            for number,days in enumerate(plan['parts']):
                if number in receipts:continue
                if max_parts is not None and fetched>=max_parts:return _status(root,db,plan,'PAUSED')
                _status(root,db,plan,'RUNNING')
                folder=root/'parts'/f'{number:05d}'
                if not folder.exists():
                    action_start=_day(pd.Timestamp(plan['parts'][number-1][-1])+pd.Timedelta(days=1)) if number else plan['start']
                    batch=provider.fetch_market_part(plan['market'],list(pd.to_datetime(days)),assets,audit=True,action_start=action_start)
                    _cache(root,number,plan,batch,days)
                manifest=_verify(folder,number,plan)
                _apply_part(db,folder,number,manifest);fetched+=1
                _status(root,db,plan,'RUNNING')
            db.execute('CHECKPOINT')
            return _status(root,db,plan,'COMPLETE')
        except Exception as exc:
            # Provider bodies can contain credentials. Persist only an implementation-owned type.
            return _status(root,db,plan,'FAILED',type(exc).__name__)
        finally:db.close()
