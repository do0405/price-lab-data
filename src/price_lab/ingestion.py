"""Durable full checkpoints with incremental collection and an atomic pointer.

The seed is copied byte-for-byte before adding managed tables: its original raw,
quarantine, manual issuer overrides and evidence tables remain recoverable.
Adjusted prices use the seed baseline and event deltas, never guessed jumps.
No historical rows are truncated. A failed attempt retains its raw batches and
working DB but cannot change current.json. This module has no app/cache imports.
"""
from contextlib import contextmanager
from datetime import datetime,timezone
from functools import lru_cache
import hashlib
import json
import math
import os
from pathlib import Path,PurePosixPath
import re
import shutil
import threading
import time
import uuid
import duckdb
import numpy as np
import pandas as pd
from .config import ROOT,SOURCE,BENCHMARKS
from .providers import BENCHMARKS as REQUIRED_BENCHMARKS,MarketBatch,ProviderError,completed_sessions

SCHEMA_VERSION=1
KEY_VERSION=1
MANAGED_ROOT=ROOT/'data'/'market'
MAX_PART_SESSIONS=20
BAR_COLUMNS=['market','symbol','session_date','open','high','low','close','volume','turnover','vwap','raw_open','raw_close','raw_volume','split_price_factor','split_share_factor','currency']
RAW_COLUMNS=['market','symbol','session_date','open','high','low','close','volume','turnover','vwap','currency','source','reference_previous_close']
_THREAD_LOCKS={};_THREAD_GUARD=threading.Lock()


class IngestionError(ValueError):
    """Only fixed, implementation-owned validation messages may use this type."""


def canonical(value):return json.dumps(value,sort_keys=True,ensure_ascii=False,default=str,separators=(',',':'),allow_nan=False)
def sha256_file(path):
    h=hashlib.sha256()
    with open(path,'rb') as stream:
        for chunk in iter(lambda:stream.read(4*1024*1024),b''):h.update(chunk)
    return h.hexdigest()
def _day(value):return str(pd.Timestamp(value).date())
def _literal(value):return "'"+str(value).replace("'","''")+"'"


def atomic_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name('.'+path.name+'.'+uuid.uuid4().hex+'.part')
    try:
        with temp.open('w',encoding='utf-8',newline='\n') as stream:stream.write(canonical(value));stream.flush();os.fsync(stream.fileno())
        os.replace(temp,path)
    finally:
        if temp.exists():temp.unlink()


@contextmanager
def managed_lock(root,timeout=900):
    root=Path(root).resolve();root.mkdir(parents=True,exist_ok=True)
    with _THREAD_GUARD:lock=_THREAD_LOCKS.setdefault(str(root),threading.Lock())
    with lock:
        with (root/'managed.lock').open('a+b') as stream:
            stream.seek(0,2)
            if stream.tell()==0:stream.write(b'0');stream.flush()
            started=time.monotonic();acquired=False
            try:
                while not acquired:
                    try:
                        stream.seek(0)
                        if os.name=='nt':
                            import msvcrt
                            msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
                        else:
                            import fcntl
                            fcntl.lockf(stream.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB,1,0)
                        acquired=True
                    except OSError:
                        if time.monotonic()-started>=timeout:raise TimeoutError('Managed dataset lock timed out') from None
                        time.sleep(.1)
                yield
            finally:
                if acquired:
                    stream.seek(0)
                    if os.name=='nt':msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)
                    else:fcntl.lockf(stream.fileno(),fcntl.LOCK_UN,1,0)


def validate_manifest(manifest):
    if not isinstance(manifest,dict):raise IngestionError('Invalid managed manifest')
    if manifest.get('schema_version')!=SCHEMA_VERSION or manifest.get('key_version')!=KEY_VERSION:raise IngestionError('Unsupported schema/key version')
    if isinstance(manifest.get('sequence'),bool) or not isinstance(manifest.get('sequence'),int) or manifest['sequence']<1:raise IngestionError('Invalid sequence')
    for field in ('sha256','parent_sha256'):
        value=manifest.get(field)
        if field=='parent_sha256' and value is None and manifest['sequence']==1:continue
        if not isinstance(value,str) or not re.fullmatch('[0-9a-f]{64}',value):raise IngestionError('Invalid dataset hash')
    ancestors=manifest.get('ancestors')
    if not isinstance(ancestors,list) or any(not isinstance(a,str) or not re.fullmatch('[0-9a-f]{64}',a) for a in ancestors) or len(set(ancestors))!=len(ancestors):raise IngestionError('Invalid lineage')
    if manifest['sha256'] in ancestors or (manifest['sequence']==1 and ancestors) or (manifest['sequence']>1 and (not ancestors or ancestors[0]!=manifest['parent_sha256'])):raise IngestionError('Invalid lineage ancestry')
    name=manifest.get('database','')
    if not isinstance(name,str) or '\\' in name or ':' in name:raise IngestionError('Unsafe database path')
    parts=PurePosixPath(name)
    if parts.is_absolute() or len(parts.parts)!=2 or parts.parts[0]!='checkpoints' or parts.parts[1] in ('.','..') or not parts.parts[1].endswith('.duckdb'):raise IngestionError('Unsafe database path')
    if manifest.get('includes_supplement') is not True:raise IngestionError('Merged benchmark supplement required')
    markets=manifest.get('markets')
    if not isinstance(markets,dict) or not markets or not set(markets)<={'us','kr','crypto'}:raise IngestionError('Invalid market watermarks')
    for item in markets.values():
        if not isinstance(item,dict) or item.get('target')!=item.get('actual') or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',str(item.get('actual',''))):raise IngestionError('Incomplete market watermark')
        pd.Timestamp(item['actual'])
    evidence=manifest.get('calendar_evidence',{})
    if not isinstance(evidence,dict) or not set(evidence)<={'us','kr','crypto'}:raise IngestionError('Invalid official calendar evidence')
    for days in evidence.values():
        if not isinstance(days,dict):raise IngestionError('Invalid official calendar evidence')
        for day,item in days.items():
            if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',day) or not isinstance(item,dict) or not isinstance(item.get('open'),bool):raise IngestionError('Invalid official calendar evidence')
    return manifest


def validate_database(path,manifest=None):
    path=Path(path)
    if not path.is_file() or Path(str(path)+'.wal').exists():raise IngestionError('Checkpoint missing or WAL present')
    with duckdb.connect(str(path),read_only=True) as db:
        names={r[0] for r in db.execute('SHOW TABLES').fetchall()}
        if not {'backtest_bars','assets','trading_calendar'}<=names:raise IngestionError('Required price tables missing')
        cols={r[0] for r in db.execute('DESCRIBE backtest_bars').fetchall()}
        if not set(BAR_COLUMNS)<=cols:raise IngestionError('Required price columns missing')
        duplicate=db.execute('SELECT count(*) FROM(SELECT market,symbol,session_date,count(*) n FROM backtest_bars GROUP BY ALL HAVING n>1)').fetchone()[0]
        invalid=db.execute('''SELECT count(*) FROM backtest_bars WHERE market NOT IN('us','kr','crypto') OR symbol IS NULL OR session_date IS NULL
            OR NOT coalesce((isfinite(open) AND isfinite(high) AND isfinite(low) AND isfinite(close) AND isfinite(volume) AND isfinite(turnover)
            AND open>0 AND low>0 AND high>=greatest(open,close,low) AND low<=least(open,close) AND volume>=0 AND turnover>=0
            AND isfinite(split_price_factor) AND split_price_factor>0 AND isfinite(split_share_factor) AND split_share_factor>0),false)
            OR currency IS NULL OR (market='us' AND currency<>'USD') OR (market='kr' AND currency<>'KRW')''').fetchone()[0]
        missing=db.execute('SELECT count(*) FROM backtest_bars b LEFT JOIN trading_calendar c USING(market,session_date) WHERE c.market IS NULL').fetchone()[0]
        caldup=db.execute('SELECT count(*) FROM(SELECT market,session_date,count(*) n FROM trading_calendar GROUP BY ALL HAVING n>1)').fetchone()[0]
        if duplicate or invalid or missing or caldup:raise IngestionError('Checkpoint key/price/calendar validation failed')
        rows=db.execute('SELECT market,count(*) AS rows,min(session_date) AS first_date,max(session_date) AS last_date FROM backtest_bars GROUP BY market').fetchall()
        if not rows:raise IngestionError('Empty checkpoint')
        if manifest:
            for market,item in manifest['markets'].items():
                actual=db.execute('SELECT max(session_date) FROM trading_calendar WHERE market=?',[market]).fetchone()[0]
                if actual is None or _day(actual)!=item['actual']:raise IngestionError('Database watermark mismatch')
                for symbol in REQUIRED_BENCHMARKS[market]:
                    if not db.execute('SELECT count(*) FROM backtest_bars WHERE market=? AND symbol=? AND session_date=?',[market,symbol,item['actual']]).fetchone()[0]:
                        raise IngestionError('Required benchmark missing at watermark')
        pending={}
        if 'ingest_pending_actions' in names:
            pending={str(s or 'future_effective'):int(n) for s,n in db.execute("SELECT json_extract_string(payload,'$.pending_status'),count(*) FROM ingest_pending_actions GROUP BY 1").fetchall()}
        return {'valid':True,'markets':{m:{'rows':n,'first':_day(a),'last':_day(b)} for m,n,a,b in rows},'pending_adjustments':pending}


@lru_cache(maxsize=24)
def _verified(path,mtime_ns,size,expected_sha,manifest_json):
    if sha256_file(path)!=expected_sha:raise IngestionError('Managed database hash mismatch')
    validate_database(path,json.loads(manifest_json))
    return True


def resolve_current(root=MANAGED_ROOT,verify=True):
    root=Path(root).resolve();pointer=root/'current.json'
    if not pointer.exists():return None
    manifest=validate_manifest(json.loads(pointer.read_text(encoding='utf-8')))
    path=(root/manifest['database']).resolve()
    if not path.is_relative_to(root) or not path.is_file():raise IngestionError('Managed database path missing/outside root')
    if Path(str(path)+'.wal').exists():raise IngestionError('Immutable checkpoint WAL present')
    if verify:
        stat=path.stat()
        _verified(str(path),stat.st_mtime_ns,stat.st_size,manifest['sha256'],canonical(manifest))
    return {**manifest,'resolved_path':str(path)}


def status(root=MANAGED_ROOT):
    current=resolve_current(root);attempts=[];folder=Path(root)/'attempts'
    if folder.exists():
        for path in sorted(folder.glob('*/status.json'),key=lambda p:p.stat().st_mtime,reverse=True)[:10]:attempts.append(json.loads(path.read_text(encoding='utf-8')))
    pending=[]
    if current:
        with duckdb.connect(current['resolved_path'],read_only=True) as db:
            if 'ingest_pending_actions' in {r[0] for r in db.execute('SHOW TABLES').fetchall()}:
                fields=('market','symbol','provider','event_id','revision','effective_date','pending_status','missing_effective_date','halt_start','last_checked','zero_volume_sessions')
                pending=[{k:v for k,v in json.loads(row[0]).items() if k in fields} for row in db.execute('SELECT payload FROM ingest_pending_actions ORDER BY market,event_id').fetchall()]
    return {'status':'MANAGED' if current else 'SEED_ONLY','current':current,'attempts':attempts,'pending_actions':pending,'stale':bool(attempts and attempts[0].get('status')=='FAILED')}


def _columns(db,name):return {r[0] for r in db.execute('DESCRIBE '+name).fetchall()}


def _seed_raw_query(db):
    if 'ohlcv' not in {r[0] for r in db.execute('SHOW TABLES').fetchall()}:return None
    cols=_columns(db,'ohlcv');core=RAW_COLUMNS[:-2]
    if not set(core)<=cols:return None
    source="coalesce(source,'seed')" if 'source' in cols else "'seed'"
    return 'SELECT '+','.join(core)+','+source+' AS source,NULL::DOUBLE AS reference_previous_close FROM ohlcv'


def _initialize(db,supplement):
    db.execute('''CREATE TABLE IF NOT EXISTS ingest_pending_actions(provider VARCHAR,event_id VARCHAR,revision VARCHAR,market VARCHAR,payload VARCHAR,PRIMARY KEY(provider,event_id))''')
    if 'managed_bars' in {r[0] for r in db.execute('SHOW TABLES').fetchall()}:
        if 'is_supplement' not in _columns(db,'managed_bars'):
            db.execute('ALTER TABLE managed_bars ADD COLUMN is_supplement BOOLEAN DEFAULT false')
            db.execute("UPDATE managed_bars SET is_supplement=true WHERE market='kr' AND symbol IN('069500','229200')")
            db.execute('CREATE OR REPLACE VIEW backtest_bars AS SELECT '+','.join(BAR_COLUMNS+['raw_high','raw_low','raw_vwap','raw_turnover','is_supplement'])+' FROM managed_bars')
        raw_source=_seed_raw_query(db)
        if raw_source:
            db.execute('INSERT INTO ingest_raw_bars SELECT r.* FROM ('+raw_source+') r WHERE NOT EXISTS(SELECT 1 FROM ingest_raw_bars i WHERE i.market=r.market AND i.symbol=r.symbol AND i.session_date=r.session_date)')
        return
    cols=_columns(db,'backtest_bars')
    extra=[]
    for name,fallback in [('raw_high','high/split_price_factor'),('raw_low','low/split_price_factor'),('raw_vwap','vwap/split_price_factor'),('raw_turnover','turnover')]:extra.append((name if name in cols else fallback)+' AS '+name)
    bad='NOT coalesce(adjustment_ready,true)' if 'adjustment_ready' in cols else 'false'
    bad+=' OR NOT coalesce((isfinite(open) AND isfinite(high) AND isfinite(low) AND isfinite(close) AND open>0 AND low>0 AND close>0 AND high>=greatest(open,close,low) AND low<=least(open,close) AND isfinite(volume) AND volume>=0 AND isfinite(turnover) AND turnover>=0),false)'
    db.execute('CREATE TABLE ingest_seed_quarantine AS SELECT market,symbol,session_date FROM backtest_bars WHERE '+bad)
    ready=' WHERE NOT ('+bad+')'
    lineage='coalesce(is_supplement,false)' if 'is_supplement' in cols else 'false'
    db.execute('CREATE TABLE managed_bars AS SELECT '+','.join(BAR_COLUMNS+extra)+',split_price_factor base_price_factor,split_share_factor base_share_factor,'+lineage+' AS is_supplement FROM backtest_bars'+ready)
    # Seed source tables (including quarantined ohlcv and event evidence) remain untouched.
    kind=db.execute("SELECT table_type FROM information_schema.tables WHERE table_name='backtest_bars'").fetchone()[0]
    db.execute(('ALTER VIEW ' if kind=='VIEW' else 'ALTER TABLE ')+'backtest_bars RENAME TO seed_backtest_bars')
    db.execute('CREATE VIEW backtest_bars AS SELECT '+','.join(BAR_COLUMNS+['raw_high','raw_low','raw_vwap','raw_turnover','is_supplement'])+' FROM managed_bars')
    if supplement is not None and Path(supplement).is_file():
        db.execute('ATTACH '+_literal(Path(supplement).resolve())+' AS incoming_supplement (READ_ONLY)')
        common=[c for c in BAR_COLUMNS if c in _columns(db,'incoming_supplement.backtest_bars')]
        if len(common)!=len(BAR_COLUMNS):raise IngestionError('Supplement schema mismatch')
        scols=_columns(db,'incoming_supplement.backtest_bars')
        extra=[name if name in scols else expression+' AS '+name for name,expression in [('raw_high','high/split_price_factor'),('raw_low','low/split_price_factor'),('raw_vwap','vwap/split_price_factor'),('raw_turnover','turnover')]]
        db.execute('INSERT INTO managed_bars SELECT '+','.join(BAR_COLUMNS+extra)+',split_price_factor,split_share_factor,true FROM incoming_supplement.backtest_bars s WHERE NOT EXISTS(SELECT 1 FROM managed_bars m WHERE m.market=s.market AND m.symbol=s.symbol AND m.session_date=s.session_date)')
        db.execute('INSERT INTO assets(market,symbol,name,exchange) SELECT market,symbol,name,exchange FROM incoming_supplement.assets s WHERE NOT EXISTS(SELECT 1 FROM assets a WHERE a.market=s.market AND a.symbol=s.symbol)')
        # The owning connection closes before publication. Keeping this read-only
        # attachment until then also permits an enclosing bootstrap transaction.
    managed_raw='''SELECT market,symbol,session_date,raw_open AS open,raw_high AS high,raw_low AS low,raw_close AS close,
        raw_volume AS volume,raw_turnover AS turnover,raw_vwap AS vwap,currency,'seed'::VARCHAR AS source,NULL::DOUBLE AS reference_previous_close FROM managed_bars'''
    db.execute('CREATE TABLE ingest_raw_bars AS '+(_seed_raw_query(db) or managed_raw))
    db.execute('INSERT INTO ingest_raw_bars SELECT r.* FROM ('+managed_raw+') r WHERE NOT EXISTS(SELECT 1 FROM ingest_raw_bars i WHERE i.market=r.market AND i.symbol=r.symbol AND i.session_date=r.session_date)')
    db.execute('''CREATE TABLE ingest_actions(provider VARCHAR,event_id VARCHAR,revision VARCHAR,market VARCHAR,symbol VARCHAR,effective_date DATE,
        price_factor DOUBLE,share_factor DOUBLE,seed_effective_date DATE,seed_price_factor DOUBLE,seed_share_factor DOUBLE,record_json VARCHAR,PRIMARY KEY(provider,event_id))''')
    if 'split_events' in {r[0] for r in db.execute('SHOW TABLES').fetchall()}:
        for r in db.execute('SELECT market,symbol,effective_date,price_factor,new_shares_per_old_share,source,source_id,source_record_json FROM split_events').fetchall():
            m,s,d,p,f,provider,event_id,raw=r
            provider='alpaca' if 'alpaca' in str(provider) else 'kis_ksd' if 'kis_ksd' in str(provider) else 'seed:'+str(provider)
            try:record=json.loads(raw) if raw else {}
            except ValueError:record={}
            if provider=='kis_ksd' and record.get('record_date'):event_id=f'{s}:{record["record_date"]}'
            event_id=str(event_id or hashlib.sha256(canonical(r).encode()).hexdigest())
            revision=hashlib.sha256(canonical(record).encode()).hexdigest()
            db.execute('INSERT OR REPLACE INTO ingest_actions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',[provider,event_id,revision,m,s,d,p,f,d,p,f,canonical(record)])


def _normalize_batch(batch,market,sessions):
    if not isinstance(batch,MarketBatch) or batch.market!=market:raise IngestionError('Wrong provider batch market')
    expected=set(pd.DatetimeIndex(sessions).normalize())
    if set(pd.DatetimeIndex(batch.completed_sessions).normalize())!=expected:raise IngestionError('Provider session coverage incomplete')
    if not batch.provenance.get('actions_complete') or not batch.provenance.get('benchmarks_complete'):raise IngestionError('Required actions/benchmark collection incomplete')
    frame=batch.bars.copy();required=set(RAW_COLUMNS)-{'source','reference_previous_close'}
    if frame.empty or not required<=set(frame):raise IngestionError('Empty/incomplete provider bars')
    frame['session_date']=pd.to_datetime(frame.session_date).dt.tz_localize(None).dt.normalize()
    if not set(frame.session_date)<=expected or set(frame.market)!={market} or frame.duplicated(['market','symbol','session_date']).any():raise IngestionError('Provider date/market/key mismatch')
    if set(frame.session_date)!=expected:raise IngestionError('Missing completed market session')
    for s in REQUIRED_BENCHMARKS[market]:
        if set(frame[frame.symbol==s].session_date)!=expected:raise IngestionError('Missing required benchmark sessions')
    if 'source' not in frame:frame['source']='provider'
    if 'reference_previous_close' not in frame:frame['reference_previous_close']=None
    nums=['open','high','low','close','volume','turnover','vwap']
    for c in nums:frame[c]=pd.to_numeric(frame[c],errors='coerce')
    if not np.isfinite(frame[nums].to_numpy()).all() or (frame[['volume','turnover']]<0).any().any():raise IngestionError('Invalid provider numeric bars')
    benchmark=frame[frame.symbol.isin(REQUIRED_BENCHMARKS[market])]
    if (benchmark[['open','low','close']]<=0).any().any() or (benchmark.high<benchmark[['open','low','close']].max(axis=1)).any() or (benchmark.low>benchmark[['open','close']].min(axis=1)).any():raise IngestionError('Invalid benchmark OHLC')
    return frame[RAW_COLUMNS]


def _upsert(db,frame,batch):
    db.register('incoming_raw',frame)
    changed=db.execute('SELECT count(*) FROM(SELECT * FROM incoming_raw EXCEPT SELECT * FROM ingest_raw_bars)').fetchone()[0]
    # Preserved source raw can predate its first eligible managed row. Publishing
    # that recovered history is a dataset change even if the raw payload matches.
    changed+=db.execute('''SELECT count(*) FROM incoming_raw n
        WHERE n.open>0 AND n.low>0 AND n.close>0 AND n.high>=greatest(n.open,n.close,n.low) AND n.low<=least(n.open,n.close)
        AND NOT EXISTS(SELECT 1 FROM managed_bars m WHERE m.market=n.market AND m.symbol=n.symbol AND m.session_date=n.session_date)
        AND NOT EXISTS(SELECT 1 FROM ingest_seed_quarantine q WHERE q.market=n.market AND q.symbol=n.symbol AND q.session_date=n.session_date)''').fetchone()[0]
    db.execute('DELETE FROM ingest_raw_bars r USING incoming_raw n WHERE r.market=n.market AND r.symbol=n.symbol AND r.session_date=n.session_date')
    db.execute('INSERT INTO ingest_raw_bars SELECT * FROM incoming_raw')
    # Preserve per-row audited/manual baselines. A historical raw hole has no
    # existing row: reconstruct its baseline from audited SEED events only;
    # post-seed action deltas are applied once by _actions after all parts.
    db.execute('''CREATE TEMP TABLE old_factors AS SELECT m.market,m.symbol,m.session_date,base_price_factor,base_share_factor,is_supplement FROM managed_bars m JOIN incoming_raw n USING(market,symbol,session_date)''')
    db.execute('''INSERT INTO old_factors
        SELECT n.market,n.symbol,n.session_date,coalesce(product(a.seed_price_factor),1),coalesce(product(a.seed_share_factor),1),
            n.market='kr' AND n.symbol IN('069500','229200')
        FROM incoming_raw n LEFT JOIN ingest_actions a ON a.market=n.market AND a.symbol=n.symbol AND a.seed_effective_date>n.session_date
        WHERE NOT EXISTS(SELECT 1 FROM old_factors f WHERE f.market=n.market AND f.symbol=n.symbol AND f.session_date=n.session_date)
        GROUP BY n.market,n.symbol,n.session_date''')
    db.execute('DELETE FROM managed_bars m USING incoming_raw n WHERE m.market=n.market AND m.symbol=n.symbol AND m.session_date=n.session_date')
    db.execute('''INSERT INTO managed_bars SELECT n.market,n.symbol,n.session_date,n.open*coalesce(f.base_price_factor,1),n.high*coalesce(f.base_price_factor,1),n.low*coalesce(f.base_price_factor,1),
        n.close*coalesce(f.base_price_factor,1),n.volume*coalesce(f.base_share_factor,1),n.turnover,n.vwap*coalesce(f.base_price_factor,1),n.open,n.close,n.volume,
        coalesce(f.base_price_factor,1),coalesce(f.base_share_factor,1),n.currency,n.high,n.low,n.vwap,n.turnover,coalesce(f.base_price_factor,1),coalesce(f.base_share_factor,1),
        coalesce(f.is_supplement,n.market='kr' AND n.symbol IN('069500','229200'))
        FROM incoming_raw n LEFT JOIN old_factors f USING(market,symbol,session_date)
        WHERE n.open>0 AND n.low>0 AND n.close>0 AND n.high>=greatest(n.open,n.close,n.low) AND n.low<=least(n.open,n.close)
        AND NOT EXISTS(SELECT 1 FROM ingest_seed_quarantine q WHERE q.market=n.market AND q.symbol=n.symbol AND q.session_date=n.session_date)''')
    db.execute('DROP TABLE old_factors');db.unregister('incoming_raw')
    assets=batch.assets[['market','symbol','name','exchange']].drop_duplicates(['market','symbol'],keep='last')
    if not assets.empty:
        db.register('incoming_assets',assets)
        changed+=db.execute('SELECT count(*) FROM(SELECT * FROM incoming_assets EXCEPT SELECT market,symbol,name,exchange FROM assets)').fetchone()[0]
        db.execute('UPDATE assets a SET name=n.name,exchange=n.exchange FROM incoming_assets n WHERE a.market=n.market AND a.symbol=n.symbol')
        db.execute('INSERT INTO assets(market,symbol,name,exchange) SELECT * FROM incoming_assets n WHERE NOT EXISTS(SELECT 1 FROM assets a WHERE a.market=n.market AND a.symbol=n.symbol)');db.unregister('incoming_assets')
    for date in batch.completed_sessions:
        exists=db.execute('SELECT count(*) FROM trading_calendar WHERE market=? AND session_date=?',[batch.market,_day(date)]).fetchone()[0]
        if not exists:db.execute('INSERT INTO trading_calendar(market,session_date) VALUES(?,?)',[batch.market,_day(date)]);changed+=1
    return changed


def _confirm_ksd(db,action,target,old):
    """Use existing audited identity or explicit relisting + KRX reference proof."""
    if action.get('provider')!='kis_ksd' or action.get('kind')!='split':return action
    action=dict(action)
    if old and action.get('price_factor')==old[7] and action.get('share_factor')==old[8] and (not action.get('effective_date') or _day(action['effective_date'])==_day(old[6])):
        action.update(effective_date=_day(old[6]),confirmed=True,confirmation='existing_audited_event')
        return action
    effective=action.get('effective_date')
    if not effective:return action
    values=db.execute('''SELECT r.session_date,r.reference_previous_close,
        (SELECT p.close FROM ingest_raw_bars p WHERE p.market='kr' AND p.symbol=r.symbol AND p.session_date<r.session_date AND p.close>0 ORDER BY p.session_date DESC LIMIT 1)
        FROM ingest_raw_bars r WHERE r.market='kr' AND r.symbol=? AND r.source='krx' AND r.session_date>=? AND r.session_date<=?
        AND r.reference_previous_close>0 ORDER BY r.session_date''',[action['symbol'],effective,_day(min(pd.Timestamp(target),pd.Timestamp(effective)+pd.Timedelta(days=20)))]).fetchall()
    for day,reference,previous in values:
        if previous is None:continue
        predicted=previous*action['price_factor']
        if abs(reference-predicted)<=max(1.,abs(predicted)*.005) and abs(reference-previous)>max(1.,abs(previous)*.02):
            action.update(confirmed=True,confirmation='KSD_list_dt_and_KRX_reference',confirmation_date=_day(day),reference_previous_close=reference,previous_raw_close=previous)
            return action
    return action


def _defer_official_halt(db,action,target):
    if action.get('provider')!='kis_ksd' or action.get('kind')!='split' or action.get('effective_date'):return None
    text=str(action.get('raw',{}).get('td_stop_dt',''))
    match=re.fullmatch(r'\s*(\d{4})[/.\-](\d{2})[/.\-](\d{2})\s*~\s*',text)
    if not match:return None
    try:start=pd.Timestamp('-'.join(match.groups()))
    except ValueError:return None
    first=db.execute("SELECT min(session_date) FROM trading_calendar WHERE market='kr'").fetchone()[0]
    if first is None or start<pd.Timestamp(first) or start>target:return None
    pf,sf=action.get('price_factor'),action.get('share_factor')
    if any(isinstance(x,bool) or not isinstance(x,(int,float)) or not math.isfinite(x) or x<=0 for x in (pf,sf)) or not math.isclose(pf*sf,1,rel_tol=1e-8):return None
    total,received,nonzero=db.execute('''SELECT count(*),count(r.session_date),count(*) FILTER(WHERE NOT coalesce(isfinite(r.volume) AND r.volume=0,false))
        FROM trading_calendar c LEFT JOIN ingest_raw_bars r ON r.market=c.market AND r.session_date=c.session_date AND r.symbol=?
        WHERE c.market='kr' AND c.session_date>=? AND c.session_date<=?''',[action['symbol'],_day(start),_day(target)]).fetchone()
    if not total or received!=total or nonzero:return None
    return {**action,'pending_status':'pending_unknown_effective','missing_effective_date':True,'halt_start':_day(start),
            'zero_volume_sessions':int(total),'last_checked':_day(target),'confirmation':'official_KSD_halt_and_complete_zero_volume_raw'}


def _store_pending(db,action):
    serialized=canonical(action)
    old=db.execute('SELECT payload FROM ingest_pending_actions WHERE provider=? AND event_id=?',[action['provider'],action['event_id']]).fetchone()
    if old and old[0]==serialized:return 0
    db.execute('INSERT OR REPLACE INTO ingest_pending_actions VALUES(?,?,?,?,?)',[action['provider'],action['event_id'],action['revision'],action['market'],serialized])
    return 1


def _actions(db,batch,target,attempt):
    changed=0;quarantine=[]
    pending={ (r[0],r[1]):json.loads(r[2]) for r in db.execute('SELECT provider,event_id,payload FROM ingest_pending_actions WHERE market=?',[batch.market]).fetchall()}
    batch_seen={}
    for incoming in batch.actions:
        identity=(incoming.get('provider'),incoming.get('event_id'))
        if identity in batch_seen and canonical(batch_seen[identity])!=canonical(incoming):
            atomic_json(attempt/'quarantine.json',[batch_seen[identity],incoming])
            raise IngestionError('Conflicting corporate action revisions in provider batch')
        batch_seen[identity]=incoming;pending[identity]=incoming
    for action in pending.values():
        action=dict(action)
        if action.get('kind') in ('cash_distribution','no_change'):continue
        if not action.get('symbol') or not action.get('event_id') or not action.get('revision'):quarantine.append(action);continue
        # Unrelated universe events are retained in the raw batch, not spliced into history.
        if not db.execute('SELECT count(*) FROM assets WHERE market=? AND symbol=?',[batch.market,action['symbol']]).fetchone()[0]:continue
        old=db.execute('SELECT revision,seed_effective_date,seed_price_factor,seed_share_factor,market,symbol,effective_date,price_factor,share_factor FROM ingest_actions WHERE provider=? AND event_id=?',[action.get('provider'),action['event_id']]).fetchone()
        if old and old[0]==action['revision']:continue
        effective=action.get('effective_date')
        if effective and pd.Timestamp(effective)>target:
            changed+=_store_pending(db,{**action,'pending_status':'future_effective','missing_effective_date':False})
            continue
        action=_confirm_ksd(db,action,target,old);effective=action.get('effective_date')
        halted=_defer_official_halt(db,action,target)
        if halted:
            changed+=_store_pending(db,halted);continue
        if action.get('market')!=batch.market or action.get('kind')!='split' or not action.get('confirmed') or not effective:
            quarantine.append(action);continue
        pf,sf=action.get('price_factor'),action.get('share_factor')
        if any(isinstance(x,bool) or not isinstance(x,(int,float)) or not math.isfinite(x) or x<=0 for x in (pf,sf)) or not math.isclose(pf*sf,1,rel_tol=1e-8):quarantine.append(action);continue
        if old and (old[4]!=batch.market or old[5]!=action['symbol']):quarantine.append(action);continue
        baseline=list(old[1:4]) if old else [None,1.,1.]
        db.execute('INSERT OR REPLACE INTO ingest_actions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',[action['provider'],action['event_id'],action['revision'],batch.market,action['symbol'],_day(effective),pf,sf,*baseline,canonical(action)])
        db.execute('DELETE FROM ingest_pending_actions WHERE provider=? AND event_id=?',[action['provider'],action['event_id']])
        changed+=1
    if quarantine:
        atomic_json(attempt/'quarantine.json',quarantine)
        raise IngestionError('Unconfirmed/unknown corporate actions quarantined; previous checkpoint retained')
    # Recompute deltas versus immutable seed factors, including changed ex-dates.
    affected=db.execute('SELECT DISTINCT market,symbol FROM ingest_actions WHERE market=? AND (seed_effective_date IS NULL OR effective_date<>seed_effective_date OR price_factor<>seed_price_factor OR share_factor<>seed_share_factor)',[batch.market]).fetchall()
    for market,symbol in affected:
        db.execute('UPDATE managed_bars SET split_price_factor=base_price_factor,split_share_factor=base_share_factor WHERE market=? AND symbol=?',[market,symbol])
        events=db.execute('SELECT effective_date,price_factor,share_factor,seed_effective_date,seed_price_factor,seed_share_factor FROM ingest_actions WHERE market=? AND symbol=?',[market,symbol]).fetchall()
        for date,p,s,base_date,bp,bs in events:
            if base_date is not None:db.execute('UPDATE managed_bars SET split_price_factor=split_price_factor/?,split_share_factor=split_share_factor/? WHERE market=? AND symbol=? AND session_date<?',[bp,bs,market,symbol,base_date])
            db.execute('UPDATE managed_bars SET split_price_factor=split_price_factor*?,split_share_factor=split_share_factor*? WHERE market=? AND symbol=? AND session_date<?',[p,s,market,symbol,date])
        db.execute('''UPDATE managed_bars SET open=raw_open*split_price_factor,high=raw_high*split_price_factor,low=raw_low*split_price_factor,close=raw_close*split_price_factor,
            volume=raw_volume*split_share_factor,vwap=raw_vwap*split_price_factor,turnover=raw_turnover WHERE market=? AND symbol=?''',[market,symbol])
    return changed


def _part_batch(db,providers,market,dates,attempt,part_dir,plan,audit,action_start):
    """Load/fetch just one bounded raw part; compatible failed work is reusable."""
    labels=[_day(d) for d in dates];action_label=_day(action_start);batch_dir=part_dir;complete=batch_dir/'complete.json'
    def compatible(meta):return meta.get('completed_sessions')==labels and meta.get('action_start',labels[0])==action_label
    if not complete.exists():
        for other in sorted((attempt.parent).glob('*'),key=lambda p:p.stat().st_mtime,reverse=True):
            old_plan,old_status=other/'plan.json',other/'status.json'
            if other==attempt or not old_plan.is_file() or not old_status.is_file():continue
            note=json.loads(old_status.read_text(encoding='utf-8'));old=json.loads(old_plan.read_text(encoding='utf-8'))
            if note.get('status')!='FAILED' or not note.get('retryable',True) or old.get('parent')!=plan['parent'] or old.get('audit')!=audit:continue
            for candidate in (other/market,other/market/('part-'+labels[0]+'_'+labels[-1])):
                meta_path=candidate/'complete.json'
                if meta_path.is_file() and compatible(json.loads(meta_path.read_text(encoding='utf-8'))):
                    batch_dir=candidate;complete=meta_path;break
            if complete.exists():break
    if complete.exists():
        meta=json.loads(complete.read_text(encoding='utf-8'))
        if not compatible(meta):raise IngestionError('Cached raw part session/action boundary mismatch')
        for name in ('bars','assets'):
            expected=meta.get(name+'_sha256')
            if expected and sha256_file(batch_dir/(name+'.parquet'))!=expected:raise IngestionError('Cached raw part hash mismatch')
        return MarketBatch(market,db.execute('SELECT * FROM read_parquet(?)',[str(batch_dir/'bars.parquet')]).df(),
            db.execute('SELECT * FROM read_parquet(?)',[str(batch_dir/'assets.parquet')]).df(),meta['actions'],[pd.Timestamp(d) for d in labels],meta['provenance'])
    assets=db.execute('SELECT market,symbol,name,exchange FROM assets WHERE market=?',[market]).df()
    provider=providers[market] if isinstance(providers,dict) else providers
    part_hook=getattr(provider,'fetch_market_part',None)
    batch=part_hook(market,dates,assets,audit=audit,action_start=action_start) if callable(part_hook) else provider.fetch_market(market,dates,assets,audit=audit)
    _normalize_batch(batch,market,dates)
    batch_dir.mkdir(parents=True,exist_ok=True)
    for name,frame in [('bars',batch.bars),('assets',batch.assets)]:
        # An interrupted COPY is never a completed checkpoint. Replace only this
        # attempt's known part file; completed immutable parts are read above.
        path=batch_dir/(name+'.parquet')
        if path.exists():path.unlink()
        db.register('saved_part',frame);db.execute('COPY saved_part TO '+_literal(path)+' (FORMAT PARQUET)');db.unregister('saved_part')
    atomic_json(complete,{'actions':batch.actions,'completed_sessions':labels,'action_start':action_label,'provenance':batch.provenance,
                         'bars_sha256':sha256_file(batch_dir/'bars.parquet'),'assets_sha256':sha256_file(batch_dir/'assets.parquet')})
    return batch


def refresh(providers,*,seed_path=SOURCE,supplement_path=BENCHMARKS,root=MANAGED_ROOT,markets=('us','kr','crypto'),now=None,overlap_sessions=5,audit=False):
    if not markets or len(set(markets))!=len(markets) or not set(markets)<={'us','kr','crypto'}:raise IngestionError('Invalid required markets')
    if isinstance(overlap_sessions,bool) or not isinstance(overlap_sessions,int) or overlap_sessions<1:raise IngestionError('overlap_sessions must be positive')
    root=Path(root).resolve();seed=Path(seed_path).resolve()
    with managed_lock(root):
        current=resolve_current(root);source=Path(current['resolved_path']) if current else seed
        if not source.is_file() or Path(str(source)+'.wal').exists():raise IngestionError('Closed seed/checkpoint required')
        with duckdb.connect(str(source),read_only=True) as db:
            first={m:db.execute('SELECT min(session_date) FROM trading_calendar WHERE market=?',[m]).fetchone()[0] for m in markets}
            collected={m:{pd.Timestamp(r[0]) for r in db.execute('SELECT session_date FROM trading_calendar WHERE market=?',[m]).fetchall()} for m in markets}
        sessions={};targets={}
        evidence=json.loads(canonical(current.get('calendar_evidence',{}) if current else {}));loaded=set()
        for market in markets:
            if first[market] is None:raise IngestionError('Seed market calendar missing')
            all_dates=completed_sessions(market,first[market],now)
            provider=providers[market] if isinstance(providers,dict) else providers
            if id(provider) not in loaded:
                if callable(getattr(provider,'begin_refresh',None)):provider.begin_refresh()
                if callable(getattr(provider,'load_session_evidence',None)):provider.load_session_evidence(evidence)
                loaded.add(id(provider))
            reconcile=getattr(provider,'reconcile_sessions',None)
            if callable(reconcile):
                try:
                    reconciled=list(pd.DatetimeIndex(reconcile(market,all_dates,collected[market])).normalize())
                    if reconciled!=sorted(set(reconciled)) or not set(reconciled)<=set(all_dates) or not (collected[market]&set(all_dates))<=set(reconciled):raise IngestionError('Invalid official calendar reconciliation')
                    all_dates=reconciled
                except Exception as exc:
                    reason=str(exc) if isinstance(exc,(IngestionError,ProviderError)) else type(exc).__name__
                    failed_id='calendar-'+uuid.uuid4().hex
                    atomic_json(root/'attempts'/failed_id/'status.json',{'attempt_id':failed_id,'status':'FAILED','phase':'calendar','market':market,'reason':reason,'error_type':type(exc).__name__})
                    return {'status':'FAILED','attempt_id':failed_id,'reason':reason,'previous_sequence':current['sequence'] if current else None}
            for m,days in getattr(provider,'calendar_evidence',{}).items():evidence.setdefault(m,{}).update(days)
            if not all_dates:raise IngestionError('No completed target sessions')
            missing=[d for d in all_dates if d not in collected[market]]
            sessions[market]=all_dates if audit else sorted(set(all_dates[-overlap_sessions:])|set(missing))
            targets[market]=all_dates[-1]
        plan={'parent':current['sha256'] if current else sha256_file(seed),'sessions':{m:[_day(d) for d in ds] for m,ds in sessions.items()},'audit':audit,'collector_version':2,'calendar_evidence':evidence}
        identity=hashlib.sha256(canonical(plan).encode()).hexdigest()[:24]
        candidates=[]
        for p in (root/'attempts').glob(identity+'*'):
            note=p/'status.json'
            if note.exists():
                note_value=json.loads(note.read_text(encoding='utf-8'))
                if note_value.get('status') in ('RUNNING','FAILED') and note_value.get('retryable',True):candidates.append(p)
        # Completed attempts are immutable evidence. Fresh refreshes must query
        # overlapping provider rows again to discover revisions/corrections.
        attempt=max(candidates,key=lambda p:p.stat().st_mtime) if candidates else root/'attempts'/(identity+'-'+uuid.uuid4().hex[:12])
        attempt_id=attempt.name;attempt.mkdir(parents=True,exist_ok=True)
        work=attempt/'working.duckdb';atomic_json(attempt/'plan.json',plan)
        # Rebuild working DB from its immutable parent; reusable completed market batches remain.
        if work.exists():work.unlink()
        if Path(str(work)+'.wal').exists():Path(str(work)+'.wal').unlink()
        shutil.copy2(source,work)
        attempt_status={'attempt_id':attempt_id,'status':'RUNNING','targets':{m:_day(d) for m,d in targets.items()},'started_at':datetime.now(timezone.utc).isoformat()}
        atomic_json(attempt/'status.json',attempt_status)
        try:
            changed=int(bool(current and canonical(current.get('calendar_evidence',{}))!=canonical(evidence)))
            with duckdb.connect(str(work)) as db:
                db.execute("SET memory_limit='1GB'");db.execute('SET threads=4');_initialize(db,supplement_path if not current else None)
                for market in markets:
                    dates=sessions[market];actions=[];parts=[dates[start:start+MAX_PART_SESSIONS] for start in range(0,len(dates),MAX_PART_SESSIONS)]
                    for number,part in enumerate(parts):
                        directory=attempt/market if len(parts)==1 else attempt/market/('part-'+_day(part[0])+'_'+_day(part[-1]))
                        action_start=part[0] if number==0 else parts[number-1][-1]+pd.Timedelta(days=1)
                        batch=_part_batch(db,providers,market,part,attempt,directory,plan,audit,action_start)
                        frame=_normalize_batch(batch,market,part);changed+=_upsert(db,frame,batch);actions.extend(batch.actions)
                        attempt_status.setdefault('markets',{})[market]={'completed_parts':number+1,'total_parts':len(parts),'last_session':_day(part[-1])}
                        atomic_json(attempt/'status.json',attempt_status)
                        del batch,frame
                    # KSD reference evidence can occur in a later part than its
                    # record date. Resolve only after ALL market raw parts exist.
                    action_batch=MarketBatch(market,pd.DataFrame(),pd.DataFrame(),actions,dates,{})
                    changed+=_actions(db,action_batch,targets[market],attempt)
                if changed or not current:
                    db.execute('CREATE TABLE IF NOT EXISTS ingest_checkpoint_meta(sequence BIGINT,calendar_evidence VARCHAR)')
                    db.execute('DELETE FROM ingest_checkpoint_meta');db.execute('INSERT INTO ingest_checkpoint_meta VALUES(?,?)',[current['sequence']+1 if current else 1,canonical(evidence)])
                db.execute('CHECKPOINT')
            watermarks={m:{'target':_day(d),'actual':_day(d)} for m,d in targets.items()}
            sequence=current['sequence']+1 if current else 1
            manifest={'schema_version':SCHEMA_VERSION,'key_version':KEY_VERSION,'sequence':sequence,'database':f'checkpoints/{sequence:012d}.duckdb',
                'sha256':sha256_file(work),'parent_sha256':current['sha256'] if current else None,'ancestors':[current['sha256'],*current['ancestors']] if current else [],
                'includes_supplement':True,'markets':watermarks,'calendar_evidence':evidence,'created_at':datetime.now(timezone.utc).isoformat()}
            manifest['validation']=validate_database(work,manifest)
            if current and not changed:
                attempt_status.update(status='UNCHANGED');atomic_json(attempt/'status.json',attempt_status)
                return {'status':'UNCHANGED','manifest':{k:v for k,v in current.items() if k!='resolved_path'},'attempt_id':attempt_id}
            validate_manifest(manifest)
            dest=root/manifest['database'];dest.parent.mkdir(parents=True,exist_ok=True)
            if dest.exists():
                if sha256_file(dest)!=manifest['sha256']:raise IngestionError('Immutable checkpoint sequence collision')
                work.unlink()
            else:os.replace(work,dest)
            atomic_json(root/'current.json',manifest)
            attempt_status.update(status='PUBLISHED',sequence=sequence);atomic_json(attempt/'status.json',attempt_status)
            return {'status':'PUBLISHED','manifest':manifest,'attempt_id':attempt_id}
        except Exception as exc:
            # Do not serialize arbitrary provider exception text: it may embed credentials.
            reason=str(exc) if isinstance(exc,(IngestionError,ProviderError)) else type(exc).__name__
            attempt_status.update(status='FAILED',reason=reason,error_type=type(exc).__name__,retryable=not isinstance(exc,ValueError) and not (attempt/'quarantine.json').exists());atomic_json(attempt/'status.json',attempt_status)
            return {'status':'FAILED','attempt_id':attempt_id,'reason':reason,'previous_sequence':current['sequence'] if current else None}
