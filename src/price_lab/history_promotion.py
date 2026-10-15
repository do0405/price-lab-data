"""Explicit promotion of completed crypto raw history into managed snapshots.

Equity archives require a separate action/identity review and cannot use this
path. Crypto uses the application's existing unspliced, unit-factor convention.
"""
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import shutil
import uuid
import duckdb
import pandas as pd
from . import history_backfill as h
from .ingestion import (MANAGED_ROOT,managed_lock,resolve_current,atomic_json,sha256_file,
                        _upsert,_initialize,validate_manifest,validate_database)
from .providers import MarketBatch


def _result(manifest,bars,status='PUBLISHED'):
    return {'status':status,'sequence':manifest['sequence'],'rows_added':len(bars) if status=='PUBLISHED' else 0,
            'historical_rows':len(bars),'first_date':str(bars.session_date.min().date()),
            'last_backfilled_date':str(bars.session_date.max().date()),'sha256':manifest['sha256'],
            'consumer_ready':True,'basis':'unit factor for unspliced USD crypto pairs'}


def _record_success(archive,folder,note,result):
    # current.json is the commit point. Auxiliary receipt failure cannot turn a
    # successfully committed dataset into FAILED; the DB plan marker recovers it.
    warnings=[]
    if result['status']=='PUBLISHED' or not (archive/'promotion.json').exists():
        try:atomic_json(archive/'promotion.json',result)
        except OSError:warnings.append('Promotion receipt write failed; managed checkpoint is already published')
    if folder:
        try:atomic_json(folder/'status.json',{**note,**result})
        except OSError:warnings.append('Attempt receipt write failed; managed checkpoint is already published')
    return {**result,**({'warnings':warnings} if warnings else {})}


def promote_crypto(archive,*,root=MANAGED_ROOT):
    archive=Path(archive);root=Path(root)
    # Collector takes only the archive lock. Publication always takes this order.
    with managed_lock(archive),managed_lock(root):
        plan=h._plan(archive)
        if plan['market']!='crypto':raise ValueError('Equity history requires corporate-action and identity review')
        state=h._read(archive/'status.json')
        if state.get('status')!='COMPLETE':raise ValueError('Historical collection is incomplete')
        if not state.get('raw_reconciled'):raise ValueError('Historical raw archive requires part reconciliation; resume collection first')
        if state.get('database_sha256')!=sha256_file(archive/'raw_history.duckdb'):raise ValueError('Historical database hash mismatch')
        current=resolve_current(root)
        if not current:raise ValueError('Managed parent checkpoint required')
        parent=plan.get('basis',{}).get('parent_dataset',{}).get('source_sha256')
        if parent and parent not in [current['sha256'],*current['ancestors']]:raise ValueError('History parent lineage mismatch')
        with duckdb.connect(str(archive/'raw_history.duckdb'),read_only=True) as db:
            receipts=db.execute('SELECT part,manifest_sha256 FROM part_receipts ORDER BY part').fetchall()
            if [r[0] for r in receipts]!=list(range(len(plan['parts']))):raise ValueError('Historical receipts incomplete')
            for number,digest in receipts:
                folder=archive/'parts'/f'{number:05d}';h._verify(folder,number,plan)
                if sha256_file(folder/'manifest.json')!=digest:raise ValueError('Historical receipt invalid')
            bars=db.execute('SELECT * FROM raw_bars ORDER BY session_date,symbol').fetchdf()
        days=sorted(set(bars.session_date))
        if [str(d.date()) for d in days]!=plan['sessions']:raise ValueError('Historical raw calendar incomplete')
        if not bars.symbol.str.endswith('/USD').all():raise ValueError('Only USD crypto pairs are supported')
        if not all(bars[c].map(lambda x:pd.notna(x) and float('-inf')<x<float('inf') and x>0).all() for c in ('open','high','low','close','vwap')):raise ValueError('Historical crypto prices invalid')
        with duckdb.connect(str(current['resolved_path']),read_only=True) as db:
            if 'history_promotions' in {r[0] for r in db.execute('SHOW TABLES').fetchall()}:
                marker=db.execute("SELECT count(*) FROM history_promotions WHERE market='crypto' AND plan_sha256=?",[plan['plan_sha256']]).fetchone()[0]
                if marker:return _record_success(archive,None,{},_result(current,bars,'ALREADY_PUBLISHED'))
            first=db.execute("SELECT min(session_date) FROM trading_calendar WHERE market='crypto'").fetchone()[0]
            latest=days[-1].date()
            if latest>=first:raise ValueError('History overlaps existing consumer dates')
            if pd.Timestamp(latest)+pd.Timedelta(days=1)!=pd.Timestamp(first):raise ValueError('History does not join the existing crypto calendar')
            # Current names/exchanges prevail. Archived current-reference metadata
            # never overwrites later names with a historical observation.
            assets=db.execute("SELECT market,symbol,name,exchange FROM assets WHERE market='crypto'").fetchdf()
            already=db.execute("SELECT count(*) FROM backtest_bars WHERE market='crypto' AND session_date BETWEEN ? AND ?",[days[0],days[-1]]).fetchone()[0]
            if already:raise ValueError('Historical consumer keys already exist')
        # Recover a validated destination moved before an interrupted pointer
        # write. Never rebuild or overwrite an immutable sequence on retry.
        for prepared in (root/'attempts').glob('history-crypto-*/prepared-manifest.json'):
            candidate=h._read(prepared);validate_manifest(candidate)
            if candidate['parent_sha256']!=current['sha256'] or candidate['sequence']!=current['sequence']+1:continue
            dest=root/candidate['database']
            if not dest.exists():continue
            if sha256_file(dest)!=candidate['sha256']:raise ValueError('Prepared history checkpoint hash mismatch')
            validate_database(dest,candidate)
            with duckdb.connect(str(dest),read_only=True) as db:
                marker=db.execute("SELECT count(*) FROM history_promotions WHERE market='crypto' AND plan_sha256=?",[plan['plan_sha256']]).fetchone()[0]
            if not marker:continue
            atomic_json(root/'current.json',candidate)
            return _record_success(archive,prepared.parent,{'plan_sha256':plan['plan_sha256']},_result(candidate,bars))
        folder=root/'attempts'/('history-crypto-'+uuid.uuid4().hex[:12]);folder.mkdir(parents=True)
        work=folder/'working.duckdb';shutil.copy2(current['resolved_path'],work)
        note={'operation':'history-crypto','plan_sha256':plan['plan_sha256'],'status':'RUNNING','rows':len(bars)}
        atomic_json(folder/'status.json',note)
        manifest=None
        try:
            with duckdb.connect(str(work)) as db:
                db.execute("SET memory_limit='1GB'");db.execute('SET threads=4');_initialize(db,None)
                _upsert(db,bars,MarketBatch('crypto',bars,assets,[],days,{}))
                count=db.execute("SELECT count(*) FROM backtest_bars WHERE market='crypto' AND session_date BETWEEN ? AND ?",[days[0],days[-1]]).fetchone()[0]
                if count!=len(bars):raise ValueError('Historical consumer row reconciliation failed')
                mismatch=db.execute("SELECT count(*) FROM backtest_bars WHERE market='crypto' AND session_date<? AND (split_price_factor<>1 OR split_share_factor<>1 OR open<>raw_open OR close<>raw_close OR volume<>raw_volume)",[first]).fetchone()[0]
                if mismatch:raise ValueError('Unexpected crypto adjustment factor')
                db.execute('DELETE FROM ingest_checkpoint_meta');db.execute('INSERT INTO ingest_checkpoint_meta VALUES(?,?)',[current['sequence']+1,json.dumps(current.get('calendar_evidence',{}))])
                db.execute('CREATE TABLE IF NOT EXISTS history_promotions(market VARCHAR,plan_sha256 VARCHAR,first_date DATE,last_date DATE,rows BIGINT,basis VARCHAR)')
                db.execute('INSERT INTO history_promotions VALUES(?,?,?,?,?,?)',['crypto',plan['plan_sha256'],days[0],days[-1],len(bars),'unspliced USD pairs; unit split factor, same convention as existing crypto data'])
                db.execute('CHECKPOINT')
            sequence=current['sequence']+1
            manifest={k:v for k,v in current.items() if k not in ('resolved_path','validation')}
            manifest.update(sequence=sequence,database=f'checkpoints/{sequence:012d}.duckdb',sha256=sha256_file(work),
                parent_sha256=current['sha256'],ancestors=[current['sha256'],*current['ancestors']],created_at=datetime.now(timezone.utc).isoformat())
            # A hard interruption after the file move must not reserve the daily
            # collector's sequence filename. The manifest carries the actual path.
            manifest['database']=f"checkpoints/history-{sequence:012d}-{manifest['sha256'][:12]}.duckdb"
            manifest['validation']=validate_database(work,manifest);validate_manifest(manifest)
            atomic_json(folder/'prepared-manifest.json',manifest)
            dest=root/manifest['database']
            if dest.exists():raise ValueError('Immutable history checkpoint sequence collision')
            os.replace(work,dest);atomic_json(root/'current.json',manifest)
            return _record_success(archive,folder,note,_result(manifest,bars))
        except Exception as exc:
            if manifest and h._read(root/'current.json').get('sha256')==manifest['sha256']:
                return _record_success(archive,folder,note,_result(manifest,bars))
            atomic_json(folder/'status.json',{**note,'status':'FAILED','error':type(exc).__name__});raise
