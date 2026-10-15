"""Closed history archives and reference deltas in one immutable managed DB.

Raw integration is deliberately separate from adjusted-price eligibility. This
module never invents an identity certificate or an adjustment factor. Network
transport and the scheduler live outside this transaction boundary.
"""
from contextlib import ExitStack
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import uuid

import duckdb

from . import history_backfill as h
from .ingestion import (MANAGED_ROOT, RAW_COLUMNS, atomic_json, canonical,
    managed_lock, resolve_current, sha256_file, validate_database, validate_manifest)

SCHEMA_VERSION=1
TABLES=('history_archive_registry','history_raw_bars','history_raw_actions',
        'history_asset_observations','history_part_receipts')


def archive_state(archive):
    archive=Path(archive); state=h._read(archive/'status.json')
    return {'archive':archive.name, 'status':'COMPLETE' if state.get('status')=='COMPLETE' else 'WAITING_COLLECTION',
        'collection_status':state.get('status','NOT_STARTED'),
        'completed_parts':state.get('completed_parts',0),'total_parts':state.get('total_parts',0),
        'rows':state.get('rows',0),'consumer_ready':False}


class _NoFetch:
    def fetch_market_part(self,*args,**kwargs):
        raise ValueError('Completed archive unexpectedly requires collection')


def seal_completed_archive(archive):
    """Upgrade old COMPLETE receipts without credentials or another API call."""
    if archive_state(archive)['status']!='COMPLETE':
        raise ValueError('Historical collection is incomplete')
    return h.collect(Path(archive),_NoFetch())


def verify_archive(archive):
    archive=Path(archive);plan=h._plan(archive);state=h._read(archive/'status.json')
    if state.get('status')!='COMPLETE':raise ValueError('Historical collection is incomplete')
    source=archive/'raw_history.duckdb'
    if not state.get('raw_reconciled') or not state.get('database_sha256'):
        raise ValueError('Historical archive must be sealed before integration')
    if not source.is_file() or Path(str(source)+'.wal').exists():raise ValueError('Closed historical database required')
    digest=sha256_file(source)
    if digest!=state['database_sha256']:raise ValueError('Historical database hash mismatch')
    with duckdb.connect(str(source),read_only=True) as db:
        db.execute("SET memory_limit='1GB'");db.execute('SET threads=4')
        receipts=db.execute('SELECT part,manifest_sha256,rows FROM part_receipts ORDER BY part').fetchall()
        if [r[0] for r in receipts]!=list(range(len(plan['parts']))):raise ValueError('Historical receipts incomplete')
        total=0
        for number,manifest_sha,rows in receipts:
            folder=archive/'parts'/f'{number:05d}';manifest=h._verify(folder,number,plan)
            if manifest_sha!=sha256_file(folder/'manifest.json') or rows!=manifest['rows']:
                raise ValueError('Historical receipt hash mismatch')
            expected={(h._digest(event),canonical(event)) for event in h._read(folder/'actions.json')}
            actual=set(db.execute('SELECT event_key,payload FROM history_actions WHERE part=?',[number]).fetchall())
            if actual!=expected:raise ValueError('Historical actions reconciliation failed')
            original_assets=db.execute('SELECT * FROM read_parquet(?)',[str(folder/'assets.parquet')]).fetchdf().to_dict('records')
            expected_assets={(r['market'],r['symbol'],canonical(r)) for r in original_assets}
            actual_assets=set(db.execute('SELECT market,symbol,payload FROM asset_observations WHERE part=?',[number]).fetchall())
            if actual_assets!=expected_assets:raise ValueError('Historical asset observations reconciliation failed')
            total+=rows
        h._reconcile_raw(archive,db,plan)
        rows=db.execute('SELECT count(*) FROM raw_bars').fetchone()[0]
        sessions=[str(r[0]) for r in db.execute('SELECT DISTINCT session_date FROM raw_bars ORDER BY 1').fetchall()]
        if sessions!=plan['sessions'] or rows!=total or rows!=state['rows']:
            raise ValueError('Historical raw calendar/count mismatch')
    return {'archive_id':plan['plan_sha256'],'market':plan['market'],
        'plan_sha256':plan['plan_sha256'],'database_sha256':digest,'rows':rows,
        'first_date':plan['sessions'][0],'last_date':plan['sessions'][-1],
        'plan':plan,'source':str(source)}


def create_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS history_archive_registry(
        archive_id VARCHAR PRIMARY KEY,market VARCHAR,plan_sha256 VARCHAR,database_sha256 VARCHAR,
        first_date DATE,last_date DATE,rows BIGINT,status VARCHAR,validation_json VARCHAR,plan_json VARCHAR)''')
    db.execute('''CREATE TABLE IF NOT EXISTS history_raw_bars(archive_id VARCHAR,
        market VARCHAR,symbol VARCHAR,session_date DATE,open DOUBLE,high DOUBLE,low DOUBLE,close DOUBLE,
        volume DOUBLE,turnover DECIMAL(38,8),vwap DOUBLE,currency VARCHAR,source VARCHAR,
        reference_previous_close DOUBLE,PRIMARY KEY(archive_id,market,symbol,session_date))''')
    db.execute('''CREATE TABLE IF NOT EXISTS history_raw_actions(archive_id VARCHAR,part INTEGER,
        event_key VARCHAR,payload VARCHAR,PRIMARY KEY(archive_id,part,event_key))''')
    db.execute('''CREATE TABLE IF NOT EXISTS history_asset_observations(archive_id VARCHAR,part INTEGER,
        market VARCHAR,symbol VARCHAR,payload VARCHAR,PRIMARY KEY(archive_id,part,market,symbol))''')
    db.execute('''CREATE TABLE IF NOT EXISTS history_part_receipts(archive_id VARCHAR,part INTEGER,
        manifest_sha256 VARCHAR,rows BIGINT,PRIMARY KEY(archive_id,part))''')
    db.execute('''CREATE TABLE IF NOT EXISTS integration_candidate_receipts(
        candidate_sha256 VARCHAR PRIMARY KEY,source_parent_sha256 VARCHAR,applied_at VARCHAR)''')
    db.execute('''CREATE TABLE IF NOT EXISTS reference_integration_receipts(
        source_sha256 VARCHAR PRIMARY KEY,applied_at VARCHAR)''')


def _gate(info):
    if info['market']=='crypto':
        reasons=['Use the existing verified crypto promotion before consumer eligibility']
    else:
        reasons=['Historical security identity continuity has not been certified',
                 'Split coverage and the pre-seed adjustment baseline require verification']
        if info['market']=='kr':reasons.append('Lifecycle and settlement coverage must be checked for the requested period')
    return {'raw_integrated':True,'consumer_ready':False,'reasons':reasons,
        'validation_version':SCHEMA_VERSION,'survivorship_bias':info['market'] in ('us','crypto')}


def _import_archive(db,info):
    existing=db.execute('SELECT database_sha256 FROM history_archive_registry WHERE archive_id=?',[info['archive_id']]).fetchone()
    if existing:
        if existing[0]!=info['database_sha256']:raise ValueError('Immutable historical archive revision conflict')
        return False
    aid=info['archive_id']
    alias='archive_'+aid[:16]
    db.execute("ATTACH '"+info['source'].replace("'","''")+"' AS "+alias+' (READ_ONLY)')
    db.execute('INSERT INTO history_raw_bars SELECT ?, '+','.join(RAW_COLUMNS)+' FROM '+alias+'.raw_bars',[aid])
    db.execute('INSERT INTO history_raw_actions SELECT ?,part,event_key,payload FROM '+alias+'.history_actions',[aid])
    db.execute('INSERT INTO history_asset_observations SELECT ?,part,market,symbol,payload FROM '+alias+'.asset_observations',[aid])
    db.execute('INSERT INTO history_part_receipts SELECT ?,part,manifest_sha256,rows FROM '+alias+'.part_receipts',[aid])
    db.execute('INSERT INTO history_archive_registry VALUES(?,?,?,?,?,?,?,?,?,?)',
        [aid,info['market'],aid,info['database_sha256'],info['first_date'],info['last_date'],info['rows'],
         'RAW_VALIDATED',canonical(_gate(info)),canonical(info['plan'])])
    # Keep this read-only attachment through CHECKPOINT/connection close.
    return True


def validate_integrated(db):
    names={r[0] for r in db.execute('SHOW TABLES').fetchall()}
    if 'history_archive_registry' not in names:return {'present':False}
    if not set(TABLES)|{'integration_candidate_receipts'}<=names:raise ValueError('Integrated history tables incomplete')
    for aid,market,rows in db.execute('SELECT archive_id,market,rows FROM history_archive_registry').fetchall():
        count,wrong=db.execute('SELECT count(*),count(*) FILTER(WHERE market<>?) FROM history_raw_bars WHERE archive_id=?',[market,aid]).fetchone()
        received=db.execute('SELECT coalesce(sum(rows),0) FROM history_part_receipts WHERE archive_id=?',[aid]).fetchone()[0]
        if count!=rows or received!=rows or wrong:raise ValueError('Integrated history reconciliation failed')
    orphan=db.execute('SELECT count(*) FROM history_raw_bars b LEFT JOIN history_archive_registry r USING(archive_id) WHERE r.archive_id IS NULL').fetchone()[0]
    if orphan:raise ValueError('Integrated history has orphan rows')
    return {'present':True,'archives':db.execute('SELECT count(*) FROM history_archive_registry').fetchone()[0]}


def _publish(root,mutate):
    """Caller owns managed_lock; mutation starts from its fresh parent."""
    root=Path(root);parent=resolve_current(root)
    if parent is None:raise ValueError('Managed parent checkpoint required')
    folder=root/'attempts'/('u-'+uuid.uuid4().hex[:12]);folder.mkdir(parents=True)
    work=folder/'working.duckdb';shutil.copy2(parent['resolved_path'],work)
    try:
        with duckdb.connect(str(work)) as db:
            db.execute("SET memory_limit='1GB'");db.execute('SET threads=4')
            create_schema(db);db.execute('BEGIN TRANSACTION')
            changed=mutate(db,parent)
            if not changed:
                db.execute('ROLLBACK')
                return {'status':'UNCHANGED','sha256':parent['sha256'],'sequence':parent['sequence']}
            validate_integrated(db)
            if 'ingest_checkpoint_meta' in {r[0] for r in db.execute('SHOW TABLES').fetchall()}:
                db.execute('DELETE FROM ingest_checkpoint_meta')
                db.execute('INSERT INTO ingest_checkpoint_meta VALUES(?,?)',[parent['sequence']+1,canonical(parent.get('calendar_evidence',{}))])
            db.execute('COMMIT');db.execute('CHECKPOINT')
        manifest={k:v for k,v in parent.items() if k not in ('resolved_path','validation')}
        manifest.update(sequence=parent['sequence']+1,parent_sha256=parent['sha256'],
            ancestors=[parent['sha256'],*parent['ancestors']],sha256=sha256_file(work),
            created_at=datetime.now(timezone.utc).isoformat())
        manifest['database']=f"checkpoints/unified-{manifest['sequence']:012d}-{manifest['sha256'][:12]}.duckdb"
        manifest['validation']=validate_database(work,manifest);validate_manifest(manifest)
        atomic_json(folder/'prepared.json',manifest)
        dest=root/manifest['database']
        if dest.exists():
            if sha256_file(dest)!=manifest['sha256']:raise ValueError('Immutable unified checkpoint collision')
        else:os.replace(work,dest)
        atomic_json(root/'current.json',manifest)
        return {'status':'PUBLISHED','sha256':manifest['sha256'],'sequence':manifest['sequence']}
    finally:
        # The immutable destination, parent and prepared receipt are retained.
        if work.exists():work.unlink()


def integrate_archives(archives,*,root=MANAGED_ROOT):
    archives=sorted({Path(a).resolve() for a in archives},key=str)
    with ExitStack() as locks:
        for archive in archives:locks.enter_context(managed_lock(archive))
        infos=[verify_archive(archive) for archive in archives]
        locks.enter_context(managed_lock(root))
        def mutate(db,parent):
            changed=False
            for info in infos:changed=_import_archive(db,info) or changed
            return changed
        return _publish(root,mutate)


def prepare_candidate_root(root,candidate):
    root=Path(root);candidate=Path(candidate)
    with managed_lock(root),managed_lock(candidate):
        parent=resolve_current(root)
        if not parent:raise ValueError('Managed parent checkpoint required')
        if (candidate/'current.json').exists():
            existing=resolve_current(candidate)
            if existing['sha256']!=parent['sha256']:raise ValueError('Candidate root is already in use')
            return existing
        dest=candidate/parent['database'];dest.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(parent['resolved_path'],dest)
        manifest={k:v for k,v in parent.items() if k!='resolved_path'}
        atomic_json(candidate/'current.json',manifest)
        return manifest


def integrate_reference(reference,*,root=MANAGED_ROOT):
    """Merge a closed, independently validated reference DB on a fresh parent."""
    reference=Path(reference)
    if not reference.is_file() or Path(str(reference)+'.wal').exists():raise ValueError('Closed reference staging required')
    with managed_lock(reference.parent,timeout=1),managed_lock(root):
        from .kr_reference_pipeline import merge_reference_database
        def mutate(db,parent):
            digest=sha256_file(reference)
            if db.execute('SELECT count(*) FROM reference_integration_receipts WHERE source_sha256=?',[digest]).fetchone()[0]:return False
            db.execute("ATTACH '"+str(reference.resolve()).replace("'","''")+"' AS reference_delta (READ_ONLY)")
            merge_reference_database(db,'reference_delta',optional=False)
            db.execute('INSERT INTO reference_integration_receipts VALUES(?,?)',[digest,datetime.now(timezone.utc).isoformat()])
            return True
        return _publish(root,mutate)


def merge_candidate(path,manifest,*,root=MANAGED_ROOT):
    path=Path(path);validate_manifest(manifest)
    if sha256_file(path)!=manifest['sha256']:raise ValueError('Candidate database hash mismatch')
    validate_database(path,manifest)
    with managed_lock(root):
        def mutate(db,parent):
            if db.execute('SELECT count(*) FROM integration_candidate_receipts WHERE candidate_sha256=?',[manifest['sha256']]).fetchone()[0]:return False
            common={parent['sha256'],*parent['ancestors']} & {manifest['parent_sha256'],*manifest['ancestors']}
            if not common:raise ValueError('Candidate has unrelated lineage')
            db.execute("ATTACH '"+str(path.resolve()).replace("'","''")+"' AS candidate_delta (READ_ONLY)")
            names={r[0] for r in db.execute('SHOW TABLES FROM candidate_delta').fetchall()}
            if not set(TABLES)<=names:raise ValueError('Candidate has no validated history delta')
            for aid,digest in db.execute('SELECT archive_id,database_sha256 FROM candidate_delta.history_archive_registry').fetchall():
                old=db.execute('SELECT database_sha256 FROM history_archive_registry WHERE archive_id=?',[aid]).fetchone()
                if old and old[0]!=digest:raise ValueError('Candidate archive revision conflict')
                if old:continue
                for table in TABLES:
                    db.execute(f'INSERT INTO {table} SELECT * FROM candidate_delta.{table} WHERE archive_id=?',[aid])
            # Reference tables are applied by the reference-specific revision
            # merger; never copy arbitrary candidate tables or candidate prices.
            if 'kr_source_manifest' in names:
                from .kr_reference_pipeline import merge_reference_database
                merge_reference_database(db,'candidate_delta',optional=True)
            if 'reference_integration_receipts' in names:
                db.execute('''INSERT INTO reference_integration_receipts
                    SELECT * FROM candidate_delta.reference_integration_receipts c
                    WHERE NOT EXISTS(SELECT 1 FROM reference_integration_receipts t WHERE t.source_sha256=c.source_sha256)''')
            db.execute('INSERT INTO integration_candidate_receipts VALUES(?,?,?)',
                [manifest['sha256'],manifest['parent_sha256'],datetime.now(timezone.utc).isoformat()])
            return True
        return _publish(root,mutate)


def integration_status(root=MANAGED_ROOT):
    current=resolve_current(root)
    if not current:return {'archives':[],'status':'NO_MANAGED_DATA'}
    with duckdb.connect(current['resolved_path'],read_only=True) as db:
        names={r[0] for r in db.execute('SHOW TABLES').fetchall()}
        if 'history_archive_registry' not in names:return {'archives':[],'status':'NO_HISTORY_INTEGRATED'}
        rows=db.execute('SELECT archive_id,market,first_date,last_date,rows,validation_json FROM history_archive_registry ORDER BY market,first_date').fetchall()
        archives=[dict(archive_id=a,market=m,first_date=str(f),last_date=str(l),rows=n,**json.loads(v)) for a,m,f,l,n,v in rows]
        if 'history_promotions' in names:
            ready={r[0] for r in db.execute('SELECT plan_sha256 FROM history_promotions').fetchall()}
            for item in archives:
                if item['archive_id'] in ready:item.update(consumer_ready=True,reasons=[],basis='Previously validated historical promotion')
    return {'archives':archives,'status':'MANAGED','sequence':current['sequence'],'sha256':current['sha256']}
