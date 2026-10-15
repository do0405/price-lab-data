"""Append-only evidence revisions, atomic receipts, and explicit availability queries."""
from datetime import datetime, timezone
import re
import pandas as pd

from .kr_reference_schema import create_schema, REFERENCE_TABLES, SCHEMA_VERSION
from .kr_reference_sources import canonical, digest, instant, day
from .kr_reference_validation import validate_row_contract, VALIDATION_VERSION

DELTA_TABLES = {
    'manifests':'kr_source_manifest','capitalization':'kr_capitalization_observations',
    'aliases':'kr_security_aliases','events':'kr_listing_events','settlements':'kr_settlements',
}
KEYS = {
    'kr_source_manifest':('manifest_id',),
    'kr_capitalization_observations':('source_id','source_row_id','revision_id'),
    'kr_security_aliases':('alias_id','revision_id'),
    'kr_listing_events':('event_id','revision_id'),
    'kr_settlements':('settlement_id','revision_id'),
    'kr_reference_receipts':('receipt_id',),
}
_IMMUTABLE_IGNORE={'manifest_id','retrieved_at','source_published_at','availability_basis',
    'research_available_at','research_policy_version','committed_at','row_count','raw_path'}


def _columns(db, table, schema=None):
    name=f'{schema}.{table}' if schema else table
    return [r[1] for r in db.execute(f"PRAGMA table_info('{name}')").fetchall()]


def _normalized(kind, original):
    row=dict(original)
    for field in ('raw_fields','null_reasons','metadata'):
        if field in row: row[field+'_json']=canonical(row.pop(field))
    for field in ('raw_fields_json','null_reasons_json','metadata_json'):
        if field not in row: row[field]='{}'
    if kind=='manifests':
        row.setdefault('completeness','UNVERIFIED'); row.setdefault('record_count',0)
    return row


def apply_delta(db, delta):
    """Append revisions and one receipt in the SAME transaction; replays are no-ops.

    The caller owns the connection, not a surrounding transaction. Malformed rows
    roll back the complete delta, including its source manifest and receipt.
    """
    create_schema(db)
    payload={key:delta.get(key,[]) for key in DELTA_TABLES}
    delta_sha=digest(payload); receipt=delta.get('receipt_id') or delta_sha
    existing=db.execute('SELECT delta_sha256 FROM kr_reference_receipts WHERE receipt_id=?',[receipt]).fetchone()
    if existing:
        if existing[0]!=delta_sha: raise ValueError('Reference receipt content collision')
        return {'inserted':0,'receipt_id':receipt,'idempotent':True}
    inserted=0
    db.execute('BEGIN TRANSACTION')
    try:
        for kind,table in DELTA_TABLES.items():
            columns=_columns(db,table); normalized=[]
            for source in payload[kind]:
                row=_normalized(kind,source)
                if kind!='manifests': validate_row_contract(kind,row)
                if kind=='manifests':
                    if not row.get('source_url') or not re.fullmatch(r'[a-f0-9]{64}',str(row.get('raw_sha256',''))):
                        raise ValueError('Reference source provenance incomplete')
                normalized.append({key:row.get(key) for key in columns})
            if not normalized: continue
            # dtype=object keeps BIGINTs/DECIMALs exact when another row is null.
            relation='_kr_delta_'+kind
            db.register(relation,pd.DataFrame(normalized,columns=columns,dtype=object))
            try:
                types={r[1]:r[2] for r in db.execute(f"PRAGMA table_info('{table}')").fetchall()}
                projection=','.join(f'CAST("{c}" AS {types[c]}) AS "{c}"' for c in columns)
                src=f'(SELECT {projection} FROM {relation})'
                if kind!='manifests' and db.execute(f'SELECT count(*) FROM {src} s WHERE NOT EXISTS(SELECT 1 FROM kr_source_manifest m WHERE m.manifest_id=s.manifest_id)').fetchone()[0]:
                    raise ValueError('Reference source manifest missing')
                keys=KEYS[table]; join=' AND '.join(f't.{k}=s.{k}' for k in keys)
                compare=[c for c in columns if c not in set(keys)|_IMMUTABLE_IGNORE]
                differences=' OR '.join(f't.{c} IS DISTINCT FROM s.{c}' for c in compare)
                if differences and db.execute(f'SELECT count(*) FROM {src} s JOIN {table} t ON {join} WHERE {differences}').fetchone()[0]:
                    raise ValueError('Immutable reference revision collision')
                changed=db.execute(f'SELECT count(*) FROM {src} s WHERE NOT EXISTS(SELECT 1 FROM {table} t WHERE {join})').fetchone()[0]
                db.execute(f'INSERT INTO {table} SELECT s.* FROM {src} s WHERE NOT EXISTS(SELECT 1 FROM {table} t WHERE {join}) ON CONFLICT DO NOTHING')
                inserted+=changed
            finally: db.unregister(relation)
        db.execute('INSERT INTO kr_reference_receipts VALUES (?,?,?,?,?)',
                   [receipt,datetime.now(timezone.utc),SCHEMA_VERSION,delta_sha,inserted])
        db.execute('COMMIT')
    except BaseException:
        db.execute('ROLLBACK'); raise
    create_views(db)
    return {'inserted':inserted,'receipt_id':receipt,'idempotent':False}


def _visible_cte(table, partition, mode, snapshot=None):
    # snapshot is a retrieval timestamp cap for callers with a pre-existing pinned
    # artifact; the managed dataset's immutable SHA is the normal snapshot pin.
    bound=' AND retrieved_at<=?::TIMESTAMPTZ' if snapshot else ''
    if mode=='publication_time_verified':
        return f"""SELECT * FROM {table} WHERE availability_basis='PUBLICATION_TIME_VERIFIED'
            AND source_published_at IS NOT NULL AND source_published_at<=?::TIMESTAMPTZ {bound}
            QUALIFY row_number() OVER (PARTITION BY {partition} ORDER BY source_published_at DESC,retrieved_at DESC,revision_id DESC)=1"""
    if mode!='effective_date_reconstructed': raise ValueError('Unknown reference information mode')
    # Unknown-publication revisions are a frozen first-observed reconstruction,
    # not the latest corrected historical value silently shown as original PIT.
    return f"""WITH baseline AS (
        SELECT * FROM {table} WHERE availability_basis!='UNKNOWN' {bound}
        QUALIFY source_published_at IS NOT NULL OR row_number() OVER
            (PARTITION BY {partition},(source_published_at IS NULL) ORDER BY retrieved_at,revision_id)=1)
        SELECT * FROM baseline WHERE
            (source_published_at IS NOT NULL AND source_published_at<=?::TIMESTAMPTZ)
            OR (source_published_at IS NULL AND coalesce(research_available_at,retrieved_at)<=?::TIMESTAMPTZ)
        QUALIFY row_number() OVER (PARTITION BY {partition} ORDER BY
            coalesce(source_published_at,research_available_at,retrieved_at) DESC,revision_id DESC)=1"""


def _visibility_params(cutoff,mode,snapshot):
    if mode=='publication_time_verified': return [cutoff]+([instant(snapshot)] if snapshot else [])
    return ([instant(snapshot)] if snapshot else [])+[cutoff,cutoff]


def select_aliases_asof(db, decision_at, *, mode='publication_time_verified', snapshot=None):
    create_schema(db); cutoff=instant(decision_at)
    sql=_visible_cte('kr_security_aliases','alias_id',mode,snapshot)
    result=db.execute(sql,_visibility_params(cutoff,mode,snapshot))
    names=[c[0] for c in result.description]
    return [dict(zip(names,row)) for row in result.fetchall()]


def select_asof(db, decision_at, *, asof_date=None, mode='publication_time_verified', snapshot=None):
    """Select cap only with a unique official identity at that exact effective day.

    strict requires source publication + exact revision evidence; reconstructed
    permits explicit next-session baseline policy and is never called PIT ready.
    """
    create_schema(db); cutoff=instant(decision_at)
    cap_sql=_visible_cte('kr_capitalization_observations','board,raw_symbol,asof_date',mode,snapshot)
    alias_sql=_visible_cte('kr_security_aliases','alias_id',mode,snapshot)
    params=_visibility_params(cutoff,mode,snapshot)*2
    where=' AND c.asof_date=?::DATE' if asof_date else ''
    if asof_date: params.append(day(asof_date))
    query=f"""WITH cap AS ({cap_sql}), aliases AS ({alias_sql}), joined AS (
        SELECT c.*,a.security_id,a.listing_id,a.isin,a.security_type,a.identity_status,
        a.availability_basis AS identity_availability_basis,
        count(*) OVER (PARTITION BY c.board,c.raw_symbol,c.asof_date) AS identity_matches
        FROM cap c JOIN aliases a ON c.raw_symbol=a.raw_symbol AND c.board=a.board
        AND c.asof_date>=a.effective_from AND (a.effective_to IS NULL OR c.asof_date<a.effective_to)
        WHERE c.asof_date<=?::TIMESTAMPTZ::DATE AND c.quality_status='VALID' AND c.raw_close>0
        AND c.listed_shares>0 AND c.market_cap_krw>0 AND abs(c.raw_close*c.listed_shares-c.market_cap_krw)<=1
        AND a.identity_status='VERIFIED' {where})
        SELECT * EXCLUDE(identity_matches) FROM joined WHERE identity_matches=1
        ORDER BY asof_date,board,raw_symbol"""
    # Date cutoff appears before the optional date parameter in the outer query.
    base=_visibility_params(cutoff,mode,snapshot)*2
    params=base+[cutoff]+([day(asof_date)] if asof_date else [])
    result=db.execute(query,params); names=[c[0] for c in result.description]
    return [dict(zip(names,row)) for row in result.fetchall()]


def create_views(db):
    """Current reconstruction view; historical decision queries MUST use select_asof."""
    db.execute("""CREATE OR REPLACE VIEW kr_capitalization_daily AS
        WITH cap AS (SELECT * FROM kr_capitalization_observations
            QUALIFY row_number() OVER (PARTITION BY board,raw_symbol,asof_date ORDER BY
                coalesce(source_published_at,retrieved_at) DESC,retrieved_at DESC,revision_id DESC)=1),
        aliases AS (SELECT * FROM kr_security_aliases QUALIFY row_number() OVER
            (PARTITION BY alias_id ORDER BY coalesce(source_published_at,retrieved_at) DESC,revision_id DESC)=1),
        joined AS (SELECT c.*,a.security_id,a.listing_id,a.isin,a.security_type,
            count(*) OVER(PARTITION BY c.board,c.raw_symbol,c.asof_date) identity_matches
            FROM cap c JOIN aliases a ON c.board=a.board AND c.raw_symbol=a.raw_symbol
            AND c.asof_date>=a.effective_from AND (a.effective_to IS NULL OR c.asof_date<a.effective_to)
            WHERE c.quality_status='VALID' AND a.identity_status='VERIFIED')
        SELECT * EXCLUDE(identity_matches) FROM joined WHERE identity_matches=1""")


def coverage_report(db, *, sessions=None, scope='observed'):
    create_schema(db)
    cap="""SELECT * FROM kr_capitalization_observations QUALIFY row_number() OVER
        (PARTITION BY board,raw_symbol,asof_date ORDER BY coalesce(source_published_at,retrieved_at) DESC,retrieved_at DESC,revision_id DESC)=1"""
    aliases="""SELECT * FROM kr_security_aliases QUALIFY row_number() OVER
        (PARTITION BY alias_id ORDER BY coalesce(source_published_at,retrieved_at) DESC,revision_id DESC)=1"""
    rows=db.execute(f"""WITH c AS ({cap}), a AS ({aliases}), matched AS (
        SELECT c.board,c.asof_date,c.raw_symbol,c.quality_status,c.availability_basis,
        count(a.alias_id) matches,count(DISTINCT a.security_id||':'||a.listing_id) identities,
        count(a.alias_id) FILTER(WHERE a.security_type='stock' AND a.identity_status='VERIFIED') stocks,
        count(a.alias_id) FILTER(WHERE a.availability_basis='PUBLICATION_TIME_VERIFIED'
            AND a.source_published_at IS NOT NULL) alias_publication_verified
        FROM c LEFT JOIN a ON c.board=a.board AND c.raw_symbol=a.raw_symbol AND c.asof_date>=a.effective_from
        AND (a.effective_to IS NULL OR c.asof_date<a.effective_to)
        GROUP BY c.board,c.asof_date,c.raw_symbol,c.quality_status,c.availability_basis)
        SELECT asof_date,board,count(*) received,
        count(*) FILTER(WHERE quality_status='VALID') valid_capitalization,
        count(*) FILTER(WHERE matches=1 AND stocks=1) identified_common_stocks,
        count(*) FILTER(WHERE matches=0) unknown_identity,
        count(*) FILTER(WHERE matches>1 OR identities>1) conflicts,
        count(*) FILTER(WHERE availability_basis='PUBLICATION_TIME_VERIFIED'
            AND matches=1 AND alias_publication_verified=1) publication_verified
        FROM matched GROUP BY asof_date,board ORDER BY asof_date,board""").fetchall()
    names=('asof_date','board','received','valid_capitalization','identified_common_stocks','unknown_identity','conflicts','publication_verified')
    details=[dict(zip(names,r)) for r in rows]
    expected_days=sorted({day(s) for s in sessions}) if sessions else sorted({r['asof_date'] for r in details})
    wanted={(d,b) for d in expected_days for b in ('KOSPI','KOSDAQ')}
    present={(r['asof_date'],r['board']) for r in details}
    missing=sorted(wanted-present)
    conflicts=sum(r['conflicts'] for r in details)
    unknown=sum(r['unknown_identity'] for r in details)
    cap_bad=sum(r['received']-r['valid_capitalization'] for r in details)
    count=sum(r['received'] for r in details)
    settlement_unknown=db.execute("SELECT count(*) FROM kr_settlements WHERE status='UNKNOWN' OR security_id IS NULL").fetchone()[0]
    capitalization_status='READY' if count and not (missing or cap_bad) else 'PARTIAL' if count else 'UNAVAILABLE'
    identity_status='READY' if count and not (missing or conflicts or unknown) else 'PARTIAL' if count else 'UNAVAILABLE'
    # No amount of daily price/cap/master rows proves complete lifecycle history.
    # Imported KIND rows are retained, but period/page/continuity certification is
    # deliberately unresolved until independently reviewed official evidence exists.
    lifecycle_status='UNVERIFIED'
    status='PARTIAL' if count else 'UNAVAILABLE'
    publication_count=sum(r['publication_verified'] for r in details)
    strict_ready=bool(count and capitalization_status=='READY' and identity_status=='READY' and publication_count==count)
    for row in details: row['asof_date']=row['asof_date'].isoformat()
    return dict(status=status,scope=scope,validation_version=VALIDATION_VERSION,rows=details,
        observations=count,identity_conflicts=conflicts,unknown_identity=unknown,invalid_capitalization=cap_bad,
        capitalization_status=capitalization_status,identity_status=identity_status,lifecycle_status=lifecycle_status,
        unknown_settlements=settlement_unknown,missing_date_boards=[{'asof_date':str(d),'board':b} for d,b in missing],
        publication_time_verified=strict_ready,publication_verified_observations=publication_count,
        information_basis='PUBLICATION_TIME_VERIFIED' if strict_ready else 'EFFECTIVE_DATE_RECONSTRUCTED',
        limitation='Historical membership, lifecycle completeness and publication-time coverage require separate official evidence.')


def refresh_coverage(db, *, sessions=None, scope='observed'):
    report=coverage_report(db,sessions=sessions,scope=scope)
    db.execute('DELETE FROM kr_reference_coverage WHERE scope=?',[scope])
    now=datetime.now(timezone.utc)
    for row in report['rows']:
        for item,received,unknown in (
            ('capitalization',row['valid_capitalization'],row['received']-row['valid_capitalization']),
            ('identity',row['identified_common_stocks'],row['unknown_identity']),
            ('publication',row['publication_verified'],row['received']-row['publication_verified'])):
            status='READY' if received and not unknown and not row['conflicts'] else 'PARTIAL'
            db.execute('INSERT INTO kr_reference_coverage VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                [scope,row['asof_date'],row['board'],item,row['received'],received,unknown,row['conflicts'],status,VALIDATION_VERSION,now])
    for row in report['missing_date_boards']:
        db.execute('INSERT INTO kr_reference_coverage VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            [scope,day(row['asof_date']),row['board'],'capitalization',None,0,0,0,'UNAVAILABLE',VALIDATION_VERSION,now])
    return report


def export_delta(db):
    """Small-delta interchange. For bulk attached databases use merge_reference_database."""
    create_schema(db); result={}
    for name,table in DELTA_TABLES.items():
        cursor=db.execute('SELECT * FROM '+table); columns=[c[0] for c in cursor.description]
        result[name]=[dict(zip(columns,row)) for row in cursor.fetchall()]
    return result


def merge_attached_reference(db, source_alias, *, optional=True):
    """SQL bulk append of an attached validated reference artifact, without Python rows."""
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',source_alias): raise ValueError('Invalid attached source alias')
    tables={r[0] for r in db.execute('SELECT table_name FROM information_schema.tables WHERE table_catalog=?',[source_alias]).fetchall()}
    available=set(REFERENCE_TABLES)&tables
    if not available:
        if optional: return 0
        raise ValueError('Candidate has no reference schema')
    if not set(REFERENCE_TABLES).issubset(tables): raise ValueError('Incomplete candidate reference schema')
    version=db.execute(f'SELECT version FROM {source_alias}.kr_reference_schema').fetchall()
    if version!=[(SCHEMA_VERSION,)]: raise ValueError('Unsupported candidate reference schema')
    create_schema(db)
    for table in REFERENCE_TABLES:
        src=db.execute(f"PRAGMA table_info('{source_alias}.{table}')").fetchall()
        dst=db.execute(f"PRAGMA table_info('{table}')").fetchall()
        if [(r[1],r[2]) for r in src]!=[(r[1],r[2]) for r in dst]:
            raise ValueError('Candidate reference table contract mismatch')
    # DuckDB caller may already be in a transaction (managed publication). This
    # function deliberately leaves begin/commit/rollback to that caller.
    for table in DELTA_TABLES.values():
        if table=='kr_source_manifest': continue
        bad=db.execute(f"""SELECT count(*) FROM {source_alias}.{table} r WHERE
            r.availability_basis NOT IN ('UNKNOWN','EFFECTIVE_DATE_RECONSTRUCTED','PUBLICATION_TIME_VERIFIED')
            OR (r.availability_basis='PUBLICATION_TIME_VERIFIED' AND r.source_published_at IS NULL)
            OR NOT EXISTS(SELECT 1 FROM {source_alias}.kr_source_manifest m WHERE m.manifest_id=r.manifest_id)""").fetchone()[0]
        if bad: raise ValueError('Candidate reference provenance invalid')
    if db.execute(f"SELECT count(*) FROM {source_alias}.kr_settlements WHERE status='UNKNOWN' AND (cash_per_raw_share IS NOT NULL OR exchange_ratio IS NOT NULL)").fetchone()[0]:
        raise ValueError('Unknown candidate recovery must remain null')
    changed=0
    for table in (*DELTA_TABLES.values(),'kr_reference_receipts'):
        keys=KEYS[table]; join=' AND '.join(f't.{k}=s.{k}' for k in keys)
        # Existing immutable revisions may recur with a different retrieval
        # envelope, but their economic/identity/document content cannot change.
        cols=_columns(db,table)
        compare=[c for c in cols if c not in set(keys)|_IMMUTABLE_IGNORE]
        differences=' OR '.join(f't.{c} IS DISTINCT FROM s.{c}' for c in compare)
        if differences and db.execute(f'SELECT count(*) FROM {source_alias}.{table} s JOIN {table} t ON {join} WHERE {differences}').fetchone()[0]:
            raise ValueError('Immutable reference revision collision')
        added=db.execute(f'SELECT count(*) FROM {source_alias}.{table} s WHERE NOT EXISTS(SELECT 1 FROM {table} t WHERE {join})').fetchone()[0]
        db.execute(f'INSERT INTO {table} SELECT s.* FROM {source_alias}.{table} s WHERE NOT EXISTS(SELECT 1 FROM {table} t WHERE {join})')
        changed+=added
    # Coverage belongs to the final merged parent and is recalculated by caller.
    create_views(db)
    return changed
