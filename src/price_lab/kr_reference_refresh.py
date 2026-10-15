"""Bounded daily KR reference refresh after the independent price publication.

Each attempt uses a private closed reference database and the existing immutable
managed publisher. The long-running historical reference staging DB is never
opened. Collection failures retain the successful price publication. A complete
recent window is a zero-network no-op; otherwise only missing day/board cells are
requested. ``force`` also checks cached revisions, without refetching known cells.
"""
from datetime import datetime, timezone
from pathlib import Path
import uuid

import duckdb

from .history_integration import integrate_reference
from .ingestion import atomic_json, managed_lock, resolve_current
from .kr_reference_pipeline import collect_reference, official_fetcher
from .kr_reference_schema import BOARDS, REFERENCE_TABLES, create_schema
from .kr_reference_store import coverage_report, create_views

REFRESH_VERSION = 'kr-recent-reference-refresh-v1'
MAX_SESSIONS = 6
_SOURCE_ALIAS = 'refresh_parent'


def _parent_window(root, max_sessions):
    parent = resolve_current(root)
    if parent is None:
        raise ValueError('Managed price checkpoint required for reference refresh')
    with duckdb.connect(parent['resolved_path'], read_only=True) as db:
        # trading_calendar contains completed, validated sessions. A watermark,
        # when present, additionally excludes any later auxiliary calendar rows.
        watermark = parent.get('markets', {}).get('kr', {}).get('actual')
        ceiling = ' AND session_date<=?::DATE' if watermark else ''
        params = [watermark, max_sessions] if watermark else [max_sessions]
        rows = db.execute('SELECT DISTINCT session_date FROM trading_calendar '
            "WHERE market='kr'" + ceiling + ' ORDER BY session_date DESC LIMIT ?', params).fetchall()
        sessions = sorted(str(row[0]) for row in rows)
        tables = {row[0] for row in db.execute('SHOW TABLES').fetchall()}
        present = {'daily': set(), 'master': set()}
        reference_present = bool(set(REFERENCE_TABLES) & tables)
        if reference_present and not set(REFERENCE_TABLES).issubset(tables):
            raise ValueError('Managed reference schema is incomplete')
        if sessions and reference_present:
            for kind, table, date_column in (
                ('daily', 'kr_capitalization_observations', 'asof_date'),
                ('master', 'kr_security_aliases', 'effective_from'),
            ):
                present[kind] = {(str(date), board) for date, board in db.execute(
                    f'SELECT DISTINCT {date_column},board FROM {table} '
                    f'WHERE {date_column} IN (SELECT unnest(?::DATE[]))', [sessions]).fetchall()}
    return parent, sessions, present, reference_present


def _seed_recent_reference(db, parent, sessions, reference_present):
    """Copy recent reference evidence only, not price rows or the full history DB."""
    create_schema(db)
    if not reference_present:
        return
    source = str(parent['resolved_path']).replace("'", "''")
    db.execute("ATTACH '" + source + "' AS " + _SOURCE_ALIAS + ' (READ_ONLY)')
    try:
        for table, date_column in (
            ('kr_capitalization_observations', 'asof_date'),
            ('kr_security_aliases', 'effective_from'),
        ):
            db.execute(f'INSERT INTO {table} SELECT * FROM {_SOURCE_ALIAS}.{table} '
                f'WHERE {date_column} IN (SELECT unnest(?::DATE[]))', [sessions])
        db.execute(f'''INSERT INTO kr_source_manifest SELECT * FROM {_SOURCE_ALIAS}.kr_source_manifest
            WHERE manifest_id IN (SELECT manifest_id FROM kr_capitalization_observations
                UNION SELECT manifest_id FROM kr_security_aliases)''')
        db.execute(f'''INSERT INTO kr_reference_receipts SELECT * FROM {_SOURCE_ALIAS}.kr_reference_receipts
            WHERE receipt_id IN (SELECT manifest_id FROM kr_source_manifest)''')
    finally:
        db.execute('DETACH ' + _SOURCE_ALIAS)
    create_views(db)


def _lazy_official_fetch():
    provider = None

    def fetch(board, date, kind):
        nonlocal provider
        # Completed cache reuse must work without initializing credentials.
        if provider is None:
            provider = official_fetcher()
        return provider(board, date, kind)

    return fetch


def _kind_summary(cache_result, collection_result):
    fields = ('network_requests', 'fetched', 'reused', 'inserted', 'failed')
    summary = {key: cache_result.get(key, 0) + collection_result.get(key, 0) for key in fields}
    summary['remaining_date_boards'] = collection_result['remaining_date_boards']
    summary['raw_scan_complete'] = collection_result['raw_scan_complete']
    return summary


def _save_report(refresh_root, result):
    result['finished_at'] = datetime.now(timezone.utc).isoformat()
    try:
        atomic_json(refresh_root / 'status.json', result)
    except OSError:
        # A report write after successful atomic publication cannot undo it or
        # falsely turn a successful publication into a collection failure.
        result['warning'] = 'Reference refresh status report could not be saved.'
    return result


def _refresh_locked(root, refresh_root, fetch, max_sessions, force):
    parent, sessions, present, reference_present = _parent_window(root, max_sessions)
    result = dict(version=REFRESH_VERSION, sessions=sessions, network_requests=0,
        source_parent_sha256=parent['sha256'], source_parent_sequence=parent['sequence'],
        consumer_ready=False, publication_time_verified=False)
    wanted = {(date, board) for date in sessions for board in BOARDS}
    if not sessions:
        result.update(status='UNCHANGED', reason='NO_COMPLETED_KR_SESSIONS', sha256=parent['sha256'])
        return _save_report(refresh_root, result)
    missing = {kind: wanted - present[kind] for kind in ('daily', 'master')}
    if not force and not any(missing.values()):
        result.update(status='UNCHANGED', reason='RECENT_REFERENCE_ALREADY_PRESENT', sha256=parent['sha256'])
        return _save_report(refresh_root, result)

    attempt = refresh_root / 'attempts' / uuid.uuid4().hex
    database = attempt / 'staging.duckdb'
    result['attempt'] = attempt.name
    fetch = fetch if fetch is not None else _lazy_official_fetch()
    summaries = {}
    # This is an attempt-specific lock; it has no relation to historical staging.
    # Release it before integrate_reference reacquires it and the managed lock.
    with managed_lock(attempt):
        with duckdb.connect(str(database)) as db:
            _seed_recent_reference(db, parent, sessions, reference_present)
            for kind in ('daily', 'master'):
                # Our persistent cache is written by atomic rename. It can be
                # safely reused immediately after an interrupted prior attempt.
                cached = collect_reference(db, sessions=sessions,
                    raw_roots=[refresh_root / 'raw'], reference_root=refresh_root,
                    kind=kind, limit=0, fetch=None, lock_held=True,
                    min_age_seconds=0, availability_sessions=sessions)
                # Legacy price raw keeps the collector's size/CRC/hash/stability
                # protections. It never receives a reference sidecar or rewrite.
                collected = collect_reference(db, sessions=sessions,
                    raw_roots=[root / 'raw'], reference_root=refresh_root,
                    kind=kind, limit=2 * max_sessions, fetch=fetch, lock_held=True,
                    availability_sessions=sessions)
                summaries[kind] = _kind_summary(cached, collected)
            report = coverage_report(db, sessions=sessions, scope='recent')
            result['validation'] = {key: report[key] for key in (
                'status', 'capitalization_status', 'identity_status', 'lifecycle_status',
                'identity_conflicts', 'unknown_identity', 'invalid_capitalization',
                'publication_time_verified')}
            db.execute('CHECKPOINT')
    result['collection'] = summaries
    result['network_requests'] = sum(row['network_requests'] for row in summaries.values())
    failed = any(row['failed'] or row['remaining_date_boards'] or not row['raw_scan_complete']
                 for row in summaries.values())
    inserted = sum(row['inserted'] for row in summaries.values())
    if failed:
        result.update(status='PARTIAL' if inserted else 'FAILED',
            error='KR reference collection incomplete; price publication retained.')
        return _save_report(refresh_root, result)
    if not inserted:
        result.update(status='UNCHANGED', reason='NO_NEW_REFERENCE_REVISIONS', sha256=parent['sha256'])
        return _save_report(refresh_root, result)
    try:
        publication = integrate_reference(database, root=root)
    except Exception:
        result.update(status='FAILED', error='KR reference publication failed; price publication retained.')
        return _save_report(refresh_root, result)
    result.update(status=publication['status'], sha256=publication['sha256'], sequence=publication['sequence'])
    return _save_report(refresh_root, result)


def refresh_recent_reference(root, fetch=None, max_sessions=6, force=False):
    """Refresh at most six actual KR sessions and atomically append to fresh current.

    An individual kind is bounded to ``2 * max_sessions`` missing-cell requests.
    When only the latest day is present, earlier gaps in this bounded window are
    still filled. Research availability uses the next actual collected calendar
    session, and stays null for the last session. Historical publication timing
    and lifecycle completeness are never certified by this routine.
    """
    if isinstance(max_sessions, bool) or not isinstance(max_sessions, int) or not 1 <= max_sessions <= MAX_SESSIONS:
        raise ValueError('KR reference refresh requires one to six sessions')
    if not isinstance(force, bool):
        raise ValueError('KR reference force flag must be boolean')
    root = Path(root).resolve()
    refresh_root = root / 'reference-refresh'
    provider = fetch if fetch is not None else _lazy_official_fetch()
    request_count = 0

    def counted_fetch(board, date, kind):
        nonlocal request_count
        request_count += 1
        return provider(board, date, kind)

    def unavailable():
        return dict(status='FAILED', version=REFRESH_VERSION, network_requests=request_count,
            error='KR reference refresh unavailable; price publication retained.',
            consumer_ready=False, publication_time_verified=False)

    try:
        # Serialize refresh attempts/cache only; price collection remains free to
        # publish a newer parent while this independent source work is running.
        with managed_lock(refresh_root, timeout=1):
            try:
                return _refresh_locked(root, refresh_root, counted_fetch, max_sessions, force)
            except Exception:
                return _save_report(refresh_root, unavailable())
    except Exception:
        # Neither provider exception text nor an HTTP body/header reaches logs.
        # Do not overwrite another refresh's report if its lock was unavailable.
        return unavailable()
