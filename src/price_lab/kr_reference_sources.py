"""Pure official-source parsing and completed-gzip reads; never infer identity by name."""
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import csv
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import time
from urllib.parse import urlsplit

from .kr_reference_schema import BOARDS

ENDPOINTS = {
    ('KOSPI', 'daily'): 'stk_bydd_trd', ('KOSDAQ', 'daily'): 'ksq_bydd_trd',
    ('KOSPI', 'master'): 'stk_isu_base_info', ('KOSDAQ', 'master'): 'ksq_isu_base_info',
}
KRX_BASE = 'https://data-dbg.krx.co.kr/svc/apis/sto/'


class SourceContractError(ValueError):
    """Safe fixed-message error; raw payloads/HTTP headers are never included."""


class RawNotReady(ValueError):
    """A file is changing, incomplete, oversized, or has an invalid gzip CRC."""


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def day(value):
    if isinstance(value, datetime): return value.date()
    if isinstance(value, date): return value
    text = str(value).strip()
    try: return datetime.strptime(text, '%Y%m%d').date() if len(text) == 8 and text.isdigit() else date.fromisoformat(text[:10])
    except (ValueError, TypeError): raise SourceContractError('Invalid official date') from None


def instant(value=None):
    result = datetime.now(timezone.utc) if value is None else value
    if isinstance(result, str):
        try: result = datetime.fromisoformat(result.replace('Z', '+00:00'))
        except ValueError: raise SourceContractError('Invalid provenance timestamp') from None
    if not isinstance(result, datetime) or result.tzinfo is None:
        raise SourceContractError('Timezone-aware provenance timestamp required')
    return result.astimezone(timezone.utc)


def exact_number(value, *, integer=False, bigint=False):
    if value is None or str(value).strip() == '': return None, 'missing_field'
    text = str(value).strip().replace(',', '')
    if text in ('-', '--', 'N/A'): return None, 'missing_marker'
    if isinstance(value, (float, bool)): raise SourceContractError('Exact decimal source value required')
    try: result = Decimal(text)
    except InvalidOperation: raise SourceContractError('Invalid official numeric field') from None
    if not result.is_finite() or result < 0 or (integer and result != result.to_integral_value()):
        raise SourceContractError('Invalid official numeric unit')
    if integer and (result >= Decimal(10) ** 38 or (bigint and result > 9223372036854775807)):
        raise SourceContractError('Official numeric field exceeds storage unit')
    if not integer and (result.adjusted() >= 30 or result.as_tuple().exponent < -8):
        raise SourceContractError('Official decimal precision exceeds storage unit')
    return (int(result) if bigint else result), None


@dataclass(frozen=True)
class CompletedRaw:
    path: Path
    payload: dict
    metadata: dict
    sha256: str
    compressed_sha256: str
    retrieved_at: datetime


def read_completed_raw(path, *, min_age_seconds=2, max_uncompressed_bytes=128_000_000):
    """Stat before/after, bounded full decompression (including CRC), SHA of actual bytes.

    Legacy gzip contains only a response body. Its mtime is an observation time,
    never proof of the HTTP envelope or an historic publication timestamp.
    """
    path = Path(path)
    if not path.name.endswith('.json.gz'): raise RawNotReady('Incomplete raw filename')
    try:
        before = path.stat()
        if not before.st_size or time.time() - before.st_mtime < min_age_seconds:
            raise RawNotReady('Raw file is not stable yet')
        if before.st_size > max_uncompressed_bytes: raise RawNotReady('Raw file exceeds bound')
        compressed = path.read_bytes()
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
            content = stream.read(max_uncompressed_bytes + 1)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise RawNotReady('Raw file changed while reading')
        if len(content) > max_uncompressed_bytes: raise RawNotReady('Raw response exceeds bound')
        value = json.loads(content.decode('utf-8-sig'))
    except RawNotReady: raise
    except (OSError, EOFError, ValueError, UnicodeError): raise RawNotReady('Raw gzip or JSON is incomplete') from None
    if not isinstance(value, dict): raise RawNotReady('Unsupported raw response shape')
    metadata = value.get('_kr_reference', {})
    payload = value.get('payload') if '_kr_reference' in value else value
    if not isinstance(payload, dict) or not isinstance(metadata, dict): raise RawNotReady('Unsupported raw envelope')
    observed = metadata.get('retrieved_at') or datetime.fromtimestamp(before.st_mtime, timezone.utc)
    return CompletedRaw(path, payload, metadata, hashlib.sha256(content).hexdigest(),
                        hashlib.sha256(compressed).hexdigest(), instant(observed))


def _records(payload):
    records = payload.get('OutBlock_1') if isinstance(payload, dict) else None
    if not isinstance(records, list) or not records or any(not isinstance(r, dict) for r in records):
        raise SourceContractError('Empty or unsupported official response')
    return records


def _board(record):
    values = {str(record[k]).strip() for k in ('MKT_NM', 'MKT_TP_NM') if record.get(k)}
    if len(values) != 1 or next(iter(values)) not in BOARDS: return None
    return next(iter(values))


def inspect_payload(payload, metadata=None):
    """Identify only a field/date/board proven in the body or collector envelope."""
    metadata = metadata or {}
    try: records = _records(payload)
    except SourceContractError: return None
    first = records[0]
    kind = 'daily' if {'BAS_DD', 'MKTCAP', 'LIST_SHRS', 'TDD_CLSPRC'}.issubset(first) else (
        'master' if {'ISU_SRT_CD', 'LIST_DD', 'SECUGRP_NM'}.issubset(first) else None)
    boards = {_board(r) for r in records}
    board = next(iter(boards)) if len(boards) == 1 else None
    if board is None or kind is None: return None
    dates = {day(r['BAS_DD']).isoformat() for r in records if r.get('BAS_DD')}
    requested = metadata.get('requested_date')
    if len(dates) > 1: raise SourceContractError('Mixed dates in official response')
    asof = next(iter(dates)) if dates else requested
    if asof is None: return None
    if requested and day(requested) != day(asof): raise SourceContractError('Official requested date mismatch')
    if metadata.get('board') and metadata['board'] != board: raise SourceContractError('Official envelope board mismatch')
    return {'kind': kind, 'board': board, 'asof_date': day(asof).isoformat(), 'record_count': len(records)}


def _symbol(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9A-Z]{6}', value):
        raise SourceContractError('Exact six-character official short code required')
    return value


def parse_capitalization(payload, *, board, requested_date=None):
    if board not in BOARDS: raise SourceContractError('Unsupported official board')
    result = []; seen = set()
    for raw in _records(payload):
        if not {'BAS_DD', 'ISU_CD', 'TDD_CLSPRC', 'MKTCAP', 'LIST_SHRS'}.issubset(raw):
            raise SourceContractError('Official daily capitalization schema changed')
        if _board(raw) != board: raise SourceContractError('Official daily board mismatch')
        asof = day(raw['BAS_DD']); symbol = _symbol(raw['ISU_CD'])
        if requested_date and asof != day(requested_date): raise SourceContractError('Official requested date mismatch')
        key = (asof, symbol)
        if key in seen: raise SourceContractError('Duplicate official daily security')
        seen.add(key); values = {}; reasons = {}
        for name, field, integer, bigint in (
            ('raw_close','TDD_CLSPRC',False,False), ('market_cap_krw','MKTCAP',True,False),
            ('listed_shares','LIST_SHRS',True,True)):
            values[name], reason = exact_number(raw[field], integer=integer, bigint=bigint)
            if reason: reasons[name] = reason
        close, cap, shares = values['raw_close'], values['market_cap_krw'], values['listed_shares']
        quality = ('MISSING' if reasons else 'ZERO_PRICE_OR_SHARES' if close == 0 or shares == 0 else
                   'CAP_MISMATCH' if abs(close * shares - cap) > 1 else 'VALID')
        result.append(dict(source_row_id=f'{board}:{asof}:{symbol}', revision_id=digest(raw), asof_date=asof,
            board=board, raw_symbol=symbol, currency='KRW', quality_status=quality,
            raw_fields=raw, null_reasons=reasons, availability_basis='EFFECTIVE_DATE_RECONSTRUCTED', **values))
    return result


def parse_master(payload, *, board, requested_date):
    if board not in BOARDS: raise SourceContractError('Unsupported official board')
    asof = day(requested_date); result = []; seen = set()
    for raw in _records(payload):
        required = {'ISU_CD','ISU_SRT_CD','LIST_DD','MKT_TP_NM','SECUGRP_NM','KIND_STKCERT_TP_NM'}
        if not required.issubset(raw): raise SourceContractError('Official master schema changed')
        if _board(raw) != board: raise SourceContractError('Official master board mismatch')
        if raw.get('BAS_DD') and day(raw['BAS_DD']) != asof: raise SourceContractError('Official master requested date mismatch')
        listed = day(raw['LIST_DD'])
        if listed > asof: raise SourceContractError('Historical master contains a future listing date')
        isin = str(raw['ISU_CD']); symbol = _symbol(raw['ISU_SRT_CD'])
        if not re.fullmatch(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', isin): raise SourceContractError('Official master ISIN missing')
        if symbol in seen: raise SourceContractError('Duplicate official master security')
        seen.add(symbol)
        # These IDs do not link a changed ISIN, a new issue, or a relisting.
        security = 'kr:security:' + digest({'isin':isin})[:24]
        listing = 'kr:listing:' + digest({'isin':isin,'board':board,'listing_date':listed})[:24]
        alias = digest({'board':board,'raw_symbol':symbol,'asof_date':asof,'isin':isin})
        common = raw['SECUGRP_NM'] in ('주권','외국주권') and raw['KIND_STKCERT_TP_NM'] == '보통주'
        result.append(dict(alias_id=alias,source_row_id=f'{board}:{asof}:{symbol}',revision_id=digest(raw),
            security_id=security,listing_id=listing,isin=isin,raw_symbol=symbol,name=raw.get('ISU_NM'),board=board,
            security_type='stock' if common else 'unknown',listing_date=listed,effective_from=asof,
            effective_to=asof+timedelta(days=1),identity_status='VERIFIED',raw_fields=raw,
            availability_basis='EFFECTIVE_DATE_RECONSTRUCTED',evidence_url=KRX_BASE+ENDPOINTS[(board,'master')]))
    return result


def parse_kind_export(path, *, source_url, retrieved_at=None, column_map=None, completeness='UNVERIFIED'):
    """Import an official CSV/JSON export with an explicit column mapping.

    No unverified automated KIND endpoint, name joins, or inferred recovery. Excel
    exports must first be saved as CSV without changing the official code text.
    """
    parsed = urlsplit(source_url)
    if parsed.scheme != 'https' or parsed.hostname != 'kind.krx.co.kr' or parsed.username or parsed.password:
        raise SourceContractError('Official KIND HTTPS source URL required')
    path = Path(path); content = path.read_bytes(); retrieved = instant(retrieved_at)
    if len(content) > 30_000_000: raise SourceContractError('KIND export exceeds import bound')
    try: text = content.decode('utf-8-sig')
    except UnicodeDecodeError: text = content.decode('cp949')
    rows = json.loads(text) if path.suffix.lower() == '.json' else list(csv.DictReader(io.StringIO(text)))
    if not isinstance(rows,list) or not rows: raise SourceContractError('Empty KIND export')
    sha = hashlib.sha256(content).hexdigest(); manifest_id = digest({'source':'kind_export','sha256':sha,'url':source_url})
    mapping = column_map or {}; events = []; settlements = []
    allowed = {'LISTING','TRANSFER','SUSPENSION','RESUMPTION','LIQUIDATION_TRADING','DELISTING','MERGER','CANCELLATION'}
    for index, source in enumerate(rows):
        raw = {mapping.get(key,key):value for key,value in source.items()}
        if not {'event_id','raw_symbol','board','event_type'}.issubset(raw):
            raise SourceContractError('KIND import requires explicit documented column mapping')
        event_id = str(raw['event_id']).strip(); symbol = _symbol(raw['raw_symbol']); board = raw['board']
        if not event_id or board not in BOARDS or raw['event_type'] not in allowed:
            raise SourceContractError('Unknown KIND lifecycle contract')
        published = instant(raw['source_published_at']) if raw.get('source_published_at') else None
        verified = raw.get('publication_evidence_verified') in (True,'true','1') and bool(published)
        common = dict(source_id='kind_official_export',source_row_id=f'{event_id}:{index}',revision_id=digest(source),
            manifest_id=manifest_id,retrieved_at=retrieved,source_published_at=published,
            availability_basis='PUBLICATION_TIME_VERIFIED' if verified else 'EFFECTIVE_DATE_RECONSTRUCTED')
        effective = day(raw['effective_date']) if raw.get('effective_date') else None
        event = dict(common,event_id=event_id,security_id=raw.get('security_id') or None,
            listing_id=raw.get('listing_id') or None,raw_symbol=symbol,board=board,event_type=raw['event_type'],
            announced_at=instant(raw['announced_at']) if raw.get('announced_at') else None,effective_date=effective,
            last_trade_date=day(raw['last_trade_date']) if raw.get('last_trade_date') else None,
            status=raw.get('event_status') or 'OBSERVED',supersedes_event_id=raw.get('supersedes_event_id') or None,
            evidence_url=source_url,raw_fields=source)
        events.append(event)
        if raw['event_type'] in ('DELISTING','MERGER') or raw.get('settlement_kind'):
            status = raw.get('status') or 'UNKNOWN'
            if status not in ('UNKNOWN','CONFIRMED','CANCELLED'): raise SourceContractError('Unknown KIND settlement status')
            cash,_ = exact_number(raw.get('cash_per_raw_share'))
            ratio,_ = exact_number(raw.get('exchange_ratio'))
            # A delisting reason is not evidence of any recovery amount.
            if status != 'CONFIRMED': cash = ratio = None
            settlements.append(dict(common,settlement_id=raw.get('settlement_id') or event_id,
                event_id=event_id,security_id=raw.get('security_id') or None,kind=raw.get('settlement_kind') or 'UNRESOLVED',
                status=status,payment_date=day(raw['payment_date']) if raw.get('payment_date') else None,
                cash_per_raw_share=cash,exchange_ratio=ratio,target_security_id=raw.get('target_security_id') or None,
                currency=raw.get('currency') or 'KRW',raw_share_basis=raw.get('raw_share_basis') or None,
                effective_date=effective,evidence_url=source_url,raw_fields=source))
    return dict(manifests=[dict(manifest_id=manifest_id,source_id='kind_official_export',source_url=source_url,
        raw_sha256=sha,raw_path=str(path),retrieved_at=retrieved,source_published_at=None,
        availability_basis='EFFECTIVE_DATE_RECONSTRUCTED',record_count=len(rows),completeness=completeness,
        metadata={'import_contract':'explicit-column-map-v1','page_completeness_verified':completeness=='COMPLETE'})],
        events=events,settlements=settlements,receipt_id=manifest_id)
