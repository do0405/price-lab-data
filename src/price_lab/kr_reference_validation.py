"""Reference-specific gates; incomplete reference data never validates price data."""
from decimal import Decimal, InvalidOperation

VALIDATION_VERSION = 'kr-reference-validation-v1'


def reconcile_cap(row, *, absolute_tolerance_krw=1):
    """Compare provider cap to same-day RAW close × RAW listed shares in KRW.

    One won is an explicit absolute rounding allowance, not a percentage used to
    hide changes in units. No adjusted close, inferred current shares, or repairs.
    """
    try:
        close=Decimal(str(row.get('raw_close')))
        shares=Decimal(str(row.get('listed_shares')))
        cap=Decimal(str(row.get('market_cap_krw')))
        if not all(v.is_finite() for v in (close,shares,cap)):
            raise InvalidOperation
    except (InvalidOperation, ValueError, TypeError):
        return dict(valid=False,status='MISSING',expected_cap_krw=None,difference_krw=None)
    if close<=0 or shares<=0 or cap<=0 or shares!=shares.to_integral_value() or cap!=cap.to_integral_value():
        return dict(valid=False,status='ZERO_OR_INVALID_UNIT',expected_cap_krw=close*shares,difference_krw=None)
    expected=close*shares; difference=cap-expected
    valid=abs(difference)<=Decimal(str(absolute_tolerance_krw))
    return dict(valid=valid,status='VALID' if valid else 'CAP_MISMATCH',expected_cap_krw=expected,difference_krw=difference)


def validate_reference(db, *, sessions=None, scope='observed', persist=False):
    from .kr_reference_store import coverage_report, refresh_coverage
    if persist: refresh_coverage(db,sessions=sessions,scope=scope)
    return coverage_report(db,sessions=sessions,scope=scope)


def validate_row_contract(kind, row):
    """Reject impossible contract values; incomplete evidence remains quarantined."""
    from .kr_reference_schema import AVAILABILITY_BASES, BOARDS
    from .kr_reference_sources import instant, exact_number
    if row.get('availability_basis') not in AVAILABILITY_BASES: raise ValueError('Unknown reference availability basis')
    if row.get('availability_basis')=='PUBLICATION_TIME_VERIFIED' and not row.get('source_published_at'):
        raise ValueError('Verified publication requires publication evidence timestamp')
    instant(row.get('retrieved_at'))
    if row.get('source_published_at'): instant(row['source_published_at'])
    if row.get('research_available_at') and row.get('research_policy_version')!='first-observed-baseline-next-session-v1':
        raise ValueError('Unknown reconstruction availability policy')
    if kind in ('capitalization','aliases','events') and row.get('board') not in BOARDS:
        raise ValueError('Unsupported reference board')
    if kind=='capitalization':
        for field,integer,bigint in [('raw_close',False,False),('market_cap_krw',True,False),('listed_shares',True,True)]:
            exact_number(row.get(field),integer=integer,bigint=bigint)
    if kind=='aliases':
        if not all(row.get(k) for k in ('security_id','listing_id','isin','raw_symbol')):
            raise ValueError('Official identity evidence incomplete')
        if row.get('effective_to') and str(row['effective_to'])[:10]<=str(row['effective_from'])[:10]:
            raise ValueError('Invalid identity effective interval')
    if kind=='settlements':
        if row.get('status')=='UNKNOWN' and (row.get('cash_per_raw_share') is not None or row.get('exchange_ratio') is not None):
            raise ValueError('Unknown recovery must remain null')
        if row.get('status')=='CONFIRMED':
            if not row.get('security_id') or not row.get('raw_share_basis') or not row.get('evidence_url'):
                raise ValueError('Confirmed settlement requires identity and raw-unit evidence')
            if row.get('kind') in ('CASH','EXCHANGE') and not row.get('payment_date'):
                raise ValueError('Confirmed settlement requires actual payment date')
            if row.get('kind')=='CASH' and row.get('cash_per_raw_share') is None:
                raise ValueError('Confirmed cash settlement amount missing')
            if row.get('kind')=='EXCHANGE' and (row.get('exchange_ratio') is None or not row.get('target_security_id')):
                raise ValueError('Confirmed exchange settlement terms missing')
