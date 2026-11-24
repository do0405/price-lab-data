"""Select an entire observed raw KR bar, retaining the caller's originals."""
import math

import pandas as pd


VERSION='kr-whole-observation-v2-kis-range-fallback'


def kr_trade_problem_sql(alias='', *, allow_outside_ohlc='false'):
    """SQL equivalent used by wide immutable-source marts, without Python rows."""
    p=alias+'.' if alias else ''
    def c(name):return p+name
    o,h,l,close,v,t,w=[c(k) for k in ('open','high','low','close','volume','turnover','vwap')]
    return f"""CASE WHEN {c('currency')} IS DISTINCT FROM 'KRW' THEN 'invalid_currency'
        WHEN NOT coalesce(isfinite({v}) AND {v}>=0 AND {v}=floor({v}),false) THEN 'missing_or_invalid_volume'
        WHEN {v}=0 THEN 'zero_volume_without_trade_evidence'
        WHEN NOT coalesce(isfinite({o}) AND isfinite({h}) AND isfinite({l}) AND isfinite({close})
            AND {o}>0 AND {l}>0 AND {close}>0 AND {h}>=greatest({o},{close},{l}) AND {l}<=least({o},{close}),false) THEN 'invalid_ohlc'
        WHEN NOT coalesce(isfinite({t}) AND {t}>=0,false) THEN 'missing_or_invalid_turnover'
        WHEN NOT coalesce(isfinite({w}) AND {w}>0 AND (({allow_outside_ohlc}) OR ({w}>={l}-1e-6 AND {w}<={h}+1e-6))
            AND abs({w}*{v}-{t})<=greatest(1e-6,abs({t})*1e-10),false) THEN 'invalid_turnover_vwap_equation' END"""


def _number(value):
    if isinstance(value,bool):return None
    try:
        value=float(value)
        return value if math.isfinite(value) else None
    except (ValueError,TypeError,OverflowError):return None


def valid_ohlc(row):
    values=[_number(row.get(k)) for k in ('open','high','low','close')]
    if any(v is None or v<=0 for v in values):return False
    o,h,l,c=values
    return h>=max(o,l,c) and l<=min(o,c)


def kr_bar_problem(row):
    """A normal tradable bundle; no evidence from a different bar is borrowed."""
    if row.get('market','kr')!='kr' or row.get('currency')!='KRW':return 'invalid_currency'
    if row.get('adjustment','raw') not in (None,'raw'):return 'non_raw_price'
    if not valid_ohlc(row):return 'invalid_ohlc'
    volume,turnover=_number(row.get('volume')),_number(row.get('turnover'))
    if volume is None or volume<0 or volume!=math.floor(volume):return 'missing_or_invalid_volume'
    if volume==0:return 'zero_volume_without_trade_evidence'
    if turnover is None or turnover<0:return 'missing_or_invalid_turnover'
    vwap=_number(row.get('vwap'))
    if vwap is None:vwap=turnover/volume
    source=str(row.get('source','')).casefold()
    raw_kis='kis' in source and ('adjust' not in source or 'unadjusted' in source)
    outside=vwap<float(row['low'])-1e-6 or vwap>float(row['high'])+1e-6
    if (vwap<=0 or (outside and not raw_kis)
            or abs(vwap*volume-turnover)>max(1e-6,abs(turnover)*1e-10)):
        return 'invalid_turnover_vwap_equation'
    return None


def _nontrading(row):
    if row.get('currency')!='KRW' or _number(row.get('volume'))!=0 or _number(row.get('turnover'))!=0:return False
    close=_number(row.get('close'))
    if close is None or close<=0:return False
    prices=[_number(row.get(k)) for k in ('open','high','low')]
    if prices not in ([0.,0.,0.],[close,close,close]):return False
    vwap=_number(row.get('vwap'))
    return vwap is None or vwap==close


def _priority(row):
    source=str(row.get('source','')).casefold()
    raw='adjusted' not in source or 'unadjusted' in source
    return (0 if 'krx' in source else 1 if 'kis' in source else 2) if raw else 2


def select_kr_price_bars(frame):
    """Normal KRX first, then normal KIS; invalid keys don't delete other keys.

    Exact nontrading quotes may remain when neither provider has a trade bar.
    Their metadata stays nontradable. Revisions resolve by observed timestamp,
    revision and source hash, never by closeness to a desired price path.
    """
    if frame.empty:return frame.copy()
    work=frame.copy(deep=True)
    if 'vwap' not in work:work['vwap']=float('nan')
    missing=work.vwap.isna() & pd.to_numeric(work.volume,errors='coerce').gt(0)
    work.loc[missing,'vwap']=work.loc[missing,'turnover']/work.loc[missing,'volume']
    work['_problem']=[kr_bar_problem(r) for r in work.to_dict('records')]
    work['_nontrade']=[_nontrading(r) for r in work.to_dict('records')]
    work['_priority']=[_priority(r) for r in work.to_dict('records')]
    work=work[(work._problem.isna()|work._nontrade)&work._priority.lt(2)].copy()
    if work.empty:return frame.iloc[:0].copy()
    work['_trade']=work._problem.isna()
    for field in ('retrieved_at','revision_id','source_sha256'):
        if field not in work:work[field]=None
    work['_stamp']=pd.to_datetime(work.retrieved_at,errors='coerce',utc=True)
    work=work.sort_values(['_trade','_priority','_stamp','revision_id','source_sha256'],
        ascending=[False,True,False,False,False],na_position='last',kind='stable')
    keys=['market','symbol','session_date'] if 'market' in work else ['symbol','session_date']
    result=work.drop_duplicates(keys).drop(columns=['_problem','_nontrade','_priority','_trade','_stamp'])
    if 'is_tradable_bar' in result:
        result['is_tradable_bar']=[kr_bar_problem(r) is None for r in result.to_dict('records')]
    result=result.sort_values(keys,kind='stable').reset_index(drop=True)
    result.attrs=dict(frame.attrs)
    return result


def complete_kr_price_bars(frame,kis,sessions,*,expected_symbols=()):
    """Request raw KIS only for absent/invalid requested KRX keys in future runs."""
    work=frame.copy()
    available=set()
    if not work.empty:
        available={(r['symbol'],pd.Timestamp(r['session_date'])) for r in work.to_dict('records')
                   if kr_bar_problem(r) is None}
    symbols=set(expected_symbols)
    if not work.empty:symbols.update(work.symbol)
    additions=[]
    for symbol in sorted(symbols):
        missing=[day for day in sessions if (symbol,pd.Timestamp(day)) not in available]
        if not missing:continue
        try:
            extra=kis.fetch_stock_bars([symbol],missing)
        except Exception as exc:
            # Provider transport/shape failure cannot discard other symbols'
            # already valid observations. Programming errors still surface.
            from .providers import ProviderError
            if not isinstance(exc,ProviderError):raise
            continue
        if not extra.empty:
            additions.append(extra[(extra.symbol==symbol)&extra.session_date.isin(missing)])
    if additions:work=pd.concat([work,*additions],ignore_index=True)
    return select_kr_price_bars(work) if not work.empty else work
