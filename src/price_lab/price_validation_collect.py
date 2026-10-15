"""Bounded provider comparison observations; this module has no DB dependency."""
from datetime import datetime, timezone
import math


def _day(value):
    return str(value)[:10]


def _number(value):
    if value is None:
        return None
    number=float(value)
    return number if math.isfinite(number) and number>=0 else None


def collect_observations(providers, batch, *, max_symbols=3):
    """Sample benchmarks plus rotating names without weakening required collection.

    Errors are fixed UNAVAILABLE records, never fabricated passing comparisons.
    US split-adjusted observations carry their own price anchor so subsequent
    global split scales cancel when compared with a local immutable snapshot.
    """
    if batch.market not in ('us','kr') or batch.bars.empty:
        return []
    if not 1<=max_symbols<=10:
        raise ValueError('Validation sample limit must be between one and ten')
    frame=batch.bars
    if batch.market=='kr' and 'source' in frame:
        frame=frame[frame.source=='krx']
    available=sorted(set(frame.symbol))
    days=sorted(batch.completed_sessions)
    if not available or not days:
        return []
    preferred=['SPY','QQQ'] if batch.market=='us' else ['005930']
    selected=[s for s in preferred if s in available][:max_symbols]
    rest=[s for s in available if s not in selected]
    # A deterministic session-dependent rotation expands coverage on later runs.
    offset=sum(ord(c) for c in _day(days[-1])) % len(rest) if rest else 0
    selected+=(rest[offset:]+rest[:offset])[:max_symbols-len(selected)]
    kind='us_split' if batch.market=='us' else 'kr_kis_raw'
    source='alpaca:'+str(getattr(providers.alpaca,'feed','unknown'))+':split' if batch.market=='us' else 'kis:raw:unadjusted'
    observed=datetime.now(timezone.utc).isoformat()
    try:
        if batch.market=='us':
            evidence=providers.alpaca.fetch_bars('us',selected,days,adjustment='split')
        else:
            import pandas as pd
            samples=[]
            for symbol in selected:
                try:
                    value=providers.kis.fetch_stock_bars([symbol],days)
                    if not value.empty:samples.append(value)
                except Exception:pass
            evidence=pd.concat(samples,ignore_index=True) if samples else pd.DataFrame(columns=['symbol','session_date'])
        rows=[]
        for symbol in selected:
            sample=evidence[evidence.symbol==symbol].sort_values('session_date')
            if sample.empty:
                rows.append(dict(kind=kind,market=batch.market,symbol=symbol,session_date=_day(days[-1]),source=source,observed_at=observed,status='UNAVAILABLE'))
                continue
            anchor=sample.iloc[-1]
            for row in sample.to_dict('records'):
                item=dict(kind=kind,market=batch.market,symbol=symbol,session_date=_day(row['session_date']),
                          source=source,observed_at=observed,status='AVAILABLE')
                raw=frame[(frame.symbol==symbol)&(frame.session_date.map(_day)==item['session_date'])]
                if not raw.empty:
                    original=raw.iloc[-1]
                    item.update({f'raw_{field}_at_collection':_number(original.get(field)) for field in ('close','volume','turnover')})
                if batch.market=='us':
                    item.update(close=_number(row.get('close')),anchor_date=_day(anchor.session_date),anchor_close=_number(anchor.close))
                else:
                    item.update({field:_number(row.get(field)) for field in ('open','high','low','close','volume','turnover')})
                rows.append(item)
        return rows
    except Exception:
        return [dict(kind=kind,market=batch.market,symbol=symbol,session_date=_day(days[-1]),
                     source=source,observed_at=observed,status='UNAVAILABLE') for symbol in selected]
