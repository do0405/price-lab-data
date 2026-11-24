"""US split companion evidence and bounded KR comparisons; no DB dependency."""
from datetime import datetime, timezone
import math


def _day(value):
    return str(value)[:10]


def _number(value):
    if value is None:
        return None
    number=float(value)
    return number if math.isfinite(number) and number>=0 else None


def _collect_us(providers,batch,selected,days,observed,full):
    from .providers import ProviderError
    frame=batch.bars
    query=frame.attrs.get('alpaca_query') or batch.provenance.get('alpaca_query') or {}
    if query.get('adjustment','raw')!='raw':raise ProviderError('US split companion requires canonical raw bars')
    feed=str(query.get('feed',getattr(providers.alpaca,'feed','unknown')))
    source='alpaca:'+feed+':split'
    kwargs=dict(adjustment='split')
    if query.get('asof') is not None:kwargs['asof']=query['asof']
    if query.get('batch_size') is not None:kwargs['batch_size']=query['batch_size']
    coverage=dict(mode='full' if full else 'sampled',status='FAILED',incomplete=True,symbols=len(selected),observed_at=observed)
    batch.provenance['price_pair_coverage']=coverage
    try:
        if query.get('feed') is not None and query['feed']!=getattr(providers.alpaca,'feed',None):
            raise ProviderError('US split companion feed differs from raw query')
        evidence=providers.alpaca.fetch_bars('us',selected,days,**kwargs)
        split_query=evidence.attrs.get('alpaca_query') or {}
        for key in ('timeframe','start','end','feed','asof'):
            if key in query and key in split_query and query[key]!=split_query[key]:
                raise ProviderError('US split companion query differs from raw query')
        if split_query.get('adjustment','split')!='split':raise ProviderError('US split companion adjustment differs')
        if not evidence.empty and not {'symbol','session_date'}<=set(evidence.columns):
            raise ProviderError('US split companion keys missing')
        raw={};split={};wanted={_day(d) for d in days};selected_set=set(selected)
        for values,records in ((raw,frame.to_dict('records')),(split,evidence.to_dict('records'))):
            for record in records:
                if record['symbol'] not in selected_set:
                    if values is split:raise ProviderError('US split companion has unexpected symbol')
                    continue
                key=(record['symbol'],_day(record['session_date']))
                if key[1] not in wanted:raise ProviderError('US split companion has unexpected session')
                if key in values:raise ProviderError('US split companion has duplicate key')
                values[key]=record
        raw_keys=set(raw);split_keys=set(split);paired=raw_keys&split_keys
        no_keys=set(selected)-{s for s,d in raw_keys|split_keys}
        missing_raw=split_keys-raw_keys;missing_split=raw_keys-split_keys
        incomplete=bool(missing_raw or missing_split or no_keys)
        # Keep exact source query and row pairs for derived certification in the
        # unpublished ingest transaction. Missing scope stays honest evidence.
        batch.provenance['unit_pair_scopes']=dict(raw_scope=query,split_scope=split_query)
        batch.provenance['unit_pair_evidence']=[dict(raw={**raw[key],'session_date':key[1]},
            split={**split[key],'session_date':key[1]})
            for key in sorted(paired)]
        coverage.update(status='INCOMPLETE' if incomplete else 'COMPLETE',incomplete=incomplete,
            raw_keys=len(raw_keys),split_keys=len(split_keys),paired_keys=len(paired),
            missing_raw_keys=len(missing_raw),missing_split_keys=len(missing_split),empty_symbols=len(no_keys))
        anchors={}
        for symbol,day in sorted(paired):anchors[symbol]=split[(symbol,day)]
        rows=[]
        keys=raw_keys|split_keys|{(symbol,_day(days[-1])) for symbol in no_keys}
        for symbol,day in sorted(keys):
            item=dict(kind='us_split',market='us',symbol=symbol,session_date=day,source=source,
                observed_at=observed,status='AVAILABLE' if (symbol,day) in paired else 'UNAVAILABLE')
            original=raw.get((symbol,day))
            if original is not None:
                item.update({f'raw_{field}_at_collection':_number(original.get(field)) for field in ('close','volume','turnover')})
            if (symbol,day) in split:
                row=split[(symbol,day)]
                item.update({field:_number(row.get(field)) for field in ('open','high','low','close','volume','turnover','vwap')})
            if symbol in anchors:
                anchor=anchors[symbol]
                item.update(anchor_date=_day(anchor['session_date']),anchor_close=_number(anchor.get('close')))
            rows.append(item)
        return rows
    except Exception:
        # Companion evidence is optional for raw-price availability. Fail closed
        # on the adjusted leg while preserving every otherwise valid raw key.
        coverage.update(status='FAILED',incomplete=True,error_code='SPLIT_COMPANION_FAILED')
        if full:
            originals={};selected_set=set(selected);wanted={_day(d) for d in days}
            for original in frame.to_dict('records'):
                key=(original['symbol'],_day(original['session_date']))
                if key[0] in selected_set and key[1] in wanted:originals[key]=original
            no_keys=selected_set-{symbol for symbol,day in originals}
            coverage.update(raw_keys=len(originals),split_keys=0,paired_keys=0,missing_raw_keys=0,
                missing_split_keys=len(originals),empty_symbols=len(no_keys))
            keys=set(originals)|{(symbol,_day(days[-1])) for symbol in no_keys}
            rows=[]
            for symbol,day in sorted(keys):
                item=dict(kind='us_split',market='us',symbol=symbol,session_date=day,
                    source=source,observed_at=observed,status='UNAVAILABLE')
                original=originals.get((symbol,day))
                if original is not None:
                    item.update({f'raw_{field}_at_collection':_number(original.get(field)) for field in ('close','volume','turnover')})
                rows.append(item)
            return rows
        return [dict(kind='us_split',market='us',symbol=symbol,session_date=_day(days[-1]),
            source=source,observed_at=observed,status='UNAVAILABLE') for symbol in selected]


def collect_observations(providers, batch, *, max_symbols=None):
    """Acquire every US companion by default; explicit limits retain sampling.

    Companion failures retain UNAVAILABLE records and failed/incomplete coverage
    without withholding valid raw bars. Missing exact keys are also explicit.
    Bounded comparisons retain safe failure records. Available US observations
    carry a common local anchor for later comparison.
    """
    if batch.market not in ('us','kr'):
        return []
    full=batch.market=='us' and max_symbols is None
    query=(batch.bars.attrs.get('alpaca_query') or batch.provenance.get('alpaca_query') or {}) if full else {}
    if batch.bars.empty and not (full and query.get('symbols')):return []
    if max_symbols is None and not full:max_symbols=3
    if max_symbols is not None and (isinstance(max_symbols,bool) or not isinstance(max_symbols,int) or not 1<=max_symbols<=10):
        raise ValueError('Validation sample limit must be between one and ten')
    frame=batch.bars
    if batch.market=='kr' and 'source' in frame:
        frame=frame[frame.source=='krx']
    available=sorted(set(frame.symbol)) if 'symbol' in frame else []
    if full:
        available=sorted(set(available)|set(query.get('symbols',[])))
    days=sorted(batch.completed_sessions)
    if not available or not days:
        return []
    if full:selected=available
    else:
        preferred=['SPY','QQQ'] if batch.market=='us' else ['005930']
        selected=[s for s in preferred if s in available][:max_symbols]
        rest=[s for s in available if s not in selected]
        # A deterministic session-dependent rotation expands coverage on later runs.
        offset=sum(ord(c) for c in _day(days[-1])) % len(rest) if rest else 0
        selected+=(rest[offset:]+rest[:offset])[:max_symbols-len(selected)]
    observed=datetime.now(timezone.utc).isoformat()
    if batch.market=='us':return _collect_us(providers,batch,selected,days,observed,full)
    kind='kr_kis_raw';source='kis:raw:unadjusted'
    try:
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
            for row in sample.to_dict('records'):
                item=dict(kind=kind,market=batch.market,symbol=symbol,session_date=_day(row['session_date']),
                          source=source,observed_at=observed,status='AVAILABLE')
                raw=frame[(frame.symbol==symbol)&(frame.session_date.map(_day)==item['session_date'])]
                if not raw.empty:
                    original=raw.iloc[-1]
                    item.update({f'raw_{field}_at_collection':_number(original.get(field)) for field in ('close','volume','turnover')})
                item.update({field:_number(row.get(field)) for field in ('open','high','low','close','volume','turnover')})
                rows.append(item)
        return rows
    except Exception:
        return [dict(kind=kind,market=batch.market,symbol=symbol,session_date=_day(days[-1]),
                     source=source,observed_at=observed,status='UNAVAILABLE') for symbol in selected]
