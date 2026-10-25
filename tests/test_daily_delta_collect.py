import copy
from datetime import datetime,timezone
from pathlib import Path

import pandas as pd
import pytest

from price_lab.providers import BENCHMARKS,MarketBatch


NOW=datetime(2024,1,6,12,tzinfo=timezone.utc)


def bootstrap(days=None):
    days=days or ['2024-01-02','2024-01-04']
    assets=[dict(market=m,symbol=s,name=s,exchange='test') for m in ('us','kr','crypto') for s in BENCHMARKS[m]]
    assets.append(dict(market='us',symbol='OLD',name='Inactive retained',exchange='NYSE'))
    return dict(schema='price-delta-bootstrap-v1',anchor_sha256='a'*64,assets=assets,
        sessions={m:list(days) for m in ('us','kr','crypto')},watermarks={m:days[-1] for m in ('us','kr','crypto')},calendar_evidence={})


class Provider:
    def __init__(self,fail=None,partial=False):self.calls=[];self.fail=fail;self.partial=partial;self.calendar_evidence={};self.started=0
    def begin_refresh(self):self.started+=1
    def load_session_evidence(self,evidence):self.calendar_evidence=copy.deepcopy(evidence)
    def reconcile_sessions(self,market,all_dates,known_sessions):return all_dates
    def published_sessions(self,market,sessions,now=None):return sessions
    def fetch_market_part(self,market,sessions,assets,audit=False,action_start=None):
        self.calls.append((market,list(sessions),assets.copy(),action_start,audit))
        if market==self.fail:raise RuntimeError('private URL and API secret must not escape')
        rows=[]
        for day in sessions:
            for symbol in BENCHMARKS[market]:
                rows.append(dict(market=market,symbol=symbol,session_date=day,open=10.,high=11.,low=9.,close=10.,
                    volume=100.,turnover=1000.,vwap=10.,currency='KRW' if market=='kr' else 'USD',source='kis_etf_raw' if market=='kr' else 'test'))
        active=assets.copy()
        if market=='us':active=pd.concat([active,pd.DataFrame([dict(market='us',symbol='NEW',name='New',exchange='NYSE')])],ignore_index=True)
        done=sessions[:-1] if self.partial else sessions
        return MarketBatch(market,pd.DataFrame(rows),active,[],done,{'actions_complete':True,'benchmarks_complete':True})


def calendar(monkeypatch,days=None):
    from price_lab import daily_delta_collect as c
    wanted=pd.to_datetime(days or ['2024-01-02','2024-01-03','2024-01-04','2024-01-05'])
    monkeypatch.setattr(c,'completed_sessions',lambda market,start,now=None:list(wanted[wanted>=pd.Timestamp(start)]))
    return c


def test_missing_sessions_overlap_assets_and_chain_are_db_free(monkeypatch):
    import requests
    c=calendar(monkeypatch);p=Provider();original=bootstrap();before=copy.deepcopy(original)
    monkeypatch.setattr(requests.Session,'request',lambda *a,**k:pytest.fail('uninjected network used'))
    first=c.collect_delta(p,bootstrap=original,now=NOW,overlap_sessions=2)
    assert original==before and p.started==1
    assert {str(d.date()) for _,days,_,_,_ in p.calls for d in days}=={'2024-01-03','2024-01-04','2024-01-05'}
    assert {'OLD','NEW'}<={a['symbol'] for a in first['state']['assets'] if a['market']=='us'}
    assert all('OLD' in assets.symbol.values for m,_,assets,_,_ in p.calls if m=='us')
    assert first['parts'][1]['bars'][0]['is_supplement'] is True
    second=c.collect_delta(Provider(),prior_payload=first,now=NOW,overlap_sessions=1)
    from price_lab.daily_delta_codec import package_digest
    assert second['sequence']==2 and second['previous_package_sha256']==package_digest(first)


def test_holiday_reconciliation_and_kr_publication_delay_preserve_known_sessions(monkeypatch):
    c=calendar(monkeypatch)
    class OfficialCalendar(Provider):
        def reconcile_sessions(self,market,days,known):
            if market=='kr':
                self.calendar_evidence={'kr':{'2024-01-03':{'open':False,'provider':'kis'}}}
                return [d for d in days if str(d.date())!='2024-01-03']
            return days
        def published_sessions(self,market,days,now=None):return days[:-1] if market=='kr' else days
    out=c.collect_delta(OfficialCalendar(),bootstrap=bootstrap(),now=NOW,overlap_sessions=1)
    assert out['state']['sessions']['kr']==['2024-01-02','2024-01-04']
    assert out['state']['watermarks']['kr']=='2024-01-04'
    assert out['state']['calendar_evidence']['kr']['2024-01-03']['open'] is False


def test_action_query_starts_at_day_after_preceding_session(monkeypatch):
    c=calendar(monkeypatch,['2024-01-05','2024-01-08']);p=Provider()
    c.collect_delta(p,bootstrap=bootstrap(['2024-01-05']),now=datetime(2024,1,9,12,tzinfo=timezone.utc),overlap_sessions=1)
    assert all(str(start.date())=='2024-01-06' for _,_,_,start,_ in p.calls)


@pytest.mark.parametrize('audit',[False,True])
def test_known_unpublished_kr_session_is_preserved_but_not_refetched(monkeypatch,capsys,audit):
    c=calendar(monkeypatch)
    class PublicationCalendar(Provider):
        def published_sessions(self,market,days,now=None):
            return days[:-1] if market=='kr' and pd.Timestamp(now).hour<13 else days
    initial=c.collect_delta(Provider(),bootstrap=bootstrap(),now=NOW)
    before=copy.deepcopy(initial);provider=PublicationCalendar();references=[]
    out=c.collect_delta(provider,prior_payload=initial,now=NOW,audit=audit,overlap_sessions=1,
        reference_fetch=lambda board,day,kind:references.append(day) or {})
    kr_calls=[call for call in provider.calls if call[0]=='kr']
    assert all(pd.Timestamp('2024-01-05') not in call[1] for call in kr_calls)
    assert out['state']['sessions']['kr']==initial['state']['sessions']['kr']
    assert out['state']['watermarks']['kr']==initial['state']['watermarks']['kr']
    assert initial==before
    assert set(references)=={'2024-01-02','2024-01-03','2024-01-04'}
    event=__import__('json').loads(capsys.readouterr().out)
    assert event==dict(event='collection_deferred',market='kr',reason='PUBLICATION_WINDOW',
        session_count=1,first_session='2024-01-05',last_session='2024-01-05')
    assert all(str(row['session_date'])!='2024-01-05' for part in out['parts'] if part['market']=='kr' for row in part['bars'])
    later=PublicationCalendar()
    c.collect_delta(later,prior_payload=out,now=NOW+pd.Timedelta(hours=1),overlap_sessions=1)
    assert any(pd.Timestamp('2024-01-05') in call[1] for call in later.calls if call[0]=='kr')
    if audit:assert out['state']['audit_cursor']['kr']==3


def test_pending_kr_session_does_not_prevent_other_markets_or_reference_filter(monkeypatch):
    c=calendar(monkeypatch);references=[]
    class PendingKorea(Provider):
        def published_sessions(self,market,days,now=None):return [] if market=='kr' else days
    provider=PendingKorea();initial=bootstrap()
    out=c.collect_delta(provider,bootstrap=initial,now=NOW,audit=True,
        reference_fetch=lambda board,day,kind:references.append(day) or {})
    assert not any(call[0]=='kr' for call in provider.calls) and references==[]
    assert {part['market'] for part in out['parts']}=={'us','crypto'}
    assert out['state']['sessions']['kr']==initial['sessions']['kr']
    assert out['state']['audit_cursor']['kr']==0


@pytest.mark.parametrize('partial',[False,True])
def test_any_required_market_failure_prevents_payload_and_state_advance(monkeypatch,partial):
    c=calendar(monkeypatch);p=Provider(fail=None if partial else 'kr',partial=partial);initial=bootstrap();before=copy.deepcopy(initial)
    with pytest.raises(ValueError,match='Required market collection failed') as error:c.collect_delta(p,bootstrap=initial,now=NOW)
    assert initial==before and 'secret' not in str(error.value) and 'URL' not in str(error.value)


def test_failure_reports_fixed_stage_without_upstream_contents(monkeypatch):
    c=calendar(monkeypatch)
    with pytest.raises(ValueError,match='market=kr; stage=fetch; type=other') as error:
        c.collect_delta(Provider(fail='kr'),bootstrap=bootstrap(),now=NOW)
    assert 'private' not in str(error.value) and 'secret' not in str(error.value)


def test_catchup_and_audit_are_bounded_to_twenty_sessions_per_part(monkeypatch):
    days=[str(d.date()) for d in pd.date_range('2024-01-01',periods=100)]
    c=calendar(monkeypatch,days);p=Provider();now=datetime(2024,4,11,12,tzinfo=timezone.utc)
    first=c.collect_delta(p,bootstrap=bootstrap(days),now=now,overlap_sessions=1,audit=True)
    for market in ('us','kr','crypto'):
        calls=[x for x in p.calls if x[0]==market]
        assert all(len(x[1])<=20 for x in calls)
        assert len({d for x in calls for d in x[1]})<=21
        assert first['state']['audit_cursor'][market]==20
    p=Provider();second=c.collect_delta(p,prior_payload=first,now=now,overlap_sessions=1,audit=True)
    assert second['state']['audit_cursor']=={m:40 for m in ('us','kr','crypto')}
    p=Provider();c.collect_delta(p,bootstrap=bootstrap(days[:1]),now=now)
    assert all(len(x[1])<=20 for x in p.calls) and len(p.calls)==15


def test_reference_failure_isolated_and_bounded_without_invented_availability(monkeypatch):
    c=calendar(monkeypatch);calls=[]
    def reference(board,date,kind):
        calls.append((board,date,kind))
        if board=='KOSDAQ' and kind=='master':raise RuntimeError('secret')
        return {'OutBlock_1':[{'BAS_DD':date.replace('-','')}]}
    out=c.collect_delta(Provider(),bootstrap=bootstrap(),now=NOW,reference_fetch=reference,reference_sessions=2)
    assert len(calls)==8 and len(out['reference_raw'])==6 and len(out['parts'])==3
    assert out['reference_errors']==['KR_REFERENCE_COLLECTION_FAILED']
    assert all(set(r)=={'board','requested_date','kind','payload','retrieved_at'} for r in out['reference_raw'])


def test_future_provider_session_is_rejected_before_price_fetch(monkeypatch):
    c=calendar(monkeypatch,['2024-01-02','2024-01-07']);p=Provider()
    with pytest.raises(ValueError):c.collect_delta(p,bootstrap=bootstrap(),now=NOW)
    assert p.calls==[]


def test_unsafe_reference_payload_does_not_discard_valid_prices(monkeypatch):
    c=calendar(monkeypatch)
    out=c.collect_delta(Provider(),bootstrap=bootstrap(),now=NOW,
        reference_fetch=lambda *a:{'AUTH_KEY':'must not be serialized'})
    assert out['parts'] and not out['reference_raw']
    assert out['reference_errors']==['KR_REFERENCE_COLLECTION_FAILED']


def test_collection_payload_budget_stops_without_state_advance(monkeypatch):
    c=calendar(monkeypatch);p=Provider();original=bootstrap();before=copy.deepcopy(original)
    monkeypatch.setattr(c,'MAX_RAW_BYTES',2048,raising=False)
    with pytest.raises(ValueError,match='Required market collection failed'):c.collect_delta(p,bootstrap=original,now=NOW)
    assert original==before and len(p.calls)<3


def test_reference_size_budget_preserves_price_parts(monkeypatch):
    import json
    c=calendar(monkeypatch);monkeypatch.setattr(c,'MAX_RAW_BYTES',20_000)
    out=c.collect_delta(Provider(),bootstrap=bootstrap(),now=NOW,
        reference_fetch=lambda *a:{'rows':['x'*10_000]})
    assert len(out['parts'])==3 and len(out['reference_raw'])<=1
    assert out['reference_errors']==['KR_REFERENCE_COLLECTION_FAILED']
    assert len(json.dumps(out,separators=(',',':')).encode())<=20_000


def test_collector_imports_no_database_modules():
    import subprocess,sys,os
    code="import sys; import price_lab.daily_delta_collect; assert not any(k in sys.modules for k in ('duckdb','price_lab.ingestion','price_lab.cloud_data'))"
    result=subprocess.run([sys.executable,'-c',code],env={**os.environ,'PYTHONPATH':str(Path(__file__).resolve().parents[1]/'src')},capture_output=True,text=True)
    assert result.returncode==0,result.stderr


def test_ordinary_halted_ohlc_is_not_repaired_or_dropped_by_collector(monkeypatch):
    c=calendar(monkeypatch)
    class RawEvidence(Provider):
        def fetch_market_part(self,market,sessions,assets,**kwargs):
            batch=super().fetch_market_part(market,sessions,assets,**kwargs)
            if market=='us':
                ordinary=batch.bars.iloc[0].copy()
                ordinary.loc[['symbol','open','high','low','close','volume','turnover','vwap']]=['HALTED',0.,0.,0.,100.,0.,0.,0.]
                batch.bars=pd.concat([batch.bars,pd.DataFrame([ordinary])],ignore_index=True)
            return batch
    value=c.collect_delta(RawEvidence(),bootstrap=bootstrap(),now=NOW)
    ordinary=[row for part in value['parts'] for row in part['bars'] if row['symbol']=='HALTED']
    assert len(ordinary)==1
    assert tuple(ordinary[0][key] for key in ('open','high','low','close','volume','turnover','vwap'))==(0.,0.,0.,100.,0.,0.,0.)


@pytest.mark.parametrize('market',['us','kr','crypto'])
@pytest.mark.parametrize('invalid',['zero_open','high_below','low_above'])
def test_invalid_required_benchmark_rejects_whole_collection(monkeypatch,market,invalid):
    c=calendar(monkeypatch)
    class BadBenchmark(Provider):
        def fetch_market_part(self,selected,sessions,assets,**kwargs):
            batch=super().fetch_market_part(selected,sessions,assets,**kwargs)
            if selected==market:
                symbol=BENCHMARKS[market][0];mask=batch.bars.symbol==symbol
                if invalid=='zero_open':batch.bars.loc[mask,'open']=0.
                elif invalid=='high_below':batch.bars.loc[mask,'high']=9.5
                else:batch.bars.loc[mask,'low']=10.5
            return batch
    initial=bootstrap();before=copy.deepcopy(initial)
    with pytest.raises(ValueError,match='Required market collection failed'):
        c.collect_delta(BadBenchmark(),bootstrap=initial,now=NOW)
    assert initial==before




@pytest.mark.parametrize('instant,ready',[
    ('2026-10-24T10:00:00+09:00',False),
    ('2026-10-25T20:37:00+09:00',False),
    ('2026-10-26T00:00:58+09:00',False),
    ('2026-10-26T08:14:59+09:00',False),
    ('2026-10-26T08:15:00+09:00',True),
    ('2026-10-25T23:15:00Z',True),
])
def test_kr_friday_collection_waits_for_monday_publication(instant,ready):
    from price_lab import providers as p
    provider=p.Providers(alpaca=object(),krx=object(),kis=object())
    days=[pd.Timestamp('2026-10-22'),pd.Timestamp('2026-10-23')]
    assert provider.published_sessions('kr',days,now=instant)==(days if ready else days[:1])


@pytest.mark.parametrize('day,before,after',[
    ('2026-10-08','2026-10-12T08:14:59+09:00','2026-10-12T08:15:00+09:00'),
    ('2024-09-13','2024-09-19T08:14:59+09:00','2024-09-19T08:15:00+09:00'),
    ('2026-12-30','2027-01-04T08:14:59+09:00','2027-01-04T08:15:00+09:00'),
])
def test_kr_publication_skips_holidays_and_year_end_closure(day,before,after):
    from price_lab import providers as p
    provider=p.Providers(alpaca=object(),krx=object(),kis=object());days=[pd.Timestamp(day)]
    assert provider.published_sessions('kr',days,now=before)==[]
    assert provider.published_sessions('kr',days,now=after)==days


def test_kr_publication_respects_recorded_official_extra_closure():
    from price_lab import providers as p
    provider=p.Providers(alpaca=object(),krx=object(),kis=object())
    provider.calendar_evidence={'kr':{'2026-10-26':{'open':False,'provider':'kis'}}}
    days=[pd.Timestamp('2026-10-23')]
    assert provider.published_sessions('kr',days,now='2026-10-26T20:37:00+09:00')==[]
    assert provider.published_sessions('kr',days,now='2026-10-27T08:15:00+09:00')==days
