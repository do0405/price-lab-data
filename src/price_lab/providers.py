"""Price-only provider adapters. Credentials stay in memory, never in artifacts.

Alpaca official contracts: docs.alpaca.markets/us/reference/stockbars and
corporateactions-1. KRX/KIS fields follow their raw daily/bulk KSD contracts.
KSD record_date is a record date, NOT an execution/ex-date. Unconfirmed nominal
changes are observations for quarantine; they are never inferred adjustments.
All HTTP dependencies are injectable; importing this module performs no I/O.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import time
from urllib.parse import urlsplit
import pandas as pd
import requests

MARKETS=('us','kr','crypto')
BENCHMARKS={'us':('SPY','QQQ'),'kr':('069500','229200'),'crypto':('BTC/USD',)}


class ProviderError(RuntimeError):pass


@dataclass
class MarketBatch:
    market:str
    bars:pd.DataFrame
    assets:pd.DataFrame
    actions:list
    completed_sessions:list
    provenance:dict


def _canonical(value):return json.dumps(value,sort_keys=True,ensure_ascii=False,default=str,separators=(',',':'),allow_nan=False)
def _revision(value):return hashlib.sha256(_canonical(value).encode()).hexdigest()
def _day(value):return str(pd.Timestamp(value).date())
def _number(value):
    try:
        n=float(str(value).replace(',',''))
        if not math.isfinite(n):raise ValueError()
        return n
    except (TypeError,ValueError,OverflowError):raise ProviderError('Invalid provider numeric field') from None


def completed_sessions(market,start,now=None,close_delay_minutes=15):
    """Exchange-calendar closes plus publication delay; crypto days are UTC."""
    if market not in MARKETS:raise ValueError('Unknown market')
    instant=pd.Timestamp(now if now is not None else datetime.now(timezone.utc))
    if instant.tzinfo is None:raise ValueError('now must be timezone-aware')
    instant=instant.tz_convert('UTC');first=pd.Timestamp(start).tz_localize(None).normalize()
    if first>instant.tz_localize(None).normalize():return []
    if market=='crypto':
        last=instant.tz_localize(None).normalize()-pd.Timedelta(days=1)
        return list(pd.date_range(first,last)) if first<=last else []
    import exchange_calendars as xcals
    cal=xcals.get_calendar('XNYS' if market=='us' else 'XKRX',start=first-pd.Timedelta(days=7),end=instant.date()+pd.Timedelta(days=7))
    schedule=cal.schedule
    closes=pd.to_datetime(schedule['close'],utc=True)+pd.Timedelta(minutes=close_delay_minutes)
    labels=pd.DatetimeIndex(schedule.index).tz_localize(None).normalize()
    return list(labels[(labels>=first)&(closes<=instant).to_numpy()])


class HttpClient:
    """Bounded retry/rate limits. Errors contain status/path, never body/headers."""
    def __init__(self,session=None,retries=4,min_interval=.25,sleep=time.sleep,clock=time.monotonic,raw_dir=None):
        self.session=session or requests.Session();self.retries=retries;self.min_interval=min_interval
        self.sleep=sleep;self.clock=clock;self.last=None;self.raw_dir=Path(raw_dir) if raw_dir else None
    def request_json(self,method,url,headers=None,params=None,body=None,private=False):
        endpoint=urlsplit(url)
        if endpoint.scheme!='https':raise ProviderError('HTTPS provider endpoint required')
        for attempt in range(self.retries+1):
            if self.last is not None:
                delay=self.min_interval-(self.clock()-self.last)
                if delay>0:self.sleep(delay)
            self.last=self.clock()
            try:
                response=self.session.request(method,url,headers=headers,params=dict(params or {}),json=body,timeout=(15,90),allow_redirects=False)
            except requests.RequestException:
                if attempt==self.retries:raise ProviderError('Provider transport exhausted retries') from None
                self.sleep(min(30,2**attempt));continue
            code=response.status_code
            if code==429 or 500<=code<600:
                if attempt==self.retries:raise ProviderError(f'Provider HTTP {code}: retry budget exhausted')
                try:delay=min(60,max(1,float(response.headers.get('Retry-After',2**attempt))))
                except (ValueError,TypeError):delay=min(30,2**attempt)
                self.sleep(delay);continue
            if code!=200:raise ProviderError(f'Provider HTTP {code}: {endpoint.path}')
            try:payload=response.json()
            except (ValueError,TypeError):raise ProviderError('Provider returned invalid JSON') from None
            if not isinstance(payload,(dict,list)):raise ProviderError('Provider JSON shape invalid')
            if self.raw_dir and not private:
                self.raw_dir.mkdir(parents=True,exist_ok=True)
                name=_revision({'endpoint':url,'params':params or {},'at':time.time_ns()})
                with gzip.open(self.raw_dir/(name+'.json.gz'),'wt',encoding='utf-8') as stream:stream.write(_canonical(payload))
            return payload
        raise ProviderError('Provider retry exhausted')


def parse_alpaca_action(group,record):
    kind='split' if group in ('forward_splits','reverse_splits') else 'cash_distribution' if group in ('cash_dividends','capital_gains_distributions') else 'unknown'
    symbol=record.get('symbol') or record.get('old_symbol') or record.get('source_symbol') or record.get('acquiree_symbol')
    result={'provider':'alpaca','event_id':str(record.get('id','')),'revision':_revision(record),'market':'us','symbol':symbol,
            'effective_date':record.get('ex_date') or record.get('effective_date') or record.get('process_date'),
            'kind':kind,'confirmed':False,'raw':record,'event_group':group}
    if kind=='split':
        old,new=_number(record.get('old_rate')),_number(record.get('new_rate'))
        if old<=0 or new<=0:raise ProviderError('Invalid split ratio')
        result.update(price_factor=old/new,share_factor=new/old,confirmed=bool(record.get('ex_date') and symbol and result['event_id']))
    elif kind=='cash_distribution':result['confirmed']=True
    return result


def parse_ksd_action(record):
    symbol=str(record.get('sht_cd','')).zfill(6);date=str(record.get('record_date',''))
    if len(date)!=8:raise ProviderError('KSD record date invalid')
    record_date=_day(pd.to_datetime(date,format='%Y%m%d'));listing=''.join(x for x in str(record.get('list_dt','')) if x.isdigit())
    effective=_day(pd.to_datetime(listing,format='%Y%m%d')) if len(listing)==8 else None
    before,after=_number(record.get('inter_bf_face_amt')),_number(record.get('inter_af_face_amt'))
    kind='split' if before>0 and after>0 and before!=after else 'no_change' if before==after and before>0 else 'unknown'
    return {'provider':'kis_ksd','event_id':f'{symbol}:{date}','revision':_revision(record),'market':'kr','symbol':symbol,
            'record_date':record_date,'effective_date':effective,'kind':kind,'confirmed':False,
            'price_factor':after/before if before>0 and after>0 else None,'share_factor':before/after if before>0 and after>0 else None,'raw':record}


def reconcile_ksd_actions(face_records,capital_records):
    """Original share-unit equation, with components deduplicated economically.

    Relisting dates are observations awaiting independent KRX confirmation.
    Capital allocation rates are already multipliers, never percentages.
    """
    groups={};listings={}
    for r in [*face_records,*capital_records]:
        key=(str(r.get('sht_cd','')).zfill(6),str(r.get('record_date','')))
        listing=''.join(c for c in str(r.get('list_dt','')) if c.isdigit())
        if len(listing)==8:listings.setdefault(key,set()).add(listing)
    for kind,records in [('face',face_records),('capital',capital_records)]:
        for record in records:
            symbol=str(record.get('sht_cd','')).zfill(6);record_date=str(record.get('record_date',''))
            if len(record_date)!=8 or not record_date.isdigit():raise ProviderError('KSD record date invalid')
            _day(pd.to_datetime(record_date,format='%Y%m%d'))
            listing=''.join(c for c in str(record.get('list_dt','')) if c.isdigit())
            known=listings.get((symbol,record_date),set())
            if len(listing)!=8:listing=next(iter(known)) if len(known)==1 else None
            effective=_day(pd.to_datetime(listing,format='%Y%m%d')) if listing else None
            group=groups.setdefault((symbol,effective or 'record:'+record_date),{'face':[],'capital':[]})
            if record not in group[kind]:group[kind].append(record)
    result=[]
    for (symbol,label),raw in sorted(groups.items()):
        face=survival=1.;valid=True
        seen=set()
        for r in raw['face']:
            try:
                before,after=_number(r.get('inter_bf_face_amt')),_number(r.get('inter_af_face_amt'))
                key=(before,after)
                if key in seen:continue
                seen.add(key)
                if before<=0 or after<=0:valid=False
                else:face*=after/before
            except ProviderError:valid=False
        seen=set()
        for r in raw['capital']:
            try:
                rate=_number(r.get('reduce_cap_rate'));way=str(r.get('comp_way','')).strip();key=(way,rate)
                if key in seen:continue
                seen.add(key)
                if rate<=0 or way not in ('곱하기','나누기'):valid=False
                else:survival*=rate if way=='곱하기' else 1/rate
            except ProviderError:valid=False
        records=raw['face']+raw['capital'];dates=sorted({str(r['record_date']) for r in records})
        halts={r.get('td_stop_dt') for r in records if r.get('td_stop_dt')}
        if len(halts)==1:raw['td_stop_dt']=next(iter(halts))
        factor=face/survival if valid and math.isfinite(face) and math.isfinite(survival) and face>0 and survival>0 else None
        if factor is not None and (not math.isfinite(factor) or factor<=0 or not math.isfinite(1/factor)):factor=None
        result.append(dict(provider='kis_ksd',event_id=f'{symbol}:{dates[0]}',revision=_revision(raw),market='kr',symbol=symbol,
            record_date=_day(pd.to_datetime(dates[0],format='%Y%m%d')),effective_date=None if label.startswith('record:') else label,
            kind='unknown' if factor is None else 'no_change' if factor==1 else 'split',confirmed=False,
            price_factor=factor,share_factor=1/factor if factor else None,raw=raw))
    return result


class AlpacaProvider:
    def __init__(self,key=None,secret=None,client=None,feed='sip',trading_base='https://paper-api.alpaca.markets'):
        self._key=key if key is not None else os.environ.get('ALPACA_API_KEY')
        self._secret=secret if secret is not None else os.environ.get('ALPACA_API_SECRET')
        self.client=client or HttpClient();self.feed=feed;self.trading_base=trading_base;self._asset_cache={}
    def _headers(self):
        if not self._key or not self._secret:raise ProviderError('Missing Alpaca credentials')
        return {'APCA-API-KEY-ID':self._key,'APCA-API-SECRET-KEY':self._secret}
    def fetch_assets(self,market):
        if market not in ('us','crypto'):raise ProviderError('Invalid Alpaca asset market')
        if market in self._asset_cache:return self._asset_cache[market].copy()
        values=self.client.request_json('GET',self.trading_base+'/v2/assets',headers=self._headers(),params={'status':'active','asset_class':'us_equity' if market=='us' else 'crypto'})
        if not isinstance(values,list) or not values:raise ProviderError('Alpaca empty active asset response')
        allowed={'NYSE','NASDAQ','AMEX','ARCA','NYSE_ARCA','BATS'}
        observed=datetime.now(timezone.utc).isoformat()
        rows=[{'market':market,'symbol':r['symbol'],'name':r.get('name') or r['symbol'],'exchange':r.get('exchange') or 'CRYPTO',
               'provider_asset_id':r.get('id'),'provider_asset_class':r.get('class'),'active_at_collection':r.get('status')=='active',
               'broker_tradable':r.get('tradable'),'universe_source':'alpaca_active_assets','observed_at':observed} for r in values
              if r.get('status')=='active' and (market=='crypto' or r.get('exchange') in allowed)]
        if not rows:raise ProviderError('Alpaca supported universe empty')
        frame=pd.DataFrame(rows);self._asset_cache[market]=frame
        return frame.copy()
    def _pages(self,path,params,field):
        token=None;seen=set()
        for _ in range(100000):
            query={**params,**({'page_token':token} if token else {})}
            payload=self.client.request_json('GET','https://data.alpaca.markets'+path,headers=self._headers(),params=query)
            if field not in payload:raise ProviderError('Missing Alpaca '+field)
            yield payload[field]
            token=payload.get('next_page_token')
            if not token:return
            if not isinstance(token,str) or token in seen:raise ProviderError('Repeated or invalid page token')
            seen.add(token)
        raise ProviderError('Alpaca page limit exceeded')
    def fetch_bars(self,market,symbols,sessions,*,batch_size=100,adjustment='raw'):
        if market not in ('us','crypto') or not sessions:raise ProviderError('Invalid Alpaca bar request')
        if adjustment not in ('raw','split') or (market=='crypto' and adjustment!='raw'):raise ValueError('Unsupported Alpaca adjustment')
        if isinstance(batch_size,bool) or not isinstance(batch_size,int) or not 1<=batch_size<=500:raise ProviderError('Invalid Alpaca symbol batch size')
        wanted=set(pd.DatetimeIndex(sessions).normalize());rows=[]
        for offset in range(0,len(symbols),batch_size):
            chunk=list(symbols[offset:offset+batch_size]);params={'symbols':','.join(chunk),'timeframe':'1Day','start':_day(min(wanted)),
                'end':_day(max(wanted)+pd.Timedelta(days=1))+'T00:00:00Z','limit':10000,'sort':'asc'}
            if market=='us':params.update(feed=self.feed,adjustment=adjustment)
            path='/v2/stocks/bars' if market=='us' else '/v1beta3/crypto/us/bars'
            for page in self._pages(path,params,'bars'):
                if not isinstance(page,dict):raise ProviderError('Alpaca bars shape invalid')
                for symbol,bars in page.items():
                    if symbol not in chunk or not isinstance(bars,list):raise ProviderError('Unexpected Alpaca symbol/page')
                    for record in bars:
                        stamp=pd.Timestamp(record['t'])
                        if stamp.tzinfo is None:raise ProviderError('Alpaca timestamp requires timezone')
                        date=stamp.tz_convert('America/New_York' if market=='us' else 'UTC').tz_localize(None).normalize()
                        if date not in wanted:continue
                        volume=_number(record['v']);vwap=_number(record['vw']) if record.get('vw') is not None else None
                        if volume>0 and vwap is None:raise ProviderError('Alpaca positive-volume VWAP missing')
                        rows.append(dict(market=market,symbol=symbol,session_date=date,**{k:_number(record[v]) for k,v in [('open','o'),('high','h'),('low','l'),('close','c')]},
                                         volume=volume,turnover=volume*vwap if vwap is not None else 0.,vwap=vwap,currency='USD' if market=='us' else symbol.rsplit('/',1)[-1],source='alpaca',
                                         trade_count=record.get('n'),feed=self.feed if market=='us' else 'us',turnover_method='provider_vwap_times_volume',vwap_method='provider',
                                         retrieved_at=datetime.now(timezone.utc).isoformat(),is_tradable_bar=bool(volume>0 and all(_number(record[c])>0 for c in ('o','h','l','c')))))
        frame=pd.DataFrame(rows)
        if frame.empty:raise ProviderError('Alpaca empty completed-session bars')
        if frame.duplicated(['symbol','session_date']).any():raise ProviderError('Duplicate Alpaca bar key')
        frame['trade_count']=pd.array(frame.trade_count,dtype='Int64')
        return frame
    def fetch_actions(self,start,end):
        result=[]
        for groups in self._pages('/v1/corporate-actions',{'start':_day(start),'end':_day(end),'types':'forward_split,reverse_split','limit':1000,'sort':'asc','data_quality':'complete'},'corporate_actions'):
            if not isinstance(groups,dict):raise ProviderError('Alpaca actions shape invalid')
            for group,records in groups.items():
                if not isinstance(records,list):raise ProviderError('Alpaca action group invalid')
                result.extend(parse_alpaca_action(group,r) for r in records)
        return result


class KrxProvider:
    def __init__(self,key=None,client=None):self._key=key if key is not None else os.environ.get('KRX_API_KEY');self.client=client or HttpClient(min_interval=1.)
    def fetch_day(self,date):
        if not self._key:raise ProviderError('Missing KRX credentials')
        rows=[];assets=[];label=pd.Timestamp(date).normalize();day=label.strftime('%Y%m%d')
        for board,endpoint in [('KOSPI','stk_bydd_trd'),('KOSDAQ','ksq_bydd_trd')]:
            payload=self.client.request_json('GET','https://data-dbg.krx.co.kr/svc/apis/sto/'+endpoint,headers={'AUTH_KEY':self._key},params={'basDd':day})
            records=payload.get('OutBlock_1')
            if not isinstance(records,list) or not records:raise ProviderError('KRX empty or missing board response')
            for record in records:
                if str(record.get('BAS_DD'))!=day:raise ProviderError('KRX requested date mismatch')
                symbol=str(record['ISU_CD']).zfill(6)
                values={k:_number(record[v]) for k,v in [('open','TDD_OPNPRC'),('high','TDD_HGPRC'),('low','TDD_LWPRC'),('close','TDD_CLSPRC'),('volume','ACC_TRDVOL'),('turnover','ACC_TRDVAL')]}
                # Suspended zero-price rows stay raw and are quarantined by ingestion.
                observed=datetime.now(timezone.utc).isoformat()
                rows.append(dict(market='kr',symbol=symbol,session_date=label,**values,vwap=values['turnover']/values['volume'] if values['volume'] else None,currency='KRW',source='krx',
                                 reference_previous_close=values['close']-_number(record.get('CMPPREVDD_PRC',0)),turnover_method='exact_krx_ACC_TRDVAL',vwap_method='turnover_div_volume',
                                 exchange=board,retrieved_at=observed,is_tradable_bar=bool(values['volume']>0 and all(values[c]>0 for c in ('open','high','low','close')))))
                assets.append(dict(market='kr',symbol=symbol,name=record.get('ISU_NM',symbol),exchange=board,universe_source='krx_daily_board',observed_at=observed))
        frame=pd.DataFrame(rows)
        if frame.duplicated(['symbol','session_date']).any():raise ProviderError('Duplicate KRX board key')
        return frame,pd.DataFrame(assets)


class KisProvider:
    BASE='https://openapi.koreainvestment.com:9443'
    def __init__(self,key=None,secret=None,client=None):
        self._key=key if key is not None else os.environ.get('KIS_APP_KEY');self._secret=secret if secret is not None else os.environ.get('KIS_APP_SECRET')
        self.client=client or HttpClient(min_interval=.2);self._token=None;self._calendar={}
    def _headers(self,tr_id):
        if not self._key or not self._secret:raise ProviderError('Missing KIS credentials')
        if self._token is None:
            response=self.client.request_json('POST',self.BASE+'/oauth2/tokenP',body={'grant_type':'client_credentials','appkey':self._key,'appsecret':self._secret},private=True)
            self._token=response.get('access_token')
            if not isinstance(self._token,str):raise ProviderError('KIS token unavailable')
        return {'authorization':'Bearer '+self._token,'appkey':self._key,'appsecret':self._secret,'tr_id':tr_id,'custtype':'P'}
    def _get(self,path,tr_id,params):
        value=self.client.request_json('GET',self.BASE+path,headers=self._headers(tr_id),params=params)
        if value.get('rt_cd')!='0':raise ProviderError('KIS unsuccessful response')
        return value
    def session_status(self,dates):
        """Reuse every returned day; never infer closure from an empty response."""
        wanted=sorted({_day(d) for d in dates})
        for date in wanted:
            if date in self._calendar:continue
            value=self._get('/uapi/domestic-stock/v1/quotations/chk-holiday','CTCA0903R',{'BASS_DT':date.replace('-',''),'CTX_AREA_FK':'','CTX_AREA_NK':''})
            records=value.get('output')
            if isinstance(records,dict):records=[records] if records else []
            if not isinstance(records,list) or not records:raise ProviderError('KIS calendar response empty')
            received={}
            for record in records:
                label=str(record.get('bass_dt',''));flag=record.get('opnd_yn')
                if len(label)!=8 or not label.isdigit() or flag not in ('Y','N'):raise ProviderError('KIS calendar row invalid')
                day=_day(pd.to_datetime(label,format='%Y%m%d'));opened=flag=='Y'
                if day in received and received[day]!=opened:raise ProviderError('KIS calendar conflicting rows')
                received[day]=opened
            if date not in received:raise ProviderError('KIS calendar requested day missing')
            self._calendar.update(received)
        return {day:self._calendar[day] for day in wanted}
    def fetch_benchmarks(self,sessions):
        frame=self.fetch_stock_bars(BENCHMARKS['kr'],sessions)
        if not frame.empty:frame['source']='kis_etf_raw';frame['is_supplement']=True
        return frame
    def fetch_stock_bars(self,symbols,sessions):
        """Bounded raw KIS stock chart crosschecks, never adjusted by default."""
        if not sessions:return pd.DataFrame()
        rows=[];wanted=set(pd.DatetimeIndex(sessions).normalize())
        for symbol in symbols:
            # The chart endpoint returns at most100 rows: bounded90-calendar-day windows.
            start=min(wanted);last=max(wanted)
            while start<=last:
                end=min(last,start+pd.Timedelta(days=89))
                response=self._get('/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice','FHKST03010100',{'FID_COND_MRKT_DIV_CODE':'J','FID_INPUT_ISCD':symbol,'FID_INPUT_DATE_1':start.strftime('%Y%m%d'),'FID_INPUT_DATE_2':end.strftime('%Y%m%d'),'FID_PERIOD_DIV_CODE':'D','FID_ORG_ADJ_PRC':'1'})
                records=response.get('output2')
                if not isinstance(records,list) or not records:raise ProviderError('KIS empty benchmark chart')
                if len(records)>=100:raise ProviderError('KIS raw chart may be truncated')
                for r in records:
                    day=pd.to_datetime(r.get('stck_bsop_date'),format='%Y%m%d',errors='raise')
                    if not start<=day<=end:raise ProviderError('KIS raw chart date outside request')
                    if day not in wanted:continue
                    values={k:_number(r[v]) for k,v in [('open','stck_oprc'),('high','stck_hgpr'),('low','stck_lwpr'),('close','stck_clpr'),('volume','acml_vol'),('turnover','acml_tr_pbmn')]}
                    rows.append(dict(market='kr',symbol=symbol,session_date=day,**values,vwap=values['turnover']/values['volume'] if values['volume'] else None,currency='KRW',source='kis_stock_raw',
                        turnover_method='exact_kis_acml_tr_pbmn',vwap_method='turnover_div_volume',retrieved_at=datetime.now(timezone.utc).isoformat(),
                        is_tradable_bar=bool(values['volume']>0 and all(values[c]>0 for c in ('open','high','low','close')))))
                start=end+pd.Timedelta(days=1)
        frame=pd.DataFrame(rows)
        if not frame.empty and frame.duplicated(['symbol','session_date']).any():raise ProviderError('Duplicate KIS raw chart key')
        return frame
    def _fetch_ksd_records(self,start,end,path,tr_id,extra=None):
        first,last=pd.Timestamp(start).normalize(),pd.Timestamp(end).normalize()
        if first.tzinfo is not None or last.tzinfo is not None:raise ProviderError('KSD date boundary invalid')
        def interval(a,b):
            value=self._get(path,tr_id,{'SHT_CD':'','CTS':'','F_DT':a.strftime('%Y%m%d'),'T_DT':b.strftime('%Y%m%d'),**(extra or {})})
            records=value.get('output1')
            if isinstance(records,dict):records=[records] if records else []
            if not isinstance(records,list):raise ProviderError('KSD actions shape invalid')
            for r in records:
                if not a.strftime('%Y%m%d')<=str(r.get('record_date',''))<=b.strftime('%Y%m%d'):raise ProviderError('KSD date boundary mismatch')
            if len(records)>=100 or value.get('tr_cont')=='M' or value.get('cts'):
                if a==b:raise ProviderError('KSD single day truncated')
                middle=a+pd.Timedelta(days=(b-a).days//2)
                return interval(a,middle)+interval(middle+pd.Timedelta(days=1),b)
            return records
        actions=[]
        while first<=last:
            end=min(last,first+pd.offsets.MonthEnd(0));actions.extend(interval(first,end));first=end+pd.Timedelta(days=1)
        return actions
    def fetch_capital_actions(self,start,end):
        """Return original cap-dcrs records for persisted enrichment evidence."""
        return self._fetch_ksd_records(start,end,'/uapi/domestic-stock/v1/ksdinfo/cap-dcrs','HHKDB669106C0')
    def fetch_actions(self,start,end):
        face=self._fetch_ksd_records(start,end,'/uapi/domestic-stock/v1/ksdinfo/rev-split','HHKDB669105C0',{'MARKET_GB':'0'})
        return reconcile_ksd_actions(face,self.fetch_capital_actions(start,end))


class Providers:
    """Composition used by ingestion; all required subproviders must succeed."""
    def __init__(self,alpaca=None,krx=None,kis=None,raw_dir=None):
        self.alpaca=alpaca or AlpacaProvider(client=HttpClient(raw_dir=raw_dir))
        self.krx=krx or KrxProvider(client=HttpClient(min_interval=1.,raw_dir=raw_dir))
        self.kis=kis or KisProvider(client=HttpClient(min_interval=.2,raw_dir=raw_dir))
        self.calendar_evidence={}
    def begin_refresh(self):
        # One current metadata snapshot for every part, refreshed next attempt.
        if hasattr(self.alpaca,'_asset_cache'):self.alpaca._asset_cache.clear()
    def load_session_evidence(self,evidence):
        self.calendar_evidence=json.loads(_canonical(evidence or {}))
        for day,item in self.calendar_evidence.get('kr',{}).items():
            if isinstance(item,dict) and isinstance(item.get('open'),bool):self.kis._calendar[day]=item['open']
    def reconcile_sessions(self,market,all_dates,known_sessions):
        if market!='kr':return list(all_dates)
        unknown=[d for d in all_dates if d not in set(known_sessions)]
        states=self.kis.session_status(unknown)
        proof=self.calendar_evidence.setdefault('kr',{})
        for day,opened in states.items():proof[day]={'open':opened,'provider':'kis','tr_id':'CTCA0903R'}
        return [d for d in all_dates if _day(d) not in states or states[_day(d)]]
    def published_sessions(self,market,sessions,now=None):
        """Conservative KRX collection policy, distinct from exchange closure/SLA.

        Collect a session's confirmed daily bulk data from the following
        calendar day at 08:15 Seoul. Do not substitute unverified same-day bars.
        Ingestion preserves already collected sessions and rejects old omissions.
        """
        if market!='kr':return list(sessions)
        instant=pd.Timestamp(now if now is not None else datetime.now(timezone.utc))
        if instant.tzinfo is None:raise ValueError('now must be timezone-aware')
        instant=instant.tz_convert('Asia/Seoul')
        return [d for d in sessions if (pd.Timestamp(d)+pd.Timedelta(days=1)).tz_localize('Asia/Seoul')+pd.Timedelta(hours=8,minutes=15)<=instant]
    def fetch_market(self,market,sessions,assets,audit=False):
        return self.fetch_market_part(market,sessions,assets,audit=audit)
    def fetch_market_part(self,market,sessions,assets,audit=False,action_start=None):
        """Bars use sessions only; actions may cover the preceding calendar gap."""
        if not sessions:raise ProviderError('No completed sessions requested')
        action_start=pd.Timestamp(action_start if action_start is not None else min(sessions)).normalize()
        if action_start.tzinfo is not None or action_start>min(sessions):raise ProviderError('Invalid corporate action collection boundary')
        actions=[];universe=assets.copy()
        if market in ('us','crypto'):
            active=self.alpaca.fetch_assets(market)
            universe=pd.concat([universe,active],ignore_index=True).drop_duplicates(['market','symbol'],keep='last')
            symbols=sorted(set(universe.symbol)|set(BENCHMARKS[market]))
            bars=self.alpaca.fetch_bars(market,symbols,sessions)
            if market=='us':actions=self.alpaca.fetch_actions(action_start,max(sessions))
        elif market=='kr':
            frames=[];universes=[]
            for date in sessions:
                b,a=self.krx.fetch_day(date);frames.append(b);universes.append(a)
            frames.append(self.kis.fetch_benchmarks(sessions));bars=pd.concat(frames,ignore_index=True)
            universe=pd.concat([universe,*universes],ignore_index=True).drop_duplicates(['market','symbol'],keep='last')
            actions=self.kis.fetch_actions(action_start,max(sessions))
        else:raise ProviderError('Unknown provider market')
        batch=MarketBatch(market,bars,universe,actions,list(sessions),{'actions_complete':True,'benchmarks_complete':True,
            'turnover':'KR exact provider value; US/crypto provider VWAP*volume','kr_actions':'KSD nominal/capital changes require relisting/reference confirmation'})
        from .price_validation_collect import collect_observations
        batch.provenance['validation_observations']=collect_observations(self,batch)
        return batch
