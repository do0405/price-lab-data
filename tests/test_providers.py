import json
import pandas as pd
import pytest


def module():
    import importlib.util
    assert importlib.util.find_spec('price_lab.providers'), 'durable providers missing'
    from price_lab import providers
    return providers


class Response:
    def __init__(self, payload, status=200):
        self.payload=payload;self.status_code=status;self.headers={}
    def json(self):return self.payload


class Session:
    def __init__(self, responses):self.responses=iter(responses);self.calls=[]
    def request(self, method, url, **kwargs):
        self.calls.append((method,url,kwargs))
        value=next(self.responses)
        if isinstance(value,Exception):raise value
        return value


def test_completed_sessions_use_exchange_close_and_crypto_utc():
    p=module()
    assert p.completed_sessions('us','2024-07-03','2024-07-05T16:59:00Z')==[pd.Timestamp('2024-07-03')]
    assert p.completed_sessions('us','2024-07-03','2024-07-05T20:20:00Z')==[pd.Timestamp('2024-07-03'),pd.Timestamp('2024-07-05')]
    assert p.completed_sessions('crypto','2024-01-01','2024-01-03T00:00:00Z')==list(pd.date_range('2024-01-01','2024-01-02'))
    assert p.completed_sessions('kr','2026-11-13','2026-11-13T15:29:59+09:00')==[]
    assert p.completed_sessions('kr','2026-11-13','2026-11-13T15:30:00+09:00')==[pd.Timestamp('2026-11-13')]


def test_krx_local_provider_retries_only_the_empty_board():
    p=module();wait=[]
    bar=dict(BAS_DD='20261113',ISU_CD='005930',TDD_OPNPRC='10',TDD_HGPRC='11',
        TDD_LWPRC='9',TDD_CLSPRC='10',ACC_TRDVOL='100',ACC_TRDVAL='1000')
    s=Session([Response({'OutBlock_1':[bar]}),Response({'OutBlock_1':[]}),
        Response({'OutBlock_1':[{**bar,'ISU_CD':'229200'}]})])
    provider=p.KrxProvider('fake',client=p.HttpClient(s,min_interval=0,sleep=wait.append))
    bars,_=provider.fetch_day('2026-11-13')
    assert set(bars.symbol)=={'005930','229200'}
    assert s.calls[1:]==[s.calls[1]]*2
    assert s.calls[1][1].endswith('/ksq_bydd_trd') and wait==[5]


def test_http_retry_does_not_echo_provider_body_or_auth(tmp_path):
    p=module();s=Session([Response({},429),Response({'ok':1})]);wait=[]
    client=p.HttpClient(s,sleep=wait.append,min_interval=0)
    assert client.request_json('GET','https://example.test/bars',headers={'Authorization':'PRIVATE'})=={'ok':1}
    assert wait and len(s.calls)==2
    s=Session([Response({'token':'PRIVATE'},401)])
    with pytest.raises(p.ProviderError) as error:p.HttpClient(s,min_interval=0).request_json('GET','https://example.test/bars')
    assert 'PRIVATE' not in str(error.value)


def test_alpaca_raw_paging_and_repeated_token_fail_closed():
    p=module();bar={'t':'2024-01-02T05:00:00Z','o':10,'h':12,'l':9,'c':11,'v':100,'vw':10.5}
    s=Session([Response({'bars':{'A':[bar]},'next_page_token':'x'}),Response({'bars':{},'next_page_token':'x'})])
    provider=p.AlpacaProvider('fake','fake',client=p.HttpClient(s,min_interval=0))
    with pytest.raises(p.ProviderError,match='token'):provider.fetch_bars('us',['A'],[pd.Timestamp('2024-01-02')])
    assert s.calls[0][2]['params']['adjustment']=='raw'


def test_alpaca_pins_current_entity_date_and_retains_exact_query(monkeypatch):
    p=module()
    class Clock:
        @staticmethod
        def now(tz):return pd.Timestamp('2026-10-18T12:00:00Z').to_pydatetime()
    monkeypatch.setattr(p,'datetime',Clock)
    bar={'t':'2024-01-02T05:00:00Z','o':10,'h':12,'l':9,'c':11,'v':100,'vw':10.5}
    session=Session([Response({'bars':{'A':[bar]},'next_page_token':None})])
    frame=p.AlpacaProvider('fake','fake',client=p.HttpClient(session,min_interval=0)).fetch_bars('us',['A'],[pd.Timestamp('2024-01-02')])
    assert session.calls[0][2]['params']['asof']=='2026-10-18'
    query=frame.attrs['alpaca_query']
    assert query==dict(symbols=['A'],timeframe='1Day',start='2024-01-02',end='2024-01-03T00:00:00Z',
        feed='sip',asof='2026-10-18',adjustment='raw',batch_size=100)


def test_alpaca_explicit_asof_is_identical_across_pages_and_split_requests():
    p=module();bar={'t':'2024-01-02T05:00:00Z','o':10,'h':12,'l':9,'c':11,'v':100,'vw':10.5}
    session=Session([Response({'bars':{'A':[bar]},'next_page_token':'raw-next'}),
        Response({'bars':{},'next_page_token':None}),Response({'bars':{'A':[bar]},'next_page_token':None})])
    provider=p.AlpacaProvider('fake','fake',client=p.HttpClient(session,min_interval=0))
    raw=provider.fetch_bars('us',['A'],[pd.Timestamp('2024-01-02')],asof='2026-10-17')
    split=provider.fetch_bars('us',['A'],[pd.Timestamp('2024-01-02')],asof=raw.attrs['alpaca_query']['asof'],adjustment='split')
    params=[call[2]['params'] for call in session.calls]
    assert all(q['asof']=='2026-10-17' for q in params)
    assert 'page_token' not in params[-1] and split.attrs['alpaca_query']['adjustment']=='split'


def test_composed_us_provider_retains_all_pairs_and_raw_query_provenance():
    p=module();symbols=['A','B','QQQ','SPY']
    assets=[dict(symbol=s,name=s,exchange='NASDAQ',status='active') for s in symbols]
    raw={'t':'2024-01-02T05:00:00Z','o':20,'h':24,'l':18,'c':22,'v':100,'vw':21}
    split={**raw,'o':10,'h':12,'l':9,'c':11,'v':200,'vw':10.5}
    session=Session([Response(assets),Response({'bars':{s:[raw] for s in symbols}}),
        Response({'corporate_actions':{}}),Response({'bars':{s:[split] for s in symbols}})])
    alpaca=p.AlpacaProvider('fake','fake',client=p.HttpClient(session,min_interval=0))
    batch=p.Providers(alpaca=alpaca,krx=object(),kis=object()).fetch_market_part('us',[pd.Timestamp('2024-01-02')],pd.DataFrame(columns=['market','symbol']))
    observations=batch.provenance['validation_observations']
    assert len(observations)==4 and all(r['close']==11 and r['volume']==200 and r['vwap']==10.5 for r in observations)
    assert list(batch.bars.close)==[22]*4
    assert batch.provenance['alpaca_query']==batch.bars.attrs['alpaca_query']
    raw_query,split_query=session.calls[1][2]['params'],session.calls[3][2]['params']
    assert {k:v for k,v in raw_query.items() if k!='adjustment'}=={k:v for k,v in split_query.items() if k!='adjustment'}


def test_krx_requires_both_nonempty_boards_and_exact_day():
    p=module();row={'BAS_DD':'20240102','ISU_CD':'005930','ISU_NM':'S','TDD_OPNPRC':'10','TDD_HGPRC':'12','TDD_LWPRC':'9','TDD_CLSPRC':'11','ACC_TRDVOL':'100','ACC_TRDVAL':'1,111'}
    s=Session([Response({'OutBlock_1':[row]})]+[Response({'OutBlock_1':[]})]*4)
    provider=p.KrxProvider('fake',client=p.HttpClient(s,min_interval=0,sleep=lambda _:None))
    with pytest.raises(p.ProviderError,match='empty'):provider.fetch_day(pd.Timestamp('2024-01-02'))
    row2={**row,'BAS_DD':'20240103'}
    with pytest.raises(p.ProviderError,match='date'):
        p.KrxProvider('fake',client=p.HttpClient(Session([Response({'OutBlock_1':[row2]})]),min_interval=0)).fetch_day(pd.Timestamp('2024-01-02'))


def test_ksd_nominal_record_date_never_becomes_verified_ex_date():
    p=module()
    action=p.parse_ksd_action({'sht_cd':'005930','record_date':'20240102','inter_bf_face_amt':'5000','inter_af_face_amt':'1000','list_dt':'20240122'})
    assert action['effective_date']=='2024-01-22' and action['price_factor']==.2
    assert action['confirmed'] is False and action['record_date']=='2024-01-02'
    assert p.parse_ksd_action({'sht_cd':'005930','record_date':'20240102','inter_bf_face_amt':'0','inter_af_face_amt':'1000'})['kind']=='unknown'


def test_alpaca_actions_keep_identity_revision_and_unknown_changes():
    p=module()
    record={'id':'split-id','symbol':'A','new_rate':4,'old_rate':1,'ex_date':'2024-01-03','process_date':'2024-01-02'}
    first=p.parse_alpaca_action('forward_splits',record)
    second=p.parse_alpaca_action('forward_splits',{**record,'new_rate':5})
    assert first['event_id']==second['event_id'] and first['revision']!=second['revision']
    assert first['price_factor']==.25 and first['share_factor']==4
    assert p.parse_alpaca_action('name_changes',{'id':'rename','old_symbol':'A','new_symbol':'B','process_date':'2024-01-03'})['kind']=='unknown'


def test_new_active_assets_are_refreshed_without_losing_old_universe():
    p=module();s=Session([Response([{'symbol':'NEW','name':'New','exchange':'NASDAQ','class':'us_equity','status':'active'},
                                  {'symbol':'OTC','name':'Off exchange','exchange':'OTC','class':'us_equity','status':'active'}])])
    provider=p.AlpacaProvider('fake','fake',client=p.HttpClient(s,min_interval=0))
    assets=provider.fetch_assets('us')
    assert list(assets.symbol)==['NEW'] and assets.iloc[0].exchange=='NASDAQ'


def test_alpaca_requests_full_identity_context_without_limiting_action_groups():
    p=module();s=Session([Response({'corporate_actions':{},'next_page_token':None})])
    assert p.AlpacaProvider('fake','fake',client=p.HttpClient(s,min_interval=0)).fetch_actions('2024-01-02','2024-01-05')==[]
    assert 'types' not in s.calls[0][2]['params']


def test_kr_fallback_requests_only_invalid_keys_and_keeps_whole_source_bar():
    from price_lab.price_source_selection import complete_kr_price_bars
    day=pd.Timestamp('2024-01-02');calls=[]
    def bar(symbol,source='krx',**changes):
        return dict(market='kr',symbol=symbol,session_date=day,open=10.,high=12.,low=9.,
            close=11.,volume=100.,turnover=1100.,vwap=11.,currency='KRW',source=source,**changes)
    good=bar('A');bad={**bar('B'),'high':2.}
    original=pd.DataFrame([good,bad])
    class Kis:
        def fetch_stock_bars(self,symbols,sessions):
            calls.append((symbols,sessions))
            return pd.DataFrame([{**bar('B','kis:raw:unadjusted'),'close':10.,'turnover':1000.,'vwap':10.}])
    result=complete_kr_price_bars(original,Kis(),[day],expected_symbols=['A','B'])
    assert calls==[(['B'],[day])]
    assert result.set_index('symbol').loc['A','source']=='krx'
    assert result.set_index('symbol').loc['B',['source','close','turnover']].tolist()==['kis:raw:unadjusted',10.,1000.]
    assert original.iloc[1].high==2.


def test_kr_zero_volume_remains_nontradable_when_no_trade_evidence_exists():
    from price_lab.price_source_selection import complete_kr_price_bars,kr_bar_problem
    day=pd.Timestamp('2024-01-02');calls=[]
    frame=pd.DataFrame([dict(market='kr',symbol='A',session_date=day,open=0.,high=0.,low=0.,
        close=10.,volume=0.,turnover=0.,vwap=10.,currency='KRW',source='krx',is_tradable_bar=False)])
    class Kis:
        def fetch_stock_bars(self,symbols,sessions):
            calls.append((symbols,sessions));return pd.DataFrame()
    result=complete_kr_price_bars(frame,Kis(),[day],expected_symbols=['A'])
    assert len(calls)==1 and len(result)==1
    assert not bool(result.iloc[0].is_tradable_bar) and kr_bar_problem(result.iloc[0]) is not None


def test_kis_calendar_batches_caches_and_requires_requested_day():
    p=module();s=Session([Response({'access_token':'fake'}),Response({'rt_cd':'0','output':[
        {'bass_dt':'20240101','opnd_yn':'N'},{'bass_dt':'20240102','opnd_yn':'Y'},{'bass_dt':'20240103','opnd_yn':'N'}]})])
    kis=p.KisProvider('fake','fake',client=p.HttpClient(s,min_interval=0))
    assert kis.session_status(pd.date_range('2024-01-01','2024-01-03'))=={'2024-01-01':False,'2024-01-02':True,'2024-01-03':False}
    assert kis.session_status([pd.Timestamp('2024-01-02')])=={'2024-01-02':True} and len(s.calls)==2
    assert s.calls[1][2]['params']=={'BASS_DT':'20240101','CTX_AREA_FK':'','CTX_AREA_NK':''}
    bad=p.KisProvider('fake','fake',client=p.HttpClient(Session([Response({'access_token':'fake'}),Response({'rt_cd':'0','output':[]})]),min_interval=0))
    with pytest.raises(p.ProviderError,match='calendar'):bad.session_status([pd.Timestamp('2024-01-02')])


def test_audit_provider_reuses_one_asset_snapshot_across_parts():
    p=module();s=Session([Response([{'symbol':'NEW','name':'New','exchange':'NASDAQ','class':'us_equity','status':'active'}])])
    provider=p.AlpacaProvider('fake','fake',client=p.HttpClient(s,min_interval=0))
    first=provider.fetch_assets('us');first.loc[0,'symbol']='MUTATED'
    second=provider.fetch_assets('us')
    assert list(second.symbol)==['NEW'] and len(s.calls)==1


def test_composed_provider_part_uses_calendar_action_start_without_extra_bars():
    p=module()
    class Alpaca:
        calls=[]
        def fetch_assets(self,market):return pd.DataFrame([dict(market='us',symbol='A',name='A',exchange='NYSE')])
        def fetch_bars(self,market,symbols,sessions):self.calls.append(('bars',list(sessions)));return pd.DataFrame()
        def fetch_actions(self,start,end):self.calls.append(('actions',pd.Timestamp(start),pd.Timestamp(end)));return []
    alpaca=Alpaca();provider=p.Providers(alpaca=alpaca,krx=object(),kis=object());dates=list(pd.bdate_range('2024-02-05',periods=3))
    assets=pd.DataFrame(columns=['market','symbol','name','exchange'])
    provider.fetch_market_part('us',dates,assets,audit=True,action_start=pd.Timestamp('2024-02-03'))
    assert alpaca.calls==[('bars',dates),('actions',pd.Timestamp('2024-02-03'),dates[-1])]


@pytest.mark.parametrize('instant',[
    '2026-11-13T15:37:00+09:00','2026-11-14T08:14:00+09:00','2026-11-15T19:37:00+09:00',
])
def test_kr_publication_accepts_completed_friday_without_an_overnight_wait(instant):
    p=module();provider=p.Providers(alpaca=object(),krx=object(),kis=object())
    dates=p.completed_sessions('kr','2026-11-13',instant)
    assert provider.published_sessions('kr',dates,now=instant)==[pd.Timestamp('2026-11-13')]
    assert provider.published_sessions('crypto',dates,now=instant)==dates


def test_publication_cutoff_requires_aware_clock_and_accepts_utc():
    p=module();provider=p.Providers(alpaca=object(),krx=object(),kis=object())
    dates=[pd.Timestamp('2026-10-15')]
    assert provider.published_sessions('kr',dates,now='2026-10-15T06:37:00Z')==dates
    with pytest.raises(ValueError,match='timezone'):
        provider.published_sessions('kr',dates,now='2026-10-15T15:37:00')

