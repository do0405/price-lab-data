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


def test_krx_requires_both_nonempty_boards_and_exact_day():
    p=module();row={'BAS_DD':'20240102','ISU_CD':'005930','ISU_NM':'S','TDD_OPNPRC':'10','TDD_HGPRC':'12','TDD_LWPRC':'9','TDD_CLSPRC':'11','ACC_TRDVOL':'100','ACC_TRDVAL':'1,111'}
    s=Session([Response({'OutBlock_1':[row]}),Response({'OutBlock_1':[]})])
    provider=p.KrxProvider('fake',client=p.HttpClient(s,min_interval=0))
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


def test_alpaca_requests_only_supported_split_actions_not_every_distribution():
    p=module();s=Session([Response({'corporate_actions':{},'next_page_token':None})])
    assert p.AlpacaProvider('fake','fake',client=p.HttpClient(s,min_interval=0)).fetch_actions('2024-01-02','2024-01-05')==[]
    assert s.calls[0][2]['params']['types']=='forward_split,reverse_split'


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


@pytest.mark.parametrize('instant,expected',[
    ('2026-10-15T20:37:00+09:00',['2026-10-13','2026-10-14']),
    ('2026-10-16T08:14:59+09:00',['2026-10-13','2026-10-14']),
    ('2026-10-16T08:15:00+09:00',['2026-10-13','2026-10-14','2026-10-15']),
    ('2026-10-17T08:37:00+09:00',['2026-10-13','2026-10-14','2026-10-15','2026-10-16']),
])
def test_kr_publication_cutoff_is_separate_from_exchange_close(instant,expected):
    p=module();provider=p.Providers(alpaca=object(),krx=object(),kis=object())
    dates=list(pd.date_range('2026-10-13','2026-10-16'))
    assert [str(d.date()) for d in provider.published_sessions('kr',dates,now=instant)]==expected
    assert provider.published_sessions('us',dates,now=instant)==dates
    assert provider.published_sessions('crypto',dates,now=instant)==dates


def test_publication_cutoff_requires_aware_clock_and_accepts_utc():
    p=module();provider=p.Providers(alpaca=object(),krx=object(),kis=object())
    dates=[pd.Timestamp('2026-10-15')]
    assert provider.published_sessions('kr',dates,now='2026-10-15T23:15:00Z')==dates
    with pytest.raises(ValueError,match='timezone'):
        provider.published_sessions('kr',dates,now='2026-10-16T08:15:00')
