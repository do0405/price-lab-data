import base64
from datetime import datetime,timezone
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import pytest
from price_lab import market_reference_sources as s,market_reference_pipeline as p
from test_market_reference_sources import fund_body,payload,jpm_body

NOW=datetime(2026,11,24,tzinfo=timezone.utc)


def test_weekly_name_catalog_and_focused_primary_reports_are_not_starved():
    state=p.initial_state('2026-11-23')
    old=s.task('SEC_PRIMARY','0000000002:0000000002-26-000001:10-K:old.htm','2026-01-01')
    urgent=s.task('SEC_PRIMARY','0000000001:0000000001-26-000001:10-K:new.htm','2026-01-01')
    state['pending']=[old,urgent];b=bootstrap();b['capitalization_ciks']=['0000000001']
    p.plan(state,b,NOW)
    assert state['pending'][0]['kind']=='SEC_CIK_NAMES'
    assert [t for t in state['pending'] if t['kind']=='SEC_PRIMARY']==[urgent,old]
    state['checked'][s.digest(s.task('SEC_CIK_NAMES'))]=dict(status='COLLECTED',checked_at=NOW.isoformat())
    state['pending']=[t for t in state['pending'] if t['kind']!='SEC_CIK_NAMES'];p.plan(state,b,NOW)
    assert not any(t['kind']=='SEC_CIK_NAMES' for t in state['pending'])


def test_secondary_quotes_resume_only_for_missing_primary_and_back_off_inactive_pages(monkeypatch):
    from datetime import timedelta
    from test_market_reference_sources import stockanalysis_body
    b=bootstrap();b['price_symbols']=['CMLSQ'];state=p.initial_state('2026-11-23')
    state['cursors']['price_quote_checks']={'CMLSQ':dict(status='HTTP_404',retry_at=(NOW+timedelta(days=30)).isoformat())}
    out=p.collect(state,b,max_requests=1,now=NOW,source_kinds=['STOCKANALYSIS_PRICE'],fetch=lambda t:(200,{},stockanalysis_body()),sleep=lambda _:None)
    assert out['requests']==1
    state=out['state'];assert state['cursors']['secondary_price_quote_checks']['CMLSQ']['retry_at']==(NOW+timedelta(days=30)).isoformat()
    p.plan(state,b,NOW);assert not any(t['kind']=='STOCKANALYSIS_PRICE' for t in state['pending'])
    state=p.initial_state('2026-11-23');state['pending']=[s.task('STOCKANALYSIS_PRICE','CMLSQ','2026-11-23')]
    monkeypatch.setattr(p,'plan',lambda *a:None)
    out=p.collect(state,b,max_requests=1,now=NOW,fetch=lambda t:(200,{},b'<html>Symbol not found</html>'),sleep=lambda _:None)
    assert out['state']['cooldowns']=={} and out['state']['pending']==[]


def test_secondary_quote_cadence_and_retry_after_survive_resume(monkeypatch):
    from datetime import timedelta
    from test_market_reference_sources import stockanalysis_body
    monkeypatch.setattr(p,'plan',lambda *a:None)
    t=s.task('STOCKANALYSIS_PRICE','CMLSQ','2026-11-23')
    state=p.initial_state('2026-11-23');state['pending']=[t];waits=[]
    out=p.collect(state,bootstrap(),max_requests=1,now=NOW,
        fetch=lambda t:(200,{},stockanalysis_body()),sleep=waits.append)
    assert waits==[6]
    state=p.initial_state('2026-11-23');state['pending']=[t]
    out=p.collect(state,bootstrap(),max_requests=1,now=NOW,
        fetch=lambda t:(429,{'Retry-After':'1800'},b''),sleep=lambda _:None)
    assert out['state']['cooldowns']['stockanalysis.com']['retry_at']==(NOW+timedelta(seconds=1800)).isoformat()
    assert p.collect(out['state'],bootstrap(),max_requests=1,now=NOW+timedelta(minutes=29),
        fetch=lambda t:pytest.fail('Retry-After ignored'),sleep=lambda _:None)['requests']==0


def test_primary_context_survives_batch_boundary_and_is_pruned_after_completion(monkeypatch):
    monkeypatch.setattr(p,'plan',lambda *args:None)
    parent=s.task('SEC_SUBMISSIONS','0000000001')
    recent=dict(form=['N-CSR'],filingDate=['2026-09-03'],reportDate=['2026-06-30'],
        accessionNumber=['0000000001-26-000001'],primaryDocument=['fund.htm'])
    body=json.dumps(dict(cik=1,name='Example Fund',tickers=['FUND'],filings={'recent':recent})).encode()
    state=p.initial_state('2026-11-23');state['pending']=[parent]
    b=bootstrap();b['capitalization_ciks']=['0000000001']
    out=p.collect(state,b,max_requests=1,now=NOW,fetch=lambda t:(200,{},body),sleep=lambda _:None)
    child=out['state']['pending'][0];key=s.digest(child)
    assert out['state']['cursors']['primary_filing_contexts'][key]['tickers']==['FUND']
    out=p.collect(out['state'],b,max_requests=1,now=NOW,fetch=lambda t:(200,{},b'<html><p>report</p></html>'),sleep=lambda _:None)
    assert key in out['state']['cursors']['primary_filing_contexts']
    out=p.collect(out['state'],b,max_requests=1,now=NOW,fetch=lambda t:None,sleep=lambda _:None)
    assert out['state']['cursors']['primary_filing_contexts']=={}


def test_nasdaq_screener_uses_public_json_request_headers(monkeypatch):
    fetcher=p.Fetcher();calls=[]
    class Response:
        status_code=200;headers={}
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def iter_content(self,*args):return iter([b'{}'])
    monkeypatch.setattr(fetcher.session,'request',lambda *a,**kw:(calls.append(kw) or Response()))
    fetcher(s.task('NASDAQ_CAPS'))
    assert calls[0]['headers']['Accept']=='application/json'
    assert calls[0]['headers']['Origin']=='https://www.nasdaq.com'
    assert calls[0]['headers']['Referer']=='https://www.nasdaq.com/market-activity/stocks/screener'
    assert calls[0]['allow_redirects'] is False


def test_transport_diagnostic_retains_only_exception_category(monkeypatch):
    import requests
    fetcher=p.Fetcher()
    def fail(*args,**kwargs):raise requests.exceptions.ReadTimeout('secret-contact-and-proxy')
    monkeypatch.setattr(fetcher.session,'request',fail)
    status,headers,body=fetcher(s.task('NASDAQ_CAPS'))
    assert (status,body)==(0,b'')
    assert headers=={'X-PriceLab-Reason':'TRANSPORT_READ_TIMEOUT'}
    monkeypatch.setattr(p,'plan',lambda *args:None)
    state=p.initial_state('2026-11-23');state['pending']=[s.task('NASDAQ_CAPS')]
    out=p.collect(state,bootstrap(),max_requests=1,now=NOW,fetch=fetcher,sleep=lambda _:None)
    assert out['state']['cooldowns']['api.nasdaq.com']['reason']=='TRANSPORT_READ_TIMEOUT'


def test_oversized_stream_closes_response_and_returns_a_bounded_diagnostic(monkeypatch):
    closed=[]
    class Response:
        status_code=200;headers={}
        def __enter__(self):return self
        def __exit__(self,*args):closed.append(True)
        def iter_content(self,*args):return iter([b'12345678',b'9'])
    fetcher=p.Fetcher()
    monkeypatch.setattr(s,'response_limit',lambda task:8)
    monkeypatch.setattr(fetcher.session,'request',lambda *a,**kw:Response())
    status,headers,body=fetcher(s.task('NASDAQ_CAPS'))
    assert (status,body)==(0,b'') and headers['X-PriceLab-Reason']=='RESPONSE_TOO_LARGE'
    assert closed==[True]


def test_one_oversized_document_keeps_completed_records_and_other_sec_requests(monkeypatch):
    from datetime import timedelta
    monkeypatch.setattr(p,'plan',lambda *a:None)
    first=s.task('SEC_SUBMISSIONS','0000000001')
    large=s.task('SEC_PRIMARY','0000000001:0000000001-26-000001:N-CSR:large.htm','2026-01-01')
    last=s.task('SEC_SUBMISSIONS','0000000002')
    state=p.initial_state('2026-11-23');state['pending']=[first,large,last]
    def fetch(task):
        if task==large:return 0,{'X-PriceLab-Reason':'RESPONSE_TOO_LARGE'},b''
        return 200,{},json.dumps({'cik':int(task['entity']),'filings':{'recent':{},'files':[]}}).encode()
    out=p.collect(state,bootstrap(),max_requests=3,now=NOW,fetch=fetch,sleep=lambda _:None)
    assert out['requests']==3 and [x['task'] for x in out['records']]==[first,last]
    check=out['state']['checked'][s.digest(large)]
    assert check['status']=='RESPONSE_TOO_LARGE' and check['task']==large
    assert check['retry_at']==(NOW+timedelta(days=30)).isoformat()
    assert out['state']['cooldowns']=={} and out['state']['pending']==[large]
    assert p.collect(out['state'],bootstrap(),max_requests=1,now=NOW+timedelta(days=29),
        fetch=lambda t:pytest.fail('oversized document retried too early'),sleep=lambda _:None)['requests']==0


def test_whole_market_snapshots_are_scheduled_without_resetting_sec_cursor():
    state=p.initial_state('2026-11-23');state['cursors']['sec']=42
    p.plan(state,bootstrap(),NOW)
    assert {t['entity'] for t in state['pending'] if t['kind']=='NASDAQ_DIRECTORY'}=={'nasdaq','other'}
    assert any(t['kind']=='NASDAQ_CAPS' for t in state['pending'])
    assert state['cursors']['sec']==42


def test_targeted_price_quotes_resume_and_refresh_weekly(monkeypatch,tmp_path):
    from test_market_reference_sources import yahoo_body
    path=tmp_path/'config.json';path.write_text(json.dumps(dict(version=1,ciks=[])))
    p.enqueue_price_symbols(path,['AATC']);cfg=json.loads(path.read_text())
    assert cfg['price_symbols']==['AATC'] and cfg['version']==6
    state=p.initial_state('2026-11-23');b=bootstrap();b['price_symbols']=['AATC']
    p.plan(state,b,NOW)
    out=p.collect(state,b,max_requests=1,now=NOW,source_kinds=['YAHOO_PRICE'],fetch=lambda t:(200,{},yahoo_body()),sleep=lambda _:None)
    state=out['state'];p.plan(state,b,NOW)
    assert not any(t['kind']=='YAHOO_PRICE' for t in state['pending'])
    from datetime import timedelta
    later=NOW+timedelta(days=8);b['through']='2026-12-01';p.plan(state,b,later)
    assert len([t for t in state['pending'] if t['kind']=='YAHOO_PRICE'])==1


def test_issuer_queue_updates_preserve_quote_targets_and_newer_config_version(tmp_path):
    path=tmp_path/'config.json';path.write_text(json.dumps(dict(version=1,ciks=[])))
    p.enqueue_price_symbols(path,['AATC'])
    p.enqueue_capitalization_targets(path,['0001739410'],[])
    cfg=json.loads(path.read_text())
    assert cfg['version']==6 and cfg['price_symbols']==['AATC']


def test_directory_footer_only_refresh_does_not_duplicate_raw_or_observations(monkeypatch):
    monkeypatch.setattr(p,'plan',lambda *args:None)
    t=s.task('NASDAQ_DIRECTORY','nasdaq');body=b'Symbol|Security Name|ETF|Test Issue\nABC|ABC Common Stock|N|N\nFile Creation Time: 1126202610:00|||\n'
    state=p.initial_state('2026-11-23');state['pending']=[t]
    out=p.collect(state,bootstrap(),max_requests=1,now=NOW,fetch=lambda t:(200,{},body),sleep=lambda _:None)
    state=out['state'];assert len(out['records'])==1;original=state['seen'][s.digest(t)]
    state['pending']=[t]
    out=p.collect(state,bootstrap(),max_requests=1,now=NOW,fetch=lambda t:(200,{},body.replace(b'10:00',b'14:00')),sleep=lambda _:None)
    assert not out['records'] and out['state']['seen'][s.digest(t)]==original


def test_capitalization_targets_are_prioritized_without_resetting_scan_or_other_queues(tmp_path):
    path=tmp_path/'config.json'
    path.write_text(json.dumps(dict(version=4,ciks=['0000000009'],research_targets=[],depositary_attachment_ids=['123'],jpm_document_ids=['a'*32])))
    document=dict(entity='0001023128:0001023128-26-000060:10-Q:lad-20260930.htm',filed_date='2026-10-28')
    assert p.enqueue_capitalization_targets(path,['0001023128'],[document])['status']=='UPDATED'
    p.enqueue_jpm_documents(path,['b'*32]);p.enqueue_attachments(path,['124']);p.enqueue_targets(path,[dict(fund='IVV',snapshot_date='2026-09-30')])
    cfg=json.loads(path.read_text());assert cfg['version']==5 and cfg['capitalization_ciks']==['0001023128']
    assert cfg['sec_primary_documents']==[document] and cfg['ciks']==['0000000009']
    assert p.enqueue_capitalization_targets(path,['0001023128'],[document])['status']=='UNCHANGED'
    state=p.initial_state('2026-11-23');state['cursors']['sec']=1
    b=bootstrap();b.update(capitalization_ciks=cfg['capitalization_ciks'],sec_primary_documents=[document])
    p.plan(state,b,NOW)
    assert state['cursors']['sec']==1
    assert next(t for t in state['pending'] if t['kind']!='SEC_CIK_NAMES')['kind']=='SEC_PRIMARY'
    assert any(t['kind']=='SEC_SUBMISSIONS' and t['entity']=='0001023128' for t in state['pending'])


def test_priority_refocus_keeps_displaced_issuers_and_existing_queues(tmp_path):
    path=tmp_path/'config.json'
    path.write_text(json.dumps(dict(version=1,ciks=['0000000009'])))
    p.enqueue_price_symbols(path,['AATC'])
    document=dict(entity='0000000001:0000000001-26-000001:10-K:old.htm',filed_date='2026-01-01')
    p.enqueue_capitalization_targets(path,['0000000001'],[document])
    p.enqueue_capitalization_targets(path,['0000000002'],replace_priority=True)
    cfg=json.loads(path.read_text())
    assert cfg['capitalization_ciks']==['0000000002']
    assert cfg['ciks']==['0000000009','0000000001','0000000002']
    assert cfg['sec_primary_documents']==[document] and cfg['price_symbols']==['AATC']
    assert p.enqueue_capitalization_targets(path,['0000000002'],replace_priority=True)['status']=='UNCHANGED'


def test_sec_identity_uses_configured_environment_even_with_other_provider_keys(monkeypatch):
    identity='PriceLab Research research@institution.edu'
    monkeypatch.setenv('SEC_USER_AGENT',identity);calls=[]
    class Response:
        status_code=200;headers={}
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def iter_content(self,*args):return iter([b'{}'])
    fetcher=p.Fetcher(credentials={'KRX_API_KEY':'not-the-sec-identity'})
    monkeypatch.setattr(fetcher.session,'request',lambda *args,**kwargs:(calls.append(kwargs['headers']) or Response()))
    fetcher(s.task('SEC_SUBMISSIONS','0000320193'))
    assert calls[-1]['User-Agent']==identity
    fetcher(s.task('CITI_CATALOG'))
    assert identity not in str(calls[-1])


def test_missing_sec_identity_does_not_send_an_undeclared_request(monkeypatch):
    monkeypatch.delenv('SEC_USER_AGENT',raising=False)
    fetcher=p.Fetcher(credentials={'KRX_API_KEY':'other'})
    monkeypatch.setattr(fetcher.session,'request',lambda *args,**kwargs:pytest.fail('Undeclared SEC request sent'))
    status,headers,body=fetcher(s.task('SEC_MAP'))
    assert status==0 and headers['X-PriceLab-Reason']=='SEC_IDENTITY_NOT_CONFIGURED'


def test_jpm_document_targets_preserve_other_config_and_enqueue_idempotently(tmp_path):
    path=tmp_path/'config.json';original=dict(version=3,ciks=['0000320193'],research_targets=[],depositary_attachment_ids=['123'])
    path.write_text(json.dumps(original));document='a'*32+':3'
    assert p.enqueue_jpm_documents(path,[document])['added']==1
    p.enqueue_attachments(path,['124']);p.enqueue_targets(path,[{'fund':'IWB','snapshot_date':'2026-10-21'}])
    updated=json.loads(path.read_text());assert updated['version']==4
    assert updated['ciks']==original['ciks'] and updated['jpm_document_ids']==[document]
    assert updated['depositary_attachment_ids']==['123','124']
    assert p.enqueue_jpm_documents(path,[document])['status']=='UNCHANGED'


@pytest.mark.parametrize('age,due',[(29,False),(31,True)])
def test_pinned_jpm_documents_recheck_monthly_not_on_every_daily_catalog(age,due):
    from datetime import timedelta
    from unittest.mock import patch
    state=p.initial_state('2026-11-23');catalog=s.task('JPM_ACTIONS');doc=s.task('JPM_DOCUMENT','a'*32+':3')
    state['pending']=[catalog];state['seen'][s.digest(doc)]='b'*64
    state['checked'][s.digest(doc)]={'status':'COLLECTED','checked_at':(NOW-timedelta(days=age)).isoformat()}
    body=jpm_body([dict(cusip='471115402',action='Ratio Change',notices={'pdf':'a'*32},sequenceNo=3)])
    with patch.object(p,'plan',lambda state,*args:state):
        out=p.collect(state,bootstrap(),now=NOW,max_requests=1,fetch=lambda t:(200,{},body),sleep=lambda _:None)
    assert (doc in out['state']['pending']) is due


def test_historical_issuer_queue_preserves_scan_positions_and_other_config(tmp_path):
    path=tmp_path/'bootstrap.json'
    original=dict(version=4,ciks=['0000000009','0000000012'],research_targets=[],
        depositary_attachment_ids=['123'],jpm_document_ids=['a'*32+':3'])
    path.write_text(json.dumps(original))
    result=p.enqueue_ciks(path,['0000000001','0000000009'])
    updated=json.loads(path.read_text())
    assert updated=={**original,'ciks':['0000000009','0000000012','0000000001']}
    assert result==dict(status='UPDATED',issuers=3,added=1)
    assert p.enqueue_ciks(path,['0000000001'])['status']=='UNCHANGED'
    prior=path.read_bytes()
    with pytest.raises(ValueError,match='Official CIK'):p.enqueue_ciks(path,['1'])
    assert path.read_bytes()==prior


def test_sec_identity_recovery_clears_only_hosts_with_valid_successful_canaries():
    from unittest.mock import patch
    state=p.initial_state('2026-11-23')
    for host in ('data.sec.gov','www.sec.gov','data.krx.co.kr'):
        state['cooldowns'][host]={'reason':'HTTP_403','retry_at':'2026-11-26T00:00:00+00:00'}
    seen=[]
    def fetch(t):
        seen.append(t['kind'])
        if t['kind']=='SEC_MAP':return 403,{'Content-Type':'text/html'},b'<h1>Access Denied</h1>'
        return 200,{},b'{"cik":320193,"filings":{"recent":{},"files":[]}}'
    with patch.object(p,'plan',lambda state,*args:state):
        out=p.collect(state,bootstrap(),now=NOW,max_requests=2,fetch=fetch,sleep=lambda _:None,
            recover_sec_identity=True,source_kinds=['SEC_MAP','SEC_SUBMISSIONS'])
    assert seen==['SEC_SUBMISSIONS','SEC_MAP']
    assert 'data.sec.gov' not in out['state']['cooldowns']
    assert out['state']['cooldowns']['www.sec.gov']['reason']=='HTTP_403'
    assert out['state']['cooldowns']['data.krx.co.kr']==state['cooldowns']['data.krx.co.kr']
    assert len(out['records'])==1 and state['cooldowns']['data.sec.gov']['reason']=='HTTP_403'


def test_run_recovery_probes_only_the_first_batch(tmp_path,monkeypatch):
    class Owner:
        head=None
        def __init__(self,*args,**kwargs):pass
        def acquire(self,**kwargs):return True
        def guard(self,**kwargs):pass
        def release(self):pass
    calls=[]
    monkeypatch.setattr(p,'ResearchOwner',Owner);monkeypatch.setattr(p,'read_control',lambda **kwargs:None)
    def collect(state,*args,**kwargs):
        calls.append(kwargs['recover_sec_identity']);return dict(state=state,records=[],requests=1)
    monkeypatch.setattr(p,'collect',collect)
    monkeypatch.setattr(p,'publish',lambda payload,*args,**kwargs:dict(sequence=payload['sequence'],package_sha256=p.package_digest(payload)))
    p.run(bootstrap(),tmp_path,seconds=600,max_requests=1,max_batches=2,key=b'x'*32,
        credentials={'SEC_USER_AGENT':'PriceLab Research research@institution.edu'},recover_sec_identity=True)
    assert calls==[True,False]


@pytest.mark.parametrize('body,expected',[
    (b'<title>Your Request Originates from an Undeclared Automated Tool</title>','UNDECLARED_AUTOMATION'),
    (b'<title>Request Rate Threshold Exceeded</title>','REQUEST_RATE_THRESHOLD'),
    (b'<h1>Access Denied</h1>','ACCESS_DENIED_UNSPECIFIED')])
def test_sec_403_diagnostic_is_bounded_and_contains_no_raw_identity(body,expected):
    d=s.sec_failure_diagnostic(403,{'Content-Type':'text/html'},body)
    assert d['classification']==expected
    assert 'body' not in d and 'headers' not in d


def test_targeted_reference_collection_preserves_other_pending_sources():
    from unittest.mock import patch
    state=p.initial_state('2026-11-23');other=s.task('CITI_NOTICE','513');target=s.task('JPM_UNIVERSE')
    state['pending']=[other,target];calls=[]
    def fetch(t):
        calls.append(t['kind']);return 200,{},jpm_body([{'cusip':'471115402','name':'Example'}])
    with patch.object(p,'plan',lambda state,*args:state):
        result=p.collect(state,bootstrap(),now=NOW,max_requests=5,fetch=fetch,sleep=lambda _:None,
            source_kinds=['JPM_UNIVERSE'])
    assert calls==['JPM_UNIVERSE']
    assert result['state']['pending']==[other] and result['requests']==1


def test_extended_sources_join_existing_window_without_replaying_completed_documents():
    b=bootstrap();state=p.initial_state('2026-11-23');state['cursors']['current_window']='2026-11-24T00'
    done=s.task('BNY_NOTICE','AD2000235');state['checked'][s.digest(done)]={'status':'COLLECTED','checked_at':NOW.isoformat()}
    p.plan(state,b,NOW)
    for t in [s.task('JPM_UNIVERSE'),s.task('JPM_ACTIONS'),s.task('CITI_CATALOG'),s.task('LSEG_WEIGHT_CATALOG'),s.task('LSEG_WEIGHTS','US1000'),s.task('RUSSELL_IPO_PAGE'),s.task('RUSSELL_IPO_PDF','3000',page=4)]:
        assert t in state['pending']
    assert done not in state['pending']


def test_citi_backfill_keeps_a_slot_for_other_reference_categories():
    state=p.initial_state('2026-11-23');state['pending']=[s.task('CITI_NOTICE',str(n)) for n in range(1,10)]+[s.task('FRED_INDEX','SP500')]
    calls=[]
    def fetch(t):
        calls.append(t['kind'])
        return 200,{},b'observation_date,SP500\n2026-11-20,100\n' if t['kind']=='FRED_INDEX' else b'%PDF-fixture'
    # Freeze planning to isolate lane fairness from current-window discovery.
    from unittest.mock import patch
    with patch.object(p,'plan',lambda state,*args:state):
        p.collect(state,bootstrap(),now=NOW,max_requests=5,fetch=fetch,sleep=lambda _:None)
    assert calls[:4]==['CITI_NOTICE']*4 and calls[4]=='FRED_INDEX'


def test_reserved_reference_slot_rotates_past_an_older_non_citi_backlog(monkeypatch):
    state=p.initial_state('2026-11-23')
    state['pending']=([s.task('CITI_NOTICE',str(n)) for n in range(1,40)]
        +[s.task('LSEG_CATALOG',str(y)) for y in range(2026,2009,-1)]
        +[s.task('SEC_SUBMISSIONS','0000320193'),s.task('FINRA_DAILY',day='2026-11-20'),s.task('JPM_UNIVERSE')])
    calls=[]
    monkeypatch.setattr(p,'plan',lambda state,*args:state)
    monkeypatch.setattr(s,'validate_response',lambda *args:None)
    monkeypatch.setattr(s,'discovered',lambda *args:[])
    def fetch(t):
        calls.append(t['kind'])
        return 200,{},b'{}'
    result=p.collect(state,bootstrap(),now=NOW,max_requests=20,fetch=fetch,sleep=lambda _:None)
    assert calls[4::5]==['SEC_SUBMISSIONS','LSEG_CATALOG','FINRA_DAILY','JPM_UNIVERSE']
    assert result['state']['pending'] and len(state['pending'])==59


def test_reference_reservation_survives_one_request_batches_and_cooldowns(monkeypatch):
    state=p.initial_state('2026-11-23')
    blocked=s.task('SEC_SUBMISSIONS','0000320193')
    state['pending']=[s.task('CITI_NOTICE',str(n)) for n in range(1,20)]+[blocked,s.task('FINRA_DAILY',day='2026-11-20')]
    state['cooldowns']['data.sec.gov']=dict(reason='HTTP_403',retry_at='2026-11-26T00:00:00+00:00')
    calls=[]
    monkeypatch.setattr(p,'plan',lambda state,*args:state)
    monkeypatch.setattr(s,'validate_response',lambda *args:None)
    monkeypatch.setattr(s,'discovered',lambda *args:[])
    def fetch(t):
        calls.append(t['kind'])
        return 200,{},b'{}'
    for _ in range(5):
        state=p.collect(state,bootstrap(),now=NOW,max_requests=1,fetch=fetch,sleep=lambda _:None)['state']
    assert calls==['CITI_NOTICE']*4+['FINRA_DAILY']
    assert blocked in state['pending'] and state['cooldowns']['data.sec.gov']['reason']=='HTTP_403'


def test_observed_additional_russell_archives_join_existing_checkpoint_once():
    state=p.initial_state('2026-11-23')
    completed=s.task('RUSSELL_PDF',s.RUSSELL_ARCHIVE_REPORTS[0])
    state['checked'][s.digest(completed)]=dict(status='COLLECTED',checked_at=NOW.isoformat())
    p.plan(state,bootstrap(),NOW)
    urls={t['url'] for t in state['pending'] if t['kind']=='RUSSELL_PDF'}
    assert completed['url'] not in urls
    assert all('https://www.lseg.com/content/dam/ftse-russell/en_us/documents/other/'+name in urls for name in (
        'ru3000-deletions-20250627.pdf','ru3000-additions-20260605.pdf','ru3000-additions-20260522.pdf'))
    original=[s.digest(t) for t in state['pending'] if t['kind']=='RUSSELL_PDF']
    p.plan(state,bootstrap(),NOW)
    assert [s.digest(t) for t in state['pending'] if t['kind']=='RUSSELL_PDF']==original


def test_attachment_targets_preserve_issuer_and_fund_targets_and_are_idempotent(tmp_path):
    path=tmp_path/'bootstrap.json';initial=dict(version=2,ciks=['0000320193'],research_targets=[])
    path.write_text(json.dumps(initial))
    assert p.enqueue_attachments(path,['6832','6832'])['added']==1
    before=path.read_bytes();assert p.enqueue_attachments(path,['6832'])['status']=='UNCHANGED';assert path.read_bytes()==before
    p.enqueue_targets(path,[dict(fund='IWB',snapshot_date='2026-10-21')])
    config=json.loads(path.read_text());assert config['depositary_attachment_ids']==['6832'] and config['ciks']==initial['ciks'] and config['version']==3
    with pytest.raises(ValueError):p.enqueue_attachments(path,['https://evil.example/'])


def test_finra_empty_day_204_is_not_a_host_denial_or_an_invented_event():
    from unittest.mock import patch
    state=p.initial_state('2026-11-23');t=s.task('FINRA_DAILY',day='2026-11-22');state['pending']=[t]
    with patch.object(p,'plan',lambda state,*args:state):
        out=p.collect(state,bootstrap(),now=NOW,max_requests=1,fetch=lambda _: (204,{},b''),sleep=lambda _:None)
    assert not out['state']['cooldowns']
    assert out['state']['checked'][s.digest(t)]['status']=='SOURCE_EMPTY'
    assert base64.b64decode(out['records'][0]['body_base64'])==b'[]'


def test_monthly_unchanged_citi_cover_still_rechecks_old_attachment(monkeypatch):
    from datetime import timedelta
    import hashlib
    cover=s.task('CITI_NOTICE','2621');child=s.task('CITI_ATTACHMENT','6832');body=b'%PDF-fixture'
    state=p.initial_state('2026-11-23');state['pending']=[cover]
    state['seen'][s.digest(cover)]=hashlib.sha256(body).hexdigest();state['seen'][s.digest(child)]='a'*64
    state['checked'][s.digest(child)]={'status':'COLLECTED','checked_at':(NOW-timedelta(days=31)).isoformat()}
    monkeypatch.setattr(p,'plan',lambda state,*args:state)
    monkeypatch.setattr(s,'discovered',lambda *args:[child])
    out=p.collect(state,bootstrap(),now=NOW,max_requests=1,fetch=lambda _: (200,{},body),sleep=lambda _:None)
    assert child in out['state']['pending'] and out['records']==[]


def test_citi_attachment_backlog_is_not_left_behind_all_cover_notices(monkeypatch):
    state=p.initial_state('2026-11-23')
    state['pending']=[s.task('CITI_NOTICE',str(n)) for n in range(1,8)]+[s.task('CITI_ATTACHMENT',str(n)) for n in range(10,15)]+[s.task('FRED_INDEX','SP500')]
    calls=[]
    monkeypatch.setattr(p,'plan',lambda state,*args:state);monkeypatch.setattr(s,'discovered',lambda *args:[])
    def fetch(t):
        calls.append(t['kind'])
        body=b'observation_date,SP500\n2026-11-20,100\n' if t['kind']=='FRED_INDEX' else b'%PDF-fixture'
        return 200,{},body+str(len(calls)).encode() if t['kind']!='FRED_INDEX' else body
    p.collect(state,bootstrap(),now=NOW,max_requests=5,fetch=fetch,sleep=lambda _:None)
    assert calls==['CITI_NOTICE','CITI_ATTACHMENT','CITI_ATTACHMENT','CITI_ATTACHMENT','FRED_INDEX']


def bootstrap():
    return dict(through='2026-11-23',kr_sessions=['2010-01-04','2026-11-23'],us_month_ends=['2010-01-29','2026-10-30'],ciks=['0000320193'])


def test_403_is_a_checkpointed_host_cooldown_not_empty_success():
    state=p.initial_state('2026-11-23'); calls=[]
    def fetch(t): calls.append(t); return 403,{},b'denied'
    result=p.collect(state,bootstrap(),now=NOW,max_requests=20,fetch=fetch,sleep=lambda _:None)
    hosts=[__import__('urllib.parse',fromlist=['urlsplit']).urlsplit(t['url']).hostname for t in calls]
    assert len(hosts)==len(set(hosts)), 'A denied host must not be hammered for every queued security'
    assert not result['records'] and not result['state']['seen']
    assert all(v['reason']=='HTTP_403' for v in result['state']['cooldowns'].values())
    assert state==p.initial_state('2026-11-23'), 'Tentative collection cannot mutate its authenticated predecessor'


def test_successful_batch_resumes_without_refetching_completed_task():
    state=p.initial_state('2026-11-23'); calls=[]
    def fetch(t): calls.append(t); return 200,{},fund_body()
    first=p.collect(state,bootstrap(),now=NOW,max_requests=1,fetch=fetch,sleep=lambda _:None,source_kinds=['FUND_LATEST'])
    assert len(first['records'])==1 and calls[0]['entity']=='IVV'
    p.collect(first['state'],bootstrap(),now=NOW,max_requests=1,fetch=lambda t:(calls.append(t) or (403,{},b'')),sleep=lambda _:None,source_kinds=['FUND_LATEST'])
    assert calls[1]['entity']=='IWB' and calls[1]!=calls[0]


def test_pending_topup_is_bounded_even_with_denied_providers():
    state=p.initial_state('2026-11-23'); b=bootstrap(); b['kr_sessions']=['2010-01-04']*1000
    for _ in range(1000): p.plan(state,b,NOW)
    # Fixed, verified archive seeds are separate from bounded queue top-ups.
    fund_maps=sum(t['kind']=='SEC_FUND_MAP' for t in state['pending'])
    assert fund_maps==1
    assert len(state['pending'])-len(s.RUSSELL_ARCHIVE_REPORTS)-fund_maps<100


def test_old_catalog_checkpoint_migrates_without_repeating_404_or_losing_scope():
    state=p.initial_state('2026-11-23'); old=s.task('SP_CATALOG','2024',page=1)
    state['pending']=[old]; state['cursors']['daily']=NOW.date().isoformat()
    p.plan(state,bootstrap(),NOW)
    assert not any(t['kind']=='SP_CATALOG' for t in state['pending'])
    assert s.task('SP_ARCHIVE','2024',page=1) in state['pending']
    assert state['checked'][s.digest(old)]['status']=='RETIRED_ENDPOINT_REPLACED'


def test_current_refresh_reuses_keys_every_six_hours_instead_of_growing_daily_ids():
    from datetime import timedelta
    state=p.initial_state('2026-11-23'); keys=set()
    for cycle in range(12):
        now=NOW+timedelta(hours=6*cycle)
        p.plan(state,bootstrap(),now)
        current=[t for t in state['pending'] if t['kind']=='FUND_LATEST']
        assert len(current)==5 and all(t['requested_date']=='' for t in current)
        keys.update(s.digest(t) for t in current)
        for t in current: state['checked'][s.digest(t)]=dict(status='COLLECTED',checked_at=now.isoformat())
        state['pending']=[t for t in state['pending'] if t['kind']!='FUND_LATEST']
    assert len(keys)==5


def test_weekly_sec_refresh_reuses_stable_issuer_task_identity():
    from datetime import timedelta
    state=p.initial_state('2026-11-23'); p.plan(state,bootstrap(),NOW)
    old=[t for t in state['pending'] if t['kind'] in ('SEC_FACTS','SEC_SUBMISSIONS')]
    assert len(old)==2
    for t in old: state['checked'][s.digest(t)]=dict(status='COLLECTED',checked_at=NOW.isoformat())
    state['pending']=[];state['cursors']['sec']=1
    p.plan(state,bootstrap(),NOW+timedelta(days=8))
    new=[t for t in state['pending'] if t['kind'] in ('SEC_FACTS','SEC_SUBMISSIONS')]
    assert new==old and all(t['requested_date']=='' for t in new)


def test_separate_owner_cannot_modify_existing_korean_stream(monkeypatch):
    from price_lab import research_ownership as own
    from test_research_ownership import Contents
    stores={}; paths=[]
    def api(method,path,**kw):
        paths.append(path)
        namespace='market' if 'market-reference-owner' in path else 'kr'
        return stores.setdefault(namespace,Contents()).api(method,path,**kw)
    monkeypatch.setattr(own,'_api',api)
    kr=own.ResearchOwner('local',key=b'x'*32,clock=lambda:NOW)
    market=own.ResearchOwner('cloud',key=b'x'*32,clock=lambda:NOW,namespace='market-references')
    assert kr.acquire(anchor_sha256='a'*64)
    before=stores['kr'].content
    assert market.acquire(anchor_sha256=s.ANCHOR)
    market.commit_head(payload()); market.release()
    assert stores['kr'].content==before
    assert any('/market-reference-owner.json' in path for path in paths)


def test_bootstrap_rejects_nonpublic_arbitrary_identifiers(tmp_path):
    config=tmp_path/'seed.json'; config.write_text(json.dumps({'version':1,'ciks':['http://evil/']}))
    with pytest.raises(ValueError): p.bootstrap(config,now=NOW)


def test_cloud_import_never_loads_database_or_private_credentials():
    root=Path(__file__).resolve().parents[1]
    code="import runpy,sys;runpy.run_path('scripts/market_reference_pipeline.py',run_name='inspect');from price_lab import market_reference_pipeline;assert 'duckdb' not in sys.modules;assert 'price_lab.local_credentials' not in sys.modules"
    result=subprocess.run([sys.executable,'-B','-c',code],cwd=root,capture_output=True)
    assert result.returncode==0,result.stderr.decode(errors='replace')
def test_immutable_nasdaq_report_is_discovered_once_without_resetting_denied_hosts():
    state=p.initial_state('2026-11-23')
    state['cooldowns']['ir.nasdaq.com']=dict(reason='HTTP_403',retry_at='2026-11-25T00:00:00+00:00')
    p.plan(state,bootstrap(),NOW)
    reports=[t for t in state['pending'] if t['kind']=='NASDAQ_PDF']
    assert len(reports)==1
    state['checked'][s.digest(reports[0])]=dict(status='COLLECTED',checked_at=NOW.isoformat())
    state['pending']=[t for t in state['pending'] if t['kind']!='NASDAQ_PDF']
    p.plan(state,bootstrap(),NOW)
    assert not any(t['kind']=='NASDAQ_PDF' for t in state['pending'])
    assert state['cooldowns']['ir.nasdaq.com']['reason']=='HTTP_403'


def test_recent_research_months_are_backfilled_without_replaying_checked_snapshots():
    state=p.initial_state('2026-11-23');b=bootstrap()
    b['us_month_ends']=['2010-01-29','2026-08-31','2026-09-30','2026-10-30']
    p.plan(state,b,NOW)
    expected=s.task('FUND','IVV','2026-09-30')
    assert expected in state['pending']
    state['checked'][s.digest(expected)]=dict(status='COLLECTED',checked_at=NOW.isoformat())
    state['pending']=[t for t in state['pending'] if t!=expected]
    p.plan(state,b,NOW)
    assert expected not in state['pending']


def test_targeted_snapshot_config_is_backward_compatible_and_idempotent(tmp_path):
    path=tmp_path/'seed.json';path.write_text(json.dumps({'version':1,'ciks':['0000320193']}))
    target=dict(fund='IWB',snapshot_date='2026-10-21')
    assert p.enqueue_targets(path,[target])['status']=='UPDATED'
    raw=path.read_bytes()
    assert p.enqueue_targets(path,[target])['status']=='UNCHANGED' and path.read_bytes()==raw
    b=p.bootstrap(path,now=NOW)
    assert b['research_targets']==[target] and b['ciks']==['0000320193']
    state=p.initial_state(b['through']);p.plan(state,b,NOW)
    task=s.task('FUND','IWB','2026-10-21')
    assert task in state['pending']
    state['checked'][s.digest(task)]=dict(status='COLLECTED',checked_at=NOW.isoformat())
    state['pending']=[t for t in state['pending'] if t!=task];p.plan(state,b,NOW)
    assert task not in state['pending']


@pytest.mark.parametrize('target',[
    dict(fund='EVIL',snapshot_date='2026-10-21'),
    dict(fund='IWB',snapshot_date='2999-01-01'),
    dict(fund='IWB',snapshot_date='invalid'),
    dict(fund='IWB',snapshot_date='2026-10-21',url='https://example.com'),
])
def test_targeted_snapshot_config_rejects_unbounded_or_nonpublic_inputs(tmp_path,target):
    path=tmp_path/'seed.json';path.write_text(json.dumps({'version':2,'ciks':[], 'research_targets':[target]}))
    with pytest.raises(ValueError):p.bootstrap(path,now=NOW)


def test_new_factsheet_is_found_even_inside_an_existing_refresh_window():
    state=p.initial_state('2026-11-23');p.plan(state,bootstrap(),NOW)
    task=s.task('NASDAQ_FACTSHEET','NDX')
    state['pending']=[t for t in state['pending'] if t!=task]
    p.plan(state,bootstrap(),NOW)
    assert task in state['pending']


def test_recent_completed_index_week_is_not_starved_by_unpublished_latest_session():
    b=bootstrap();b['kr_sessions']=['2026-11-16','2026-11-17','2026-11-18','2026-11-19','2026-11-20','2026-11-23']
    state=p.initial_state(b['through']);p.plan(state,b,NOW)
    for day in b['kr_sessions'][-5:]:
        assert all(s.task('KRX_INDEX',family,day) in state['pending'] for family in ('kospi','kosdaq','krx'))
    completed=s.task('KRX_INDEX','kospi','2026-11-20')
    state['checked'][s.digest(completed)]=dict(status='COLLECTED',checked_at=NOW.isoformat())
    state['pending']=[t for t in state['pending'] if t!=completed];p.plan(state,b,NOW)
    assert completed not in state['pending']
