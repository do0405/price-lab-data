import base64
from datetime import datetime,timezone
import hashlib
import json
import pytest


def test_current_us_market_sources_have_fixed_bounded_contracts():
    from price_lab import market_reference_sources as source
    body=b'Symbol|Security Name|ETF|Test Issue\nABC|ABC Common Stock|N|N\nFile Creation Time: 1126202610:00|||\n'
    source.validate_response(source.task('NASDAQ_DIRECTORY','nasdaq'),body)
    with pytest.raises(ValueError):source.task('NASDAQ_DIRECTORY','https://example.com')
    with pytest.raises(ValueError):source.validate_response(source.task('NASDAQ_DIRECTORY','nasdaq'),body.split(b'File Creation')[0])
    source.validate_response(source.task('NASDAQ_CAPS'),json.dumps({'data':{'table':{'rows':[{'symbol':'ABC','marketCap':'1000'}]}}}).encode())
    with pytest.raises(ValueError):source.task('NASDAQ_CAPS','arbitrary')


def test_sec_primary_uses_exact_official_document_and_dated_metadata():
    entity='0001023128:0001023128-26-000060:10-Q:lad-20260930.htm'
    t=s.task('SEC_PRIMARY',entity,'2026-10-28')
    assert t['url']=='https://www.sec.gov/Archives/edgar/data/1023128/000102312826000060/lad-20260930.htm'
    assert s.sec_primary_metadata(t)['form']=='10-Q'
    s.validate_task(t)
    for bad in ['../other.htm','https://other.test/doc.htm','one.htm?token=x']:
        with pytest.raises(ValueError):s.task('SEC_PRIMARY',entity.rsplit(':',1)[0]+':'+bad,'2026-10-28')
    with pytest.raises(ValueError):s.validate_response(t,b'{"error":"not a filing"}')


def test_fund_reports_are_discovered_for_continuous_share_updates():
    recent=dict(form=['N-CSRS','N-CSR'],filingDate=['2026-09-03','2026-03-03'],
        accessionNumber=['0001193125-26-381557','0001193125-26-081557'],primaryDocument=['semi.htm','annual.htm'])
    body=json.dumps(dict(cik=1160864,filings={'recent':recent,'files':[]})).encode()
    tasks=s.discovered(s.task('SEC_SUBMISSIONS','0001160864'),body,primary=True,through='2026-11-24')
    assert {s.sec_primary_metadata(t)['form'] for t in tasks}=={'N-CSRS','N-CSR'}


def yahoo_body(symbol='AATC',currency='USD'):
    stamp=int(datetime(2026,11,24,14,30,tzinfo=timezone.utc).timestamp())
    return json.dumps({'chart':{'error':None,'result':[{'meta':{'symbol':symbol,'currency':currency},
        'timestamp':[stamp],'indicators':{'quote':[{'close':[2.5],'volume':[100]}]}}]}}).encode()


def test_yahoo_price_is_an_exact_bounded_dated_public_quote():
    t=s.task('YAHOO_PRICE','AATC','2026-11-24');s.validate_task(t)
    assert t['url']=='https://query1.finance.yahoo.com/v8/finance/chart/AATC'
    s.validate_response(t,yahoo_body())
    with pytest.raises(ValueError):s.task('YAHOO_PRICE','../secret','2026-11-24')
    with pytest.raises(ValueError):s.validate_response(t,yahoo_body('OTHER'))
    with pytest.raises(ValueError):s.validate_response(t,yahoo_body(currency='CAD'))


def test_targeted_submissions_discover_bounded_primary_reports_only_when_requested():
    t=s.task('SEC_SUBMISSIONS','0001023128')
    recent=dict(form=['10-Q','10-Q','10-K','8-K','10-Q'],filingDate=['2026-10-28','2026-07-28','2026-02-28','2026-11-01','2027-01-01'],
        accessionNumber=['0001023128-26-000060','0001023128-26-000040','0001023128-26-000020','0001023128-26-000070','0001023128-27-000001'],
        primaryDocument=['q3.htm','q2.htm','annual.htm','event.htm','future.htm'])
    body=json.dumps(dict(cik=1023128,filings={'recent':recent,'files':[]})).encode()
    assert not s.discovered(t,body)
    tasks=s.discovered(t,body,primary=True,through='2026-11-24')
    assert len(tasks)==3 and all(x['kind']=='SEC_PRIMARY' for x in tasks)
    assert {s.sec_primary_metadata(x)['form'] for x in tasks}=={'10-Q','10-K'}
from price_lab import market_reference_sources as s
from price_lab.research_delta_codec import seal_package,open_package,package_digest


def fund_body(day=20261120):
    points={'asOfDate':{'value':day},'ticker':{'value':['A']},'sectorName':{'value':['Technology']},
            'assetClass':{'value':['Equity']},'marketWeight':{'value':[2.5]}}
    return json.dumps({'productId':239726,'currencyCode':'USD','componentsByNameMap':{'holdings':{'containersByNameMap':{'all':{'dataPointsByNameMap':points}}}}}).encode()


def jpm_body(items,*,offset=0,total=None):
    total=len(items)+offset if total is None else total
    return json.dumps({'data':{'items':items,'pagination':{'currentItemCount':len(items),
        'hasMore':offset+len(items)<total,'limit':1000,'offset':offset,'totalItems':total}}}).encode()


def test_jpm_public_catalog_pagination_is_exact_and_bounded():
    t=s.task('JPM_UNIVERSE');row={'cusip':'471115402','ticker':'JMHLY','name':'Example','globalMarketCap':123.5}
    body=jpm_body([{**row,'cusip':str(100000000+n)} for n in range(1000)],total=1001)
    s.validate_response(t,body)
    assert s.discovered(t,body)==[s.task('JPM_UNIVERSE',page=1)]
    s.validate_response(s.task('JPM_UNIVERSE',page=1),jpm_body([row],offset=1000,total=1001))
    assert not s.discovered(s.task('JPM_UNIVERSE',page=1),jpm_body([row],offset=1000,total=1001))
    for bad in [jpm_body([row],offset=1000),jpm_body([{**row,'globalMarketCap':-1}]),jpm_body([{**row,'cusip':'https://evil.example'}])]:
        with pytest.raises(ValueError):s.validate_response(t,bad)
    lower={**row,'cusip':'471115a02'};s.validate_response(t,jpm_body([lower]))
    assert s.jpm_page(t,jpm_body([row,row]))[0]==[row]
    with pytest.raises(ValueError):s.task('JPM_UNIVERSE','https://evil.example')


def test_jpm_action_contract_retains_reported_date_and_document_revision():
    row={'cusip':'471115402','action':'Ratio Change','eventDate':'2024-10-01T00:00:00.000Z',
        'announcementDate':'2024-09-10T00:00:00.000Z','notices':{'pdf':'a'*32},'sequenceNo':3}
    t=s.task('JPM_ACTIONS');s.validate_response(t,jpm_body([row]))
    for action in (None,''):
        s.validate_response(t,jpm_body([{**row,'action':action}]))
    with pytest.raises(ValueError):s.validate_response(t,jpm_body([{**row,'eventDate':'bad'}]))
    with pytest.raises(ValueError):s.validate_response(t,jpm_body([{**row,'notices':{'pdf':'https://evil.example'}}]))
    found=s.discovered(t,jpm_body([row]))
    assert found==[s.task('JPM_DOCUMENT','a'*32+':3')]
    assert found[0]['params']=={'cmsId':'a'*32,'sequenceNo':3}
    assert not s.discovered(t,jpm_body([{**row,'action':'Dividend'}]))
    with pytest.raises(ValueError):s.task('JPM_DOCUMENT','https://evil.example')


def payload():
    from price_lab.market_reference_pipeline import initial_state
    body=fund_body(); t=s.task('FUND','IVV','2026-11-20')
    return dict(schema='research-delta-v1',stream_id=s.STREAM,sequence=1,previous_package_sha256=None,
                bootstrap_anchor_sha256=s.ANCHOR,collected_at='2026-11-24T00:00:00+00:00',
                records=[dict(task=t,retrieved_at='2026-11-23T23:00:00+00:00',body_base64=base64.b64encode(body).decode(),body_sha256=hashlib.sha256(body).hexdigest())],
                state=initial_state('2026-11-23'))


@pytest.mark.parametrize('field,value',[('url','https://attacker.example/'),('params',{'AUTH_KEY':'secret'}),('method','PUT'),('entity','IWM')])
def test_endpoint_and_parameter_tampering_rejected(field,value):
    t=s.task('FUND','IVV','2026-11-20'); t[field]=value
    with pytest.raises(ValueError): s.validate_task(t)


def test_exact_historical_fund_date_and_column_lengths():
    t=s.task('FUND','IVV','2026-11-20'); s.validate_response(t,fund_body())
    with pytest.raises(ValueError): s.validate_response(t,fund_body(20261119))
    value=json.loads(fund_body()); value['componentsByNameMap']['holdings']['containersByNameMap']['all']['dataPointsByNameMap']['sectorName']['value']=[]
    with pytest.raises(ValueError): s.validate_response(t,json.dumps(value).encode())


def test_html_login_or_challenge_is_not_data():
    t=s.task('SP_CATALOG','2026')
    with pytest.raises(ValueError): s.validate_response(t,b'<html><body>Access Denied</body></html>')
    with pytest.raises(ValueError): s.validate_response(s.task('KIND_INDUSTRY'),b'<html>Login</html>')


def test_followups_are_official_and_deduplicated():
    t=s.task('RUSSELL_PAGE')
    body=b'<html><a href="/content/dam/ftse-russell/en_us/documents/other/ru3000-additions-20261120.pdf">a</a><a href="https://evil.example/ru3000-additions-20261120.pdf">b</a></html>'
    found=s.discovered(t,body)
    assert len(found)==1 and found[0]['kind']=='RUSSELL_PDF'
    assert not s.discovered(s.task('SP_CATALOG','2026'),b'<a href="https://evil.example/2026-11-20-S-P-500">x</a>')


def test_lseg_pagination_retains_year_and_stops_at_total():
    t=s.task('LSEG_CATALOG','2016')
    assert s.discovered(t,b'{"Data":[],"TotalCount":51}')[0]['page']==1
    assert not s.discovered(s.task('LSEG_CATALOG','2016',page=1),b'{"Data":[],"TotalCount":51}')


def test_lseg_accepts_official_integral_float_count_but_rejects_fractional():
    t=s.task('LSEG_CATALOG','2026')
    s.validate_response(t,b'{"Data":[],"TotalCount":2842.0}')
    with pytest.raises(ValueError): s.validate_response(t,b'{"Data":[],"TotalCount":2.5}')


def test_sp_archive_follows_only_next_page_of_requested_year():
    t=s.task('SP_ARCHIVE','2024',day='2026-11-24')
    found=s.discovered(t,b'<html><a href="https://press.spglobal.com/index.php?s=2429&amp;year=2024&amp;l=100&amp;o=100">Next</a></html>')
    assert len(found)==1 and found[0]==s.task('SP_ARCHIVE','2024',day='2026-11-24',page=1)


def test_authenticated_separate_stream_roundtrip_and_corruption(tmp_path):
    p=payload(); path=tmp_path/'reference.pldelta'; key=b'x'*32
    seal_package(p,path,key); assert open_package(path,key)==p
    altered=dict(p,bootstrap_anchor_sha256='b'*64)
    with pytest.raises(ValueError): package_digest(altered)
    p['records'][0]['body_base64']=base64.b64encode(b'changed').decode()
    with pytest.raises(ValueError): package_digest(p)


def test_index_response_date_cannot_be_substituted():
    t=s.task('KRX_INDEX','kospi','2026-11-20')
    with pytest.raises(ValueError): s.validate_response(t,b'{"OutBlock_1":[{"BAS_DD":"20261119","IDX_NM":"KOSPI","CLSPRC_IDX":"100"}]}')


def test_index_response_must_match_requested_family():
    t=s.task('KRX_INDEX','kospi','2026-11-20')
    body=b'{"OutBlock_1":[{"BAS_DD":"20261120","IDX_CLSS":"KOSDAQ","IDX_NM":"KOSDAQ","CLSPRC_IDX":"100"}]}'
    with pytest.raises(ValueError): s.validate_response(t,body)
def test_nasdaq_research_pdf_has_an_exact_official_scope():
    from price_lab import market_reference_sources as s
    url='https://indexes.nasdaqomx.com/docs/202601%20NDX%20Reconstitution%20and%20Performance%20Highlights.pdf'
    task=s.task('NASDAQ_PDF',url)
    assert task['url']==url and s.validate_response(task,b'%PDF-1.7\nfixture')
    for invalid in (url.replace('indexes.nasdaqomx.com','example.com'),url+'?token=secret',url.replace('/docs/','/private/')):
        with pytest.raises(ValueError):s.task('NASDAQ_PDF',invalid)


def test_nasdaq_factsheet_endpoint_is_fixed_and_current_not_an_arbitrary_pdf():
    t=s.task('NASDAQ_FACTSHEET','NDX')
    assert t['url']=='https://indexes.nasdaq.com/docs/FS_NDX.pdf'
    assert s.validate_response(t,b'%PDF-1.7\nfixture')
    with pytest.raises(ValueError):s.task('NASDAQ_FACTSHEET','https://example.com/a.pdf')


def test_official_weight_document_uses_the_fixed_public_pdf_endpoint():
    task=s.task('NASDAQ_WEIGHTS','NDX')
    assert task['url']=='https://www.nasdaq.com/docs/global-indexes/NDX'
    assert s.validate_response(task,b'%PDF-1.7\nfixture')
    with pytest.raises(ValueError):s.task('NASDAQ_WEIGHTS','SP500')


def citi_catalog_body():
    return b'''<html><table id="tblCANotices"><tr><td>Company</td><td>Date of Notice</td><td></td><td>Ticker</td><td>CUSIP</td><td>Ratio (ORD:DR)</td><td>Type</td><td>Country</td><td>Exchange</td><td>Corporate Action Type</td></tr>
    <tr><td>Example</td><td>07/14/2016</td><td><a href="previewPdf.aspx?caId=513">PDF</a></td><td>EXM</td><td>756568101</td><td>1:2</td><td>ADR</td><td>Spain</td><td>OTC</td><td>Ratio Change</td></tr></table></html>'''


def test_citi_catalog_requires_exact_table_and_discovers_only_reported_pdf_ids():
    task=s.task('CITI_CATALOG');body=citi_catalog_body()
    assert s.validate_response(task,body)==body
    row=s.citi_notices(body)[0]
    assert row['notice_id']=='513' and row['notice_date']=='2016-07-14'
    assert row['ratio_reported']=='1:2' and row['cusip']=='756568101'
    assert s.discovered(task,body)==[s.task('CITI_NOTICE','513')]
    for bad in [body.replace(b'previewPdf.aspx?caId=513',b'https://evil.example/previewPdf.aspx?caId=513'),body.replace(b'1:2',b'0:2'),body.replace(b'1:2',b'9'*400+b':1'),body.replace(b'07/14/2016',b'02/31/2016')]:
        with pytest.raises(ValueError):s.validate_response(task,bad)


def test_extended_reference_endpoints_are_fixed_and_narrow():
    for kind,entity in [('CITI_NOTICE','513'),('LSEG_WEIGHTS','US1000'),('RUSSELL_IPO_PDF','3000'),('BNY_NOTICE','AD2000235')]:
        t=s.task(kind,entity);assert s.validate_response(t,b'%PDF-1.7\nfixture')
        with pytest.raises(ValueError):s.task(kind,'https://evil.example/notice.pdf')
    with pytest.raises(ValueError):s.task('LSEG_WEIGHTS','UNKNOWN')
    for kind in ['CITI_CATALOG','LSEG_WEIGHT_CATALOG','RUSSELL_IPO_PAGE']:
        with pytest.raises(ValueError):s.task(kind,'https://evil.example/')


def test_lseg_weights_catalog_selects_allowlisted_core_indices_and_exact_download_url():
    t=s.task('LSEG_WEIGHT_CATALOG')
    url=s.task('LSEG_WEIGHTS','US1000')['url']+'?indexdetails=US1000'
    body=json.dumps({'Data':[{'IndexId':'US1000','IndexName':'Russell 1000','Url':url,'ReportType':'StocksWeight'},
        {'IndexId':'OTHER','IndexName':'Other','Url':'https://evil.example/','ReportType':'StocksWeight'}]}).encode()
    assert s.discovered(t,body)==[s.task('LSEG_WEIGHTS','US1000')]
    assert s.weight_catalog(body.replace(b'Russell 1000',b'Russell\xa01000'))==['US1000']
    with pytest.raises(ValueError):s.validate_response(t,body.replace(url.encode(),b'https://evil.example/'))


def test_finra_daily_is_date_scoped_json_and_paginates_only_a_full_page():
    t=s.task('FINRA_DAILY',day='2026-11-20')
    assert t['method']=='POST' and t['params']['dateRangeFilters'][0]=={
        'fieldName':'calendarDay','startDate':'2026-11-20','endDate':'2026-11-20'}
    rows=[{'OTCDailyListID':n+1,'calendarDay':'2026-11-20','dailyListEventCode':'DA'} for n in range(1000)]
    body=json.dumps(rows).encode();assert s.validate_response(t,body)==body
    assert s.discovered(t,body)==[s.task('FINRA_DAILY',day='2026-11-20',page=1)]
    assert not s.discovered(t,b'[]') and s.validate_response(t,b'[]')==b'[]'
    with pytest.raises(ValueError):s.validate_response(t,body.replace(b'2026-11-20',b'2026-11-19'))
    with pytest.raises(ValueError):s.task('FINRA_DAILY',day='2010-01-01')
    with pytest.raises(ValueError):s.task('FINRA_DAILY',day='2026-11-20',page=501)


def test_citi_pdf_follows_only_exact_official_attachment_annotations(monkeypatch):
    import pypdf
    from types import SimpleNamespace
    class Ref(dict):
        def get_object(self):return self
    urls=['https://depositaryreceipts.citi.com/adr/common/file.aspx?idf=6832',
        'https://evil.example/adr/common/file.aspx?idf=6832',
        'https://depositaryreceipts.citi.com/private/file.aspx?idf=6832']
    page={'/Annots':[Ref({'/A':{'/URI':u}}) for u in urls]}
    monkeypatch.setattr(pypdf,'PdfReader',lambda *_:SimpleNamespace(pages=[page]))
    assert s.discovered(s.task('CITI_NOTICE','2621'),b'%PDF-fixture')==[s.task('CITI_ATTACHMENT','6832')]


def test_ipo_discovery_follows_new_quarters_without_guessing_absent_files():
    t=s.task('RUSSELL_IPO_PAGE');q1=s.task('RUSSELL_IPO_PDF','3000',page=1)
    body=('<html><a href="'+q1['url']+'">Q1</a><a href="https://evil.example/final-ipo-additions-4-qtr-r3000.pdf">x</a></html>').encode()
    assert s.discovered(t,body)==[q1]
    assert '3-qtr' in s.task('RUSSELL_IPO_PDF','3000')['url']
