import base64
from datetime import datetime,timezone
import hashlib
import json
import pytest
from price_lab import market_reference_sources as s
from price_lab.research_delta_codec import seal_package,open_package,package_digest


def fund_body(day=20261120):
    points={'asOfDate':{'value':day},'ticker':{'value':['A']},'sectorName':{'value':['Technology']},
            'assetClass':{'value':['Equity']},'marketWeight':{'value':[2.5]}}
    return json.dumps({'productId':239726,'currencyCode':'USD','componentsByNameMap':{'holdings':{'containersByNameMap':{'all':{'dataPointsByNameMap':points}}}}}).encode()


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
