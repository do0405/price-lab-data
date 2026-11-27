"""Public reference/secondary quote contracts; no database or credential I/O."""
import base64
import csv
from decimal import Decimal,InvalidOperation
from datetime import date, datetime, timezone
import hashlib
import io
from html.parser import HTMLParser
import json
import math
import re
from urllib.parse import urljoin, urlsplit,parse_qs

STREAM = 'market-reference-v1'
ANCHOR = hashlib.sha256(b'official-market-reference-plan-v1').hexdigest()
FUNDS = {'IVV': (239726, 'SP500', 'us-ishares', 'en_US'),
         'IWB': (239707, 'RUSSELL1000', 'us-ishares', 'en_US'),
         'IWM': (239710, 'RUSSELL2000', 'us-ishares', 'en_US'),
         'IWV': (239714, 'RUSSELL3000', 'us-ishares', 'en_US'),
         'CNDX': (253741, 'NASDAQ100', 'uk', 'en_GB')}
ISHARES = 'https://www.ishares.com/varnish-api/blk-one01-product-data/product-data/api/v2/get-product-data'
NASDAQ_DIRECTORIES={'nasdaq':'https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt',
                    'other':'https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt'}
LSEG = 'https://www.lseg.com/content/lseg/en_us/ftse-russell/index-resources/notices/jcr:content/root/container/page_content_region/page-content-region/section/section-fw/index_announcements_.indexannouncement.json'
MAX_BODY = 28 * 1024**2


def response_limit(request):
    # The complete SEC name directory is ~39 MiB. Other provider contracts
    # keep their original limit; this still fits one bounded encrypted batch.
    return 42*1024**2 if request['kind']=='SEC_CIK_NAMES' else MAX_BODY


def sec_cik_names(body):
    try:text=body.decode('utf-8-sig')
    except UnicodeDecodeError:text=body.decode('latin-1')
    for line in text.splitlines():
        if not line.strip():continue
        fields=line.rsplit(':',2)
        if len(fields)!=3 or fields[2] or not re.fullmatch(r'[0-9]{10}',fields[1]):
            raise ValueError('SEC name/CIK line contract mismatch')
        if fields[0].strip() and int(fields[1])>0:yield fields[0].strip(),fields[1]


def stockanalysis_quote(request,body):
    """Read a flat public quote literal; never evaluate the page's scripts."""
    text=body.decode('utf-8-sig')
    def field(value,key):
        m=re.search(r'(?:^|,)\s*'+re.escape(key)+r':("(?:\\.|[^"\\])*"|-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?|true|false|null)(?=\s*(?:,|$))',value)
        if not m:return None
        token=m[1]
        return json.loads(token) if token.startswith('"') or token in ('true','false','null') else float(token)
    currencies=re.findall(r'\bcurr:\{([^{}]{1,1000})\}',text)
    if len(currencies)!=1 or field(currencies[0],'price')!='USD':raise ValueError('Explicit USD quote currency required')
    declared=set(re.findall(r'\bexchange_symbol:"([A-Z][A-Z0-9.-]{0,19})"',text))
    if declared!={request['entity']}:raise ValueError('Exact main-page exchange symbol required')
    capital=None
    tokens=re.findall(r'\bmarketCap:(-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)',text)
    if field(currencies[0],'main')=='USD' and len(tokens)==1:
        try:
            value=Decimal(tokens[0])
            if value.is_finite() and value>0:capital=str(value)
        except InvalidOperation:pass
    candidates=[]
    for value in re.findall(r'\bquote:\{([^{}]{1,6000})\}',text):
        if field(value,'uid')!='OTC-'+request['entity'] or field(value,'symbol')!='OTC-'+request['entity']:continue
        day=field(value,'td');price=field(value,'p');volume=field(value,'v')
        if not isinstance(day,str):continue
        date.fromisoformat(day)
        if not '2010-01-01'<=day<=request['requested_date']:continue
        if type(price) not in (int,float) or not math.isfinite(price) or price<=0:continue
        if type(volume) not in (int,float) or not math.isfinite(volume) or volume<0:continue
        candidates.append(dict(symbol=request['entity'],currency='USD',price_asof=day,price=price,volume=volume,
            price_basis='SECONDARY_PUBLIC_QUOTE_SPLIT_BASIS_UNVERIFIED',provider='STOCKANALYSIS_PUBLIC_PAGE',
            market_cap_usd=capital,capitalization_basis='CURRENT_AT_RETRIEVAL_NOT_HISTORICAL'))
    if len(candidates)!=1:raise ValueError('One exact dated public quote required')
    return candidates[0]
NASDAQ_REPORTS=('https://indexes.nasdaqomx.com/docs/202601%20NDX%20Reconstitution%20and%20Performance%20Highlights.pdf',)
LSEG_WEIGHT_CATALOG='https://www.lseg.com/content/lseg/en_us/ftse-russell/index-resources/constituent-weights/jcr:content/root/container/page_content_region/page-content-region/section/section-fw/data_table.constituentsandweights.json'
LSEG_WEIGHT_DOWNLOAD='https://research.ftserussell.com/analytics/factsheets/Home/DownloadConstituentsWeights/'
LSEG_WEIGHT_INDICES={f'US{int(size)+offset}':('RUSSELL'+size+suffix,'Russell '+size+label)
    for size in ('1000','2000','3000') for offset,suffix,label in (
        (0,'',''),(1,'_GROWTH',' Growth'),(2,'_VALUE',' Value'))}
RUSSELL_ARCHIVE_REPORTS=(
    'https://www.lseg.com/content/dam/ftse-russell/en_us/documents/other/ru3000-additions-20250613.pdf',
    'https://www.lseg.com/content/dam/ftse-russell/en_us/documents/other/ru3000-additions-20260618.pdf',
    'https://www.lseg.com/content/dam/ftse-russell/en_us/documents/other/ru3000-additions-preliminary-20240524.pdf',
    'https://www.lseg.com/content/dam/ftse-russell/en_us/documents/other/ru3000-deletions-preliminary-20240524.pdf',
    'https://www.lseg.com/content/dam/ftse-russell/en_us/documents/other/ru3000-deletions-20250627.pdf',
    'https://www.lseg.com/content/dam/ftse-russell/en_us/documents/other/ru3000-additions-20260605.pdf',
    'https://www.lseg.com/content/dam/ftse-russell/en_us/documents/other/ru3000-additions-20260522.pdf')
CITI_CATALOG='https://depositaryreceipts.citi.com/adr/notices/pgm_dispCA.aspx?pageId=16&subpageID=112'
CITI_PDF='https://depositaryreceipts.citi.com/adr/notices/previewPdf.aspx'
JPM_PUBLIC='https://adr.api.wsod.com/jpmadr-public/v1/'
JPM_ECONOMIC_ACTIONS={'Ratio Change','Termination','Merger/Exchange','Stock Split','Share Consolidation/Capital Repayment','Spin-off'}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def sec_request_identity(value):
    """Validate an existing declared bot identity; never invent contact details."""
    if not isinstance(value,str) or not value.isascii() or not 5<=len(value)<=512 or any(c in value for c in '\r\n\x00'):
        return None
    match=re.search(r'[^\s<>@]+@([A-Za-z0-9.-]+\.[A-Za-z]{2,})',value)
    if not match or len(value[:match.start()].strip())<3:return None
    domain=match[1].lower()
    if domain in ('example.com','example.org','example.net') or domain.endswith(('.invalid','.test')):return None
    return value.strip()


def sec_failure_diagnostic(status,headers,body):
    """One bounded fingerprint/classification, no raw HTML, contact or headers."""
    headers={k.lower():str(v) for k,v in headers.items()};sample=body[:65536].decode('utf-8',errors='replace').lower()
    signals=[]
    if headers.get('x-mitmproxy-blocked-reason','').upper()=='ROBOTS_DENIED':signals.append('EGRESS_PROXY_ROBOTS_DENIED')
    if 'undeclared automated tool' in sample:signals.append('UNDECLARED_AUTOMATION')
    if 'request rate threshold exceeded' in sample:signals.append('REQUEST_RATE_THRESHOLD')
    classification=signals[0] if signals else 'ACCESS_DENIED_UNSPECIFIED'
    result=dict(http_status=status,classification=classification,policy_signals=signals,
        body_bytes=len(body),body_sha256=hashlib.sha256(body).hexdigest())
    content=headers.get('content-type','').split(';',1)[0].strip().lower()
    if content in ('text/html','text/plain','application/json'):result['content_type']=content
    retry=headers.get('retry-after','')
    if retry.isdecimal():result['retry_after_seconds']=min(int(retry),86400)
    return result


def sec_primary_metadata(request):
    match=re.fullmatch(r'([0-9]{10}):([0-9]{10}-[0-9]{2}-[0-9]{6}):(10-[KQ](?:/A)?|20-F(?:/A)?|40-F(?:/A)?|N-CSR[S]?(?:/A)?|N-2(?:ASR|/A)?|[SF]-1(?:/A)?):([A-Za-z0-9_.-]{1,180}\.(?:htm|html|xml|txt))',request['entity'])
    if not match or '..' in match[4] or not request['requested_date']:
        raise ValueError('Exact SEC primary filing metadata required')
    date.fromisoformat(request['requested_date'])
    return dict(cik=match[1],accession=match[2],form=match[3],primary_document=match[4],filed_date=request['requested_date'])


def primary_filing_context(parent,body,child):
    """Carry exact official submission fields, without changing request IDs."""
    if parent['kind']!='SEC_SUBMISSIONS' or child['kind']!='SEC_PRIMARY':return None
    meta=sec_primary_metadata(child)
    if meta['cik']!=parent['entity']:return None
    value=json.loads(body)
    if str(value.get('cik','')).zfill(10)!=meta['cik']:return None
    recent=value.get('filings',{}).get('recent',{})
    for n,accession in enumerate(recent.get('accessionNumber',[])):
        def field(k):
            values=recent.get(k,[])
            return values[n] if n<len(values) else None
        if (accession,field('form'),field('primaryDocument'),field('filingDate'))!=(
                meta['accession'],meta['form'],meta['primary_document'],meta['filed_date']):continue
        report=field('reportDate')
        if report:
            try:date.fromisoformat(report)
            except (ValueError,TypeError):return None
            if report>meta['filed_date']:return None
        return dict(report_date=report,issuer_name=value.get('name',''),
            tickers=[x for x in value.get('tickers',[]) if isinstance(x,str) and re.fullmatch(r'[A-Z][A-Z0-9.-]{0,19}',x)],
            parent_source_id=hashlib.sha256(body).hexdigest())
    return None


def task(kind, entity='', day='', page=0):
    """Only fixed official endpoints; discovered notices have narrow URL scopes."""
    if day:
        date.fromisoformat(day)
    if type(page) is not int or not 0 <= page <= 10000:
        raise ValueError('Invalid official page')
    params = {}; method = 'GET'
    if kind in ('FUND', 'FUND_LATEST'):
        pid, _, site, locale = FUNDS[entity]
        url = ISHARES
        params = dict(appType='PRODUCT_PAGE', appSubType='ISHARES', targetSite=site,
                      locale=locale, portfolioId=pid, userType='individual', component='holdings')
        if kind == 'FUND':
            if not day: raise ValueError('Historical fund date required')
            params['asOfDate'] = day.replace('-', '')
    elif kind == 'KIND_INDUSTRY':
        url = 'https://kind.krx.co.kr/corpgeneral/corpList.do'
        params = dict(method='download', searchType='13')
    elif kind == 'KRX_INDEX':
        if entity not in ('kospi', 'kosdaq', 'krx') or not day: raise ValueError('Index/date required')
        url = f'https://data-dbg.krx.co.kr/svc/apis/idx/{entity}_dd_trd'
        params = dict(basDd=day.replace('-', ''))
    elif kind in ('KRX_MEMBERS', 'KRX_SECTOR'):
        url = 'https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'; method = 'POST'
        if not day: raise ValueError('KRX date required')
        if kind == 'KRX_MEMBERS':
            a, b = {'KOSPI200': ('1', '028'), 'KOSDAQ150': ('2', '203')}[entity]
            params = dict(bld='dbms/MDC/STAT/standard/MDCSTAT00601', indIdx=a, indIdx2=b, trdDd=day.replace('-', ''), money='1', csvxls_isNo='false')
        else:
            params = dict(bld='dbms/MDC/STAT/standard/MDCSTAT03901', mktId={'KOSPI': 'STK', 'KOSDAQ': 'KSQ'}[entity], trdDd=day.replace('-', ''), money='1', csvxls_isNo='false')
    elif kind == 'SEC_MAP':
        url='https://www.sec.gov/files/company_tickers_exchange.json'
    elif kind=='SEC_FUND_MAP':
        if entity or day or page:raise ValueError('Fixed SEC fund-class directory required')
        url='https://www.sec.gov/files/company_tickers_mf.json'
    elif kind=='SEC_CIK_NAMES':
        if entity or day or page:raise ValueError('Fixed SEC name directory required')
        url='https://www.sec.gov/Archives/edgar/cik-lookup-data.txt'
    elif kind=='NASDAQ_DIRECTORY':
        if entity not in NASDAQ_DIRECTORIES or day or page:raise ValueError('Exact Nasdaq directory required')
        url=NASDAQ_DIRECTORIES[entity]
    elif kind=='NASDAQ_CAPS':
        if entity or day or page:raise ValueError('Fixed Nasdaq capitalization snapshot required')
        url='https://api.nasdaq.com/api/screener/stocks';params={'tableonly':'true','limit':'25000'}
    elif kind=='YAHOO_PRICE':
        if not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,19}',entity) or not day or day<'2010-01-01' or page:
            raise ValueError('Exact dated public quote target required')
        end=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())+86400
        url='https://query1.finance.yahoo.com/v8/finance/chart/'+entity
        params={'period1':end-8*86400,'period2':end,'interval':'1d','includePrePost':'false'}
    elif kind=='STOCKANALYSIS_PRICE':
        if not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,19}',entity) or not day or day<'2010-01-01' or page:
            raise ValueError('Exact dated OTC quote target required')
        url='https://stockanalysis.com/quote/otc/'+entity+'/'
    elif kind=='SEC_PRIMARY':
        if page:raise ValueError('SEC primary document has no page cursor')
        meta=sec_primary_metadata(dict(entity=entity,requested_date=day))
        url=f"https://www.sec.gov/Archives/edgar/data/{int(meta['cik'])}/{meta['accession'].replace('-','')}/{meta['primary_document']}"
    elif kind in ('SEC_FACTS', 'SEC_SUBMISSIONS', 'SEC_ARCHIVE'):
        if not re.fullmatch(r'\d{10}', entity): raise ValueError('Exact CIK required')
        if kind=='SEC_ARCHIVE':
            if not 1<=page<=999: raise ValueError('SEC declared archive number required')
            url=f'https://data.sec.gov/submissions/CIK{entity}-submissions-{page:03d}.json'
        else:
            url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{entity}.json' if kind == 'SEC_FACTS' else f'https://data.sec.gov/submissions/CIK{entity}.json'
    elif kind == 'LSEG_CATALOG':
        if not re.fullmatch(r'20\d{2}', entity): raise ValueError('Catalog year required')
        url = LSEG; method = 'POST'
        params = dict(content_type='tech-notice, meth-review', family_mnemonic='', page_number=page, page_size=50, qry='', year=int(entity))
    elif kind=='LSEG_WEIGHT_CATALOG':
        if entity or day or page:raise ValueError('Fixed weight catalog required')
        url=LSEG_WEIGHT_CATALOG
    elif kind=='LSEG_WEIGHTS':
        if entity not in LSEG_WEIGHT_INDICES or day or page:raise ValueError('Supported official weight index required')
        url=LSEG_WEIGHT_DOWNLOAD;params=dict(indexdetails=entity)
    elif kind=='RUSSELL_IPO_PAGE':
        if entity or day or page:raise ValueError('Fixed IPO page required')
        url='https://www.lseg.com/en/ftse-russell/russell-reconstitution/ipo-additions'
    elif kind=='RUSSELL_IPO_PDF':
        if entity not in ('1000','2000','3000') or day or page not in (0,1,2,4):raise ValueError('Supported Russell IPO index/quarter required')
        quarter=page or 3  # Keep the original signed Q3 task identity.
        url=f'https://www.lseg.com/content/dam/ftse-russell/en_us/documents/other/final-ipo-additions-{quarter}-qtr-r{entity}.pdf'
    elif kind in ('JPM_UNIVERSE','JPM_ACTIONS'):
        if entity or day or page>199:raise ValueError('Bounded public JPM catalog required')
        url=JPM_PUBLIC+('drUniverse' if kind=='JPM_UNIVERSE' else 'corporateActions')
        params=dict(limit=1000,offset=page*1000)
    elif kind=='JPM_DOCUMENT':
        match=re.fullmatch(r'([a-f0-9]{32})(?::([1-9][0-9]{0,8}))?',entity)
        if not match or day or page:raise ValueError('Exact public JPM document/revision required')
        url=JPM_PUBLIC+'cms/document';params={'cmsId':match[1]}
        if match[2]:params['sequenceNo']=int(match[2])
    elif kind=='CITI_CATALOG':
        if entity or day or page:raise ValueError('Fixed depositary catalog required')
        url=CITI_CATALOG
    elif kind=='CITI_NOTICE':
        if not re.fullmatch(r'[1-9]\d{0,7}',entity) or day or page:raise ValueError('Exact public Citi notice ID required')
        url=CITI_PDF;params=dict(caId=entity)
    elif kind=='CITI_ATTACHMENT':
        if not re.fullmatch(r'[1-9]\d{0,7}',entity) or day or page:raise ValueError('Exact public Citi attachment ID required')
        url='https://depositaryreceipts.citi.com/adr/common/file.aspx';params=dict(idf=entity)
    elif kind=='BNY_NOTICE':
        if not re.fullmatch(r'AD\d{7}',entity) or day or page:raise ValueError('Exact public BNY document ID required')
        url=f'https://www.adrbny.com/content/dam/adr/documents/corporate-actions-dr/files/{entity}.pdf'
    elif kind=='FINRA_DAILY':
        if entity or not day or day<'2018-12-18' or page>500:raise ValueError('Bounded FINRA calendar day required')
        url='https://api.finra.org/data/group/otcMarket/name/otcDailyList';method='POST'
        params=dict(limit=1000,offset=page*1000,dateRangeFilters=[dict(fieldName='calendarDay',startDate=day,endDate=day)])
    elif kind == 'RUSSELL_PAGE':
        url = 'https://www.lseg.com/en/ftse-russell/russell-reconstitution'
    elif kind == 'NASDAQ_NOTICE':
        if not re.fullmatch(r'https://ir\.nasdaq\.com/news-releases/news-release-details/annual-changes-nasdaq-100-indexr(?:-\d+)?',entity): raise ValueError('Official Nasdaq notice required')
        url=entity
    elif kind == 'FRED_INDEX':
        if entity not in ('SP500','NASDAQ100','NASDAQCOM'): raise ValueError('Supported official index series required')
        url='https://fred.stlouisfed.org/graph/fredgraph.csv'; params=dict(id=entity,cosd='2010-01-01')
    elif kind == 'CBOE_INDEX':
        if entity not in ('VIX','VVIX'): raise ValueError('Supported Cboe index required')
        url=f'https://cdn-api.cboe.com/api/global/us_indices/daily_prices/{entity}_History.csv'
    elif kind == 'RUSSELL_PDF':
        if not re.fullmatch(r'https://www\.lseg\.com/content/dam/ftse-russell/en_us/documents/other/ru(?:1000|2000|3000)-(?:additions|deletions)-(?:preliminary-)?\d{8}\.pdf', entity): raise ValueError('Official Russell PDF required')
        url = entity
    elif kind == 'NASDAQ_PDF':
        if not re.fullmatch(r'https://indexes\.nasdaqomx\.com/docs/20\d{2}(?:0[1-9]|1[0-2])%20NDX%20Reconstitution%20and%20Performance%20Highlights\.pdf',entity):
            raise ValueError('Official Nasdaq research PDF required')
        url=entity
    elif kind == 'NASDAQ_FACTSHEET':
        if entity!='NDX':raise ValueError('Official Nasdaq factsheet required')
        url='https://indexes.nasdaq.com/docs/FS_NDX.pdf'
    elif kind == 'NASDAQ_WEIGHTS':
        if entity!='NDX':raise ValueError('Official Nasdaq weight document required')
        url='https://www.nasdaq.com/docs/global-indexes/NDX'
    elif kind == 'SP_CATALOG':
        if not re.fullmatch(r'20\d{2}', entity): raise ValueError('Catalog year required')
        url = 'https://press.spglobal.com/news-releases'; params = dict(year=entity, l=100, o=page * 100)
    elif kind == 'SP_ARCHIVE':
        if not re.fullmatch(r'20\d{2}', entity): raise ValueError('Catalog year required')
        url='https://press.spglobal.com/index.php'; params=dict(s=2429,year=entity,l=100,o=page*100)
    elif kind == 'SP_NOTICE':
        if not re.fullmatch(r'https://press\.spglobal\.com/20\d{2}-\d{2}-\d{2}-[A-Za-z0-9%,._~-]+', entity): raise ValueError('Official S&P notice required')
        url = entity
    else:
        raise ValueError('Unsupported official reference kind')
    return dict(source='market_reference', kind=kind, entity=entity, requested_date=day,
                page=page, method=method, url=url, params=params)


def validate_task(value):
    if not isinstance(value, dict) or set(value) != {'source','kind','entity','requested_date','page','method','url','params'}:
        raise ValueError('Reference task shape mismatch')
    if value != task(value['kind'], value['entity'], value['requested_date'], value['page']):
        raise ValueError('Reference task endpoint/parameter mismatch')
    return value


class Tables(HTMLParser):
    def __init__(self):
        super().__init__(); self.rows=[]; self.row=None; self.cell=None; self.links=[]; self.text=[]
    def handle_starttag(self, tag, attrs):
        if tag == 'tr': self.row=[]
        elif tag in ('td','th') and self.row is not None: self.cell=[]
        elif tag == 'a' and dict(attrs).get('href'): self.links.append(dict(attrs)['href'])
    def handle_data(self, value):
        self.text.append(value)
        if self.cell is not None: self.cell.append(value)
    def handle_endtag(self, tag):
        if tag in ('td','th') and self.cell is not None:
            self.row.append(' '.join(' '.join(self.cell).split())); self.cell=None
        elif tag == 'tr' and self.row is not None:
            self.rows.append(self.row); self.row=None


def html(body):
    try: text=body.decode('utf-8-sig')
    except UnicodeDecodeError: text=body.decode('cp949')
    parser=Tables(); parser.feed(text); return parser


def fund_points(body, request):
    payload=json.loads(body); pid=FUNDS[request['entity']][0]
    if payload.get('productId') != pid: raise ValueError('Wrong fund returned')
    points=payload['componentsByNameMap']['holdings']['containersByNameMap']['all']['dataPointsByNameMap']
    actual=datetime.strptime(str(points['asOfDate']['value']), '%Y%m%d').date().isoformat()
    if request['kind']=='FUND' and actual!=request['requested_date']: raise ValueError('Historical fund date substituted')
    tickers=points['ticker']['value']
    if not isinstance(tickers,list) or not tickers: raise ValueError('Empty fund holdings')
    for name,point in points.items():
        if name=='dateList': continue
        values=point.get('value')
        if isinstance(values,list) and len(values)!=len(tickers): raise ValueError('Fund column length mismatch')
    return payload, points, actual


class _CitiNotices(HTMLParser):
    def __init__(self):
        super().__init__();self.active=False;self.row=None;self.cell=None;self.rows=[];self.links=[]
    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag=='table' and attrs.get('id')=='tblCANotices':self.active=True
        elif self.active:
            if tag=='tr':self.row=[];self.links=[]
            elif tag in ('td','th') and self.row is not None:self.cell=[]
            elif tag=='a' and attrs.get('href') and self.row is not None:self.links.append(attrs['href'])
    def handle_data(self,value):
        if self.cell is not None:self.cell.append(value)
    def handle_endtag(self,tag):
        if tag=='table' and self.active:self.active=False
        elif self.active and tag in ('td','th') and self.cell is not None:
            self.row.append(' '.join(' '.join(self.cell).split()));self.cell=None
        elif self.active and tag=='tr' and self.row is not None:
            self.rows.append((self.row,self.links));self.row=None


def citi_notices(body):
    parser=_CitiNotices();parser.feed(body.decode('utf-8-sig'))
    header=['Company','Date of Notice','','Ticker','CUSIP','Ratio (ORD:DR)','Type','Country','Exchange','Corporate Action Type']
    if not parser.rows or parser.rows[0][0]!=header:raise ValueError('Citi corporate action table contract mismatch')
    result=[]
    for cells,links in parser.rows[1:]:
        if len(cells)!=10:raise ValueError('Citi notice column mismatch')
        ids=[]
        for link in links:
            u=urlsplit(urljoin(CITI_CATALOG,link));q=parse_qs(u.query)
            if u.path.endswith('/previewPdf.aspx'):
                if u.scheme!='https' or u.netloc!='depositaryreceipts.citi.com' or u.path!='/adr/notices/previewPdf.aspx' or set(q)!={'caId'} or len(q['caId'])!=1:
                    raise ValueError('Citi notice link is outside the public endpoint')
                task('CITI_NOTICE',q['caId'][0]);ids.append(q['caId'][0])
        if len(set(ids))!=1:raise ValueError('Unique Citi notice PDF required')
        ratio=re.fullmatch(r'(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)',cells[5])
        if not ratio or any(not math.isfinite(float(x)) or float(x)<=0 for x in ratio.groups()):raise ValueError('Positive reported ORD:DR ratio required')
        if not math.isfinite(float(ratio[1])/float(ratio[2])):raise ValueError('Finite reported ORD:DR ratio required')
        if not re.fullmatch(r'[0-9A-Z*@#]{9}',cells[4]):raise ValueError('Reported Citi CUSIP required')
        result.append(dict(notice_id=ids[0],company=cells[0],notice_date=datetime.strptime(cells[1],'%m/%d/%Y').date().isoformat(),
            raw_symbol=cells[3],cusip=cells[4],ratio_reported=cells[5],ordinary_per_dr=float(ratio[1])/float(ratio[2]),
            dr_type=cells[6],country=cells[7],exchange=cells[8],action_type=cells[9]))
    if not 1<=len(result)<=10000:raise ValueError('Bounded nonempty Citi notice list required')
    return result


def weight_catalog_records(body):
    try:text=body.decode('utf-8-sig')
    except UnicodeDecodeError:text=body.decode('cp1252')
    value=json.loads(text)
    if not isinstance(value,dict) or not isinstance(value.get('Data'),list) or not 1<=len(value['Data'])<=5000:
        raise ValueError('LSEG constituent weight catalog mismatch')
    found=[]
    for row in value['Data']:
        if not isinstance(row,dict):raise ValueError('LSEG weight catalog row required')
        code=row.get('IndexId')
        if code not in LSEG_WEIGHT_INDICES:continue
        expected=task('LSEG_WEIGHTS',code)
        if row.get('ReportType')!='StocksWeight' or row.get('Url')!=expected['url']+'?indexdetails='+code:
            raise ValueError('LSEG weight catalog download mismatch')
        found.append(row)
    return list({row['IndexId']:row for row in found}.values())


def weight_catalog(body):
    return [row['IndexId'] for row in weight_catalog_records(body)]


def citi_attachment_tasks(reader):
    if len(reader.pages)>40:raise ValueError('Citi attachment discovery page bound')
    result=[]
    for page in reader.pages:
        if not hasattr(page,'get'):continue
        for ref in page.get('/Annots',[]):
            action=ref.get_object().get('/A')
            if action is None:continue
            action=action.get_object() if hasattr(action,'get_object') else action
            uri=action.get('/URI')
            if not uri:continue
            u=urlsplit(str(uri));q=parse_qs(u.query)
            if u.scheme in ('https','http') and u.netloc=='depositaryreceipts.citi.com' and u.path=='/adr/common/file.aspx' and set(q)=={'idf'} and len(q['idf'])==1:
                try:result.append(task('CITI_ATTACHMENT',q['idf'][0]))
                except ValueError:continue
    return list({digest(t):t for t in result}.values())


def jpm_document(raw):
    if raw.get('action') not in JPM_ECONOMIC_ACTIONS:return None
    pdf=(raw.get('notices') or {}).get('pdf')
    if not pdf:return None
    sequence=raw.get('sequenceNo')
    if sequence not in (None,0) and (type(sequence) is not int or not 1<=sequence<=999999999):return None
    return task('JPM_DOCUMENT',pdf+(':'+str(sequence) if sequence else ''))


def jpm_page(request,body):
    """Public page contract; midnight dates and global cap do not certify PIT/units."""
    value=json.loads(body)
    if not isinstance(value,dict) or not isinstance(value.get('data'),dict):raise ValueError('JPM data object required')
    data=value['data'];rows=data.get('items');p=data.get('pagination',{})
    if not isinstance(p,dict):raise ValueError('JPM pagination object required')
    if not isinstance(rows,list) or len(rows)>1000:raise ValueError('Bounded JPM rows required')
    if (type(p.get('offset')) is not int or p['offset']!=request['page']*1000 or p.get('limit')!=1000
            or type(p.get('currentItemCount')) is not int or p['currentItemCount']!=len(rows)
            or type(p.get('totalItems')) is not int or not 0<=p['totalItems']<=200000
            or p['offset']+len(rows)>p['totalItems'] or type(p.get('hasMore')) is not bool
            or p['hasMore']!=(p['offset']+len(rows)<p['totalItems']) or p['hasMore'] and len(rows)!=1000):
        raise ValueError('JPM pagination mismatch')
    seen=set();unique=[]
    for row in rows:
        if not isinstance(row,dict) or not isinstance(row.get('cusip'),str) or not re.fullmatch(r'[A-Za-z0-9@#*]{1,32}',row['cusip']):
            raise ValueError('Bounded reported JPM identifier required')
        identity=digest(row)
        # The official response can repeat the exact same action row. Keep
        # its original response bytes, normalize that identical value once.
        if identity in seen:continue
        seen.add(identity);unique.append(row)
        if request['kind']=='JPM_UNIVERSE':
            if not isinstance(row.get('name'),str) or not row['name'].strip():raise ValueError('JPM program name required')
            for key in ('globalMarketCap','price'):
                value=row.get(key)
                if value is not None and (type(value) not in (int,float) or not math.isfinite(value) or value<0):
                    raise ValueError('Invalid reported JPM value')
        else:
            if row.get('action') is not None and not isinstance(row['action'],str):raise ValueError('Invalid JPM action')
            for key in ('eventDate','announcementDate'):
                value=row.get(key)
                if value not in (None,''):
                    try:datetime.fromisoformat(value.replace('Z','+00:00'))
                    except (ValueError,TypeError,AttributeError):raise ValueError('Invalid JPM reported date') from None
            notices=row.get('notices') or {}
            if not isinstance(notices,dict):raise ValueError('JPM document metadata required')
            for key in ('pdf','xbrl'):
                if notices.get(key) and not re.fullmatch(r'[a-f0-9]{32}',notices[key]):raise ValueError('Exact JPM document ID required')
    return unique,p


def yahoo_quotes(request,body):
    value=json.loads(body).get('chart',{})
    records=value.get('result')
    if value.get('error') or not isinstance(records,list) or len(records)!=1:raise ValueError('Public quote result required')
    row=records[0]
    if not isinstance(row,dict):raise ValueError('Public quote object required')
    meta=row.get('meta',{})
    if not isinstance(meta,dict):raise ValueError('Public quote metadata required')
    if meta.get('symbol')!=request['entity'] or meta.get('currency')!='USD':raise ValueError('Public quote identity/currency mismatch')
    stamps=row.get('timestamp',[]);quotes=row.get('indicators',{}).get('quote',[])
    if not isinstance(stamps,list) or not 1<=len(stamps)<=14 or len(quotes)!=1:raise ValueError('Bounded daily quote series required')
    closes=quotes[0].get('close',[]);volumes=quotes[0].get('volume',[])
    if len(closes)!=len(stamps):raise ValueError('Aligned daily quote values required')
    accepted=[]
    for i,stamp in enumerate(stamps):
        if type(stamp) not in (int,float) or not request['params']['period1']<=stamp<request['params']['period2']:raise ValueError('Quote outside requested date window')
        close=closes[i]
        if type(close) not in (int,float) or not math.isfinite(close) or close<=0:continue
        on=datetime.fromtimestamp(stamp,timezone.utc).date().isoformat()
        volume=volumes[i] if i<len(volumes) else None
        if type(volume) not in (int,float) or not math.isfinite(volume) or volume<0:volume=None
        accepted.append({'symbol':meta['symbol'],'currency':'USD','price':close,'price_asof':on,
            'reported_volume':volume,'price_basis':'PROVIDER_DAILY_CLOSE_SPLIT_BASIS_UNVERIFIED'})
    if not accepted:raise ValueError('Positive dated reference quote required')
    return accepted


def yahoo_quote(request,body):
    return max(yahoo_quotes(request,body),key=lambda r:r['price_asof'])


def validate_response(request, body):
    validate_task(request)
    if not isinstance(body,bytes) or not 0<len(body)<=response_limit(request): raise ValueError('Reference body bound')
    kind=request['kind']
    if kind=='YAHOO_PRICE':yahoo_quote(request,body)
    elif kind=='STOCKANALYSIS_PRICE':stockanalysis_quote(request,body)
    elif kind=='SEC_CIK_NAMES':
        if not any(sec_cik_names(body)):raise ValueError('Nonempty SEC company name directory required')
        # Validate the complete file, not just its first usable row.
        for _ in sec_cik_names(body):pass
    elif kind in ('JPM_UNIVERSE','JPM_ACTIONS'):jpm_page(request,body)
    elif kind=='FINRA_DAILY':
        rows=json.loads(body)
        if not isinstance(rows,list) or len(rows)>1000:raise ValueError('Bounded FINRA rows required')
        ids=set()
        for row in rows:
            if (not isinstance(row,dict) or row.get('calendarDay')!=request['requested_date'] or
                    type(row.get('OTCDailyListID')) is not int or row['OTCDailyListID']<=0 or
                    not isinstance(row.get('dailyListEventCode'),str) or row['OTCDailyListID'] in ids):
                raise ValueError('FINRA date/event identity mismatch')
            ids.add(row['OTCDailyListID'])
    elif kind=='CITI_CATALOG':citi_notices(body)
    elif kind=='LSEG_WEIGHT_CATALOG':weight_catalog(body)
    elif kind.startswith('FUND'): fund_points(body,request)
    elif kind in ('FRED_INDEX','CBOE_INDEX'):
        rows=list(csv.DictReader(io.StringIO(body.decode('utf-8-sig'))))
        fields={'DATE','VVIX' if request['entity']=='VVIX' else 'CLOSE'} if kind=='CBOE_INDEX' else {request['entity']}
        if not rows or not fields<=set(rows[0]): raise ValueError('Official index CSV schema mismatch')
    elif kind.startswith('KRX'):
        value=json.loads(body); key='OutBlock_1' if kind=='KRX_INDEX' else ('output' if kind=='KRX_MEMBERS' else 'block1')
        rows=value.get(key)
        if not isinstance(rows,list) or not rows: raise ValueError('Missing official KRX rows')
        for row in rows:
            if kind=='KRX_INDEX':
                if (row.get('BAS_DD')!=request['requested_date'].replace('-','') or
                    row.get('IDX_CLSS')!=request['entity'].upper() or not row.get('IDX_NM') or 'CLSPRC_IDX' not in row): raise ValueError('KRX index/date/family mismatch')
            elif not row.get('ISU_SRT_CD'): raise ValueError('KRX security identity missing')
    elif kind=='SEC_MAP':
        value=json.loads(body)
        if value.get('fields')!=['cik','name','ticker','exchange'] or not isinstance(value.get('data'),list) or not value['data']: raise ValueError('SEC directory contract mismatch')
    elif kind=='SEC_FUND_MAP':
        value=json.loads(body)
        if value.get('fields')!=['cik','seriesId','classId','symbol'] or not isinstance(value.get('data'),list) or not 1<=len(value['data'])<=100000:raise ValueError('SEC fund-class directory contract mismatch')
        for row in value['data']:
            if (not isinstance(row,list) or len(row)!=4 or type(row[0]) is not int or not 0<row[0]<10**10
                or not isinstance(row[1],str) or not re.fullmatch(r'S[0-9]{9}',row[1])
                or not isinstance(row[2],str) or not re.fullmatch(r'C[0-9]{9}',row[2])
                or not isinstance(row[3],str) or len(row[3])>30):raise ValueError('SEC fund-class row mismatch')
    elif kind=='NASDAQ_DIRECTORY':
        text=body.decode('utf-8-sig');reader=csv.DictReader(io.StringIO(text),delimiter='|')
        symbol='Symbol' if request['entity']=='nasdaq' else 'ACT Symbol'
        if not {symbol,'Security Name','ETF','Test Issue'}<=set(reader.fieldnames or []):raise ValueError('Nasdaq directory schema mismatch')
        rows=[r for r in reader if not str(r.get(symbol,'')).startswith('File Creation Time')]
        if not rows or len(rows)>50000 or not any(x.startswith('File Creation Time') for x in text.splitlines()):raise ValueError('Bounded complete Nasdaq directory required')
        if any(not r.get(symbol) or r.get('Test Issue') not in ('Y','N') or r.get('ETF') not in ('Y','N') for r in rows):raise ValueError('Nasdaq directory flag mismatch')
    elif kind=='NASDAQ_CAPS':
        rows=json.loads(body).get('data',{}).get('table',{}).get('rows')
        if not isinstance(rows,list) or not 1<=len(rows)<=25000 or any(not r.get('symbol') for r in rows):raise ValueError('Nasdaq capitalization snapshot contract mismatch')
    elif kind=='SEC_PRIMARY':
        lower=body[:3000].lower()
        if not any(marker in lower for marker in (b'<html',b'<!doctype html',b'<xbrl',b'<xbrli:xbrl')):
            raise ValueError('SEC primary filing markup required')
        if b'undeclared automated tool' in body.lower() or b'request rate threshold exceeded' in body.lower():
            raise ValueError('SEC access denial is not a filing')
    elif kind=='SEC_ARCHIVE':
        value=json.loads(body)
        if not isinstance(value.get('accessionNumber'),list) or not isinstance(value.get('filingDate'),list): raise ValueError('SEC archive contract mismatch')
    elif kind.startswith('SEC_'):
        value=json.loads(body)
        if str(int(value.get('cik',-1))).zfill(10)!=request['entity']: raise ValueError('SEC issuer mismatch')
        if kind=='SEC_FACTS' and not isinstance(value.get('facts'),dict): raise ValueError('Missing SEC facts')
        if kind=='SEC_SUBMISSIONS' and not isinstance(value.get('filings'),dict): raise ValueError('Missing SEC filings')
    elif kind=='KIND_INDUSTRY':
        rows=html(body).rows
        if not any('종목코드' in r and '업종' in r for r in rows): raise ValueError('KIND industry table missing')
    elif kind=='LSEG_CATALOG':
        value=json.loads(body)
        count=value.get('TotalCount')
        if ('ErrorCode' in value or not isinstance(value.get('Data'),list) or type(count) not in (int,float)
                or not math.isfinite(count) or not 0<=count<=1000000 or int(count)!=count): raise ValueError('LSEG catalog mismatch')
    elif kind in ('RUSSELL_PDF','NASDAQ_PDF','NASDAQ_FACTSHEET','NASDAQ_WEIGHTS','LSEG_WEIGHTS','RUSSELL_IPO_PDF','CITI_NOTICE','CITI_ATTACHMENT','BNY_NOTICE','JPM_DOCUMENT'):
        if not body.startswith(b'%PDF-'): raise ValueError('Official PDF required')
    else:
        p=html(body)
        if not p.text or not re.search(br'<(?:html|!DOCTYPE)',body,re.I): raise ValueError('Official HTML required')
        if re.search(r'access denied|request unsuccessful|captcha', ' '.join(p.text), re.I): raise ValueError('Access challenge is not reference data')
    return body


def discovered(request, body,*,primary=False,through=None):
    """Discover only official, narrowly scoped follow-ups; no arbitrary URL fetch."""
    kind=request['kind']; result=[]
    if kind in ('JPM_UNIVERSE','JPM_ACTIONS'):
        records,page=jpm_page(request,body)
        if page['hasMore']:result=[task(kind,page=request['page']+1)]
        if kind=='JPM_ACTIONS':result += [doc for raw in records if (doc:=jpm_document(raw)) is not None]
    elif kind=='CITI_NOTICE':
        from pypdf import PdfReader
        from pypdf.errors import PdfReadError
        try:
            reader=PdfReader(io.BytesIO(body))
            result=citi_attachment_tasks(reader)
        except PdfReadError:pass
    elif kind=='FINRA_DAILY':
        if len(json.loads(body))==1000:
            result=[task('FINRA_DAILY',day=request['requested_date'],page=request['page']+1)]
    elif kind=='CITI_CATALOG':
        notices=sorted(citi_notices(body),key=lambda r:r['notice_date'],reverse=True)
        result=[task('CITI_NOTICE',r['notice_id']) for r in notices]
    elif kind=='LSEG_WEIGHT_CATALOG':result=[task('LSEG_WEIGHTS',code) for code in weight_catalog(body)]
    elif kind=='RUSSELL_IPO_PAGE':
        links={urljoin(request['url'],link) for link in html(body).links}
        candidates=[task('RUSSELL_IPO_PDF',code,page=q) for code in ('1000','2000','3000') for q in (0,1,2,4)]
        result=[t for t in candidates if t['url'] in links]
    elif kind in ('SP_CATALOG','SP_ARCHIVE','RUSSELL_PAGE'):
        for link in html(body).links:
            url=urljoin(request['url'],link).split('#')[0]
            candidate='RUSSELL_PDF' if kind=='RUSSELL_PAGE' else 'SP_NOTICE'
            if candidate=='SP_NOTICE' and not any(x in url for x in ('S-P-500','S-P-MidCap','S-P-SmallCap')): continue
            try: result.append(task(candidate,url))
            except (ValueError,KeyError): continue
        if kind=='SP_ARCHIVE':
            for link in html(body).links:
                u=urlsplit(urljoin(request['url'],link)); q=parse_qs(u.query)
                if u.hostname!='press.spglobal.com' or u.path!='/index.php' or q.get('year')!=[request['entity']]: continue
                if q.get('o')==[str((request['page']+1)*100)]:
                    result.append(task(kind,request['entity'],day=request['requested_date'],page=request['page']+1)); break
    elif kind=='LSEG_CATALOG':
        value=json.loads(body)
        if (request['page']+1)*50<value['TotalCount']: result.append(task(kind,request['entity'],day=request['requested_date'],page=request['page']+1))
    elif kind=='SEC_SUBMISSIONS':
        filings=json.loads(body)['filings']
        for item in filings.get('files',[]):
            name=item.get('name','')
            match=re.fullmatch(r'CIK'+request['entity']+r'-submissions-(\d{3})\.json',name)
            if match: result.append(task('SEC_ARCHIVE',request['entity'],page=int(match.group(1))))
        if primary:
            recent=filings.get('recent',{});forms=recent.get('form',[]);candidates=[]
            for n,form in enumerate(forms):
                if form not in ('10-K','10-K/A','10-Q','10-Q/A','20-F','20-F/A','40-F','40-F/A','N-CSR','N-CSR/A','N-CSRS','N-CSRS/A','N-2','N-2/A','N-2ASR','S-1','S-1/A','F-1','F-1/A'):continue
                filed=recent.get('filingDate',[])[n]
                if through and filed>through:continue
                entity=':'.join((request['entity'],recent['accessionNumber'][n],form,recent['primaryDocument'][n]))
                try:candidate=task('SEC_PRIMARY',entity,filed)
                except ValueError:continue
                candidates.append(candidate)
            candidates.sort(key=lambda x:(x['requested_date'],x['entity']),reverse=True)
            selected=candidates[:2]
            annual=next((t for t in candidates if sec_primary_metadata(t)['form'].split('/')[0] in ('10-K','20-F','40-F','N-CSR')),None)
            if annual is not None and annual not in selected:selected.append(annual)
            result.extend(selected)
    return list({digest(t):t for t in result}.values())


def validate_payload(payload):
    from .daily_delta_codec import _fields,_hash,_instant,_json_tree
    _fields(payload,('schema','stream_id','sequence','previous_package_sha256','bootstrap_anchor_sha256','collected_at','records','state'))
    if payload['schema']!='research-delta-v1' or payload['stream_id']!=STREAM or payload['bootstrap_anchor_sha256']!=ANCHOR: raise ValueError('Market reference stream mismatch')
    seq=payload['sequence']
    if type(seq) is not int or not 1<=seq<2**63: raise ValueError('Invalid reference sequence')
    if seq==1:
        if payload['previous_package_sha256'] is not None: raise ValueError('First predecessor forbidden')
    else: _hash(payload['previous_package_sha256'])
    stamp=_instant(payload['collected_at']); _json_tree(payload['state']); validate_state(payload['state'])
    records=payload['records']
    if not isinstance(records,list) or len(records)>100: raise ValueError('Bounded reference records required')
    ids=set()
    for record in records:
        _fields(record,('task','retrieved_at','body_base64','body_sha256'))
        validate_task(record['task']); _hash(record['body_sha256'])
        if _instant(record['retrieved_at'])>stamp: raise ValueError('Future retrieval')
        encoded=record['body_base64']
        limit=response_limit(record['task'])
        if not isinstance(encoded,str) or len(encoded)>limit*4//3+4: raise ValueError('Encoded body bound')
        body=base64.b64decode(encoded,validate=True)
        if not 0<len(body)<=limit or hashlib.sha256(body).hexdigest()!=record['body_sha256']: raise ValueError('Body integrity failure')
        key=digest(record['task'])
        if key in ids: raise ValueError('Duplicate task record')
        ids.add(key)
    if len(canonical(payload).encode())>100*1024**2: raise ValueError('Market package bound')
    return payload


def validate_state(state):
    if not isinstance(state,dict) or set(state)!={'version','cursors','seen','checked','cooldowns','pending','requests','through'} or state['version']!='market-reference-state-v1': raise ValueError('Market state contract mismatch')
    date.fromisoformat(state['through'])
    for key in ('cursors','seen','checked','cooldowns','requests'):
        if not isinstance(state[key],dict): raise ValueError('Reference map required')
    for request in state['pending']: validate_task(request)
    if len(state['pending'])>5000 or len(canonical(state).encode())>32*1024**2: raise ValueError('Reference state bound')
    return state
