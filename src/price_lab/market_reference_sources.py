"""Portable official-reference contracts. No database or credential I/O on import."""
import base64
import csv
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
LSEG = 'https://www.lseg.com/content/lseg/en_us/ftse-russell/index-resources/notices/jcr:content/root/container/page_content_region/page-content-region/section/section-fw/index_announcements_.indexannouncement.json'
MAX_BODY = 28 * 1024**2
NASDAQ_REPORTS=('https://indexes.nasdaqomx.com/docs/202601%20NDX%20Reconstitution%20and%20Performance%20Highlights.pdf',)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


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
        if not re.fullmatch(r'https://www\.lseg\.com/content/dam/ftse-russell/en_us/documents/other/ru(?:1000|2000|3000)-(?:additions|deletions)-\d{8}\.pdf', entity): raise ValueError('Official Russell PDF required')
        url = entity
    elif kind == 'NASDAQ_PDF':
        if not re.fullmatch(r'https://indexes\.nasdaqomx\.com/docs/20\d{2}(?:0[1-9]|1[0-2])%20NDX%20Reconstitution%20and%20Performance%20Highlights\.pdf',entity):
            raise ValueError('Official Nasdaq research PDF required')
        url=entity
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


def validate_response(request, body):
    validate_task(request)
    if not isinstance(body,bytes) or not 0<len(body)<=MAX_BODY: raise ValueError('Reference body bound')
    kind=request['kind']
    if kind.startswith('FUND'): fund_points(body,request)
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
    elif kind in ('RUSSELL_PDF','NASDAQ_PDF'):
        if not body.startswith(b'%PDF-'): raise ValueError('Official PDF required')
    else:
        p=html(body)
        if not p.text or not re.search(br'<(?:html|!DOCTYPE)',body,re.I): raise ValueError('Official HTML required')
        if re.search(r'access denied|request unsuccessful|captcha', ' '.join(p.text), re.I): raise ValueError('Access challenge is not reference data')
    return body


def discovered(request, body):
    """Discover only official, narrowly scoped follow-ups; no arbitrary URL fetch."""
    kind=request['kind']; result=[]
    if kind in ('SP_CATALOG','SP_ARCHIVE','RUSSELL_PAGE'):
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
        for item in json.loads(body)['filings'].get('files',[]):
            name=item.get('name','')
            match=re.fullmatch(r'CIK'+request['entity']+r'-submissions-(\d{3})\.json',name)
            if match: result.append(task('SEC_ARCHIVE',request['entity'],page=int(match.group(1))))
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
        if not isinstance(encoded,str) or len(encoded)>MAX_BODY*4//3+4: raise ValueError('Encoded body bound')
        body=base64.b64decode(encoded,validate=True)
        if not 0<len(body)<=MAX_BODY or hashlib.sha256(body).hexdigest()!=record['body_sha256']: raise ValueError('Body integrity failure')
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
