"""Official KR evidence request and parsing helpers; stdlib only, no I/O on import."""
from datetime import date, datetime, timedelta, timezone
import hashlib
from html import unescape
from html.parser import HTMLParser
import json
import re
from urllib.parse import urljoin, urlsplit, parse_qs, unquote

KIND='https://kind.krx.co.kr'
KIS='https://openapi.koreainvestment.com:9443'
MERGER_PATH='/uapi/domestic-stock/v1/ksdinfo/merger-split'
MERGER_TR='HHKDB669104C0'
MAX_BYTES=30_000_000
KIND_REPORT_NAMES=('상장폐지','합병','분할','주식교환','주식이전','청산','잔여재산','주식매수청구권')
KRX_BASE='https://data-dbg.krx.co.kr/svc/apis/sto/'
ENDPOINTS={('KOSPI','daily'):'stk_bydd_trd',('KOSDAQ','daily'):'ksq_bydd_trd',
           ('KOSPI','master'):'stk_isu_base_info',('KOSDAQ','master'):'ksq_isu_base_info'}


def canonical(value):
    return json.dumps(value,sort_keys=True,ensure_ascii=False,default=str,separators=(',',':'),allow_nan=False)


def day(value):
    if isinstance(value,datetime):return value.date()
    if isinstance(value,date):return value
    text=str(value).strip()
    try:return datetime.strptime(text,'%Y%m%d').date() if len(text)==8 and text.isdigit() else date.fromisoformat(text[:10])
    except (ValueError,TypeError):raise ValueError('Invalid official date') from None


def instant(value=None):
    result=datetime.now(timezone.utc) if value is None else value
    if isinstance(result,str):
        try:result=datetime.fromisoformat(result.replace('Z','+00:00'))
        except ValueError:raise ValueError('Invalid provenance timestamp') from None
    if not isinstance(result,datetime) or result.tzinfo is None:raise ValueError('Timezone-aware provenance timestamp required')
    return result.astimezone(timezone.utc)


def _sha(content):return hashlib.sha256(content).hexdigest()


def _hash(value):return _sha(canonical(value).encode())


def _id(task):return _hash(task)


def _code(value):
    if not isinstance(value,str) or not re.fullmatch('[0-9A-Z]{6}',value):
        raise ValueError('Exact official six-character code required')
    return value


def _kis_task(start,end):
    return dict(source='kis_merger_split',url=KIS+MERGER_PATH,method='GET',start_date=start,end_date=end,
        params=dict(F_DT=start.replace('-',''),T_DT=end.replace('-',''),SHT_CD='',CTS=''))


def _kind_options(report_name,lookback_days):
    if not isinstance(report_name,str) or report_name not in KIND_REPORT_NAMES:
        raise ValueError('One supported KIND report name required')
    if isinstance(lookback_days,bool) or not isinstance(lookback_days,int) or not 1<=lookback_days<=730:
        raise ValueError('KIND lookback must be an integer from 1 to 730 days')
    return report_name,lookback_days


def _kind_task(event,start,end,page=1,*,report_name='상장폐지',lookback_days=90):
    # The known KIND issuer key is five characters and cannot prove a six-digit
    # security/share class. Discover by board/title/date; match issuer candidates
    # from the native response without making a company-name identity join.
    report_name,lookback_days=_kind_options(report_name,lookback_days)
    task=dict(source='kind_index',url=KIND+'/disclosure/details.do',method='POST',event_id=event['event_id'],
        params=dict(method='searchDetailsSub',forward='details_sub',currentPageSize='100',pageIndex=str(page),
            orderMode='1',orderStat='D',reportNm=report_name,reportNmTemp=report_name,
            searchCorpName='',repIsuSrtCd='',allRepIsuSrtCd='',marketType='1' if event['board']=='KOSPI' else '2',
            fromDate=str(max(day(start),day(event['effective_date'])-timedelta(days=lookback_days))),
            toDate=str(min(day(end),day(event['effective_date'])+timedelta(days=30)))))
    # Preserve legacy default receipt IDs, but distinguish a selected window even
    # when the explicit queue boundary clips both requests to the same dates.
    if lookback_days!=90:task['lookback_days']=lookback_days
    return task


class _Rows(HTMLParser):
    def __init__(self):super().__init__();self.rows=[];self.row=None;self.cell=None
    def handle_starttag(self,tag,attrs):
        if tag=='tr':self.row=[]
        if tag=='td' and self.row is not None:self.cell={'text':'','attrs':[]}
        if self.cell is not None:self.cell['attrs'].append(dict(attrs))
    def handle_data(self,data):
        if self.cell is not None:self.cell['text']+=data
    def handle_endtag(self,tag):
        if tag=='td' and self.cell is not None:
            self.row.append(self.cell);self.cell=None
        if tag=='tr' and self.row is not None:
            if self.row:self.rows.append(self.row)
            self.row=None


def _text(content):
    try:return content.decode('utf-8-sig')
    except UnicodeDecodeError:return content.decode('cp949')


def _kind_rows(content,event):
    body=_text(content);parser=_Rows();parser.feed(body);result=[];row_ids=[]
    for row in parser.rows:
        if len(row)<4:continue
        attrs=[item for cell in row for item in cell['attrs']]
        script=' '.join(a.get('onclick','') for a in attrs)
        receipt=re.search(r"openDisclsViewer\(['\"]([0-9]{14})['\"]",script)
        if receipt:row_ids.append(receipt[1])
        native=re.search(r"(?:companysummary_open|openDisclsChart)\(['\"]([0-9A-Z]{5,6})['\"]",script)
        if not receipt or not native:continue
        native=native[1]
        match='EXACT_CODE_OBSERVATION' if native==event['raw_symbol'] else (
            'ISSUER_CANDIDATE' if len(native)==5 and native==event['raw_symbol'][:5] else None)
        if not match:continue
        stamp=re.search(r'\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?',row[1]['text'])
        stamp=stamp[0] if stamp else None;precision='SECOND' if stamp and len(stamp)==19 else 'MINUTE' if stamp and len(stamp)==16 else 'DATE' if stamp else 'UNKNOWN'
        published=datetime.fromisoformat(stamp).replace(tzinfo=timezone(timedelta(hours=9))).isoformat() if stamp and precision in ('MINUTE','SECOND') else None
        result.append(dict(event_id=event['event_id'],raw_symbol=event['raw_symbol'],native_issuer_code=native,
            receipt_id=receipt[1],title=row[3]['text'].strip(),code_match=match,
            publication_precision=precision,source_publication_date=stamp[:10] if stamp else None,
            source_published_at=published,publication_evidence_verified=False,settlement_status='UNKNOWN',
            payment_date=None,cash_per_raw_share=None,exchange_ratio=None,
            url=KIND+'/common/disclsviewer.do?method=searchInitInfo&acptNo='+receipt[1]+'&docNo='))
    plain=unescape(re.sub('<[^>]+>',' ',body));total=re.search(r'(?:전체|총)\s*([\d,]+)\s*건',plain)
    marker=re.search(r'''<input[^>]*name=["']pageIndex["'][^>]*value=["']([0-9]+)["']''',body,re.I)
    return result,int(total[1].replace(',','')) if total else None,dict(row_ids=row_ids,response_page=int(marker[1]) if marker else None)


def _document_links(content,url):
    """Follow source-observed official document links only, never guessed URLs."""
    body=_text(content);links=set()
    values=re.findall(r'''(?:href|src)\s*=\s*["']([^"']+)["']''',body,re.I)
    values+=re.findall(r'''setPath\d?\(\s*["'][^"']*["']\s*,\s*["']([^"']+)["']''',body,re.I)
    for value in values:
        target=urljoin(url,unescape(value));parsed=urlsplit(target)
        if parsed.scheme=='http' and parsed.hostname=='kind.krx.co.kr':
            target='https'+target[4:];parsed=urlsplit(target)
        if (parsed.scheme=='https' and parsed.hostname=='kind.krx.co.kr' and parsed.port in (None,443)
            and not parsed.username and not parsed.password and parsed.path.startswith('/external/')
            and re.search(r'\.(?:htm|html|pdf)$',parsed.path,re.I)):
            links.add(target)
    return sorted(links)


def _viewer_document_task(content,receipt_id):
    body=_text(content)
    if not re.search(r'''<form[^>]*(?:id|name)=["']docpathfrm["'][^>]*action=["']/common/disclsviewer\.do["']''',body,re.I):return None
    if not re.search(r'''name=["']method["'][^>]*value=["']searchContents["']''',body,re.I):return None
    select=re.search(r'''<select[^>]*id=["']mainDoc["'][^>]*>(.*?)</select>''',body,re.I|re.S)
    if not select:return None
    options=re.findall(r'''<option[^>]*value=["']([0-9]{14})(?:\|[^"']*)?["']([^>]*)>''',select[1],re.I)
    if not options:return None
    doc_id=next((value for value,attrs in options if 'selected' in attrs.lower()),options[0][0])
    return dict(source='kind_document_path',url=KIND+'/common/disclsviewer.do',method='GET',
        receipt_id=receipt_id,params=dict(method='searchContents',docNo=doc_id))


def _merger_rows(payload,task,metadata):
    if not isinstance(payload,dict) or payload.get('rt_cd')!='0':raise ValueError('KIS evidence response unsuccessful')
    records=payload.get('output1')
    if isinstance(records,dict):records=[records] if records else []
    if not isinstance(records,list):raise ValueError('KIS evidence response shape invalid')
    result=[]
    for record in records:
        if not isinstance(record,dict) or not re.fullmatch('[0-9]{8}',str(record.get('record_date',''))):raise ValueError('KIS evidence date missing')
        date_=day(record['record_date'])
        if not day(task['start_date'])<=date_<=day(task['end_date']):raise ValueError('KIS evidence date outside requested interval')
        def optional_date(value):
            digits=''.join(c for c in str(value or '') if c.isdigit())
            try:return str(day(digits)) if len(digits)==8 else None
            except ValueError:return None
        result.append(dict(source='kis_ksd_merger_split',raw=dict(record),record_date=str(date_),
            raw_symbol=record.get('sht_cd'),counterparty_codes=dict(cust_cd=record.get('cust_cd'),opp_cust_cd=record.get('opp_cust_cd')),
            ratio_direction='UNVERIFIED',listing_date_observed=optional_date(record.get('list_dt')),
            odd_lot_payment_date=optional_date(record.get('odd_amt_pay_dt')),payment_date=None,
            cash_per_raw_share=None,exchange_ratio=None,settlement_status='UNKNOWN',publication_precision='UNKNOWN'))
    truncated=len(records)>=100 or metadata.get('tr_cont')=='M' or payload.get('tr_cont')=='M' or bool(payload.get('cts'))
    return result,bool(truncated)


def _date_label(value):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value) or str(day(value))!=value:
        raise ValueError('Exact official date label required')
    return value


def krx_task(board,kind,requested_date):
    if (board,kind) not in ENDPOINTS:raise ValueError('Unsupported KRX reference scope')
    label=_date_label(requested_date)
    return dict(source='krx_reference',url=KRX_BASE+ENDPOINTS[(board,kind)],method='GET',board=board,
        kind=kind,requested_date=label,params={'basDd':label.replace('-','')})


def kind_list_task(board,start,end):
    if board not in ('KOSPI','KOSDAQ'):raise ValueError('Official KIND board required')
    _date_label(start);_date_label(end)
    if day(start)<date(2010,1,1) or start>end:raise ValueError('KIND list scope invalid')
    return dict(source='kind_list',url=KIND+'/investwarn/delcompany.do',method='POST',board=board,
        start_date=start,end_date=end,params=dict(method='searchDelCompanySub',currentPageSize='3000',pageIndex='1',
            orderMode='2',orderStat='D',tabType='1',searchMode='1',searchCodeType='',searchCorpName='',
            repIsuSrtCd='',forward='delcompany_down',searchType='',marketType='1' if board=='KOSPI' else '2',
            searchCorpNameTmp='',fromDate=start,toDate=end))


class _NativeRows(HTMLParser):
    def __init__(self):super().__init__();self.rows=[];self.row=None;self.cell=None
    def handle_starttag(self,tag,attrs):
        if tag=='tr':self.row=[]
        if tag in ('td','th') and self.row is not None:self.cell=''
    def handle_data(self,data):
        if self.cell is not None:self.cell+=data
    def handle_endtag(self,tag):
        if tag in ('td','th') and self.cell is not None:self.row.append(self.cell.strip());self.cell=None
        if tag=='tr' and self.row is not None:
            if self.row:self.rows.append(self.row)
            self.row=None


def kind_list_rows(content,task):
    """Parse native HTML-table export, requiring complete display numbering.

    This proves the returned query scope only, never an issuer's lifecycle or
    actual settlement. Native ordinal coverage is retained as a truncation gate.
    """
    validate_task(task);parser=_NativeRows();body=_text(content);parser.feed(body)
    header=['번호','회사명','종목코드','폐지일자','폐지사유','비고']
    headers=[i for i,row in enumerate(parser.rows) if row==header]
    if len(headers)!=1:raise ValueError('KIND native export header missing')
    result=[];numbers=[];identities=set()
    for row in parser.rows[headers[0]+1:]:
        if len(row)!=6 or not row[0].isdigit():
            if any(v for v in row) and not any('없' in v for v in row):raise ValueError('KIND native export row invalid')
            continue
        number=int(row[0]);symbol=_code(row[2]);label=_date_label(row[3])
        if not task['start_date']<=label<=task['end_date']:raise ValueError('KIND list date outside query')
        identity=(symbol,label)
        if identity in identities:raise ValueError('Duplicate or conflicting KIND event')
        identities.add(identity);numbers.append(number)
        result.append(dict(event_id='KIND:'+task['board']+':'+symbol+':'+label,raw_symbol=symbol,
            board=task['board'],effective_date=label,reason=row[4],settlement_status='UNKNOWN'))
    total=re.search(r'(?:전체|총)\s*([\d,]+)\s*건',unescape(re.sub('<[^>]+>',' ',body)))
    if len(result)>=3000 or len(numbers)!=len(set(numbers)) or (numbers and set(numbers)!=set(range(1,max(numbers)+1))):
        raise ValueError('KIND list incomplete or truncated')
    if total and int(total[1].replace(',',''))!=len(result):raise ValueError('KIND list total mismatch')
    if not result and (not total or int(total[1].replace(',',''))!=0):raise ValueError('KIND list empty without explicit zero')
    return result


def validate_task(task):
    """Exact source-specific request allowlist. No auth, local paths or SQL."""
    if not isinstance(task,dict):raise ValueError('Official source task required')
    source=task.get('source');params=task.get('params',{})
    if not isinstance(params,dict) or any(not isinstance(k,str) or not isinstance(v,str) for k,v in params.items()):
        raise ValueError('Official scalar request params required')
    if source=='krx_reference':
        expected=krx_task(task.get('board'),task.get('kind'),task.get('requested_date'))
        if task!=expected:raise ValueError('KRX request contract mismatch')
    elif source=='kis_merger_split':
        start=_date_label(task.get('start_date'));end=_date_label(task.get('end_date'))
        if start>end or day(start)<date(2010,1,1) or task!=_kis_task(start,end):raise ValueError('KIS request contract mismatch')
    elif source=='kind_list':
        if task!=kind_list_task(task.get('board'),task.get('start_date'),task.get('end_date')):raise ValueError('KIND list request mismatch')
    elif source=='kind_index':
        allowed={'source','url','method','event_id','params','lookback_days'}
        if not set(task)<=allowed or set(task)-{'lookback_days'}!=allowed-{'lookback_days'}:raise ValueError('KIND index fields invalid')
        if task['url']!=KIND+'/disclosure/details.do' or task['method']!='POST':raise ValueError('KIND index endpoint invalid')
        if not isinstance(task['event_id'],str) or not 1<=len(task['event_id'])<=256:raise ValueError('KIND event id invalid')
        _kind_options(params.get('reportNm'),task.get('lookback_days',90))
        _date_label(params.get('fromDate'));_date_label(params.get('toDate'))
        if params['fromDate']>params['toDate'] or params.get('marketType') not in ('1','2'):raise ValueError('KIND query range invalid')
        page=params.get('pageIndex','')
        if not page.isdigit() or not 1<=int(page)<=1000:raise ValueError('KIND index page invalid')
        fixed=dict(method='searchDetailsSub',forward='details_sub',currentPageSize='100',pageIndex=page,
            orderMode='1',orderStat='D',reportNm=params['reportNm'],reportNmTemp=params['reportNm'],
            searchCorpName='',repIsuSrtCd='',allRepIsuSrtCd='',marketType=params['marketType'],
            fromDate=params['fromDate'],toDate=params['toDate'])
        if params!=fixed:raise ValueError('KIND index params invalid')
    elif source in ('kind_document','kind_document_path'):
        allowed={'source','url','method','receipt_id'}|({'params'} if source=='kind_document_path' else set())
        if set(task)!=allowed or task.get('method')!='GET' or not re.fullmatch('[0-9]{14}',str(task.get('receipt_id',''))):
            raise ValueError('KIND document identity invalid')
        parsed=urlsplit(task.get('url',''));path=unquote(parsed.path)
        if parsed.scheme!='https' or parsed.hostname!='kind.krx.co.kr' or parsed.port not in (None,443) or parsed.username or parsed.password or parsed.fragment:
            raise ValueError('Official document URL required')
        if any(p in ('.','..') for p in path.split('/')):raise ValueError('Unsafe document URL')
        if source=='kind_document_path':
            if task['url']!=KIND+'/common/disclsviewer.do' or set(params)!={'method','docNo'} or params['method']!='searchContents' or not re.fullmatch('[0-9]{14}',params['docNo']):
                raise ValueError('KIND document path params invalid')
        elif path.startswith('/external/'):
            if parsed.query or not re.search(r'\.(?:htm|html|pdf)$',path,re.I):raise ValueError('Official original document required')
        elif path!='/common/disclsviewer.do' or parse_qs(parsed.query,keep_blank_values=True)!=dict(method=['searchInitInfo'],acptNo=[task['receipt_id']],docNo=['']):
            raise ValueError('KIND viewer URL invalid')
    else:raise ValueError('Unknown official source task')
    if len(canonical(task).encode())>16384:raise ValueError('Source task exceeds bound')
    return task
