import base64
import copy
from datetime import datetime,timedelta,timezone
import importlib
import json

import pytest

from test_kr_evidence_sources import native_list

NOW=datetime(2024,2,1,0,0,tzinfo=timezone.utc)


def module():return importlib.import_module('price_lab.research_delta_collect')
def sources():return importlib.import_module('price_lab.kr_evidence_sources')


def state():return module().build_state(sessions=['2024-01-15'],start_date='2024-01-01',end_date='2024-01-31')


def grant(counts,**kwargs):return dict(status='GRANTED',reservation_id=kwargs['reservation_id'],ceilings=counts,quota_day='2024-02-01',next_retry_at=None)


def only(q,provider,tasks):
    q=copy.deepcopy(q);q['pending']={p:[] for p in ('krx','kis','kind')};q['pending'][provider]=tasks
    q['discovery']['list_date']='2024-02-01'
    return q


def raw_reference(task):
    return {'OutBlock_1':[{'BAS_DD':task['requested_date'].replace('-',''),'ISU_SRT_CD':'000001','MKT_NM':task['board'],
        'LIST_DD':'20000101','SECUGRP_NM':'주권','MKTCAP':'1000000','LIST_SHRS':'100','TDD_CLSPRC':'10000'}]}


def test_portable_state_is_pure_deterministic_and_extends_only_known_new_sessions():
    q=state();assert q==state()
    assert q['version']=='research-collection-state-v1' and len(q['pending']['krx'])==4
    assert len(q['pending']['kis'])==1 and len(q['pending']['kind'])==2
    before=copy.deepcopy(q);extended=module().extend_sessions(q,['2024-01-15','2024-02-01'])
    assert q==before and extended['sessions']==['2024-01-15','2024-02-01']
    assert len(extended['pending']['krx'])==8 and extended['end_date']=='2024-02-01'
    assert module().extend_sessions(extended,['2024-01-15'])==extended
    assert 'path' not in json.dumps(q)


def test_reservation_precedes_fetch_and_waiting_or_insufficient_grant_never_sends():
    q=only(state(),'krx',[sources().krx_task('KOSPI','master','2024-01-15')]);order=[]
    def reserve(counts,**kwargs):order.append('reserve');return grant(counts,**kwargs)
    def fetch(task):order.append('fetch');return raw_reference(task)
    result=module().collect_batch(q,fetch=fetch,reserve=reserve,now=NOW,max_requests=1,reservation_id='batch-1')
    assert order==['reserve','fetch'] and result['requests_by_provider']=={'krx':1,'kis':0,'kind':0}
    assert result['state']!=q and result['records'][0]['body_sha256']
    for reply in (dict(status='WAITING_QUOTA',next_retry_at='2024-02-02T00:00:00+00:00'),
                  dict(status='GRANTED',reservation_id='wrong',ceilings={'krx':1}),
                  dict(status='GRANTED',reservation_id='batch-2',ceilings={'krx':0,'kis':0,'kind':0})):
        held=module().collect_batch(q,fetch=lambda task:pytest.fail('no grant'),reserve=lambda *a,**k:reply,
            now=NOW,max_requests=1,reservation_id='batch-2')
        assert not held['records'] and held['state']==q and sum(held['requests_by_provider'].values())==0


def test_unchanged_body_is_omitted_and_changed_body_is_new_observation():
    task=sources().krx_task('KOSPI','master','2024-01-15');q=only(state(),'krx',[task]);original=copy.deepcopy(q)
    first=module().collect_batch(q,fetch=raw_reference,reserve=grant,now=NOW,max_requests=1,reservation_id='first')
    assert q==original and len(first['records'])==1
    second=module().collect_batch(only(first['state'],'krx',[task]),fetch=raw_reference,reserve=grant,
        now=NOW+timedelta(hours=1),max_requests=1,reservation_id='repeat')
    assert second['records']==[] and second['unchanged_records']==1
    def changed(t):value=raw_reference(t);value['OutBlock_1'][0]['ISU_NM']='correction';return value
    third=module().collect_batch(only(second['state'],'krx',[task]),fetch=changed,reserve=grant,
        now=NOW,max_requests=1,reservation_id='revised')
    assert len(third['records'])==1 and third['records'][0]['body_sha256']!=first['records'][0]['body_sha256']


def test_kind_list_discovers_new_event_and_indexes_then_observed_documents():
    q=only(state(),'kind',[sources().kind_list_task('KOSPI','2024-01-01','2024-01-31')])
    calls=[]
    def fetch(task):
        calls.append(task)
        if task['source']=='kind_list':return native_list()
        if task['source']=='kind_index':return ('''<table><tr><td>1</td><td>2024-01-14</td><td><a onclick="companysummary_open('00000')">name</a></td><td><a onclick="openDisclsViewer('20240114000001','')">상장폐지</a></td></tr></table>전체 1 건''').encode()
        if '/external/' in task['url']:return b'<html>Original observed document</html>'
        return b'<html><iframe src="/external/2024/01/14/file.htm"></iframe></html>'
    result=module().collect_batch(q,fetch=fetch,reserve=grant,now=NOW,max_requests=5,reservation_id='kind')
    assert [r['task']['source'] for r in result['records']][:3]==['kind_list','kind_index','kind_document']
    assert len([r for r in result['records'] if r['task']['source']=='kind_document'])==2
    assert result['state']['events'][0]['raw_symbol']=='000001'
    assert len(result['state']['discovery']['documents'])==2
    assert all(set(r)=={'task','retrieved_at','body_base64','body_sha256','response_metadata'} for r in result['records'])


def test_kis_truncation_splits_and_resume_does_not_close_parent_early():
    task=sources()._kis_task('2024-01-01','2024-01-31');q=only(state(),'kis',[task])
    def fetch(t):
        rows=[dict(record_date=t['start_date'].replace('-',''),sht_cd='000001')]
        if t==task:rows=[{**rows[0],'seq':str(n)} for n in range(100)]
        return dict(rt_cd='0',output1=rows)
    first=module().collect_batch(q,fetch=fetch,reserve=grant,now=NOW,max_requests=1,reservation_id='truncated')
    assert sources()._id(task) not in first['state']['completed'] and len(first['state']['pending']['kis'])==2
    second=module().collect_batch(first['state'],fetch=fetch,reserve=grant,now=NOW,max_requests=2,reservation_id='children')
    assert not second['state']['pending']['kis'] and len(second['records'])==2


def test_repeated_kind_pages_and_invalid_response_remain_pending_with_prior_state():
    q=only(state(),'kind',[sources()._kind_task(dict(event_id='e',board='KOSPI',raw_symbol='000001',effective_date='2024-01-15'),'2024-01-01','2024-01-31')])
    q['events']=[dict(event_id='e',board='KOSPI',raw_symbol='000001',effective_date='2024-01-15',settlement_status='UNKNOWN')]
    rows=''.join('<tr><td>1</td><td>2024-01-14</td><td>x</td><td><a onclick="openDisclsViewer(\'20240114'+str(n).zfill(6)+'\',\'\')">x</a></td></tr>' for n in range(100))
    body=('<table>'+rows+'</table>전체 150 건').encode()
    first=module().collect_batch(q,fetch=lambda task:body,reserve=grant,now=NOW,max_requests=2,reservation_id='pages')
    assert first['status']=='DEFERRED' and first['state']['pending']['kind']
    assert sources()._id(q['pending']['kind'][0]) not in first['state']['completed']
    bad=module().collect_batch(only(state(),'krx',[sources().krx_task('KOSPI','master','2024-01-15')]),
        fetch=lambda task:{'OutBlock_1':[]},reserve=grant,now=NOW,max_requests=1,reservation_id='empty')
    assert bad['records']==[] and bad['state']['pending']['krx']


def test_round_robin_does_not_starve_documents_behind_master_or_index_backfill():
    q=state();event=dict(event_id='e',board='KOSPI',raw_symbol='000001',effective_date='2024-01-15',settlement_status='UNKNOWN');q['events']=[event]
    q['pending']['kind']=[sources()._kind_task(event,'2024-01-01','2024-01-31'),
        dict(source='kind_document',url='https://kind.krx.co.kr/common/disclsviewer.do?method=searchInitInfo&acptNo=20240114000001&docNo=',method='GET',receipt_id='20240114000001')]
    q['cursors']['kind_turn']=2;calls=[]
    def fetch(task):
        calls.append(task['source'])
        if task['source']=='krx_reference':return raw_reference(task)
        if task['source']=='kis_merger_split':return dict(rt_cd='0',output1=[])
        if task['source']=='kind_index':return '<table></table>전체 0 건'.encode()
        return b'<html>source viewer</html>'
    result=module().collect_batch(q,fetch=fetch,reserve=grant,now=NOW,max_requests=3,reservation_id='fair')
    assert set(calls)=={'krx_reference','kis_merger_split','kind_document'}


def test_completed_backfill_rotates_audits_and_daily_list_discovery():
    q=state();q['completed']=[sources()._id(t) for values in q['pending'].values() for t in values]
    q['pending']={p:[] for p in ('krx','kis','kind')};q['discovery']['list_date']='2024-02-01'
    first=module().collect_batch(q,fetch=lambda task:raw_reference(task),reserve=grant,now=NOW,
        max_requests=1,reservation_id='audit')
    assert first['requests_by_provider']['krx']==1 and first['state']['cursors']['audit']>0
    advanced=module().extend_sessions(first['state'],['2024-02-02'])
    assert advanced['end_date']=='2024-02-02'
    assert any(t['source']=='kind_list' and t['end_date']=='2024-02-02' for t in advanced['pending']['kind'])


def test_deadline_and_http_date_retry_after_are_persisted_without_error_body_or_extra_sends():
    q=only(state(),'krx',[sources().krx_task('KOSPI','master','2024-01-15')])
    held=module().collect_batch(q,fetch=lambda task:pytest.fail('deadline'),reserve=lambda *a,**k:pytest.fail('deadline'),
        now=NOW,clock=lambda:NOW,max_requests=1,reservation_id='time',deadline=NOW)
    assert held['status']=='WAITING_DEADLINE' and held['state']==q
    response=dict(status_code=429,body=b'authenticated forbidden detail',headers={'Retry-After':'Fri, 02 Feb 2024 00:00:00 GMT'})
    result=module().collect_batch(q,fetch=lambda task:response,reserve=grant,now=NOW,clock=lambda:NOW,max_requests=1,reservation_id='429')
    assert result['records']==[] and result['state']['cooldowns']['krx']['next_retry_at']=='2024-02-02T00:00:00+00:00'
    assert 'authenticated forbidden' not in json.dumps(result)
    waiting=module().collect_batch(result['state'],fetch=lambda task:pytest.fail('cooldown'),reserve=grant,now=NOW,clock=lambda:NOW,max_requests=1,reservation_id='wait')
    assert sum(waiting['requests_by_provider'].values())==0


def test_official_transport_counts_oauth_and_never_exceeds_granted_ceiling():
    q=only(state(),'kis',[sources()._kis_task('2024-01-01','2024-01-31')]);calls=[]
    class Response:
        status_code=200;headers={}
        def __init__(self,body):self.content=json.dumps(body).encode()
        def json(self):return json.loads(self.content)
    class Session:
        def request(self,method,url,**kwargs):
            calls.append(url)
            return Response({'access_token':'do-not-store'}) if 'tokenP' in url else Response(dict(rt_cd='0',output1=[]))
    fetch=module().official_fetcher({'KIS_APP_KEY':'do-not-store','KIS_APP_SECRET':'do-not-store'},session=Session())
    result=module().collect_batch(q,fetch=fetch,reserve=grant,now=NOW,max_requests=2,reservation_id='oauth')
    assert len(calls)==2 and result['requests_by_provider']['kis']==2 and len(result['records'])==1
    assert 'do-not-store' not in json.dumps(result)


def test_unknown_task_or_nonportable_state_is_rejected_before_reservation():
    q=state();q['pending']['krx'][0]['headers']={'AUTH_KEY':'forbidden'}
    with pytest.raises(ValueError):module().collect_batch(q,fetch=lambda t:pytest.fail('bad'),reserve=lambda *a,**k:pytest.fail('bad'),max_requests=1,reservation_id='bad')
    q=state();q['local_path']='D:/private/staging.duckdb'
    with pytest.raises(ValueError):module().validate_state(q)


def test_exhausted_krx_reservation_does_not_block_kis_or_kind():
    q=state();q['discovery']['list_date']='2024-02-01';reservations=[];calls=[]
    def reserve(counts,**kwargs):
        reservations.append((counts,kwargs['reservation_id']))
        if counts['krx']:return dict(status='WAITING_QUOTA',next_retry_at='2024-02-02T00:00:00+00:00')
        return grant(counts,**kwargs)
    def fetch(task):
        calls.append(task['source'])
        return dict(rt_cd='0',output1=[]) if task['source']=='kis_merger_split' else native_list()
    result=module().collect_batch(q,fetch=fetch,reserve=reserve,now=NOW,max_requests=3,reservation_id='vector')
    assert calls==['kis_merger_split','kind_list'] and result['requests_by_provider']=={'krx':0,'kis':1,'kind':1}
    assert {rid for counts,rid in reservations}=={'vector-krx','vector-kis','vector-kind'}
    assert all(sum(v>0 for v in counts.values())==1 for counts,rid in reservations)


def test_real_collector_payload_roundtrips_current_wire_validator():
    codec=importlib.import_module('price_lab.research_delta_codec')
    q=only(state(),'krx',[sources().krx_task('KOSPI','master','2024-01-15')])
    result=module().collect_batch(q,fetch=raw_reference,reserve=grant,now=NOW,clock=lambda:NOW,max_requests=1,reservation_id='wire')
    payload=dict(schema='research-delta-v1',stream_id='kr-reference-evidence-v1',sequence=1,previous_package_sha256=None,
        bootstrap_anchor_sha256='a'*64,collected_at=NOW.isoformat(),records=result['records'],state=result['state'])
    assert codec.validate_payload(payload)==payload


def test_wrong_reference_kind_date_or_board_is_not_a_completed_scope():
    task=sources().krx_task('KOSPI','master','2024-01-15');q=only(state(),'krx',[task])
    for row in ({'ISU_SRT_CD':'000001','MKT_NM':'KOSPI'},
                {**raw_reference(task)['OutBlock_1'][0],'BAS_DD':'20240116'},
                {**raw_reference(task)['OutBlock_1'][0],'MKT_NM':'KOSDAQ'}):
        result=module().collect_batch(q,fetch=lambda t:{'OutBlock_1':[row]},reserve=grant,now=NOW,max_requests=1,reservation_id='wrong-reference')
        assert result['status']=='DEFERRED' and not result['records']
        assert sources()._id(task) not in result['state']['completed']


def test_large_portable_queue_construction_is_linear_in_task_identity_work(monkeypatch):
    m=module();identity=m._id;calls=[]
    def counted(task):calls.append(None);return identity(task)
    monkeypatch.setattr(m,'_id',counted)
    labels=[str(date_.date()) for date_ in (NOW-timedelta(days=n) for n in range(100))]
    q=m.build_state(sessions=labels,start_date=min(labels),end_date=max(labels))
    assert len(q['pending']['krx'])==400 and len(calls)<8000


def test_new_completed_session_is_prioritized_ahead_of_historical_backfill():
    q=module().extend_sessions(state(),['2024-02-01'])
    assert [t['requested_date'] for t in q['pending']['krx'][:4]]==['2024-02-01']*4


def test_exhausted_daily_audit_window_is_explicit_idle_without_empty_reservations():
    q=state();q['completed']=[sources()._id(t) for values in q['pending'].values() for t in values]
    q['pending']={p:[] for p in ('krx','kis','kind')};q['discovery']['list_date']='2024-02-01'
    q['discovery']['audit_date']='2024-02-01';q['discovery']['audit_remaining']={p:0 for p in ('krx','kis','kind')}
    result=module().collect_batch(q,fetch=lambda task:pytest.fail('idle'),reserve=lambda *a,**k:pytest.fail('idle'),
        now=NOW,max_requests=1,reservation_id='idle')
    assert result['status']=='IDLE' and result['state']==q and not result['records']


def test_price_guard_stops_next_send_but_keeps_completed_body_with_actual_retrieval_time():
    tasks=[sources().krx_task('KOSPI',kind,'2024-01-15') for kind in ('master','daily')]
    q=only(state(),'krx',tasks);calls=[];current=[NOW]
    def fetch(task):calls.append(task);current[0]=NOW+timedelta(seconds=5);return raw_reference(task)
    result=module().collect_batch(q,fetch=fetch,reserve=grant,now=NOW,clock=lambda:current[0],
        can_send=lambda:not calls,max_requests=2,reservation_id='guard')
    assert len(calls)==1 and len(result['records'])==1 and result['status']=='WAITING_PRICE_WINDOW'
    assert result['records'][0]['retrieved_at']=='2024-02-01T00:00:05+00:00'
    assert result['state']['pending']['krx']==[tasks[1]]


def test_same_kis_body_with_changed_continuation_metadata_is_a_new_observation():
    task=sources()._kis_task('2024-01-01','2024-01-31');q=only(state(),'kis',[task])
    payload=dict(rt_cd='0',output1=[dict(record_date='20240115',sht_cd='000001')])
    first=module().collect_batch(q,fetch=lambda t:dict(payload=payload,response_metadata={'tr_cont':'M'}),
        reserve=grant,now=NOW,max_requests=1,reservation_id='meta-first')
    repeated=module().collect_batch(only(first['state'],'kis',[task]),
        fetch=lambda t:dict(payload=payload,response_metadata={'tr_cont':'E'}),reserve=grant,now=NOW,max_requests=1,reservation_id='meta-final')
    assert len(repeated['records'])==1
    assert repeated['records'][0]['body_sha256']==first['records'][0]['body_sha256']
    assert repeated['records'][0]['response_metadata']=={'tr_cont':'E'}


def test_observed_original_is_prioritized_over_existing_viewer_backlog_and_revisions_requeue_it():
    viewer=dict(source='kind_document',url='https://kind.krx.co.kr/common/disclsviewer.do?method=searchInitInfo&acptNo=20240114000001&docNo=',
        method='GET',receipt_id='20240114000001')
    original={**viewer,'url':'https://kind.krx.co.kr/external/2024/01/14/file.htm'}
    another={**viewer,'url':viewer['url'].replace('20240114000001','20240114000002'),'receipt_id':'20240114000002'}
    q=only(state(),'kind',[viewer,another]);q['cursors']['kind_turn']=2;calls=[]
    def fetch(task):
        calls.append(task)
        return b'<html><iframe src="/external/2024/01/14/file.htm"></iframe></html>' if '/external/' not in task['url'] else b'<html>Original</html>'
    first=module().collect_batch(q,fetch=fetch,reserve=grant,now=NOW,max_requests=2,reservation_id='original-priority')
    assert calls==[viewer,original]
    changed=only(first['state'],'kind',[viewer]);changed['cursors']['kind_turn']=2
    result=module().collect_batch(changed,fetch=lambda t:b'<html>corrected<iframe src="/external/2024/01/14/file.htm"></iframe></html>',
        reserve=grant,now=NOW,max_requests=1,reservation_id='revised-viewer')
    assert original in result['state']['pending']['kind']


def test_new_session_reopens_recent_event_index_with_extended_official_window():
    event=dict(event_id='recent',raw_symbol='000001',board='KOSPI',effective_date='2024-01-15')
    q=module().build_state(sessions=['2024-01-15'],start_date='2024-01-01',end_date='2024-01-31',events=[event])
    q['completed']=[sources()._id(t) for t in q['pending']['kind']];q['pending']['kind']=[]
    extended=module().extend_sessions(q,['2024-02-01'])
    indexes=[t for t in extended['pending']['kind'] if t['source']=='kind_index']
    assert len(indexes)==8 and all(t['params']['toDate']=='2024-02-01' for t in indexes)


def test_new_day_schedules_current_kis_month_overlap_after_incremental_tail_queries():
    q=module().extend_sessions(module().extend_sessions(state(),['2024-02-01']),['2024-02-02'])
    q['completed']=[sources()._id(t) for values in q['pending'].values() for t in values]
    q['pending']={p:[] for p in ('krx','kis','kind')};q['discovery']['list_date']='2024-02-01'
    result=module().collect_batch(q,fetch=raw_reference,reserve=grant,now=NOW+timedelta(days=2),
        max_requests=1,reservation_id='new-day-overlap')
    assert sources()._kis_task('2024-02-01','2024-02-02') in result['state']['pending']['kis']
