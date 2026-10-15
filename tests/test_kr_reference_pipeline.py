import gzip
import json
import duckdb
from price_lab.kr_reference_pipeline import collect_reference, prepare_delta_from_raw, reference_status
from test_kr_reference_sources import daily


def test_raw_reuse_precedes_missing_only_fetch_and_restart_is_idempotent(tmp_path):
    raw=tmp_path/'old'; raw.mkdir()
    with gzip.open(raw/'old.json.gz','wt',encoding='utf-8') as stream: json.dump(daily(),stream)
    calls=[]
    def fetch(board,day,kind):
        calls.append((board,day,kind))
        payload=daily(); payload['OutBlock_1'][0]['MKT_NM']=board
        return payload
    output=tmp_path/'reference'; db=duckdb.connect()
    result=collect_reference(db,sessions=['2024-01-02'],raw_roots=[raw],reference_root=output,fetch=fetch,limit=1,min_age_seconds=0)
    assert calls==[('KOSDAQ','2024-01-02','daily')]
    assert result['reused']>=1 and result['fetched']==1
    again=collect_reference(db,sessions=['2024-01-02'],raw_roots=[raw],reference_root=output,fetch=fetch,limit=1,min_age_seconds=0)
    assert len(calls)==1 and again['fetched']==0
    assert reference_status(db)['status']=='PARTIAL'


def test_raw_without_board_evidence_cannot_be_guessed_from_codes(tmp_path):
    payload=daily(); del payload['OutBlock_1'][0]['MKT_NM']
    path=tmp_path/'unknown.json.gz'
    with gzip.open(path,'wt',encoding='utf-8') as stream: json.dump(payload,stream)
    assert prepare_delta_from_raw(path,min_age_seconds=0) is None


def test_merge_bulk_respects_caller_transaction_and_is_idempotent(tmp_path):
    from price_lab.kr_reference_store import apply_delta
    from price_lab.kr_reference_pipeline import merge_reference_database
    from test_kr_reference_store import delta
    path=tmp_path/'candidate.duckdb'
    with duckdb.connect(str(path)) as source: apply_delta(source,delta())
    db=duckdb.connect(); db.execute("ATTACH '"+str(path).replace("'","''")+"' AS candidate_delta")
    db.execute('BEGIN TRANSACTION')
    assert merge_reference_database(db,'candidate_delta')>0
    db.execute('COMMIT')
    assert merge_reference_database(db,'candidate_delta')==0
    assert db.execute('select count(*) from kr_capitalization_daily').fetchone()[0]==1


def test_failed_master_semantics_keeps_success_body_for_review_without_identity(tmp_path):
    from test_kr_reference_sources import master
    db=duckdb.connect()
    def fetch(board,*_):
        value=master('20240101'); value['OutBlock_1'][0]['MKT_TP_NM']=board; return value
    result=collect_reference(db,sessions=['2010-01-04'],raw_roots=[],reference_root=tmp_path/'reference',
        fetch=fetch,kind='master',limit=1)
    assert result['failed']==1
    assert list((tmp_path/'reference'/'raw').rglob('*.json.gz'))
    assert db.execute('select count(*) from kr_security_aliases').fetchone()[0]==0
