"""Cloud collector: publish encrypted daily additions/corrections only."""
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))

def official_reference_fetch():
    from price_lab.providers import HttpClient
    endpoints={('KOSPI','daily'):'stk_bydd_trd',('KOSDAQ','daily'):'ksq_bydd_trd',
        ('KOSPI','master'):'stk_isu_base_info',('KOSDAQ','master'):'ksq_isu_base_info'}
    key=os.environ.get('KRX_API_KEY')
    if not key:raise ValueError('KR reference credential is unavailable')
    client=HttpClient(retries=2,min_interval=1,raw_dir=None)
    def fetch(board,label,kind):
        return client.request_json('GET','https://data-dbg.krx.co.kr/svc/apis/sto/'+endpoints[(board,kind)],
            headers={'AUTH_KEY':key},params={'basDd':str(label).replace('-','')})
    return fetch


def run_pipeline(*,root=None,audit=False,publish_only=False,integration_only=False,
                 providers=None,reference_fetch=None,bootstrap_path=None,now=None):
    from price_lab.daily_delta_codec import verify_bootstrap
    from price_lab.daily_delta_collect import collect_delta
    from price_lab.daily_delta_transport import newest_package,load_package,publish_package,record_periodic_status
    root=Path(root or ROOT/'data'/'daily');root.mkdir(parents=True,exist_ok=True)
    key=os.environ.get('PRICE_DATA_KEY')
    latest=newest_package()
    prior=load_package(latest,root,key=key) if latest else None
    if integration_only:
        result={'stage':'daily_package_check','status':'VERIFIED' if prior else 'WAITING_FIRST_PACKAGE',
            'sequence':prior['sequence'] if prior else 0,'cloud_database':False}
        print(json.dumps(result),flush=True)
        if prior:result['periodic_status']=record_periodic_status(prior)
        return result
    if publish_only:raise ValueError('Full-file publication is retired; collect daily packages instead')
    bootstrap=None
    if prior is None:
        path=Path(bootstrap_path or ROOT/'config'/'daily-bootstrap.json')
        if path.stat().st_size>8*1024**2:raise ValueError('Bootstrap control exceeds limit')
        bootstrap=verify_bootstrap(json.loads(path.read_text(encoding='utf-8')),key=key)
    if providers is None:
        from price_lab.providers import Providers
        providers=Providers(raw_dir=None)
    if reference_fetch is None:reference_fetch=official_reference_fetch()
    instant=now or datetime.now(timezone.utc)
    # Audit rechecks one bounded historical window, alongside recent overlap.
    audit=audit or instant.weekday()==6
    payload=collect_delta(providers,prior_payload=prior,bootstrap=bootstrap,now=instant,
        audit=audit,reference_fetch=reference_fetch)
    result=publish_package(payload,work_root=root,key=key)
    result.update(stage='daily_package',cloud_database=False,
        parts=len(payload['parts']),reference_errors=payload.get('reference_errors',[]))
    result['periodic_status']=record_periodic_status(payload)
    print(json.dumps(result),flush=True)
    return result


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--audit',action='store_true')
    parser.add_argument('--integration-only',action='store_true')
    args=parser.parse_args()
    assert_database_free_runtime()
    run_pipeline(audit=args.audit,integration_only=args.integration_only)
    assert_database_free_runtime()
    print(json.dumps({'stage':'cloud_runtime_validation','database_engine_loaded':False,'database_files':0}),flush=True)


def assert_database_free_runtime():
    if 'duckdb' in sys.modules:
        raise ValueError('The cloud collector must not load a database engine')
    for pattern in ('*.duckdb','*.sqlite','*.sqlite3','*.pldata'):
        if next(ROOT.rglob(pattern),None) is not None:
            raise ValueError('The cloud workspace must contain daily packages only')

if __name__=='__main__':main()
