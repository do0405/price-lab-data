"""Cloud collector: publish encrypted daily additions/corrections only."""
import argparse
from datetime import datetime,timezone
from functools import wraps
import json
import os
from pathlib import Path
import re
import sys
import time
from urllib.parse import urlsplit

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))

class _TraceSession:
    """Keep only safe request metadata; never log headers or response bodies."""
    def __init__(self,session):self.session=session;self.last={}

    def request(self,method,url,**kwargs):
        self.last={'endpoint':urlsplit(url).path,'http_status':None}
        response=self.session.request(method,url,**kwargs)
        self.last['http_status']=response.status_code
        # Large successful market-data responses need no extra JSON decoding.
        if len(response.content)<=16384:
            try:body=response.json()
            except (ValueError,TypeError):body=None
            if isinstance(body,dict):
                for field in ('msg_cd','error_code'):
                    code=body.get(field)
                    if isinstance(code,str) and re.fullmatch(r'(?:EGW\d{5}|APBK\d{4}|OPSQ\d{4})',code):
                        self.last['error_code']=code;break
        return response


def observe_provider(provider,name):
    """Observe original calls without changing inputs, outputs or validation."""
    from price_lab.providers import ProviderError
    trace=_TraceSession(provider.client.session);provider.client.session=trace
    class Observed:
        def __getattr__(self,attribute):
            value=getattr(provider,attribute)
            if not callable(value) or attribute.startswith('_'):return value
            @wraps(value)
            def call(*args,**kwargs):
                trace.last={}
                try:return value(*args,**kwargs)
                except ProviderError as exc:
                    reason={
                        'KRX empty or missing board response':'EMPTY_BOARD',
                        'KIS empty benchmark chart':'EMPTY_CHART',
                        'KIS unsuccessful response':'BUSINESS_ERROR',
                        'KIS token unavailable':'TOKEN_MISSING',
                        'Provider transport exhausted retries':'TRANSPORT_RETRIES_EXHAUSTED',
                    }.get(str(exc),'PROVIDER_ERROR')
                    print(json.dumps(dict(event='provider_call_failed',provider=name,
                        operation=attribute,reason=reason,**trace.last)),flush=True)
                    raise
            return call
    return Observed()


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
        for name in ('alpaca','krx','kis'):
            setattr(providers,name,observe_provider(getattr(providers,name),name))
    if reference_fetch is None:reference_fetch=official_reference_fetch()
    instant=now or datetime.now(timezone.utc)
    # Audit rechecks one bounded historical window, alongside recent overlap.
    audit=audit or instant.weekday()==6
    for attempt in range(2):
        try:
            payload=collect_delta(providers,prior_payload=prior,bootstrap=bootstrap,now=instant,
                audit=audit,reference_fetch=reference_fetch)
            break
        except ValueError as exc:
            failure=re.fullmatch(r'Required market collection failed \(market=(us|kr|crypto|all); stage=(calendar|fetch); type=provider\)',str(exc))
            if not failure or attempt:raise
            print(json.dumps(dict(event='collection_retry',market=failure[1],stage=failure[2],
                attempt=2,maximum_attempts=2,wait_seconds=65)),flush=True)
            time.sleep(65)
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
