"""Standalone data-only scheduled pipeline, safe to publish independently."""
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--audit',action='store_true')
    parser.add_argument('--publish-only',action='store_true')
    parser.add_argument('--integration-only',action='store_true')
    args=parser.parse_args()
    run_pipeline(audit=args.audit,publish_only=args.publish_only,integration_only=args.integration_only)


def run_pipeline(*,root=None,audit=False,publish_only=False,integration_only=False):
    from price_lab.ingestion import resolve_current,refresh
    from price_lab.providers import Providers
    from price_lab.release_transport import pull_latest,publish_current,record_periodic_status
    from price_lab.history_transport import process_pending
    root=Path(root or ROOT/'data'/'market')
    if not publish_only:
        pull_latest(root)
        # Weekly complete historical revision audit; daily overlap repairs recent data.
        if not integration_only:
            audit=audit or datetime.now(timezone.utc).weekday()==6
            out=refresh(Providers(raw_dir=root/'raw'),root=root,audit=audit)
            print(json.dumps({'stage':'refresh','status':out['status'],'reason':out.get('reason'),'attempt_id':out.get('attempt_id')}),flush=True)
            if out['status'] not in ('PUBLISHED','UNCHANGED'):raise SystemExit(1)
    current=resolve_current(root)
    if current is None:raise ValueError('Checkpoint bootstrap required')
    result=publish_current(root)
    candidates=process_pending(root)
    result['integration']=candidates
    current=resolve_current(root)
    result['periodic_status']=record_periodic_status(current)
    print(json.dumps({'stage':'release',**result,'sequence':current['sequence'],'markets':current['markets']}),flush=True)
    if any(c['status']=='FAILED' for c in candidates):raise SystemExit(1)
    return result

if __name__=='__main__':main()
